package daemon

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/holder"
	"github.com/yasyf/fusekit/tenant"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

const (
	tenantProvisionConcurrency        = 4
	backgroundProvisionConcurrency    = tenantProvisionConcurrency - 1
	accountRemovalRecoveryConcurrency = backgroundProvisionConcurrency
)

type sourcePreparer interface {
	Prepare(context.Context, tenantfs.Account, tenantfs.PreparationLease) (catalogproto.TenantPreparationProof, error)
	Validate(context.Context, tenantfs.Account, tenantfs.PreparationLease, catalogproto.TenantPreparationProof) error
}

type tenantLifecycleRuntime interface {
	ProvisionTenant(context.Context, tenantfs.Account) (holder.LocalTenantAcknowledgement, error)
	ReplaceTenant(context.Context, tenantfs.Account, uint64) (holder.LocalTenantAcknowledgement, error)
	RetireTenant(context.Context, tenantfs.Account, uint64) (holder.LocalTenantRetirementProof, error)
	TenantState(context.Context, catalog.TenantID) (tenant.TenantStatus, error)
}

// tenantCoordinator owns product account-to-tenant lifecycle and on-demand preparation.
// FuseKit's signed runtime owns source observation, publication, and convergence.
type tenantCoordinator struct {
	server         *Server
	preparer       sourcePreparer
	runtime        tenantLifecycleRuntime
	lifecycle      context.Context
	provisionSlots chan struct{}
	provisionGroup singleflight.Group
	readyMu        sync.RWMutex
	ready          map[catalog.TenantID]uint64
	laneMu         sync.Mutex
	lanes          map[catalog.TenantID]*tenantLifecycleLane
}

type tenantLifecycleLane struct {
	available  chan struct{}
	references int
}

func contextWithoutCancelUntil(
	ctx context.Context,
	done <-chan struct{},
) (context.Context, context.CancelCauseFunc) {
	result, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	go func(ctx context.Context) {
		select {
		case <-ctx.Done():
		case <-done:
			cancel(context.Canceled)
		}
	}(result)
	return result, cancel
}

func newTenantCoordinator(
	lifecycle context.Context,
	server *Server,
	preparer sourcePreparer,
	runtime tenantLifecycleRuntime,
) *tenantCoordinator {
	return &tenantCoordinator{
		server: server, preparer: preparer, runtime: runtime, lifecycle: lifecycle,
		provisionSlots: make(chan struct{}, tenantProvisionConcurrency),
		ready:          make(map[catalog.TenantID]uint64),
		lanes:          make(map[catalog.TenantID]*tenantLifecycleLane),
	}
}

func (c *tenantCoordinator) initialize(ctx context.Context) error {
	if err := c.server.m.RecoverAccountPresentationRepairs(ctx); err != nil {
		err = fmt.Errorf("recover account presentation repairs: %w", err)
		c.server.finishBootstrap(err)
		return err
	}
	if err := recoverAccountRemovals(
		ctx,
		c.server.m.Store.PageAccountRemovals,
		c.finishRemoval,
	); err != nil {
		c.server.finishBootstrap(err)
		return err
	}
	accounts, err := c.server.m.Store.ListDesiredAccounts()
	if err != nil {
		err = fmt.Errorf("list desired accounts for tenant recovery: %w", err)
		c.server.finishBootstrap(err)
		return err
	}
	c.server.setBootstrapTotal(len(accounts))
	prepareContext := ctx
	if c.lifecycle != nil {
		var cancel context.CancelCauseFunc
		prepareContext, cancel = contextWithoutCancelUntil(ctx, c.lifecycle.Done())
		defer cancel(context.Canceled)
	}
	var group errgroup.Group
	group.SetLimit(backgroundProvisionConcurrency)
	for _, desired := range accounts {
		account := desired
		group.Go(func() error {
			quarantined, err := c.prepareDesiredAccount(prepareContext, account)
			if err != nil {
				err = fmt.Errorf("recover acct-%02d tenant: %w", account.ID, err)
			}
			c.server.settleBootstrapAccount(account.ID, quarantined, err)
			return err
		})
	}
	err = group.Wait()
	c.server.finishBootstrap(err)
	return err
}

func (c *tenantCoordinator) prepareDesiredAccount(ctx context.Context, account store.Account) (bool, error) {
	tenantAccount := pool.TenantAccount(account)
	if err := c.ensureTenant(ctx, account, tenantAccount); err != nil {
		return false, err
	}
	if err := c.server.m.RecoverAccountConfigDir(account); err != nil {
		return false, fmt.Errorf("recover acct-%02d stable config dir: %w", account.ID, err)
	}
	return false, nil
}

func expectedPresentationIdentity(account store.Account) (store.FileProviderPresentationIdentity, error) {
	tenantAccount := pool.TenantAccount(account)
	tenantID, err := tenantAccount.TenantID()
	if err != nil {
		return store.FileProviderPresentationIdentity{}, err
	}
	domainID, err := catalogproto.DeriveDomainID(
		tenantfs.OwnerID,
		catalogproto.PresentationInstanceID(account.InstanceID),
	)
	if err != nil {
		return store.FileProviderPresentationIdentity{}, err
	}
	return store.FileProviderPresentationIdentity{
		TenantID: string(tenantID), DomainID: string(domainID),
		Generation: account.Generation, PublicPath: pool.FileProviderConfigDir(account.ID),
	}, nil
}

func recoverAccountRemovals(
	ctx context.Context,
	pageRemovals func(context.Context, int, int) (store.AccountRemovalPage, error),
	finishRemoval func(context.Context, store.AccountRemoval) error,
) error {
	for after := 0; ; {
		page, err := pageRemovals(
			ctx,
			after,
			store.AccountRemovalPageLimit,
		)
		if err != nil {
			return fmt.Errorf("page pending account removals: %w", err)
		}
		group, groupContext := errgroup.WithContext(ctx)
		group.SetLimit(accountRemovalRecoveryConcurrency)
		for _, pending := range page.Removals {
			removal := pending
			group.Go(func() error {
				if err := finishRemoval(groupContext, removal); err != nil {
					return fmt.Errorf("resume acct-%02d removal: %w", removal.AccountID, err)
				}
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			return err
		}
		if page.Next == 0 {
			break
		}
		if page.Next <= after {
			return errors.New("account removal cursor did not advance")
		}
		after = page.Next
	}
	return nil
}

func (c *tenantCoordinator) prepare(
	ctx context.Context,
	account store.Account,
	lease tenantfs.PreparationLease,
) (catalogproto.TenantPreparationProof, error) {
	tenantAccount := pool.TenantAccount(account)
	if err := c.ensureTenant(ctx, account, tenantAccount); err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	return c.preparer.Prepare(ctx, tenantAccount, lease)
}

func (c *tenantCoordinator) ensureTenant(
	ctx context.Context,
	account store.Account,
	tenantAccount tenantfs.Account,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tenantID, err := tenantAccount.TenantID()
	if err != nil {
		return err
	}
	if c.tenantReady(tenantID, tenantAccount.Generation) {
		return nil
	}
	key := string(tenantID) + "\x00" + strconv.FormatUint(tenantAccount.Generation, 10)
	result := c.provisionGroup.DoChan(key, func() (any, error) {
		lifecycle := ctx
		if c.lifecycle != nil {
			var cancel context.CancelCauseFunc
			lifecycle, cancel = contextWithoutCancelUntil(ctx, c.lifecycle.Done())
			defer cancel(context.Canceled)
		}
		if c.tenantReady(tenantID, tenantAccount.Generation) {
			return nil, nil
		}
		release, err := c.acquireTenantLane(lifecycle, tenantID)
		if err != nil {
			return nil, err
		}
		defer release()
		if c.tenantReady(tenantID, tenantAccount.Generation) {
			return nil, nil
		}
		if c.provisionSlots != nil {
			select {
			case c.provisionSlots <- struct{}{}:
				defer func() { <-c.provisionSlots }()
			case <-lifecycle.Done():
				return nil, lifecycle.Err()
			}
		}
		if err := c.ensureTenantOnce(lifecycle, account, tenantAccount, tenantID); err != nil {
			return nil, err
		}
		c.markTenantReady(tenantID, tenantAccount.Generation)
		return nil, nil
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case outcome := <-result:
		return outcome.Err
	}
}

func (c *tenantCoordinator) acquireTenantLane(
	ctx context.Context,
	id catalog.TenantID,
) (func(), error) {
	c.laneMu.Lock()
	if c.lanes == nil {
		c.lanes = make(map[catalog.TenantID]*tenantLifecycleLane)
	}
	lane := c.lanes[id]
	if lane == nil {
		lane = &tenantLifecycleLane{available: make(chan struct{}, 1)}
		lane.available <- struct{}{}
		c.lanes[id] = lane
	}
	lane.references++
	c.laneMu.Unlock()
	select {
	case <-ctx.Done():
		c.releaseTenantLaneReference(id, lane)
		return nil, ctx.Err()
	case <-lane.available:
		if err := ctx.Err(); err != nil {
			lane.available <- struct{}{}
			c.releaseTenantLaneReference(id, lane)
			return nil, err
		}
		return func() {
			lane.available <- struct{}{}
			c.releaseTenantLaneReference(id, lane)
		}, nil
	}
}

func (c *tenantCoordinator) releaseTenantLaneReference(
	id catalog.TenantID,
	lane *tenantLifecycleLane,
) {
	c.laneMu.Lock()
	defer c.laneMu.Unlock()
	lane.references--
	if lane.references == 0 && c.lanes[id] == lane {
		delete(c.lanes, id)
	}
}

func (c *tenantCoordinator) tenantReady(id catalog.TenantID, generation uint64) bool {
	c.readyMu.RLock()
	defer c.readyMu.RUnlock()
	return c.ready[id] == generation
}

func (c *tenantCoordinator) markTenantReady(id catalog.TenantID, generation uint64) {
	c.readyMu.Lock()
	defer c.readyMu.Unlock()
	if c.ready == nil {
		c.ready = make(map[catalog.TenantID]uint64)
	}
	if c.ready[id] < generation {
		c.ready[id] = generation
	}
}

func (c *tenantCoordinator) forgetTenant(id catalog.TenantID) {
	c.readyMu.Lock()
	defer c.readyMu.Unlock()
	delete(c.ready, id)
}

func (c *tenantCoordinator) ensureTenantOnce(
	ctx context.Context,
	account store.Account,
	tenantAccount tenantfs.Account,
	tenantID catalog.TenantID,
) error {
	response, err := c.runtime.ProvisionTenant(ctx, tenantAccount)
	if err == nil {
		if validTenantAcknowledgement(response, tenantID, tenantAccount.Generation) {
			return nil
		}
		return fmt.Errorf("provision acct-%02d: invalid FuseKit proof", account.ID)
	}
	if !isControlCode(err, tenantfs.ControlErrorConflict) {
		return fmt.Errorf("provision acct-%02d: %w", account.ID, err)
	}
	state, present, err := c.tenantState(ctx, tenantAccount)
	if err != nil {
		return fmt.Errorf("inspect conflicted acct-%02d tenant: %w", account.ID, err)
	}
	if !present {
		retried, retryErr := c.runtime.ProvisionTenant(ctx, tenantAccount)
		if retryErr != nil {
			return fmt.Errorf("provision acct-%02d after absent state: %w", account.ID, retryErr)
		}
		if !validTenantAcknowledgement(retried, tenantID, tenantAccount.Generation) {
			return fmt.Errorf("provision acct-%02d after absent state: invalid FuseKit proof", account.ID)
		}
		return nil
	}
	if !state.ReplacementEligible {
		return fmt.Errorf("replace acct-%02d: FuseKit tenant is not replacement eligible", account.ID)
	}
	if state.State.Generation >= catalog.Generation(tenantAccount.Generation) {
		return fmt.Errorf(
			"replace acct-%02d: durable FuseKit generation %d is not older than desired %d",
			account.ID, state.State.Generation, tenantAccount.Generation,
		)
	}
	replaced, err := c.runtime.ReplaceTenant(ctx, tenantAccount, uint64(state.State.Generation))
	if err != nil {
		return fmt.Errorf(
			"replace acct-%02d generation %d from durable generation %d: %w",
			account.ID, account.Generation, state.State.Generation, err,
		)
	}
	if !validTenantAcknowledgement(replaced, tenantID, tenantAccount.Generation) {
		return fmt.Errorf("replace acct-%02d generation %d: invalid FuseKit proof", account.ID, account.Generation)
	}
	return nil
}

func (c *tenantCoordinator) tenantState(
	ctx context.Context,
	tenantAccount tenantfs.Account,
) (tenant.TenantStatus, bool, error) {
	tenantID, err := tenantAccount.TenantID()
	if err != nil {
		return tenant.TenantStatus{}, false, err
	}
	response, err := c.runtime.TenantState(ctx, tenantID)
	if err != nil {
		if isControlCode(err, tenantfs.ControlErrorNotFound) {
			return tenant.TenantStatus{}, false, nil
		}
		return tenant.TenantStatus{}, false, err
	}
	if response.Owner != tenant.OwnerID(tenantfs.OwnerID) || response.State.Tenant != tenantID ||
		response.State.Generation == 0 {
		return tenant.TenantStatus{}, false, errors.New("invalid owner-fenced FuseKit tenant state")
	}
	return response, true, nil
}

func isControlCode(err error, code tenantfs.ControlErrorCode) bool {
	var remote *tenantfs.ControlRemoteError
	return errors.As(err, &remote) && remote.Code == code
}

func validTenantAcknowledgement(
	acknowledgement holder.LocalTenantAcknowledgement,
	tenantID catalog.TenantID,
	generation uint64,
) bool {
	return acknowledgement.Tenant == tenantID && acknowledgement.Generation == catalog.Generation(generation) &&
		acknowledgement.Presentations == catalog.PresentFileProvider
}

func (c *tenantCoordinator) retireReservedAccount(
	ctx context.Context,
	reservation store.PendingAccountReservation,
) (pool.PendingAddRetirementProof, error) {
	configDir, err := pool.AccountConfigDir(reservation.InstanceID)
	if err != nil {
		return pool.PendingAddRetirementProof{}, err
	}
	account := store.Account{
		ID: reservation.ID, InstanceID: reservation.InstanceID,
		Generation: reservation.Generation, ConfigDir: configDir,
	}
	identity, err := expectedPresentationIdentity(account)
	if err != nil {
		return pool.PendingAddRetirementProof{}, err
	}
	if err := store.ValidateReservedPresentationIdentity(reservation, identity); err != nil {
		return pool.PendingAddRetirementProof{}, err
	}
	tenantAccount := pool.TenantAccount(account)
	tenantID, err := tenantAccount.TenantID()
	if err != nil {
		return pool.PendingAddRetirementProof{}, err
	}
	release, err := c.acquireTenantLane(ctx, tenantID)
	if err != nil {
		return pool.PendingAddRetirementProof{}, err
	}
	defer release()
	state, present, err := c.tenantState(ctx, tenantAccount)
	if err != nil {
		return pool.PendingAddRetirementProof{}, fmt.Errorf("inspect reserved FuseKit tenant before retirement: %w", err)
	}
	if present && state.State.Generation != catalog.Generation(reservation.Generation) {
		return pool.PendingAddRetirementProof{}, errors.New("reserved FuseKit tenant generation drifted")
	}
	retired, err := c.runtime.RetireTenant(ctx, tenantAccount, reservation.Generation)
	switch {
	case err == nil:
		if retired.Tenant != tenantID || retired.Generation != catalog.Generation(reservation.Generation) ||
			!retired.FileProviderAbsent {
			return pool.PendingAddRetirementProof{}, errors.New("retire reserved FuseKit tenant: invalid proof")
		}
	case !errors.Is(err, catalog.ErrNotFound):
		return pool.PendingAddRetirementProof{}, fmt.Errorf("retire reserved FuseKit tenant: %w", err)
	}
	c.forgetTenant(tenantID)
	return pool.PendingAddRetirementProof{
		AccountID: reservation.ID, AccountInstanceID: reservation.InstanceID,
		AccountGeneration: reservation.Generation, PublicPath: identity.PublicPath,
	}, nil
}

func (c *tenantCoordinator) finishRemoval(ctx context.Context, removal store.AccountRemoval) error {
	account, err := c.server.m.Store.GetAccount(removal.AccountID)
	if errors.Is(err, store.ErrAccountNotFound) && removal.DeleteCredential {
		account, err = c.server.m.Store.CredentialRemovalSubject(removal)
	}
	if err != nil {
		return err
	}
	if account.InstanceID != removal.AccountInstanceID ||
		account.Generation != removal.AccountGeneration {
		return errors.New("account identity changed after removal intent")
	}
	tenantAccount := pool.TenantAccount(account)
	tenantID, err := tenantAccount.TenantID()
	if err != nil {
		return err
	}
	release, err := c.acquireTenantLane(ctx, tenantID)
	if err != nil {
		return err
	}
	defer release()
	account, err = c.server.m.Store.GetAccount(removal.AccountID)
	if err != nil {
		return err
	}
	if account.InstanceID != removal.AccountInstanceID ||
		account.Generation != removal.AccountGeneration {
		return errors.New("account identity changed while awaiting removal lane")
	}
	presentation, err := c.server.m.Store.AccountPresentation(account.ID)
	if err != nil {
		return fmt.Errorf("read account presentation before removal: %w", err)
	}
	if presentation.AccountInstanceID != removal.AccountInstanceID ||
		presentation.AccountGeneration != removal.AccountGeneration {
		return errors.New("account presentation identity changed while awaiting removal lane")
	}
	tenantAccount = pool.TenantAccount(account)
	state, present, err := c.tenantState(ctx, tenantAccount)
	if err != nil {
		return fmt.Errorf("inspect FuseKit tenant before removal: %w", err)
	}
	if present && state.State.Generation != catalog.Generation(removal.AccountGeneration) {
		return errors.New("FuseKit tenant generation drifted from removal intent")
	}
	response, err := c.runtime.RetireTenant(ctx, tenantAccount, removal.AccountGeneration)
	if err != nil {
		return fmt.Errorf("retire FuseKit tenant: %w", err)
	}
	if response.Tenant != tenantID || response.Generation != catalog.Generation(removal.AccountGeneration) ||
		!response.FileProviderAbsent {
		return errors.New("retire FuseKit tenant: invalid proof")
	}
	c.forgetTenant(tenantID)
	return c.server.m.FinishAccountRemoval(ctx, removal, presentation.Identity.PublicPath)
}

func (s *Server) prepareTenant(
	ctx context.Context,
	account store.Account,
	lease tenantfs.PreparationLease,
) (catalogproto.TenantPreparationProof, error) {
	if s.prepareAccount != nil {
		return s.prepareAccount(ctx, account, lease)
	}
	if s.tenantCoordinator == nil {
		return catalogproto.TenantPreparationProof{}, errors.New("FuseKit tenant coordinator is unavailable")
	}
	return s.tenantCoordinator.prepare(ctx, account, lease)
}

func projectPreparationIdentity(
	proof catalogproto.TenantPreparationProof,
) (store.FileProviderPresentationIdentity, error) {
	publicPath, err := tenantfs.FileProviderPublicPath(proof)
	if err != nil {
		return store.FileProviderPresentationIdentity{}, err
	}
	fileProvider := proof.Presentation.FileProvider
	if fileProvider == nil {
		return store.FileProviderPresentationIdentity{}, tenantfs.ErrPreparationConflict
	}
	return store.FileProviderPresentationIdentity{
		TenantID: string(fileProvider.TenantID), DomainID: string(fileProvider.DomainID),
		Generation: fileProvider.Generation, PublicPath: publicPath,
	}, nil
}

func (c *tenantCoordinator) activatePrepared(
	ctx context.Context,
	account store.Account,
	lease tenantfs.PreparationLease,
	proof catalogproto.TenantPreparationProof,
	activate func() error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.preparer.Validate(ctx, pool.TenantAccount(account), lease, proof); err != nil {
		return err
	}
	return activate()
}
