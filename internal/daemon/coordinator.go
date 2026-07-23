package daemon

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

const (
	accountRemovalRecoveryConcurrency = 4
	tenantProvisionConcurrency        = 4
	tenantStartingRetryDelay          = 100 * time.Millisecond
)

type sourcePreparer interface {
	Prepare(context.Context, tenantfs.Account) (catalogproto.TenantPreparationProof, error)
	Validate(context.Context, tenantfs.Account, catalogproto.TenantPreparationProof) error
}

type tenantLifecycleRuntime interface {
	ProvisionTenant(context.Context, tenantfs.Account) (mountproto.ProvisionTenantResponse, error)
	ReplaceTenant(context.Context, tenantfs.Account, uint64) (mountproto.ReplaceTenantResponse, error)
	RemoveTenant(context.Context, tenantfs.Account, uint64) (mountproto.RemoveTenantResponse, error)
	TenantState(context.Context, tenantfs.Account) (mountproto.StateResponse, error)
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
	if err := recoverAccountRemovals(
		ctx,
		c.server.m.Store.PageAccountRemovals,
		c.finishRemoval,
	); err != nil {
		return err
	}
	accounts, err := c.server.m.Store.ListDesiredAccounts()
	if err != nil {
		return fmt.Errorf("list desired accounts for tenant recovery: %w", err)
	}
	prepareContext := ctx
	if c.lifecycle != nil {
		var cancel context.CancelCauseFunc
		prepareContext, cancel = contextWithoutCancelUntil(ctx, c.lifecycle.Done())
		defer cancel(context.Canceled)
	}
	var group errgroup.Group
	group.SetLimit(tenantProvisionConcurrency)
	for _, desired := range accounts {
		account := desired
		group.Go(func() error {
			if err := c.prepareDesiredAccount(prepareContext, account); err != nil {
				return fmt.Errorf("recover acct-%02d tenant: %w", account.ID, err)
			}
			return nil
		})
	}
	return group.Wait()
}

func (c *tenantCoordinator) prepareDesiredAccount(ctx context.Context, account store.Account) error {
	for {
		proof, err := c.prepare(ctx, account)
		if errors.Is(err, wire.ErrNotReady) {
			timer := time.NewTimer(tenantStartingRetryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if err != nil {
			return err
		}
		if err := c.activatePrepared(ctx, account, proof, func() error { return nil }); err != nil {
			return err
		}
		stored, err := projectPreparationProof(proof)
		if err != nil {
			return err
		}
		expected, err := expectedPresentationIdentity(account)
		if err != nil {
			return err
		}
		if err := c.server.m.Store.BindDesiredAccountPresentation(account, expected, stored); err != nil {
			if errors.Is(err, store.ErrAccountPresentationQuarantined) {
				return nil
			}
			return err
		}
		return nil
	}
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
		Generation: account.Generation, PublicPath: account.ConfigDir,
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
) (catalogproto.TenantPreparationProof, error) {
	tenantAccount := pool.TenantAccount(account)
	if err := c.ensureTenant(ctx, account, tenantAccount); err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	return c.preparer.Prepare(ctx, tenantAccount)
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
		if validTenantAcknowledgement(
			response.Protocol, response.Code, response.Message, response.TenantID, response.Generation,
			mountproto.TenantID(tenantID), tenantAccount.Generation,
		) {
			return nil
		}
		return fmt.Errorf("provision acct-%02d: invalid FuseKit proof", account.ID)
	}
	if !isRemoteCode(err, mountproto.ErrorCodeConflict) {
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
		if !validTenantAcknowledgement(
			retried.Protocol, retried.Code, retried.Message, retried.TenantID, retried.Generation,
			mountproto.TenantID(tenantID), tenantAccount.Generation,
		) {
			return fmt.Errorf("provision acct-%02d after absent state: invalid FuseKit proof", account.ID)
		}
		return nil
	}
	if !state.ReplacementEligible {
		return fmt.Errorf("replace acct-%02d: FuseKit tenant is not replacement eligible", account.ID)
	}
	if state.Generation >= tenantAccount.Generation {
		return fmt.Errorf(
			"replace acct-%02d: durable FuseKit generation %d is not older than desired %d",
			account.ID, state.Generation, tenantAccount.Generation,
		)
	}
	replaced, err := c.runtime.ReplaceTenant(ctx, tenantAccount, state.Generation)
	if err != nil {
		return fmt.Errorf(
			"replace acct-%02d generation %d from durable generation %d: %w",
			account.ID, account.Generation, state.Generation, err,
		)
	}
	if !validTenantAcknowledgement(
		replaced.Protocol, replaced.Code, replaced.Message, replaced.TenantID, replaced.Generation,
		mountproto.TenantID(tenantID), tenantAccount.Generation,
	) {
		return fmt.Errorf("replace acct-%02d generation %d: invalid FuseKit proof", account.ID, account.Generation)
	}
	return nil
}

func (c *tenantCoordinator) tenantState(
	ctx context.Context,
	tenantAccount tenantfs.Account,
) (mountproto.TenantState, bool, error) {
	response, err := c.runtime.TenantState(ctx, tenantAccount)
	if err != nil {
		if isRemoteCode(err, mountproto.ErrorCodeNotFound) {
			return mountproto.TenantState{}, false, nil
		}
		return mountproto.TenantState{}, false, err
	}
	tenantID, err := tenantAccount.TenantID()
	if err != nil {
		return mountproto.TenantState{}, false, err
	}
	if response.Protocol != mountproto.Version || response.Code != mountproto.ErrorCodeOk ||
		response.Message != "" || response.State == nil ||
		response.State.OwnerID != mountproto.OwnerID(tenantfs.OwnerID) ||
		response.State.TenantID != mountproto.TenantID(tenantID) ||
		response.State.Generation == 0 {
		return mountproto.TenantState{}, false, errors.New("invalid owner-fenced FuseKit tenant state")
	}
	return *response.State, true, nil
}

func isRemoteCode(err error, code mountproto.ErrorCode) bool {
	var remote *mountservice.RemoteError
	return errors.As(err, &remote) && remote.Code == code
}

func validTenantAcknowledgement(
	protocol uint16,
	code mountproto.ErrorCode,
	message string,
	tenantID mountproto.TenantID,
	generation uint64,
	wantID mountproto.TenantID,
	wantGeneration uint64,
) bool {
	return protocol == mountproto.Version && code == mountproto.ErrorCodeOk && message == "" &&
		tenantID == wantID && generation == wantGeneration
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
	tenantAccount = pool.TenantAccount(account)
	state, present, err := c.tenantState(ctx, tenantAccount)
	if err != nil {
		return fmt.Errorf("inspect FuseKit tenant before removal: %w", err)
	}
	if present {
		if state.Generation != removal.AccountGeneration {
			return errors.New("FuseKit tenant generation drifted from removal intent")
		}
		response, err := c.runtime.RemoveTenant(ctx, tenantAccount, removal.AccountGeneration)
		if err != nil {
			return fmt.Errorf("remove FuseKit tenant: %w", err)
		}
		if response.Protocol != mountproto.Version || response.Code != mountproto.ErrorCodeOk ||
			response.Message != "" || response.TenantID != mountproto.TenantID(tenantID) ||
			response.Generation != removal.AccountGeneration || !response.FileProviderAbsent {
			return errors.New("remove FuseKit tenant: invalid proof")
		}
	}
	c.forgetTenant(tenantID)
	return c.server.m.FinishAccountRemoval(ctx, removal)
}

func (s *Server) prepareTenant(
	ctx context.Context,
	account store.Account,
) (catalogproto.TenantPreparationProof, error) {
	if s.prepareAccount != nil {
		return s.prepareAccount(ctx, account)
	}
	if s.tenantCoordinator == nil {
		return catalogproto.TenantPreparationProof{}, errors.New("FuseKit tenant coordinator is unavailable")
	}
	return s.tenantCoordinator.prepare(ctx, account)
}

func (s *Server) prepareReservedAccountProof(
	ctx context.Context,
	reservation store.PendingAccountReservation,
) (store.PresentationPreparationProof, error) {
	account := store.Account{
		ID: reservation.ID, InstanceID: reservation.InstanceID, Generation: reservation.Generation,
	}
	var proof catalogproto.TenantPreparationProof
	var err error
	if s.prepareReservedAccount != nil {
		proof, err = s.prepareReservedAccount(ctx, reservation)
	} else {
		proof, err = s.prepareTenant(ctx, account)
	}
	if err != nil {
		return store.PresentationPreparationProof{}, err
	}
	activate := func() error { return nil }
	if s.activatePrepared != nil {
		err = s.activatePrepared(ctx, account, proof, activate)
	} else if s.tenantCoordinator != nil {
		err = s.tenantCoordinator.activatePrepared(ctx, account, proof, activate)
	} else {
		err = errors.New("FuseKit tenant coordinator is unavailable")
	}
	if err != nil {
		return store.PresentationPreparationProof{}, err
	}
	storedProof, err := projectPreparationProof(proof)
	if err != nil {
		return store.PresentationPreparationProof{}, err
	}
	if err := validateReservedPreparationProof(reservation, storedProof); err != nil {
		return store.PresentationPreparationProof{}, err
	}
	return storedProof, nil
}

func (s *Server) prepareAccountProof(
	ctx context.Context,
	account store.Account,
) (store.PresentationPreparationProof, error) {
	proof, err := s.prepareTenant(ctx, account)
	if err != nil {
		return store.PresentationPreparationProof{}, err
	}
	activate := func() error { return nil }
	if s.activatePrepared != nil {
		err = s.activatePrepared(ctx, account, proof, activate)
	} else if s.tenantCoordinator != nil {
		err = s.tenantCoordinator.activatePrepared(ctx, account, proof, activate)
	} else {
		err = errors.New("FuseKit tenant coordinator is unavailable")
	}
	if err != nil {
		return store.PresentationPreparationProof{}, err
	}
	return projectPreparationProof(proof)
}

func (s *Server) revalidatePreparationProof(
	ctx context.Context,
	account store.Account,
	storedProof store.PresentationPreparationProof,
) error {
	proof, err := catalogPreparationProof(storedProof)
	if err != nil {
		return err
	}
	activate := func() error { return nil }
	if s.activatePrepared != nil {
		return s.activatePrepared(ctx, account, proof, activate)
	}
	if s.tenantCoordinator == nil {
		return errors.New("FuseKit tenant coordinator is unavailable")
	}
	return s.tenantCoordinator.activatePrepared(ctx, account, proof, activate)
}

func projectPreparationProof(
	proof catalogproto.TenantPreparationProof,
) (store.PresentationPreparationProof, error) {
	publicPath, err := tenantfs.FileProviderPublicPath(proof)
	if err != nil {
		return store.PresentationPreparationProof{}, err
	}
	fileProvider := proof.Presentation.FileProvider
	if fileProvider == nil {
		return store.PresentationPreparationProof{}, tenantfs.ErrPreparationConflict
	}
	return store.PresentationPreparationProof{
		CatalogTenantID:   string(proof.Catalog.Tenant),
		CatalogGeneration: proof.Catalog.Generation,
		Requested:         proof.Catalog.Requested,
		Desired:           proof.Catalog.Desired,
		Observed:          proof.Catalog.Observed,
		Verified:          proof.Catalog.Verified,
		Applied:           proof.Catalog.Applied,
		SourceAuthority:   string(proof.SourceAuthority),
		SourceRevision:    proof.SourceRevision,
		CatalogRevision:   proof.CatalogRevision,
		ChangeID:          string(proof.ChangeID),
		OperationID:       string(proof.OperationID),
		PresentationKind:  store.PresentationKindFileProvider,
		FileProvider: store.FileProviderPreparationProof{
			TenantID:             string(fileProvider.TenantID),
			DomainID:             string(fileProvider.DomainID),
			Generation:           fileProvider.Generation,
			ActivationGeneration: fileProvider.ActivationGeneration,
			PublicPath:           publicPath,
		},
	}, nil
}

func catalogPreparationProof(
	proof store.PresentationPreparationProof,
) (catalogproto.TenantPreparationProof, error) {
	if proof.PresentationKind != store.PresentationKindFileProvider ||
		proof.FileProvider.TenantID == "" || proof.FileProvider.DomainID == "" ||
		proof.FileProvider.Generation == 0 || proof.FileProvider.ActivationGeneration == "" {
		return catalogproto.TenantPreparationProof{}, tenantfs.ErrPreparationConflict
	}
	if err := tenantfs.ValidateFileProviderPublicPath(proof.FileProvider.PublicPath); err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	return catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant:     catalogproto.TenantID(proof.CatalogTenantID),
			Generation: proof.CatalogGeneration,
			Requested:  proof.Requested,
			Desired:    proof.Desired,
			Observed:   proof.Observed,
			Verified:   proof.Verified,
			Applied:    proof.Applied,
		},
		Presentation: catalogproto.PresentationProof{
			Kind: catalogproto.PresentationKindFileProvider,
			FileProvider: &catalogproto.FileProviderPresentationProof{
				TenantID:             catalogproto.TenantID(proof.FileProvider.TenantID),
				DomainID:             catalogproto.DomainID(proof.FileProvider.DomainID),
				Generation:           proof.FileProvider.Generation,
				PublicPath:           proof.FileProvider.PublicPath,
				ActivationGeneration: proof.FileProvider.ActivationGeneration,
			},
		},
		SourceAuthority: catalogproto.SourceAuthorityID(proof.SourceAuthority),
		SourceRevision:  proof.SourceRevision,
		CatalogRevision: proof.CatalogRevision,
		ChangeID:        catalogproto.ChangeID(proof.ChangeID),
		OperationID:     catalogproto.OperationID(proof.OperationID),
	}, nil
}

func validateReservedPreparationProof(
	reservation store.PendingAccountReservation,
	proof store.PresentationPreparationProof,
) error {
	account := tenantfs.Account{InstanceID: reservation.InstanceID, Generation: reservation.Generation}
	tenantID, err := account.TenantID()
	if err != nil {
		return err
	}
	domainID, err := catalogproto.DeriveDomainID(
		tenantfs.OwnerID,
		catalogproto.PresentationInstanceID(reservation.InstanceID),
	)
	if err != nil {
		return err
	}
	if proof.CatalogTenantID != string(tenantID) || proof.FileProvider.TenantID != string(tenantID) ||
		proof.FileProvider.DomainID != string(domainID) ||
		proof.CatalogGeneration != reservation.Generation ||
		proof.FileProvider.Generation != reservation.Generation {
		return tenantfs.ErrPreparationConflict
	}
	return nil
}

func (c *tenantCoordinator) activatePrepared(
	ctx context.Context,
	account store.Account,
	proof catalogproto.TenantPreparationProof,
	activate func() error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.preparer.Validate(ctx, pool.TenantAccount(account), proof); err != nil {
		return err
	}
	return activate()
}
