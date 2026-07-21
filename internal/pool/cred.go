package pool

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
)

// CredMove reports what MoveCredential did.
type CredMove struct {
	From, To     creds.Source
	Moved        bool // false: already on the target backend
	CleanedStray bool // a diverged copy on the non-authoritative backend was deleted
}

// credProbe records one candidate store's read outcome from probeCredentialStores.
type credProbe struct {
	store creds.Store
	cred  *creds.Credential
	err   error
}

// MoveCredential moves a's credential to target — a MOVE, never a mirror (Claude
// refresh tokens are single-use; two live copies diverge and one chain dies). The
// credential is transferred as-is, never refreshed mid-move. A write while the
// Keychain state is unknowable (creds.ErrUnavailable) is refused. At every exit at
// most one backend holds a live credential. See ccn doc 935d323.
func (m *Manager) MoveCredential(ctx context.Context, a store.Account, target creds.Source) (*CredMove, error) {
	if target != creds.SourceKeychain && target != creds.SourceFile {
		return nil, fmt.Errorf("unknown credential backend %d: move target must be the keychain or the plaintext file", target)
	}
	return runCredentialOperation(
		ctx,
		m,
		a,
		store.CredentialOperationMove,
		moveCredentialOperationCodec(target),
		func(ctx context.Context, boundary *credentialOperationBoundary) (*CredMove, error) {
			return m.moveCredential(ctx, a, target, boundary)
		},
		target.String(),
	)
}

func (m *Manager) moveCredential(
	ctx context.Context,
	a store.Account,
	target creds.Source,
	boundary *credentialOperationBoundary,
) (*CredMove, error) {
	probes, win, err := m.probeCredentialStores(ctx, a)
	if err != nil {
		return nil, err
	}
	if win == nil {
		for _, p := range probes {
			if errors.Is(p.err, creds.ErrUnavailable) {
				return nil, fmt.Errorf("locate acct-%02d's credential: %w — keychain state is unknowable in this session, so a safe move is impossible; retry from a GUI session", a.ID, p.err)
			}
		}
		return nil, fmt.Errorf("acct-%02d has no credential in any backend — run `ccp login %d`: %w", a.ID, a.ID, creds.ErrNotFound)
	}

	src := win.store.Source()
	if src == target {
		return alreadyOnTarget(ctx, probes, target, boundary)
	}
	if target == creds.SourceKeychain {
		// src is the file, so the Keychain probe missed. ErrNotFound proved the
		// slot empty; ErrUnavailable means an unseen item may exist and the
		// written item could not be read back for verification, so refuse
		// rather than clobber another chain or leave two diverging copies.
		if p := probeBySource(probes, creds.SourceKeychain); p != nil && errors.Is(p.err, creds.ErrUnavailable) {
			return nil, fmt.Errorf("move acct-%02d's credential to the keychain: %w — the written item could not be verified in this session; retry from a GUI session", a.ID, p.err)
		}
	}

	if err := boundary.recordCredentialWrite(win.cred); err != nil {
		return nil, err
	}
	if err := boundary.Cross(ctx); err != nil {
		return nil, err
	}
	if m.credentialCAS == nil {
		return nil, errors.New("credential CAS worker is unavailable")
	}
	var rollback *creds.Credential
	if targetProbe := probeBySource(probes, target); targetProbe != nil && targetProbe.cred != nil {
		rollback = targetProbe.cred
	}
	if _, err := m.credentialCAS(ctx, a, boundary.expected, credentialCASMutation{
		Target: target, Credential: win.cred, DeleteOther: true, RollbackTarget: rollback,
	}); err != nil {
		if errors.Is(err, errCredentialCASConflict) {
			return nil, ErrCredentialChangedUnderfoot
		}
		return nil, err
	}
	return &CredMove{From: src, To: target, Moved: true}, nil
}

// probeCredentialStores reads every candidate store in ReadCredential's resolution
// order (Keychain first), recording each outcome instead of stopping at the first
// hit — MoveCredential also needs the losers' state. win is the highest-ranked
// clean read per credOutranks (Keychain first on a tie), or nil when every store
// missed. See ccn doc 935d323.
func (m *Manager) probeCredentialStores(
	ctx context.Context,
	a store.Account,
) ([]credProbe, *credProbe, error) {
	stores := m.Creds.Stores(a)
	probes := make([]credProbe, len(stores))
	winIdx := -1
	for i, s := range stores {
		cred, err := s.Read(ctx)
		probes[i] = credProbe{store: s, cred: cred, err: err}
		switch {
		case err == nil:
			if winIdx == -1 || credOutranks(cred, probes[winIdx].cred) {
				winIdx = i
			}
		case errors.Is(err, creds.ErrNotFound), errors.Is(err, creds.ErrUnavailable):
		default:
			if winIdx == -1 {
				return nil, nil, fmt.Errorf("read credential from %s: %w", s, err)
			}
		}
	}
	if winIdx == -1 {
		return probes, nil, nil
	}
	return probes, &probes[winIdx], nil
}

// credOutranks reports whether a wins resolution over the incumbent b: an
// OWNED credential (refresh token present — the refreshable source of truth)
// always beats a SYNCED replica, which must never win or DropDivergentCopy
// would delete the live owned chain; within an ownership class the later
// expiry wins, probe order (Keychain first) breaking exact ties.
func credOutranks(a, b *creds.Credential) bool {
	if a.HasRefreshToken() != b.HasRefreshToken() {
		return a.HasRefreshToken()
	}
	return a.Expiry().After(b.Expiry())
}

// alreadyOnTarget finishes a MoveCredential whose winning credential already
// resolves to the target backend. Nothing moves, but the losing copy on the
// OTHER backend is deleted: it is the diverged shadow a later headless session
// would resurrect. Only an exactly readable shadow is deletable; corrupt or
// unreachable state fails closed.
func alreadyOnTarget(
	ctx context.Context,
	probes []credProbe,
	target creds.Source,
	boundary *credentialOperationBoundary,
) (*CredMove, error) {
	out := &CredMove{From: target, To: target}
	stray := probeBySource(probes, otherSource(target))
	if !strayPresent(stray) {
		return out, nil
	}
	if stray.err != nil || stray.cred == nil {
		return nil, fmt.Errorf("%w: cannot fingerprint stray credential at %s", ErrCredentialUnverifiable, stray.store)
	}
	if err := boundary.Cross(ctx); err != nil {
		return nil, err
	}
	if boundary.manager.credentialCAS == nil {
		return nil, errors.New("credential CAS worker is unavailable")
	}
	if _, err := boundary.manager.credentialCAS(
		ctx, boundary.account, boundary.expected,
		credentialCASMutation{Target: stray.store.Source(), DeleteTarget: true},
	); err != nil {
		if errors.Is(err, errCredentialCASConflict) {
			return nil, ErrCredentialChangedUnderfoot
		}
		return nil, err
	}
	out.CleanedStray = true
	return out, nil
}

// DropDivergentCopy settles an account on one backend after a cross-session
// re-login left a stale shadow: it removes any credential copy on the backend
// OTHER than the one resolution prefers (see credOutranks). A copy on an
// unsearchable Keychain (headless) is left untouched — resolution already
// prefers the winner, so the shadow is harmless until a GUI session can reach
// it. No credential anywhere, or nothing on the other backend, is a no-op,
// not an error.
func (m *Manager) DropDivergentCopy(ctx context.Context, a store.Account) error {
	target := store.CredentialTargetKeychain
	if probes, win, probeErr := m.probeCredentialStores(ctx, a); probeErr == nil && win != nil {
		stray := probeBySource(probes, otherSource(win.store.Source()))
		if strayPresent(stray) {
			target = credentialTarget(stray.store.Source())
		} else {
			target = credentialTarget(win.store.Source())
		}
	}
	_, err := runCredentialOperation(
		ctx,
		m,
		a,
		store.CredentialOperationDropDivergent,
		unitCredentialOperationCodec(store.CredentialOperationDropDivergent, target),
		func(ctx context.Context, boundary *credentialOperationBoundary) (struct{}, error) {
			return struct{}{}, m.dropDivergentCopy(ctx, a, boundary)
		},
	)
	return err
}

func (m *Manager) dropDivergentCopy(
	ctx context.Context,
	a store.Account,
	boundary *credentialOperationBoundary,
) error {
	probes, win, err := m.probeCredentialStores(ctx, a)
	if err != nil {
		return err
	}
	if win == nil {
		return nil
	}
	stray := probeBySource(probes, otherSource(win.store.Source()))
	if !strayPresent(stray) {
		return nil
	}
	if stray.err != nil || stray.cred == nil {
		return fmt.Errorf("%w: cannot fingerprint divergent credential at %s", ErrCredentialUnverifiable, stray.store)
	}
	if boundary == nil {
		return errors.New("credential mutation boundary is required")
	}
	if m.credentialCAS == nil {
		return errors.New("credential CAS worker is unavailable")
	}
	if err := boundary.Cross(ctx); err != nil {
		return err
	}
	if _, err := m.credentialCAS(ctx, a, boundary.expected, credentialCASMutation{
		Target: stray.store.Source(), DeleteTarget: true,
	}); err != nil {
		if errors.Is(err, errCredentialCASConflict) {
			return ErrCredentialChangedUnderfoot
		}
		return err
	}
	return nil
}

// otherSource returns the backend that is not src.
func otherSource(src creds.Source) creds.Source {
	if src == creds.SourceKeychain {
		return creds.SourceFile
	}
	return creds.SourceKeychain
}

// strayPresent reports whether p is a deletable diverging copy: a real read or a
// hard error (corrupt blob), but not a proven-absent (ErrNotFound) or
// unreachable (ErrUnavailable) slot.
func strayPresent(p *credProbe) bool {
	if p == nil {
		return false
	}
	return !errors.Is(p.err, creds.ErrNotFound) && !errors.Is(p.err, creds.ErrUnavailable)
}

// probeBySource returns the probe for the store backed by src, or nil.
func probeBySource(probes []credProbe, src creds.Source) *credProbe {
	for i := range probes {
		if probes[i].store.Source() == src {
			return &probes[i]
		}
	}
	return nil
}
