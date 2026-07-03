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

// MoveCredential moves a's credential to target — a MOVE, never a mirror
// (Claude refresh tokens are single-use; two live copies diverge and one chain
// dies). The credential is transferred as-is, never refreshed mid-move: an
// expired access token is fine, the refresh token is the asset. Any move that
// would write while the Keychain's state is unknowable (creds.ErrUnavailable,
// headless session) is refused; an already-on-target no-op writes nothing and
// is always safe. At every exit at most one backend holds a live credential —
// transiently two identical copies exist between the target write and the
// source delete — except the double fault where the source delete and the
// rollback both fail, which names both locations in the returned error.
func (m *Manager) MoveCredential(ctx context.Context, a store.Account, target creds.Source) (*CredMove, error) {
	if target != creds.SourceKeychain && target != creds.SourceFile {
		return nil, fmt.Errorf("unknown credential backend %d: move target must be the keychain or the plaintext file", target)
	}
	release, err := m.lockAccount(ctx, a.ID)
	if err != nil {
		return nil, err
	}
	defer release()

	probes, win, err := m.probeCredentialStores(a)
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
		return alreadyOnTarget(probes, target)
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

	dst := m.Creds.Store(a, target)
	if err := dst.Write(win.cred); err != nil {
		return nil, fmt.Errorf("write credential to %s: %w", dst, err)
	}
	readback, err := dst.Read()
	if err != nil {
		return nil, unwindTarget(dst, fmt.Errorf("read back credential from %s: %w", dst, err))
	}
	if !sameTokens(readback, win.cred) {
		return nil, unwindTarget(dst, fmt.Errorf("credential read back from %s does not match what was written", dst))
	}
	if err := win.store.Delete(); err != nil {
		derr := fmt.Errorf("delete source credential at %s: %w", win.store, err)
		if rerr := dst.Delete(); rerr != nil {
			return nil, errors.Join(derr, fmt.Errorf("roll back target copy at %s: %w", dst, rerr))
		}
		return nil, fmt.Errorf("%w (rolled back the copy at %s; the credential stays at %s)", derr, dst, win.store)
	}
	return &CredMove{From: src, To: target, Moved: true}, nil
}

// probeCredentialStores reads every candidate store in ReadCredential's
// resolution order (Keychain first), recording each outcome instead of
// stopping at the first hit: MoveCredential also needs the losers' state (a
// stray file copy, an unsearchable Keychain). win is the winning probe — the
// freshest clean read (later token expiry, Keychain first on a tie) — or nil
// when every store missed. A hard read error with no winner yet fails fast (the
// authoritative backend is unknowable); after a winner it is recorded as a
// presence signal for the caller to act on.
func (m *Manager) probeCredentialStores(a store.Account) ([]credProbe, *credProbe, error) {
	stores := m.Creds.Stores(a)
	probes := make([]credProbe, len(stores))
	winIdx := -1
	for i, s := range stores {
		cred, err := s.Read()
		probes[i] = credProbe{store: s, cred: cred, err: err}
		switch {
		case err == nil:
			// Fresher-wins: when more than one backend holds a credential —
			// transient drift from a cross-session re-login or an interrupted
			// move — the later-expiring one is the live chain (a fresh login
			// mints a later expiry; the other copy is the stale shadow). A
			// blind keychain-first rule could keep the stale copy and, worse,
			// let cleanup delete the fresh one, signing the account out. The
			// resolution order (Keychain first) only breaks exact-expiry ties.
			if winIdx == -1 || cred.Expiry().After(probes[winIdx].cred.Expiry()) {
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

// alreadyOnTarget finishes a MoveCredential whose (fresher) credential already
// resolves to the target backend. Nothing moves, but a staler copy on the OTHER
// backend is deleted: it is the diverged shadow a later headless session would
// resurrect. Any non-absent, reachable outcome — a parsed credential or a hard
// read error (corrupt JSON) — counts as presence; content is never compared,
// because the winner is already the fresher. A copy we cannot reach (an
// unsearchable Keychain, ErrUnavailable) is left untouched.
func alreadyOnTarget(probes []credProbe, target creds.Source) (*CredMove, error) {
	out := &CredMove{From: target, To: target}
	stray := probeBySource(probes, otherSource(target))
	if !strayPresent(stray) {
		return out, nil
	}
	if err := stray.store.Delete(); err != nil {
		return nil, fmt.Errorf("delete stray credential copy at %s: %w", stray.store, err)
	}
	out.CleanedStray = true
	return out, nil
}

// DropDivergentCopy settles an account on one backend after a cross-session
// re-login left a stale shadow: it removes any credential copy on the backend
// OTHER than the one resolution prefers (the fresher). A copy on an unsearchable
// Keychain (headless) is left untouched — resolution already prefers the fresher
// copy, so the shadow is harmless until a GUI session can reach it. No credential
// anywhere, or nothing on the other backend, is a no-op, not an error.
func (m *Manager) DropDivergentCopy(ctx context.Context, a store.Account) error {
	release, err := m.lockAccount(ctx, a.ID)
	if err != nil {
		return err
	}
	defer release()

	probes, win, err := m.probeCredentialStores(a)
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
	if err := stray.store.Delete(); err != nil {
		return fmt.Errorf("drop divergent credential copy at %s: %w", stray.store, err)
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

// unwindTarget deletes an unverified target copy so the source stays the only
// live credential, joining a failed delete onto cause.
func unwindTarget(dst creds.Store, cause error) error {
	if err := dst.Delete(); err != nil {
		return errors.Join(cause, fmt.Errorf("delete unverified target copy at %s: %w", dst, err))
	}
	return cause
}
