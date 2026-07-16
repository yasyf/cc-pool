package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

func setupReservePool(t *testing.T) *Manager {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("USER", "tester")
	if err := os.MkdirAll(ClaudeDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	m := &Manager{Store: openTestStore(t), Creds: credstest.NewFake(), DetectOverlay: detectSymlink}
	if _, err := m.Init(); err != nil {
		t.Fatal(err)
	}
	return m
}

// TestAbandonAddFreesReservedIndex pins that AbandonAdd releases the index
// reservation, so the next PrepareAdd reuses it instead of counting up.
func TestAbandonAddFreesReservedIndex(t *testing.T) {
	m := setupReservePool(t)
	p1, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if p1.Index != 1 {
		t.Fatalf("first index = %d, want 1", p1.Index)
	}
	if err := m.AbandonAdd(t.Context(), p1); err != nil {
		t.Fatal(err)
	}
	p2, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if p2.Index != 1 {
		t.Fatalf("index after abandon = %d, want the freed 1", p2.Index)
	}
}

// TestReleaseAddResumesSameIndex pins the resume flow: a keep-dir exit
// releases only the reservation, so a retry re-reserves the same index while a
// live attempt still forces a different one.
func TestReleaseAddResumesSameIndex(t *testing.T) {
	m := setupReservePool(t)
	p1, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if p1.Index != 1 {
		t.Fatalf("first index = %d, want 1", p1.Index)
	}
	// The login completed before the keep-dir exit (e.g. the user declined the
	// rollback): the dir holds an identity.
	identity := `{"oauthAccount": {"accountUuid": "u-kept", "emailAddress": "kept@example.com"}}`
	if err := os.WriteFile(filepath.Join(p1.ConfigDir, ".claude.json"), []byte(identity), 0o600); err != nil {
		t.Fatal(err)
	}

	// A live reservation holds the index against concurrent adds.
	live, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if live.Index == p1.Index {
		t.Fatalf("concurrent PrepareAdd got the live index %d", live.Index)
	}
	if err := m.AbandonAdd(t.Context(), live); err != nil {
		t.Fatal(err)
	}

	if err := m.ReleaseAdd(p1); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p1.ConfigDir); err != nil {
		t.Fatalf("ReleaseAdd must keep the dir: %v", err)
	}

	p2, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if p2.Index != p1.Index {
		t.Fatalf("retry index = %d, want the released %d", p2.Index, p1.Index)
	}
	if p2.ClaudeJSONSeed != SeedKeptExisting {
		t.Fatalf("retry seed = %q, want %q (the kept login must be adopted)", p2.ClaudeJSONSeed, SeedKeptExisting)
	}
}

// TestPrepareAddFailureReleasesReservation pins the rollback inside PrepareAdd
// itself: a fatal overlay failure must not leak the reservation it just took.
func TestPrepareAddFailureReleasesReservation(t *testing.T) {
	m := setupReservePool(t)
	boom := errors.New("disk full")
	m.OverlayFor = func(fkoverlay.Backend) (fkoverlay.Provider, error) {
		return &stubOverlay{backend: fkoverlay.BackendSymlink, reconcileErr: boom}, nil
	}
	if _, err := m.PrepareAdd(t.Context()); !errors.Is(err, boom) {
		t.Fatalf("PrepareAdd = %v, want the setup failure", err)
	}
	m.OverlayFor = nil
	p, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if p.Index != 1 {
		t.Fatalf("index after failed PrepareAdd = %d, want 1 (the failure must release its reservation)", p.Index)
	}
}

// TestStaleReservationSweep pins the daemon-startup backstop: an orphaned
// reservation (its `ccp add` died without AbandonAdd) blocks the index until
// SweepPendingAdds reclaims it — and a fresh reservation survives the sweep.
func TestStaleReservationSweep(t *testing.T) {
	m := setupReservePool(t)
	p1, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if p1.Index != 1 {
		t.Fatalf("first index = %d, want 1", p1.Index)
	}
	// p1's process "died": no FinalizeAdd, no AbandonAdd.

	swept, err := m.Store.SweepPendingAdds(time.Now().Add(-store.PendingAddTTL))
	if err != nil {
		t.Fatal(err)
	}
	if swept != 0 {
		t.Fatalf("swept %d fresh reservations, want 0", swept)
	}
	p2, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if p2.Index != 2 {
		t.Fatalf("index while the orphan is fresh = %d, want 2", p2.Index)
	}
	if err := m.AbandonAdd(t.Context(), p2); err != nil {
		t.Fatal(err)
	}

	swept, err = m.Store.SweepPendingAdds(time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if swept != 1 {
		t.Fatalf("swept = %d, want exactly the orphan", swept)
	}
	p3, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if p3.Index != 1 {
		t.Fatalf("index after sweep = %d, want the reclaimed 1", p3.Index)
	}
}

// teardownProbe wraps stubOverlay to observe AbandonAdd mid-teardown.
type teardownProbe struct {
	stubOverlay
	onTeardown func(context.Context)
}

func (p *teardownProbe) Teardown(ctx context.Context, _, _ string) (string, error) {
	p.onTeardown(ctx)
	return "", nil
}

// TestAbandonAddReleasesAfterTeardown pins release ordering: the reservation
// is held while the dir is torn down and freed once AbandonAdd completes.
func TestAbandonAddReleasesAfterTeardown(t *testing.T) {
	m := setupReservePool(t)
	p, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	m.OverlayFor = func(fkoverlay.Backend) (fkoverlay.Provider, error) {
		return &teardownProbe{
			stubOverlay: stubOverlay{backend: fkoverlay.BackendSymlink},
			onTeardown: func(context.Context) {
				// Mid-teardown the lowest free index must not be p's: its
				// reservation is still live.
				n, rerr := m.Store.ReserveAccountIndex()
				if rerr != nil {
					t.Errorf("reserve during teardown: %v", rerr)
					return
				}
				if n == p.Index {
					t.Errorf("index %d handed out mid-teardown — released before the dir came down", n)
				}
				if rerr := m.Store.ReleaseAccountIndex(n); rerr != nil {
					t.Errorf("release probe index: %v", rerr)
				}
			},
		}, nil
	}
	if err := m.AbandonAdd(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	m.OverlayFor = nil
	next, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if next.Index != p.Index {
		t.Fatalf("index after abandon = %d, want the freed %d", next.Index, p.Index)
	}
}

func TestManagerTeardownPropagatesContext(t *testing.T) {
	type requestKey struct{}
	const requestValue = "caller"

	for _, tc := range []struct {
		name string
		run  func(context.Context, *Manager) error
	}{
		{
			name: "abandon add",
			run: func(ctx context.Context, m *Manager) error {
				p, err := m.PrepareAdd(ctx)
				if err != nil {
					return err
				}
				return m.AbandonAdd(ctx, p)
			},
		},
		{
			name: "remove",
			run: func(ctx context.Context, m *Manager) error {
				a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc", OverlayKind: string(fkoverlay.BackendSymlink)}
				if err := m.Store.UpsertAccount(a); err != nil {
					return err
				}
				return m.Remove(ctx, a.ID, true)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := setupReservePool(t)
			var got any
			probe := &teardownProbe{
				stubOverlay: stubOverlay{backend: fkoverlay.BackendSymlink},
				onTeardown: func(ctx context.Context) {
					got = ctx.Value(requestKey{})
				},
			}
			m.OverlayFor = func(fkoverlay.Backend) (fkoverlay.Provider, error) { return probe, nil }
			ctx := context.WithValue(t.Context(), requestKey{}, requestValue)
			if err := tc.run(ctx, m); err != nil {
				t.Fatal(err)
			}
			if got != requestValue {
				t.Fatalf("provider context value = %v, want %q", got, requestValue)
			}
		})
	}
}

// TestFinalizeAddRefusesSpentReservation pins the conditional promote: once the
// reservation is gone (swept or released) FinalizeAdd must fail loud instead of
// silently upserting over an index a concurrent add may hold.
func TestFinalizeAddRefusesSpentReservation(t *testing.T) {
	m := setupReservePool(t)
	m.OAuth = &fakeOAuth{currentRT: "rt-0"}
	m.LockDir = t.TempDir()
	p, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The login completed…
	identity := `{"oauthAccount": {"accountUuid": "u-new", "emailAddress": "new@example.com"}}`
	if err := os.WriteFile(filepath.Join(p.ConfigDir, ".claude.json"), []byte(identity), 0o600); err != nil {
		t.Fatal(err)
	}
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
	if err := creds.WriteFileCredential(p.ConfigDir, cred); err != nil {
		t.Fatal(err)
	}
	// …but the reservation was reclaimed (TTL sweep) before FinalizeAdd ran.
	if _, err := m.Store.SweepPendingAdds(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	acct, err := m.FinalizeAdd(context.Background(), p, "")
	if err == nil {
		t.Fatal("FinalizeAdd succeeded on a spent reservation, want fail-loud")
	}
	if acct != nil {
		t.Fatalf("FinalizeAdd returned acct %+v with the refusal", acct)
	}
	accounts, lerr := m.Store.ListAccounts()
	if lerr != nil {
		t.Fatal(lerr)
	}
	if len(accounts) != 0 {
		t.Fatalf("accounts = %+v, want none registered after the refusal", accounts)
	}
}

// TestFinalizeAddPromotesReservation pins promotion: after FinalizeAdd the
// pending_adds marker is spent (nothing left to sweep) while the index stays
// taken by the accounts row.
func TestFinalizeAddPromotesReservation(t *testing.T) {
	m := setupReservePool(t)
	m.OAuth = &fakeOAuth{currentRT: "rt-0"}
	m.LockDir = t.TempDir()
	p, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// Simulate the completed login: identity plus a fresh file credential.
	identity := `{"oauthAccount": {"accountUuid": "u-new", "emailAddress": "new@example.com"}}`
	if err := os.WriteFile(filepath.Join(p.ConfigDir, ".claude.json"), []byte(identity), 0o600); err != nil {
		t.Fatal(err)
	}
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
	if err := creds.WriteFileCredential(p.ConfigDir, cred); err != nil {
		t.Fatal(err)
	}

	if _, err := m.FinalizeAdd(context.Background(), p, ""); err != nil {
		t.Fatal(err)
	}

	swept, err := m.Store.SweepPendingAdds(time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if swept != 0 {
		t.Fatalf("swept = %d, want 0 (FinalizeAdd must have spent the reservation)", swept)
	}
	next, err := m.PrepareAdd(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if next.Index != 2 {
		t.Fatalf("next index = %d, want 2 (index 1 is held by the finalized row)", next.Index)
	}
}
