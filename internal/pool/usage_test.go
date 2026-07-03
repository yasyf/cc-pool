package pool

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
)

// TestPoolNeverTouchesDefaultKeychainItem pins the #1 safety invariant: no
// credential-seam op ever names the canonical unsuffixed item plain `claude`
// owns ("Claude Code-credentials"). It drives the real ops — pre-flight refresh,
// rotated-token re-assert, remove — asserting each names only the account's own
// suffixed service.
func TestPoolNeverTouchesDefaultKeychainItem(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("USER", "user")
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	svc := creds.ServiceName("/tmp/ccp-test/acct-01")
	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: svc, KeychainAccount: "user", OverlayKind: "symlink"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	fk := credstest.NewFake()
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	// Near-expiry so SampleUsage's pre-flight must POST-refresh.
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	fk.Put(svc, a.KeychainAccount, cred)
	fo := &fakeOAuth{currentRT: "rt-0"}
	m := &Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()}

	if _, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true}); err != nil {
		t.Fatalf("SampleUsage: %v", err)
	}
	if got := fo.refreshes; got != 1 {
		t.Fatalf("refreshes = %d, want 1 (near-expiry token must be refreshed)", got)
	}
	if err := m.AdoptRotatedToken(context.Background(), a); err != nil {
		t.Fatalf("AdoptRotatedToken: %v", err)
	}
	if err := m.Remove(a.ID, true); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	touched := fk.TouchedServices()
	if len(touched) == 0 {
		t.Fatal("no keychain ops recorded; the test exercised nothing")
	}
	for i, s := range touched {
		if s == "Claude Code-credentials" {
			t.Fatalf("op %d named the canonical unsuffixed item", i)
		}
		if s != svc {
			t.Errorf("op %d named service %q, want %q", i, s, svc)
		}
	}
	if del := fk.DeletedServices(); len(del) != 1 || del[0] != svc {
		t.Errorf("deletes = %v, want exactly [%q]", del, svc)
	}
}

// TestSampleUsagePersistsExtraUsage pins the oauth→store join in recordSample:
// the usage windows and extra-usage block must land in the latest stored sample
// verbatim (status and the exhausted-fallback billing warning read them there) —
// a mapping each layer's isolated tests would miss if dropped.
func TestSampleUsagePersistsExtraUsage(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc", KeychainAccount: "user", OverlayKind: "symlink"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	fk := credstest.NewFake()
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli() // fresh: no refresh needed
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	m := &Manager{Store: st, OAuth: &fakeOAuth{currentRT: "rt-0"}, Creds: fk, LockDir: t.TempDir()}

	if _, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: false}); err != nil {
		t.Fatalf("SampleUsage: %v", err)
	}
	got, ok, err := st.LatestUsageSample(1)
	if err != nil || !ok {
		t.Fatalf("latest sample: ok=%v err=%v", ok, err)
	}
	if got.Util5h != 31 || got.Util7d != 7 {
		t.Fatalf("windows not persisted: %+v", got)
	}
	if !got.ExtraEnabled || got.ExtraUsed != 177 || got.ExtraLimit != 5000 {
		t.Fatalf("extra usage not persisted: %+v", got)
	}
}

// TestRecordSampleCollapsesScoped pins recordSample's collapse of oauth's full
// per-model weekly slice to the single binding (max-utilization) bucket stored
// on the sample: multiple buckets ⇒ the highest-util one wins; none ⇒ the scoped
// fields stay empty (the presence signal is Scoped7dModel == "").
func TestRecordSampleCollapsesScoped(t *testing.T) {
	reset := time.Now().Add(48 * time.Hour).Truncate(time.Second)
	cases := map[string]struct {
		scoped     []oauth.ScopedWindow
		wantModel  string
		wantUtil   float64
		wantResets bool // true ⇒ Scoped7dResets equals reset; false ⇒ zero
	}{
		"multiple buckets: highest utilization wins": {
			scoped: []oauth.ScopedWindow{
				{ModelName: "Sonnet", Window: oauth.Window{Utilization: 40, ResetsAt: reset}},
				{ModelName: "Fable", Window: oauth.Window{Utilization: 100, ResetsAt: reset}},
				{ModelName: "Opus", Window: oauth.Window{Utilization: 12, ResetsAt: reset}},
			},
			wantModel:  "Fable",
			wantUtil:   100,
			wantResets: true,
		},
		"no scoped bucket leaves the fields empty": {
			scoped:     nil,
			wantModel:  "",
			wantUtil:   0,
			wantResets: false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc", KeychainAccount: "user"}
			if err := st.UpsertAccount(a); err != nil {
				t.Fatal(err)
			}
			m := &Manager{Store: st, LockDir: t.TempDir()}
			m.recordSample(1, &oauth.Usage{
				FiveHour:     oauth.Window{Utilization: 12},
				SevenDay:     oauth.Window{Utilization: 60},
				ScopedWeekly: tc.scoped,
			}, false)

			got, ok, err := st.LatestUsageSample(1)
			if err != nil || !ok {
				t.Fatalf("latest sample: ok=%v err=%v", ok, err)
			}
			if got.Scoped7dModel != tc.wantModel {
				t.Errorf("Scoped7dModel = %q, want %q", got.Scoped7dModel, tc.wantModel)
			}
			if got.Scoped7dUtil != tc.wantUtil {
				t.Errorf("Scoped7dUtil = %v, want %v", got.Scoped7dUtil, tc.wantUtil)
			}
			if tc.wantResets {
				if !got.Scoped7dResets.Equal(reset) {
					t.Errorf("Scoped7dResets = %v, want %v", got.Scoped7dResets, reset)
				}
			} else if !got.Scoped7dResets.IsZero() {
				t.Errorf("Scoped7dResets = %v, want zero", got.Scoped7dResets)
			}
		})
	}
}
