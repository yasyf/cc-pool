package daemon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

// TestCredMirrorRunsOutsideAccountLock pins the ABBA-inversion fix: a
// credential write completes without blocking while the registry flock is
// held, because the hook only enqueues.
func TestCredMirrorRunsOutsideAccountLock(t *testing.T) {
	dir := t.TempDir()
	rf := &hostsync.RegistryFile{
		Path:     filepath.Join(dir, "registry.json"),
		LockPath: filepath.Join(dir, "registry.lock"),
	}
	svc := &hostsync.Service{Registry: rf, StampDir: filepath.Join(dir, "stamps")}
	if err := svc.PublishAccount(t.Context(), hostsync.AccountValue{
		UUID:  "u1",
		Chain: hostsync.ChainStamp{ExpiresAt: 100, Origin: "host-b"},
	}); err != nil {
		t.Fatal(err)
	}

	mirror := newCredMirror(svc.NoteCredWrite, "host-a", log.New(io.Discard, "", 0))
	runDone := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		defer close(runDone)
		mirror.Run(ctx)
	}()

	st, err := store.Open(filepath.Join(dir, "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(dir, "acct"),
		KeychainService: "svc", KeychainAccount: "user", AccountUUID: "u1",
	}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fk := credstest.NewFake()
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-1"
	cred.ClaudeAiOauth.RefreshToken = "rt-1"
	cred.ClaudeAiOauth.ExpiresAt = 200 // strictly fresher than the seeded 100
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	m := &pool.Manager{Store: st, Creds: fk, LockDir: t.TempDir(), OnCredWrite: mirror.Hook}

	// Hold the registry flock, simulating a converge pass mid-flight.
	locked := make(chan struct{})
	release := make(chan struct{})
	flockDone := make(chan error, 1)
	go func() {
		flockDone <- rf.WithLock(context.Background(), func() error {
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked

	// The account-path credential write must return without the registry lock.
	adoptDone := make(chan error, 1)
	go func() { adoptDone <- m.AdoptRotatedToken(context.Background(), a) }()
	select {
	case err := <-adoptDone:
		if err != nil {
			t.Fatalf("AdoptRotatedToken: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("credential write blocked while the registry flock was held — the ABBA inversion the mirror exists to prevent")
	}

	// Free the flock; the mirror goroutine now lands the stamp.
	close(release)
	if err := <-flockDone; err != nil {
		t.Fatalf("WithLock: %v", err)
	}
	wantHash := creds.AccessHash(cred)
	deadline := time.Now().Add(10 * time.Second)
	for {
		reg, err := rf.Load()
		if err != nil {
			t.Fatal(err)
		}
		if e, ok := reg["u1"]; ok && e.Present() && e.Value.Chain.ExpiresAt == 200 {
			if e.Value.Chain.Hash != wantHash {
				t.Fatalf("mirrored hash = %q, want AccessHash of the written credential %q", e.Value.Chain.Hash, wantHash)
			}
			if e.Value.Chain.Origin != "host-a" {
				t.Fatalf("mirrored origin = %q, want the rotating host %q", e.Value.Chain.Origin, "host-a")
			}
			if e.Value.Chain.RotatedAt <= 0 {
				t.Fatalf("mirrored RotatedAt = %d, want a wall-clock stamp", e.Value.Chain.RotatedAt)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mirror never landed the chain stamp; registry entry = %+v", reg["u1"])
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("mirror Run did not exit on context cancellation")
	}
}

// TestCredMirrorSkipsSyncedWrites pins the origin invariant: a synced
// (refresh-token-free) credential write — a synced install or a stripped
// double-spend loser — never publishes a stamp claiming this host as origin;
// only an owned rotation does.
func TestCredMirrorSkipsSyncedWrites(t *testing.T) {
	var mu sync.Mutex
	var noted []string
	note := func(_ context.Context, uuid string, _ hostsync.ChainStamp) error {
		mu.Lock()
		defer mu.Unlock()
		noted = append(noted, uuid)
		return nil
	}
	mirror := newCredMirror(note, "host-a", log.New(io.Discard, "", 0))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		mirror.Run(ctx)
	}()

	synced := &creds.Credential{}
	synced.ClaudeAiOauth.AccessToken = "at-synced" // no refresh token: a synced copy
	owned := &creds.Credential{}
	owned.ClaudeAiOauth.AccessToken = "at-owned"
	owned.ClaudeAiOauth.RefreshToken = "rt-owned"

	a := store.Account{ID: 1, AccountUUID: "u1"}
	if err := mirror.Hook(a, synced); err != nil {
		t.Fatal(err)
	}
	if err := mirror.Hook(a, owned); err != nil {
		t.Fatal(err)
	}

	// Only the owned write reaches the note seam; the synced write was never
	// enqueued, so "u1" can appear at most once.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		got := append([]string(nil), noted...)
		mu.Unlock()
		if len(got) >= 1 {
			if len(got) != 1 || got[0] != "u1" {
				t.Fatalf("noted = %v, want exactly [u1] (the owned write only)", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owned write never reached the mirror")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("mirror Run did not exit on context cancellation")
	}
}

// TestCredMirrorQueueFullDropsLoudly pins the bounded-queue contract: the hook
// never blocks, a full queue drops loudly, queued events drain in order, and a
// uuid-less account never touches the queue.
func TestCredMirrorQueueFullDropsLoudly(t *testing.T) {
	orig := credMirrorQueueSize
	credMirrorQueueSize = 2
	t.Cleanup(func() { credMirrorQueueSize = orig })

	var buf bytes.Buffer
	var mu sync.Mutex
	var noted []string
	started := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	note := func(_ context.Context, uuid string, _ hostsync.ChainStamp) error {
		once.Do(func() { close(started) })
		<-gate
		mu.Lock()
		defer mu.Unlock()
		noted = append(noted, uuid)
		return nil
	}
	mirror := newCredMirror(note, "host-a", log.New(&buf, "", 0))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		mirror.Run(ctx)
	}()

	acct := func(i int) store.Account {
		return store.Account{ID: i, AccountUUID: fmt.Sprintf("u%d", i)}
	}
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at"
	cred.ClaudeAiOauth.RefreshToken = "rt"

	// e1 is picked up by Run and blocks in note; e2+e3 fill the size-2 queue.
	if err := mirror.Hook(acct(1), cred); err != nil {
		t.Fatal(err)
	}
	<-started
	for i := 2; i <= 3; i++ {
		if err := mirror.Hook(acct(i), cred); err != nil {
			t.Fatal(err)
		}
	}
	if got := buf.String(); got != "" {
		t.Fatalf("no drop expected while the queue has room; log = %q", got)
	}

	// e4 finds the queue full: dropped, loudly, without blocking.
	hookDone := make(chan struct{})
	go func() {
		defer close(hookDone)
		_ = mirror.Hook(acct(4), cred)
	}()
	select {
	case <-hookDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Hook blocked on a full queue — it must drop instead")
	}
	if got := buf.String(); !strings.Contains(got, "queue full") || !strings.Contains(got, "acct-04") {
		t.Fatalf("full-queue drop must log loudly with the account; log = %q", got)
	}

	// A uuid-less account never touches the queue (and never logs a drop).
	before := buf.Len()
	if err := mirror.Hook(store.Account{ID: 9}, cred); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != before {
		t.Fatalf("uuid-less hook must be a silent no-op; log grew: %q", buf.String()[before:])
	}

	close(gate)
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		got := append([]string(nil), noted...)
		mu.Unlock()
		if len(got) >= 3 {
			want := []string{"u1", "u2", "u3"}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("noted = %v, want %v in FIFO order", got, want)
				}
			}
			if len(got) > 3 {
				t.Fatalf("dropped event u4 must never land; noted = %v", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queued events never drained; noted = %v", got)
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("mirror Run did not exit on context cancellation")
	}
}
