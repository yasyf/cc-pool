package mountd

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
)

// deadEndProvider returns a RemoteProvider whose socket has no listener and
// whose log path cannot be opened, so ANY holder contact — an RPC or a spawn
// attempt — fails loudly in every build variant. A nil return from its methods
// proves the zero-RPC path was taken.
func deadEndProvider(t *testing.T) *RemoteProvider {
	t.Helper()
	return &RemoteProvider{
		Socket:       filepath.Join(shortSockDir(t), "m.sock"),
		LogPath:      filepath.Join(t.TempDir(), "missing", "holder.log"),
		SpawnTimeout: time.Second,
	}
}

func TestRemoteProviderKindAlwaysFuse(t *testing.T) {
	providers := map[string]*RemoteProvider{
		"constructed": NewRemoteProvider("/tmp/s", "/tmp/l"),
		"zero value":  {},
	}
	for name, p := range providers {
		if got := p.Kind(); got != overlay.KindFuse {
			t.Errorf("%s: Kind() = %q, want %q", name, got, overlay.KindFuse)
		}
	}
}

func TestRemoteProviderSpawnTimeoutDefault(t *testing.T) {
	if got := (&RemoteProvider{}).spawnTimeout(); got != DefaultSpawnTimeout {
		t.Errorf("zero SpawnTimeout = %v, want %v", got, DefaultSpawnTimeout)
	}
	if got := (&RemoteProvider{SpawnTimeout: time.Second}).spawnTimeout(); got != time.Second {
		t.Errorf("explicit SpawnTimeout = %v, want 1s", got)
	}
}

// TestRemoteProviderSetupAdoptsLiveMountWithZeroRPC: a shallow-live mirror is
// adopted with zero RPC — the holder kept serving it across a daemon restart.
// Detecting a partial wedge is the daemon's job (its own deep probe), not
// Setup's, so Setup does no deep read here.
func TestRemoteProviderSetupAdoptsLiveMountWithZeroRPC(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fakeMounted(t, func(string) bool { return true })
	fakeMountAlive(t, func(string, string) bool { return true })

	if err := deadEndProvider(t).Setup(base, dir); err != nil {
		t.Fatalf("Setup of an already-live mirror = %v, want nil (adopt, zero RPC)", err)
	}
}

func TestRemoteProviderSetupMountsViaHolder(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fake := &fakeHost{}
	installLiveSeams(t, fake)
	_, cl, _, _ := startServer(t, fake)

	p := NewRemoteProvider(cl.Socket, filepath.Join(t.TempDir(), "holder.log"))
	if err := p.Setup(base, dir); err != nil {
		t.Fatalf("Setup = %v, want nil", err)
	}
	setups, teardowns := fake.calls()
	if want := []hostCall{{base, dir}}; !reflect.DeepEqual(setups, want) {
		t.Errorf("holder Setup calls = %v, want %v", setups, want)
	}
	if len(teardowns) != 0 {
		t.Errorf("holder Teardown calls = %v, want none", teardowns)
	}
}

func TestOverlayClassTranslation(t *testing.T) {
	plain := errors.New("no class at all")
	tests := []struct {
		name    string
		in      error
		wantIs  []error
		wantNot []error
	}{
		{
			name:    "TCC gains the overlay mount-not-live identity",
			in:      fmt.Errorf("%w: grant pending", ErrTCCDenied),
			wantIs:  []error{ErrTCCDenied, overlay.ErrMountNotLive},
			wantNot: []error{overlay.ErrUnmountWedged, overlay.ErrMountTimeout},
		},
		{
			// The honest-timeout class: a proven grant means it must NEVER pick
			// up the TCC identity — that polarity is the whole point.
			name:    "mount-timeout gains the overlay mount-timeout identity, never mount-not-live",
			in:      fmt.Errorf("%w: still settling", ErrMountTimeout),
			wantIs:  []error{ErrMountTimeout, overlay.ErrMountTimeout},
			wantNot: []error{overlay.ErrMountNotLive, overlay.ErrUnmountWedged, ErrTCCDenied},
		},
		{
			name:    "wedged gains the overlay wedged identity",
			in:      fmt.Errorf("%w: still mounted", ErrUnmountWedged),
			wantIs:  []error{ErrUnmountWedged, overlay.ErrUnmountWedged},
			wantNot: []error{overlay.ErrMountNotLive, overlay.ErrMountTimeout},
		},
		{
			name:    "mount-failed has no overlay equivalent",
			in:      fmt.Errorf("%w: boom", ErrMountFailed),
			wantIs:  []error{ErrMountFailed},
			wantNot: []error{overlay.ErrMountNotLive, overlay.ErrMountTimeout, overlay.ErrUnmountWedged},
		},
		{
			name:    "classless error passes through untouched",
			in:      plain,
			wantIs:  []error{plain},
			wantNot: []error{overlay.ErrMountNotLive, overlay.ErrMountTimeout, overlay.ErrUnmountWedged},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := overlayClass(tc.in)
			for _, want := range tc.wantIs {
				if !errors.Is(got, want) {
					t.Errorf("overlayClass(%v) = %v, want errors.Is %v", tc.in, got, want)
				}
			}
			for _, not := range tc.wantNot {
				if errors.Is(got, not) {
					t.Errorf("overlayClass(%v) = %v, want NOT errors.Is %v", tc.in, got, not)
				}
			}
		})
	}
}

func TestRemoteProviderSetupTranslatesTCCClass(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fakeMounted(t, func(string) bool { return false })
	fakeMountAlive(t, func(string, string) bool { return false })
	// The holder's mount never comes live (the TCC grant case): its Setup
	// fails with overlay.ErrMountNotLive, which crosses the wire as ClassTCC.
	fake := &fakeHost{setupFn: func(string, string) error {
		return fmt.Errorf("mount did not come live: %w", overlay.ErrMountNotLive)
	}}
	_, cl, _, _ := startServer(t, fake)

	p := NewRemoteProvider(cl.Socket, filepath.Join(t.TempDir(), "holder.log"))
	err := p.Setup(base, dir)
	if err == nil {
		t.Fatal("Setup with a TCC-blocked holder mount succeeded, want error")
	}
	// Both identities must hold: the wire sentinel for mountd-aware callers
	// AND the overlay sentinel — overlay/errors.go promises classification
	// across the process boundary without importing mountd.
	if !errors.Is(err, ErrTCCDenied) {
		t.Errorf("error = %v, want errors.Is mountd.ErrTCCDenied", err)
	}
	if !errors.Is(err, overlay.ErrMountNotLive) {
		t.Errorf("error = %v, want errors.Is overlay.ErrMountNotLive", err)
	}
}

func TestRemoteProviderSetupCarcassNeedsTeardownThenRetry(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	// A dead HOLDER's carcass: dir is still a mountpoint per the kernel but
	// the mirror is dead, and the fresh holder's registry has no row for it.
	// Setup must refuse it as foreign (the holder never stacks mounts); the
	// documented recovery is Teardown — whose registry-miss path clears the
	// carcass — then a Setup retry.
	var stillMounted atomic.Bool
	stillMounted.Store(true)
	fakeMounted(t, func(string) bool { return stillMounted.Load() })
	fakeMountAlive(t, func(string, string) bool { return false })
	fake := &fakeHost{teardownFn: func(string, string) error {
		stillMounted.Store(false)
		return nil
	}}
	_, cl, _, _ := startServer(t, fake)
	p := NewRemoteProvider(cl.Socket, filepath.Join(t.TempDir(), "holder.log"))

	err := p.Setup(base, dir)
	if !errors.Is(err, ErrForeignMount) {
		t.Fatalf("Setup against a carcass = %v, want errors.Is ErrForeignMount", err)
	}
	if err := p.Teardown(base, dir); err != nil {
		t.Fatalf("Teardown of the carcass = %v, want nil", err)
	}
	if err := p.Setup(base, dir); err != nil {
		t.Fatalf("Setup after clearing the carcass = %v, want nil", err)
	}
	setups, teardowns := fake.calls()
	if want := []hostCall{{base, dir}}; !reflect.DeepEqual(teardowns, want) {
		t.Errorf("holder Teardown calls = %v, want %v", teardowns, want)
	}
	if want := []hostCall{{base, dir}}; !reflect.DeepEqual(setups, want) {
		t.Errorf("holder Setup calls = %v, want %v", setups, want)
	}
}

func TestRemoteProviderTeardownNotMountedIsNoOpWithZeroRPC(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fakeMounted(t, func(string) bool { return false })
	fakeMountAlive(t, func(string, string) bool { return false })

	if err := deadEndProvider(t).Teardown(base, dir); err != nil {
		t.Fatalf("Teardown of an unmounted dir = %v, want nil (no holder contact)", err)
	}
}

func TestRemoteProviderTeardownUnmountsViaHolder(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fake := &fakeHost{}
	fake.setLive(dir, true) // the holder's registry-miss carcass path serves it
	installLiveSeams(t, fake)
	_, cl, _, _ := startServer(t, fake)

	p := NewRemoteProvider(cl.Socket, filepath.Join(t.TempDir(), "holder.log"))
	if err := p.Teardown(base, dir); err != nil {
		t.Fatalf("Teardown = %v, want nil", err)
	}
	_, teardowns := fake.calls()
	if want := []hostCall{{base, dir}}; !reflect.DeepEqual(teardowns, want) {
		t.Errorf("holder Teardown calls = %v, want %v", teardowns, want)
	}
}

func TestRemoteProviderTeardownTranslatesWedgedClass(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fakeMounted(t, func(string) bool { return true })
	fakeMountAlive(t, func(string, string) bool { return true })
	// The holder's unmount wedges: its Teardown fails with
	// overlay.ErrUnmountWedged, which crosses the wire as ClassWedged.
	fake := &fakeHost{teardownFn: func(string, string) error {
		return fmt.Errorf("umount refused: %w", overlay.ErrUnmountWedged)
	}}
	_, cl, _, _ := startServer(t, fake)

	p := NewRemoteProvider(cl.Socket, filepath.Join(t.TempDir(), "holder.log"))
	err := p.Teardown(base, dir)
	if err == nil {
		t.Fatal("Teardown with a wedged holder unmount succeeded, want error")
	}
	// Both identities, exactly like the local re-verify path: a wedge must
	// classify the same regardless of which process detected it.
	if !errors.Is(err, ErrUnmountWedged) {
		t.Errorf("error = %v, want errors.Is mountd.ErrUnmountWedged", err)
	}
	if !errors.Is(err, overlay.ErrUnmountWedged) {
		t.Errorf("error = %v, want errors.Is overlay.ErrUnmountWedged", err)
	}
}

func TestRemoteProviderTeardownReVerifiesAfterOKReply(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	// The holder's fake Teardown "succeeds" (OK reply on the wire), but the
	// local kernel seam keeps reporting a mountpoint — a lost-unmount skew the
	// provider must refuse to call a clean teardown.
	fakeMounted(t, func(string) bool { return true })
	fakeMountAlive(t, func(string, string) bool { return true })
	fake := &fakeHost{}
	_, cl, _, _ := startServer(t, fake)

	p := NewRemoteProvider(cl.Socket, filepath.Join(t.TempDir(), "holder.log"))
	err := p.Teardown(base, dir)
	if err == nil {
		t.Fatal("Teardown with a still-mounted dir after an OK reply succeeded, want error")
	}
	if !errors.Is(err, overlay.ErrUnmountWedged) {
		t.Errorf("error = %v, want errors.Is ErrUnmountWedged", err)
	}
	if !strings.Contains(err.Error(), "still a mountpoint") {
		t.Errorf("error = %q, want it to say the dir is still a mountpoint", err)
	}
	_, teardowns := fake.calls()
	if want := []hostCall{{base, dir}}; !reflect.DeepEqual(teardowns, want) {
		t.Errorf("holder Teardown calls = %v, want %v (the RPC must have landed)", teardowns, want)
	}
}

func TestRemoteProviderTeardownMountedButHolderUnreachable(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fakeMounted(t, func(string) bool { return true })
	fakeMountAlive(t, func(string, string) bool { return true })

	err := deadEndProvider(t).Teardown(base, dir)
	if err == nil {
		t.Fatal("Teardown of a mounted dir with no reachable or spawnable holder succeeded, want error")
	}
	if !strings.Contains(err.Error(), "unmount "+dir) {
		t.Errorf("error = %q, want it wrapped with the unmount %s context", err, dir)
	}
}

func TestRemoteProviderHealthAndSync(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	tests := []struct {
		name           string
		mounted, alive bool
		wantErr        string // empty means healthy
	}{
		{name: "mounted and live is healthy", mounted: true, alive: true},
		{name: "not mounted", mounted: false, alive: false, wantErr: "not a mountpoint"},
		{name: "not mounted trumps an alive-looking dir", mounted: false, alive: true, wantErr: "not a mountpoint"},
		{name: "mounted but dead mirror", mounted: true, alive: false, wantErr: "dead"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeMounted(t, func(string) bool { return tc.mounted })
			fakeMountAlive(t, func(string, string) bool { return tc.alive })
			p := deadEndProvider(t) // Health and Sync are local-only: zero RPC

			for method, err := range map[string]error{
				"Health": p.Health(base, dir),
				"Sync":   p.Sync(base, dir),
			} {
				if tc.wantErr == "" {
					if err != nil {
						t.Errorf("%s = %v, want nil", method, err)
					}
					continue
				}
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("%s = %v, want error containing %q", method, err, tc.wantErr)
				}
			}
		})
	}
}

func TestRemoteProviderPrivateRoot(t *testing.T) {
	const dir = "/pool/acct-01"
	got := (&RemoteProvider{}).PrivateRoot(dir)
	if want := overlay.FusePrivateRoot(dir); got != want {
		t.Errorf("PrivateRoot(%q) = %q, want %q", dir, got, want)
	}
	if !strings.HasSuffix(got, ".private") {
		t.Errorf("PrivateRoot(%q) = %q, want the .private suffix", dir, got)
	}
}
