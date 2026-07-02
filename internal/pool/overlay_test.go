package pool

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// TestCanHostFuse pins CanHostFuse's build-tag-independent logic via the
// holderExe seam: no cask and no holder reachable → cannot host; the cask
// binary present → can host without a running holder.
func TestCanHostFuse(t *testing.T) {
	t.Setenv("HOME", t.TempDir()) // DefaultHolderSocket → temp/.fusekit; nothing listening
	orig := holderExe
	t.Cleanup(func() { holderExe = orig })

	holderExe = filepath.Join(t.TempDir(), "absent", "fusekit-holder")
	if CanHostFuse() {
		t.Fatal("CanHostFuse() = true with no cask and no holder, want false")
	}

	exe := filepath.Join(t.TempDir(), "fusekit-holder")
	if err := os.WriteFile(exe, []byte("stub"), 0o600); err != nil {
		t.Fatal(err)
	}
	holderExe = exe
	if !CanHostFuse() {
		t.Fatal("CanHostFuse() = false with the cask installed, want true")
	}
}

func TestOverlayProviderFor(t *testing.T) {
	tests := []struct {
		name     string
		backend  fkoverlay.Backend
		wantFuse bool // RemoteFuseProvider wired to the pool paths; else symlink
	}{
		{name: "nfs maps to the remote fuse provider", backend: fkoverlay.BackendNFS, wantFuse: true},
		{name: "fskit maps to the remote fuse provider", backend: fkoverlay.BackendFSKit, wantFuse: true},
		{name: "symlink maps to the symlink provider", backend: fkoverlay.BackendSymlink},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := OverlayProviderFor(tc.backend)
			if err != nil {
				t.Fatalf("OverlayProviderFor(%q) error: %v", tc.backend, err)
			}
			if !tc.wantFuse {
				if _, ok := p.(*fkoverlay.SymlinkProvider); !ok {
					t.Fatalf("OverlayProviderFor(%q) = %T, want *fkoverlay.SymlinkProvider", tc.backend, p)
				}
				if got := p.Backend(); got != fkoverlay.BackendSymlink {
					t.Errorf("Backend() = %q, want %q", got, fkoverlay.BackendSymlink)
				}
				return
			}
			// A fuse provider always reports its stored backend — even in a
			// non-hosting build — so stored-backend fences never silently flip.
			if got := p.Backend(); got != tc.backend {
				t.Errorf("Backend() = %q, want %q", got, tc.backend)
			}
			if got := p.PrivateRoot("/p/acct-01"); got != fkoverlay.FusePrivateRoot("/p/acct-01") {
				t.Errorf("PrivateRoot = %q, want %q", got, fkoverlay.FusePrivateRoot("/p/acct-01"))
			}
			// Every real fuse mount rides through this resolution, so the
			// mitigation gate must be wrapped here — a bare remote provider
			// would let `ccp add` mount on a pre-mitigation holder.
			gate, ok := p.(mitigationGate)
			if !ok {
				t.Fatalf("OverlayProviderFor(%q) = %T, want the mitigation-gated fuse provider", tc.backend, p)
			}
			if _, ok := gate.Provider.(*fkoverlay.RemoteFuseProvider); !ok {
				t.Fatalf("gated inner provider = %T, want *fkoverlay.RemoteFuseProvider", gate.Provider)
			}
			if gate.health == nil {
				t.Fatal("mitigation gate wired without a holder health probe")
			}
		})
	}
}

// TestHolderVersionMitigated pins the one shared decision (daemon gate +
// doctor) on whether a holder carries the NFS kernel-panic mitigations.
// Fail-open arms are deliberate: "dev"/empty/garbage mean a locally-built,
// current-source holder — only a real release older than MinHolderVersion is
// refused.
func TestHolderVersionMitigated(t *testing.T) {
	if MinHolderVersion != "v0.23.0" {
		t.Fatalf("MinHolderVersion = %q; the mitigation floor moved — re-derive this matrix", MinHolderVersion)
	}
	cases := map[string]struct {
		version string
		want    bool
	}{
		"last pre-mitigation release refused":     {"v0.22.9", false},
		"older release refused":                   {"v0.20.0", false},
		"boundary v0.23.0 mitigated":              {"v0.23.0", true},
		"v0.24.0 mitigated":                       {"v0.24.0", true},
		"v0.25.0 mitigated":                       {"v0.25.0", true},
		"future major mitigated":                  {"v1.0.0", true},
		"dev build is current source":             {"dev", true},
		"empty version fails open":                {"", true},
		"unparseable version fails open":          {"not-a-version", true},
		"commit-suffixed wire version passes":     {"v0.23.0 (abc1234)", true},
		"commit-suffixed old version still fails": {"v0.22.9 (abc1234)", false},
		"surrounding whitespace is trimmed":       {"  v0.22.9  ", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := HolderVersionMitigated(tc.version); got != tc.want {
				t.Fatalf("HolderVersionMitigated(%q) = %v, want %v", tc.version, got, tc.want)
			}
		})
	}
}

// gateStub is a fuse-kind provider for mitigationGate tests, tracking Setup
// and Teardown calls.
type gateStub struct {
	setupErr    error
	teardownErr error
	setups      int
	teardowns   int
}

func (s *gateStub) Backend() fkoverlay.Backend    { return fkoverlay.BackendNFS }
func (s *gateStub) Sync(_, _ string) error        { return nil }
func (s *gateStub) Health(_, _ string) error      { return nil }
func (s *gateStub) PrivateRoot(dir string) string { return fkoverlay.FusePrivateRoot(dir) }
func (s *gateStub) Setup(_, _ string) error       { s.setups++; return s.setupErr }
func (s *gateStub) Teardown(_, _ string) error    { s.teardowns++; return s.teardownErr }

// TestMitigationGateSetup pins the choke point closing the nfs_vinvalbuf2
// panic vector: no fuse mirror is left hosted on a holder that predates
// MinHolderVersion — refused before mounting when the old holder is already
// serving, and torn down when Setup itself just spawned one from an old cask
// binary.
func TestMitigationGateSetup(t *testing.T) {
	const oldVer, curVer = "v0.22.1", "v0.25.0"
	absent := errors.New("dial holder socket: no such file")
	type probe struct {
		ver string
		err error
	}
	cases := map[string]struct {
		health        []probe // consumed in order; the last repeats
		setupErr      error
		teardownErr   error
		wantUnmit     bool   // errors.Is(err, ErrHolderUnmitigated)
		wantErrSubstr string // "" with wantUnmit=false means Setup must succeed
		wantSetups    int
		wantTeardowns int
	}{
		"an old holder already serving is refused before any mount": {
			health:    []probe{{ver: oldVer}},
			wantUnmit: true, wantErrSubstr: "brew upgrade --cask fusekit-holder",
			wantSetups: 0, wantTeardowns: 0,
		},
		"a mitigated holder passes through": {
			health:     []probe{{ver: curVer}},
			wantSetups: 1, wantTeardowns: 0,
		},
		"a dev holder is current source and passes": {
			health:     []probe{{ver: "dev"}},
			wantSetups: 1, wantTeardowns: 0,
		},
		"an old holder spawned by Setup itself is torn down": {
			health:    []probe{{err: absent}, {ver: oldVer}},
			wantUnmit: true, wantErrSubstr: "brew upgrade --cask fusekit-holder",
			wantSetups: 1, wantTeardowns: 1,
		},
		"an absent holder spawning current passes": {
			health:     []probe{{err: absent}, {ver: curVer}},
			wantSetups: 1, wantTeardowns: 0,
		},
		"an unanswerable post-mount health fails closed": {
			health:        []probe{{err: absent}},
			wantErrSubstr: "verify holder mitigations after mount",
			wantSetups:    1, wantTeardowns: 1,
		},
		"a mount failure propagates untouched, nothing to tear down": {
			health:        []probe{{err: absent}},
			setupErr:      errors.New("mount exploded"),
			wantErrSubstr: "mount exploded",
			wantSetups:    1, wantTeardowns: 0,
		},
		"a failed teardown keeps the unmitigated cause matchable": {
			health:      []probe{{err: absent}, {ver: oldVer}},
			teardownErr: errors.New("unmount wedged"),
			wantUnmit:   true, wantErrSubstr: "unmount wedged",
			wantSetups: 1, wantTeardowns: 1,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			stub := &gateStub{setupErr: tc.setupErr, teardownErr: tc.teardownErr}
			calls := 0
			gate := mitigationGate{Provider: stub, health: func() (string, error) {
				p := tc.health[min(calls, len(tc.health)-1)]
				calls++
				return p.ver, p.err
			}}

			err := gate.Setup("/base", "/pool/acct-01")

			if wantErr := tc.wantUnmit || tc.wantErrSubstr != ""; (err != nil) != wantErr {
				t.Fatalf("Setup error = %v, want error: %v", err, wantErr)
			}
			if got := errors.Is(err, ErrHolderUnmitigated); got != tc.wantUnmit {
				t.Fatalf("errors.Is(err, ErrHolderUnmitigated) = %v, want %v (err = %v)", got, tc.wantUnmit, err)
			}
			if tc.wantErrSubstr != "" && !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Fatalf("error %q missing %q", err, tc.wantErrSubstr)
			}
			if stub.setups != tc.wantSetups {
				t.Fatalf("inner setups = %d, want %d", stub.setups, tc.wantSetups)
			}
			if stub.teardowns != tc.wantTeardowns {
				t.Fatalf("inner teardowns = %d, want %d", stub.teardowns, tc.wantTeardowns)
			}
		})
	}
}

// TestOverlaySpecSkipsAppleDoubleLitter pins the SkipPrefixes wiring: "._*"
// AppleDouble sidecars (pre-mitigation fuse-mount litter) classify as skip
// litter through cc-pool's Spec, while neighboring dotfiles keep their old
// classes.
func TestOverlaySpecSkipsAppleDoubleLitter(t *testing.T) {
	spec := overlaySpec()
	cases := map[string]bool{
		"._history.jsonl": true,
		"._.claude.json":  true,
		"._":              true,
		".DS_Store":       true, // SkipEntries, unchanged
		".foo":            false,
		"history.jsonl":   false,
		"_notasidecar":    false,
	}
	for name, want := range cases {
		if got := spec.Skipped(name); got != want {
			t.Errorf("Skipped(%q) = %v, want %v", name, got, want)
		}
	}
	// A sidecar name must not read as private either, or conversions would move
	// litter between roots instead of clearing it.
	if spec.IsPrivate("._.claude.json") {
		t.Error(`IsPrivate("._.claude.json") = true; sidecar litter must stay in the skip class`)
	}
}

// TestOverlayProviderForUnknownBackend pins that a bad stored backend string
// fails loud rather than silently degrading to symlink.
func TestOverlayProviderForUnknownBackend(t *testing.T) {
	if _, err := OverlayProviderFor(fkoverlay.Backend("bogus")); err == nil {
		t.Fatal("OverlayProviderFor(\"bogus\") = nil error, want a loud failure")
	}
}

// TestOverlaySpecHolderWiring pins the Spec cc-pool hands fusekit: drive the
// SHARED holder over RPC (cask socket/binary, Owner, content) to register a
// synth-serving AddMount, never self-exec (no StableExecDir/argv) or
// version-replace it (no Version).
func TestOverlaySpecHolderWiring(t *testing.T) {
	spec := overlaySpec()
	if spec.PassthroughOnly {
		t.Error("PassthroughOnly = true; cc-pool's mirror serves synthetic content, want false (always NFS)")
	}
	h := spec.Holder
	if h == nil {
		t.Fatal("Holder is nil; fuse selection would be disabled")
	}
	socket := mountd.DefaultHolderSocket()
	switch {
	case h.Socket != socket:
		t.Errorf("Holder.Socket = %q, want the shared socket %q", h.Socket, socket)
	case h.ExecPath != mountd.HolderExe:
		t.Errorf("Holder.ExecPath = %q, want the cask binary %q", h.ExecPath, mountd.HolderExe)
	case h.Owner != HolderOwner:
		t.Errorf("Holder.Owner = %q, want %q", h.Owner, HolderOwner)
	case h.BridgeSocket != BridgeSocketPath():
		t.Errorf("Holder.BridgeSocket = %q, want %q", h.BridgeSocket, BridgeSocketPath())
	case h.ContentMode != "source":
		t.Errorf("Holder.ContentMode = %q, want \"source\"", h.ContentMode)
	case h.ProbePath != "/"+overlay.ProbeFileName:
		t.Errorf("Holder.ProbePath = %q, want %q", h.ProbePath, "/"+overlay.ProbeFileName)
	case h.StableExecDir != "":
		t.Errorf("Holder.StableExecDir = %q, want empty (no self-exec onto the shared holder)", h.StableExecDir)
	case h.Version != "":
		t.Errorf("Holder.Version = %q, want empty (cc-pool must not version-replace a shared holder)", h.Version)
	}
	wantArgs := []string{"--socket", socket}
	if len(h.Args) != len(wantArgs) || h.Args[0] != wantArgs[0] || h.Args[1] != wantArgs[1] {
		t.Fatalf("Holder.Args = %v, want %v (the cask binary's own flags, not a subcommand)", h.Args, wantArgs)
	}
	if len(h.PrivatePrefixes) != len(overlay.PrivatePrefixes) {
		t.Fatalf("Holder.PrivatePrefixes = %v, want %v", h.PrivatePrefixes, overlay.PrivatePrefixes)
	}
}
