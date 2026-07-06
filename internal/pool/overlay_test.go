package pool

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

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
	if MinHolderVersion != "v0.29.0" {
		t.Fatalf("MinHolderVersion = %q; the mitigation floor moved — re-derive this matrix", MinHolderVersion)
	}
	cases := map[string]struct {
		version string
		want    bool
	}{
		"last pre-mux release refused":            {"v0.28.0", false},
		"older release refused":                   {"v0.20.0", false},
		"boundary v0.29.0 mitigated":              {"v0.29.0", true},
		"v0.30.0 mitigated":                       {"v0.30.0", true},
		"future major mitigated":                  {"v1.0.0", true},
		"dev build is current source":             {"dev", true},
		"empty version fails open":                {"", true},
		"unparseable version fails open":          {"not-a-version", true},
		"commit-suffixed wire version passes":     {"v0.29.0 (abc1234)", true},
		"commit-suffixed old version still fails": {"v0.28.0 (abc1234)", false},
		"surrounding whitespace is trimmed":       {"  v0.28.0  ", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := HolderVersionMitigated(tc.version); got != tc.want {
				t.Fatalf("HolderVersionMitigated(%q) = %v, want %v", tc.version, got, tc.want)
			}
		})
	}
}

// TestWidgetVersionSupported pins the companion-app floor for the probe-domain
// op. The wire carries a bare "0.44.0" (CFBundleShortVersionString, no leading
// "v"), so the "v" is prepended before the semver compare; fail-open arms mean a
// locally-built (dev/empty/garbage) app is current-source.
func TestWidgetVersionSupported(t *testing.T) {
	if MinWidgetVersion != "v0.44.0" {
		t.Fatalf("MinWidgetVersion = %q; the floor moved — re-derive this matrix", MinWidgetVersion)
	}
	cases := map[string]struct {
		version string
		want    bool
	}{
		"bare wire version at the floor supported":  {"0.44.0", true},
		"bare wire version below the floor refused": {"0.43.9", false},
		"older bare version refused":                {"0.1.0", false},
		"future bare version supported":             {"1.2.3", true},
		"v-prefixed floor supported":                {"v0.44.0", true},
		"v-prefixed old version refused":            {"v0.43.0", false},
		"dev build is current source":               {"dev", true},
		"empty version fails open":                  {"", true},
		"unparseable version fails open":            {"not-a-version", true},
		"commit-suffixed wire version passes":       {"0.44.0 (abc1234)", true},
		"commit-suffixed old version still fails":   {"0.43.0 (abc1234)", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := WidgetVersionSupported(tc.version); got != tc.want {
				t.Fatalf("WidgetVersionSupported(%q) = %v, want %v", tc.version, got, tc.want)
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
	const oldVer, curVer = "v0.28.0", "v0.29.0"
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

// TestOverlaySpecNeverOptsIntoAttrCache pins the VM-proven contraindication:
// a mount serving synthetic documents whose size changes and are promptly
// re-read (cc-pool's merged /.claude.json) tears at ANY go-nfsv4 attrcache
// TTL — stale-small clamps a grown document mid-JSON, stale-large over-reads
// a shrunk one into NUL padding — and attr stabilization cannot help because
// the size legitimately changes on every rewrite. cc-pool must never opt in
// (fusekit `ccn doc show 130274e`; the gate is validate-attrcache.sh).
func TestOverlaySpecNeverOptsIntoAttrCache(t *testing.T) {
	spec := overlaySpec()
	if spec.Holder == nil {
		t.Fatal("overlaySpec().Holder = nil; want a holder spec")
	}
	if spec.Holder.AttrCache {
		t.Fatal("overlaySpec() opts into the go-nfsv4 attr cache; VM-proven torn reads for synth-document mounts — must stay off")
	}
	if spec.Holder.AttrCacheTimeout != 0 {
		t.Fatalf("overlaySpec().Holder.AttrCacheTimeout = %v, want 0 (attrcache must stay off)", spec.Holder.AttrCacheTimeout)
	}
}

// TestOverlaySpecMcpNeedsAuthCachePrivate pins mcp-needs-auth-cache.json's
// private classification end to end: PrivateEntry — and therefore the built
// Spec's IsPrivate — claims the file and its atomic-write temp siblings,
// look-alike names keep their shared default, and the holder's PrivatePrefixes
// route the family to the per-account private root. Unclassified, both
// overlays shared the name, so claude's atomic rewrite clobbered the symlink
// into a real per-account file that a symlink→fuse conversion then shadowed
// under the mount (silent data loss).
func TestOverlaySpecMcpNeedsAuthCachePrivate(t *testing.T) {
	spec := overlaySpec()
	cases := map[string]bool{
		"mcp-needs-auth-cache.json":          true,
		"mcp-needs-auth-cache.json.tmp.abcd": true,
		"mcp-needs-auth-cache.json.lock":     true,
		"mcp-needs-auth.json":                false, // look-alike, not the cache file
		"mcp-needs-auth-cache.jsonx":         false, // exact name or dot-sibling only
		"mcp-needs-auth-cache":               false,
	}
	for name, want := range cases {
		if got := overlay.PrivateEntry(name); got != want {
			t.Errorf("PrivateEntry(%q) = %v, want %v", name, got, want)
		}
		if got := spec.IsPrivate(name); got != want {
			t.Errorf("spec.IsPrivate(%q) = %v, want %v", name, got, want)
		}
	}
	// The shared holder routes by prefix; without this entry a fuse account's
	// MCP auth cache would write through to the shared base.
	if !slices.Contains(overlay.PrivatePrefixes, "mcp-needs-auth-cache.json") {
		t.Errorf("overlay.PrivatePrefixes = %v, missing %q", overlay.PrivatePrefixes, "mcp-needs-auth-cache.json")
	}
	if !slices.Contains(spec.Holder.PrivatePrefixes, "mcp-needs-auth-cache.json") {
		t.Errorf("Holder.PrivatePrefixes = %v, missing %q", spec.Holder.PrivatePrefixes, "mcp-needs-auth-cache.json")
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

// TestOverlaySpecFileProviderWiring pins the File Provider arm of cc-pool's
// Spec: companion app, both sockets under their proper roots, and the identity
// constants release.yml asserts against the built appex's Info.plist.
func TestOverlaySpecFileProviderWiring(t *testing.T) {
	fp := overlaySpec().FileProvider
	if fp == nil {
		t.Fatal("overlaySpec().FileProvider = nil; File Provider selection would be disabled")
	}
	wantBridge := filepath.Join(mustHome(), "Library", "Group Containers", AppGroupID, "b.sock")
	switch {
	case fp.AppPath != "/Applications/CCPoolStatus.app":
		t.Errorf("FileProvider.AppPath = %q, want %q", fp.AppPath, "/Applications/CCPoolStatus.app")
	case fp.ControlSocket != filepath.Join(StateDir(), "domains.sock"):
		t.Errorf("FileProvider.ControlSocket = %q, want %q", fp.ControlSocket, filepath.Join(StateDir(), "domains.sock"))
	case fp.BridgeSocket != wantBridge:
		t.Errorf("FileProvider.BridgeSocket = %q, want %q", fp.BridgeSocket, wantBridge)
	case fp.BridgeSocket == BridgeSocketPath():
		t.Errorf("FileProvider.BridgeSocket = %q collides with the holder bridge socket", fp.BridgeSocket)
	case fp.ExtensionBundleID != "com.yasyf.cc-pool.status.fileprovider":
		t.Errorf("FileProvider.ExtensionBundleID = %q, want %q", fp.ExtensionBundleID, "com.yasyf.cc-pool.status.fileprovider")
	case fp.AppGroup != "SXKCTF23Q2.ccp":
		t.Errorf("FileProvider.AppGroup = %q, want %q", fp.AppGroup, "SXKCTF23Q2.ccp")
	case fp.SpawnTimeout != 30*time.Second:
		t.Errorf("FileProvider.SpawnTimeout = %v, want %v", fp.SpawnTimeout, 30*time.Second)
	}
}
