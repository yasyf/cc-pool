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
			// Every real fuse mount rides through this resolution, so the feature
			// gate must be wrapped here — a bare remote provider would let `ccp add`
			// mount through a holder missing a required capability.
			gate, ok := p.(featureGate)
			if !ok {
				t.Fatalf("OverlayProviderFor(%q) = %T, want the feature-gated fuse provider", tc.backend, p)
			}
			if _, ok := gate.Provider.(*fkoverlay.RemoteFuseProvider); !ok {
				t.Fatalf("gated inner provider = %T, want *fkoverlay.RemoteFuseProvider", gate.Provider)
			}
			if gate.hello == nil {
				t.Fatal("feature gate wired without a holder hello probe")
			}
		})
	}
}

// TestHolderMountFeatures pins the capability set every fuse mount depends on —
// the mux root, a hosted content bridge, and the lease-ladder teardown. The
// feature handshake replaces version arithmetic, so this list IS the gate.
func TestHolderMountFeatures(t *testing.T) {
	want := []string{mountd.FeatureMux, mountd.FeatureBridge, mountd.FeatureLeaseGate, mountd.FeatureWarning}
	if !slices.Equal(HolderMountFeatures, want) {
		t.Fatalf("HolderMountFeatures = %v, want %v", HolderMountFeatures, want)
	}
}

// TestWidgetVersionSupported pins the companion-app floor for shallow
// probe-domain and prepare-domain. The wire carries a bare "0.55.0"
// (CFBundleShortVersionString, no leading "v"), so the "v" is prepended before
// the semver compare; fail-open arms mean a locally-built app is current-source.
func TestWidgetVersionSupported(t *testing.T) {
	if MinWidgetVersion != "v0.55.0" {
		t.Fatalf("MinWidgetVersion = %q; the floor moved — re-derive this matrix", MinWidgetVersion)
	}
	cases := map[string]struct {
		version string
		want    bool
	}{
		"bare wire version at the floor supported":  {"0.55.0", true},
		"bare wire version below the floor refused": {"0.54.0", false},
		"older bare version refused":                {"0.1.0", false},
		"future bare version supported":             {"1.2.3", true},
		"v-prefixed floor supported":                {"v0.55.0", true},
		"v-prefixed old version refused":            {"v0.54.0", false},
		"dev build is current source":               {"dev", true},
		"empty version fails open":                  {"", true},
		"unparseable version fails open":            {"not-a-version", true},
		"commit-suffixed wire version passes":       {"0.55.0 (abc1234)", true},
		"commit-suffixed old version still fails":   {"0.54.0 (abc1234)", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := WidgetVersionSupported(tc.version); got != tc.want {
				t.Fatalf("WidgetVersionSupported(%q) = %v, want %v", tc.version, got, tc.want)
			}
		})
	}
}

// gateStub is a fuse-kind provider for featureGate tests, tracking Setup calls.
type gateStub struct {
	setupErr  error
	setups    int
	teardowns int
}

func (s *gateStub) Backend() fkoverlay.Backend    { return fkoverlay.BackendNFS }
func (s *gateStub) Sync(_, _ string) error        { return nil }
func (s *gateStub) Health(_, _ string) error      { return nil }
func (s *gateStub) PrivateRoot(dir string) string { return fkoverlay.FusePrivateRoot(dir) }
func (s *gateStub) Setup(_, _ string) error       { s.setups++; return s.setupErr }
func (s *gateStub) Teardown(_, _ string) (string, error) {
	s.teardowns++
	return "", nil
}

// TestFeatureGateSetupPostMountRequire pins F8's post-mount re-negotiation: when
// the holder that actually MOUNTED lacks a required feature — whether the pre-check
// was unreachable (cold start) or reached a different holder that then exited — Setup
// refuses. It does NOT tear the mount down (an unmount through a distrusted holder
// could race a live session); the caller's rollback / the daemon reconcile removes
// it through a supported holder.
func TestFeatureGateSetupPostMountRequire(t *testing.T) {
	missingWarn := &mountd.HelloInfo{Version: "v1.0.0", Features: []string{mountd.FeatureMux, mountd.FeatureBridge, mountd.FeatureLeaseGate}}
	full := &mountd.HelloInfo{Version: "v1.0.0", Features: mountd.HolderFeatures}
	cases := map[string]struct {
		first    *mountd.HelloInfo
		firstErr error
	}{
		"cold start (pre-check unreachable), post-mount holder underfeatured": {nil, mountd.ErrHolderUnavailable},
		"pre-check holder exits, Setup mounts an underfeatured successor":     {full, nil},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			calls := 0
			hello := func() (*mountd.HelloInfo, error) {
				calls++
				if calls == 1 {
					return tc.first, tc.firstErr
				}
				return missingWarn, nil // the holder that actually mounted lacks FeatureWarning
			}
			stub := &gateStub{}
			gate := featureGate{Provider: stub, hello: hello}

			err := gate.Setup("/base", "/pool/acct-01")
			if !errors.Is(err, ErrHolderUnsupported) {
				t.Fatalf("Setup post-mount require = %v, want ErrHolderUnsupported", err)
			}
			if stub.setups != 1 {
				t.Fatalf("inner setups = %d, want 1 (Setup ran before the post-mount check)", stub.setups)
			}
			if stub.teardowns != 0 {
				t.Fatalf("inner teardowns = %d, want 0 (a refusal must NOT unmount through a distrusted holder)", stub.teardowns)
			}
		})
	}
}

// TestFeatureGateSetupPostMountRetry pins G6's bounded retry: the mount is already
// live (its holder was reachable moments ago), so a momentarily-unreachable post-mount
// holder is retried rather than failed on the first miss — Setup succeeds once the
// holder answers within the window.
func TestFeatureGateSetupPostMountRetry(t *testing.T) {
	restore := holderVerifyTimeout
	holderVerifyTimeout = 2 * time.Second
	defer func() { holderVerifyTimeout = restore }()

	full := &mountd.HelloInfo{Version: "v1.0.0", Features: mountd.HolderFeatures}
	calls := 0
	hello := func() (*mountd.HelloInfo, error) {
		calls++
		switch calls {
		case 1:
			return full, nil // pre-check: reachable and full
		case 2, 3:
			return nil, mountd.ErrHolderUnavailable // post-mount: momentarily unreachable
		default:
			return full, nil // recovered within the window
		}
	}
	stub := &gateStub{}
	gate := featureGate{Provider: stub, hello: hello}
	if err := gate.Setup("/base", "/pool/acct-01"); err != nil {
		t.Fatalf("Setup with a briefly-unreachable-then-recovered holder = %v, want nil (bounded retry recovers)", err)
	}
	if stub.setups != 1 {
		t.Fatalf("inner setups = %d, want 1", stub.setups)
	}
	if calls < 4 {
		t.Fatalf("hello calls = %d, want >= 4 (the post-mount check retried until the holder answered)", calls)
	}
}

// TestFeatureGateTeardownUngated pins that Teardown is NEVER gated: even a holder
// missing a required feature (or proto-mismatched) tears down, so a conversion's
// rollback cleanup or a removal can always undo a mount and never strand identity.
func TestFeatureGateTeardownUngated(t *testing.T) {
	missingWarn := &mountd.HelloInfo{Version: "v1.0.0", Features: []string{mountd.FeatureMux, mountd.FeatureBridge}}
	stub := &gateStub{}
	gate := featureGate{Provider: stub, hello: func() (*mountd.HelloInfo, error) { return missingWarn, nil }}
	if _, err := gate.Teardown("/base", "/pool/acct-01"); err != nil {
		t.Fatalf("Teardown through an underfeatured holder = %v, want nil (teardown is ungated)", err)
	}
	if stub.teardowns != 1 {
		t.Fatalf("inner teardowns = %d, want 1 (Teardown must always call through)", stub.teardowns)
	}
}

// TestFeatureGateSetup pins the choke point that replaces version arithmetic: a
// reachable holder is required to serve HolderMountFeatures before any mount, a
// proto-mismatched holder is refused, an unreachable holder falls through to the
// mount (a cold start whose remedy is the spawn), and — because the post-mount check
// is MANDATORY (G6) — a holder that never answers after mounting FAILS CLOSED.
func TestFeatureGateSetup(t *testing.T) {
	restore := holderVerifyTimeout
	holderVerifyTimeout = 50 * time.Millisecond
	defer func() { holderVerifyTimeout = restore }()

	full := &mountd.HelloInfo{Version: "v1.0.0", Features: mountd.HolderFeatures}
	missingMux := &mountd.HelloInfo{Version: "v0.9.0", Features: []string{mountd.FeatureBridge, mountd.FeatureLeaseGate}}
	cases := map[string]struct {
		info          *mountd.HelloInfo
		helloErr      error
		setupErr      error
		wantUnsup     bool   // errors.Is(err, ErrHolderUnsupported)
		wantErrSubstr string // "" with wantUnsup=false means Setup must succeed
		wantSetups    int
	}{
		"a holder serving all features passes through": {
			info: full, wantSetups: 1,
		},
		"a holder missing a required feature is refused before mounting": {
			info: missingMux, wantUnsup: true,
			wantErrSubstr: "brew upgrade --cask fusekit-holder", wantSetups: 0,
		},
		"a proto-mismatched holder is refused before mounting": {
			helloErr: mountd.ErrProtoMismatch, wantUnsup: true, wantSetups: 0,
		},
		"a holder unreachable through the post-mount check fails closed": {
			helloErr: mountd.ErrHolderUnavailable, wantUnsup: true,
			wantErrSubstr: "unverified", wantSetups: 1,
		},
		"a mount failure propagates untouched": {
			info: full, setupErr: errors.New("mount exploded"),
			wantErrSubstr: "mount exploded", wantSetups: 1,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			stub := &gateStub{setupErr: tc.setupErr}
			gate := featureGate{Provider: stub, hello: func() (*mountd.HelloInfo, error) {
				return tc.info, tc.helloErr
			}}

			err := gate.Setup("/base", "/pool/acct-01")

			if wantErr := tc.wantUnsup || tc.wantErrSubstr != ""; (err != nil) != wantErr {
				t.Fatalf("Setup error = %v, want error: %v", err, wantErr)
			}
			if got := errors.Is(err, ErrHolderUnsupported); got != tc.wantUnsup {
				t.Fatalf("errors.Is(err, ErrHolderUnsupported) = %v, want %v (err = %v)", got, tc.wantUnsup, err)
			}
			if tc.wantErrSubstr != "" && !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Fatalf("error %q missing %q", err, tc.wantErrSubstr)
			}
			if stub.setups != tc.wantSetups {
				t.Fatalf("inner setups = %d, want %d", stub.setups, tc.wantSetups)
			}
		})
	}
}

// TestFeatureGateSetupMountedButUnverified pins J4c: when the inner Setup errors but
// the holder's mount list shows dir IS mounted (a lost ack, or a mid-Setup protocol
// mismatch), Setup wraps the failure ErrMountedUnverified so the caller reclaims the
// live mount before any fallback. A dir NOT in the mount list stays a clean pre-mount
// miss (the raw error, no wrap), so a cold-start fallback is never treated as a live
// mount.
func TestFeatureGateSetupMountedButUnverified(t *testing.T) {
	full := &mountd.HelloInfo{Version: "v1.0.0", Features: mountd.HolderFeatures}
	boom := errors.New("lost the mount ack")
	const dir = "/pool/acct-01"

	t.Run("mounted despite the error wraps ErrMountedUnverified", func(t *testing.T) {
		gate := featureGate{
			Provider: &gateStub{setupErr: boom},
			hello:    func() (*mountd.HelloInfo, error) { return full, nil },
			mounts:   func() ([]mountd.MountInfo, error) { return []mountd.MountInfo{{Dir: dir}}, nil },
		}
		err := gate.Setup("/base", dir)
		if !errors.Is(err, ErrMountedUnverified) || !errors.Is(err, boom) {
			t.Fatalf("Setup = %v, want ErrMountedUnverified wrapping the cause", err)
		}
	})

	// A mux row's holder Dir is the subtree under MuxRootDir(), NOT the account
	// ConfigDir Setup receives; the shape-aware translation must still flag it.
	t.Run("mounted mux subtree matches its account ConfigDir", func(t *testing.T) {
		acctDir := filepath.Join(AccountsDir(), "acct-07")
		subtree := filepath.Join(MuxRootDir(), "acct-07") // the holder's Dir for a mux row
		gate := featureGate{
			Provider: &gateStub{setupErr: boom},
			hello:    func() (*mountd.HelloInfo, error) { return full, nil },
			mounts: func() ([]mountd.MountInfo, error) {
				return []mountd.MountInfo{{Dir: subtree, MuxRoot: MuxRootDir()}}, nil
			},
		}
		err := gate.Setup("/base", acctDir)
		if !errors.Is(err, ErrMountedUnverified) || !errors.Is(err, boom) {
			t.Fatalf("Setup on a mounted mux subtree = %v, want ErrMountedUnverified wrapping the cause (the lost-ack reclaim path)", err)
		}
	})

	t.Run("never mounted returns the raw cause", func(t *testing.T) {
		gate := featureGate{
			Provider: &gateStub{setupErr: boom},
			hello:    func() (*mountd.HelloInfo, error) { return full, nil },
			mounts:   func() ([]mountd.MountInfo, error) { return nil, nil },
		}
		err := gate.Setup("/base", dir)
		if errors.Is(err, ErrMountedUnverified) || !errors.Is(err, boom) {
			t.Fatalf("Setup = %v, want the raw cause with no ErrMountedUnverified", err)
		}
	})

	t.Run("an unreachable mount list is treated as not mounted", func(t *testing.T) {
		gate := featureGate{
			Provider: &gateStub{setupErr: boom},
			hello:    func() (*mountd.HelloInfo, error) { return full, nil },
			mounts:   func() ([]mountd.MountInfo, error) { return nil, errors.New("holder unreachable") },
		}
		err := gate.Setup("/base", dir)
		if errors.Is(err, ErrMountedUnverified) || !errors.Is(err, boom) {
			t.Fatalf("Setup = %v, want the raw cause (an unlistable holder holds no reclaimable mount)", err)
		}
	})
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
