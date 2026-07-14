package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/content"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"golang.org/x/mod/semver"
)

// HolderOwner tags cc-pool's mounts on the shared fusekit-holder so the daemon
// reclaims only its own.
const HolderOwner = "cc-pool"

// cannotHostHint is appended to mountd.ErrCannotHost when the holder cask is absent.
const cannotHostHint = "run `ccp fuse enable` to install the fusekit-holder cask"

// HolderMountFeatures are the shared-holder capabilities every cc-pool fuse mount
// depends on: the mux root, a hosted content bridge, the lease-ladder teardown
// that gates every unmount on a live session lease, and the persist-warning an OK
// reply carries so a journal-delete failure surfaces instead of reading as a clean
// teardown. The feature handshake (OpHello) replaces version arithmetic — a holder
// missing any of these is refused with a cask-upgrade error, never mounted through.
var HolderMountFeatures = []string{mountd.FeatureMux, mountd.FeatureBridge, mountd.FeatureLeaseGate, mountd.FeatureWarning}

// MinWidgetVersion is the oldest CCPoolStatus companion app cc-pool trusts to
// drive File Provider domains: the first release whose control server answers
// the probe-domain op — the app-side, TCC-safe data-plane probe the daemon's
// wedge ladder, the migrate readiness gate, onboarding, and doctor all key on.
// An older app silently lacks it (its unknown-op arm reads as ErrOpUnsupported),
// so onboarding refuses it and points at the cask upgrade.
const MinWidgetVersion = "v0.44.0"

// WidgetVersionSupported reports whether a companion app's reported version is
// new enough to answer probe-domain (>= MinWidgetVersion). The app reports its
// CFBundleShortVersionString, which the release strips to a bare "0.44.0" (no
// leading "v"), so a "v" is prepended before the semver compare; a trailing
// commit paren is dropped before the compare. "dev", empty, or otherwise
// unparseable versions pass: a locally-built app is current-source.
func WidgetVersionSupported(version string) bool {
	v, _, _ := strings.Cut(strings.TrimSpace(version), " ")
	if v != "" && !strings.HasPrefix(v, "v") {
		v = "v" + v
	}
	if !semver.IsValid(v) {
		return true
	}
	return semver.Compare(v, MinWidgetVersion) >= 0
}

// ErrHolderUnsupported means the shared mount holder lacks a capability cc-pool
// depends on — a proto-1 holder, or a proto-2 holder missing a required feature.
// The wrapped error names the cask upgrade; every fuse mount entry point refuses
// until `brew upgrade --cask fusekit-holder`.
var ErrHolderUnsupported = errors.New("mount holder lacks a required capability")

// ErrMountedUnverified means a fuse Setup failed AFTER the holder reported the mount
// live (a lost ack, or a mid-Setup protocol mismatch): the mount is up but unverified,
// so the caller must reclaim it before any fallback rather than strand a rowless mount.
var ErrMountedUnverified = errors.New("fuse mount is live but its setup did not complete")

// holderHello negotiates the shared holder's capabilities via OpHello; a var so
// tests can fake an old, current, or absent holder.
var holderHello = func() (*mountd.HelloInfo, error) {
	return mountd.NewClient(mountd.DefaultHolderSocket()).Hello()
}

// holderMounts lists cc-pool's OWN live mounts on the shared holder (its source of
// truth for whether a dir actually mounted); a var so tests can fake it. Owner-scoped:
// an ownerless List is refused ClassInvalidOwner, and a cross-tenant view could match a
// foreign owner's subtree.
var holderMounts = func() ([]mountd.MountInfo, error) {
	c := mountd.NewClient(mountd.DefaultHolderSocket())
	c.Owner = HolderOwner
	return c.List()
}

// featureGate wraps the remote fuse provider so every mount entry point (`ccp
// add`/`ccp init`, migrate's conversion, the daemon's heal loop) refuses to mount
// through a holder missing a required capability — replacing version arithmetic
// with the feature handshake. ONLY Setup is gated: Teardown, Health, and Sync must
// keep working against ANY reachable holder — gating Teardown would strand a
// conversion's identity when its rollback cleanup-teardown is refused, and could
// unmount under a live session through a holder cc-pool distrusts.
type featureGate struct {
	fkoverlay.Provider
	hello func() (*mountd.HelloInfo, error)
	// mounts lists the holder's live mounts, so Setup can tell a mount that came up
	// then errored (a lost ack) from one that never mounted. Nil ⇒ dirMounted reads
	// false, so a bare-provider test never spuriously reports a live mount.
	mounts func() ([]mountd.MountInfo, error)
}

// dirMounted reports whether the holder's mount list currently carries dir — the
// source of truth for "did the inner Setup actually mount". False on a nil seam or any
// list error (an unreachable holder can hold no mount cc-pool could reclaim anyway).
func (g featureGate) dirMounted(dir string) bool {
	if g.mounts == nil {
		return false
	}
	mounts, err := g.mounts()
	if err != nil {
		return false
	}
	for _, mi := range mounts {
		// A mux row's holder Dir is the subtree under MuxRootDir(), not the account
		// ConfigDir; translate every reported Dir back to its ConfigDir (the single
		// wire→ConfigDir mapping) so a mounted-but-unverified mux subtree matches, not
		// just a legacy per-dir row.
		if ConfigDirForMount(mi.Dir) == dir {
			return true
		}
	}
	return false
}

// requireHolder negotiates HolderMountFeatures: a reachable holder missing one (or
// a proto-mismatched one) is refused with ErrHolderUnsupported; an unreachable
// holder (still spawning, or not running) returns nil so the caller falls through.
// This is the PRE-Setup check — an unreachable holder here is a cold start whose
// remedy IS the spawn Provider.Setup performs, so it fails open.
func (g featureGate) requireHolder() error {
	switch info, herr := g.hello(); {
	case herr == nil:
		if ferr := info.Require(HolderMountFeatures...); ferr != nil {
			return fmt.Errorf("%w: %w", ErrHolderUnsupported, ferr)
		}
		return nil
	case errors.Is(herr, mountd.ErrProtoMismatch):
		return fmt.Errorf("%w: %w", ErrHolderUnsupported, herr)
	default:
		return nil
	}
}

// holderVerifyTimeout bounds the post-Setup capability re-check's retry: the holder
// that mounted was reachable moments ago, so a brief unavailability is a restart race
// the cask relauncher closes; past this window an unverifiable holder fails closed. A
// var so tests can shrink it.
var holderVerifyTimeout = mountd.DefaultSpawnTimeout

// holderVerifyPoll is the post-Setup re-check poll interval.
const holderVerifyPoll = 100 * time.Millisecond

// requireHolderVerified is the POST-Setup check: unlike requireHolder it is
// MANDATORY. A reachable holder is negotiated as usual; a momentarily-unreachable one
// is retried within holderVerifyTimeout (the mount is already live, so its holder was
// reachable moments ago), and a holder that never answers within the window FAILS
// CLOSED — the mount is unverified and left for the caller's rollback to tear down.
func (g featureGate) requireHolderVerified() error {
	deadline := time.Now().Add(holderVerifyTimeout)
	for {
		switch info, herr := g.hello(); {
		case herr == nil:
			if ferr := info.Require(HolderMountFeatures...); ferr != nil {
				return fmt.Errorf("%w: %w", ErrHolderUnsupported, ferr)
			}
			return nil
		case errors.Is(herr, mountd.ErrProtoMismatch):
			return fmt.Errorf("%w: %w", ErrHolderUnsupported, herr)
		default:
			if time.Now().After(deadline) {
				return fmt.Errorf("%w: the mount holder did not answer a capability handshake after mounting (%v) — the mount is unverified; run `ccp doctor`", ErrHolderUnsupported, herr)
			}
			time.Sleep(holderVerifyPoll)
		}
	}
}

// Setup refuses a reachable-but-underfeatured (or proto-mismatched) holder BEFORE
// mounting, then — because the pre-check's holder may have exited and Setup
// spawned/attached a DIFFERENT one, or the pre-check was unreachable (cold start) —
// MANDATORILY re-negotiates against the holder that actually mounted (bounded retry,
// then fail closed). A post-mount refusal returns ErrHolderUnsupported and LEAVES the
// mount for the caller's rollback (or the daemon reconcile) to tear down through a
// supported holder: an unmount here, through a holder cc-pool distrusts, could race a
// live session onto the kernel force-unmount panic.
func (g featureGate) Setup(base, dir string) error {
	if err := g.requireHolder(); err != nil {
		return err // PRE-mount refusal: nothing mounted
	}
	if setupErr := g.Provider.Setup(base, dir); setupErr != nil {
		// The mount may be live despite the failure — a lost ack after the holder
		// mounted, or a mid-Setup protocol mismatch. Consult the holder's mount list;
		// if dir is mounted, mark the failure POST-mount so the caller reclaims it
		// before any fallback rather than strand a rowless live mount. A dir that never
		// mounted is a clean pre-mount miss the caller falls back from.
		if g.dirMounted(dir) {
			return fmt.Errorf("%w: %w", ErrMountedUnverified, setupErr)
		}
		return setupErr
	}
	return g.requireHolderVerified()
}

// overlaySpec builds cc-pool's fusekit/overlay Spec with no content source (the
// CLI default: an unconditional FP enumerator signal).
func overlaySpec() fkoverlay.Spec { return overlaySpecWithSource(nil) }

// overlaySpecWithSource builds cc-pool's fusekit/overlay Spec, wiring src into the
// File Provider backend so its enumerator signal is fingerprint-gated (nil leaves it
// unconditional). PassthroughOnly is false because cc-pool serves synthetic content
// (the merged /.claude.json), forcing fuse-t's NFS backend.
func overlaySpecWithSource(src content.Source) fkoverlay.Spec {
	socket := mountd.DefaultHolderSocket()
	return fkoverlay.Spec{
		IsPrivate:       overlay.PrivateEntry,
		Excluded:        overlay.ExcludedEntries,
		Shared:          overlay.SharedEntries,
		Skip:            overlay.SkipEntries,
		SkipPrefixes:    overlay.SkipPrefixes,
		PassthroughOnly: false,
		Holder: &fkoverlay.HolderSpec{
			Socket:         socket,
			LogPath:        MountHolderLogPath(),
			Args:           []string{"--socket", socket},
			ExecPath:       holderExe,
			Owner:          HolderOwner,
			CannotHostHint: cannotHostHint,
			SpawnTimeout:   mountd.DefaultSpawnTimeout,
			// No Version: cc-pool must NEVER version-replace the shared holder — that
			// tears down another tenant's mounts. The holder self-retires on version
			// skew (lease-gated); a holder missing a required capability is refused loud
			// by the feature gate until the operator upgrades the cask by hand
			// (`brew upgrade --cask fusekit-holder`).
			BridgeSocket: BridgeSocketPath(),
			ContentMode:  "source",
			ProbePath:    "/" + overlay.ProbeFileName,
			// Only the prefix set rides the wire; overlay.PrivatePatterns (glob
			// private, e.g. *.lock) intentionally stays off it — see its doc.
			PrivatePrefixes: overlay.PrivatePrefixes,
			// MuxRoot makes the provider serve every account as a subtree of ONE
			// native mount at ~/.cc-pool/mnt and bridge each account dir to its
			// subtree with a fail-closed symlink — one go-nfsv4 for the whole pool.
			MuxRoot: MuxRootDir(),
		},
		FileProvider: &fkoverlay.FileProviderSpec{
			AppPath:           WidgetAppPath(),
			ControlSocket:     FPControlSocketPath(),
			BridgeSocket:      FPBridgeSocketPath(),
			ExtensionBundleID: FPExtensionBundleID,
			AppGroup:          AppGroupID,
			SpawnTimeout:      30 * time.Second,
			// Setup fails loud (never a silent raw-read fallback) when the companion
			// app is too old to answer probe-domain; the hint names the cask upgrade.
			UpgradeHint: "upgrade the cc-pool-status cask (brew upgrade --cask cc-pool-status)",
			// Source gates the enumerator signal on a content-fingerprint change; nil
			// (the CLI) keeps the unconditional signal-every-Sync.
			Source: src,
		},
	}
}

func (m *Manager) overlaySpec() fkoverlay.Spec { return overlaySpec() }

// OverlaySpec exposes cc-pool's fusekit/overlay Spec to packages that drive the
// migration primitives directly.
func (m *Manager) OverlaySpec() fkoverlay.Spec { return overlaySpec() }

// OverlaySpec exposes cc-pool's fusekit/overlay Spec without a Manager, for
// callers that need the File Provider enablement gate before any pool exists —
// e.g. `ccp fp onboard --post-install`, which must not open or create pool state.
func OverlaySpec() fkoverlay.Spec { return overlaySpec() }

// OverlayProviderFor returns the fusekit/overlay provider for a stored backend,
// wired with cc-pool's Spec; a bad backend fails loud. Fuse providers come
// wrapped in the feature gate — this is the one choke point every real mount
// rides through.
func OverlayProviderFor(b fkoverlay.Backend) (fkoverlay.Provider, error) {
	prov, err := fkoverlay.ProviderFor(b, overlaySpec())
	if err != nil {
		return nil, err
	}
	if b.IsFuse() {
		return featureGate{Provider: prov, hello: holderHello, mounts: holderMounts}, nil
	}
	return prov, nil
}

// holderExe is the cask holder binary CanHostFuse stats; a var so tests can point
// it at an absent/present path.
var holderExe = mountd.HolderExe

// CanHostFuse reports whether this machine can host fuse mounts via the shared
// holder: the cask is installed or a holder already serves the socket.
func CanHostFuse() bool {
	if _, err := os.Stat(holderExe); err == nil {
		return true
	}
	return mountd.NewClient(mountd.DefaultHolderSocket()).Available()
}

// DetectOverlayBackend picks fuse via fusekit/overlay's Select when the shared
// holder's probe mount succeeds, else symlink. The probe runs in the holder (the
// macOS mount grant is per-process); a symlink verdict can leave an orphan holder.
func DetectOverlayBackend(ctx context.Context) (fkoverlay.Backend, string) {
	_, backend, reason, _ := fkoverlay.Select(ctx, overlaySpec())
	return backend, reason
}

// DetectFuseBackend runs Select's fuse→symlink ladder with the File Provider
// arm disabled. Gates asking "can this machine host fuse?" (the daemon's
// fuseGate) must use this, not DetectOverlayBackend: Select prefers File
// Provider whenever the extension is enabled, and an FP verdict says nothing
// about fuse — folding it in would refuse `ccp migrate --to fuse` on every
// FP-enabled machine.
func DetectFuseBackend(ctx context.Context) (fkoverlay.Backend, string) {
	spec := overlaySpec()
	spec.FileProvider = nil
	_, backend, reason, _ := fkoverlay.Select(ctx, spec)
	return backend, reason
}
