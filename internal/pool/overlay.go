package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"golang.org/x/mod/semver"
)

// HolderOwner tags cc-pool's mounts on the shared fusekit-holder so the daemon
// reclaims only its own.
const HolderOwner = "cc-pool"

// cannotHostHint is appended to mountd.ErrCannotHost when the holder cask is absent.
const cannotHostHint = "run `ccp fuse enable` to install the fusekit-holder cask"

// MinHolderVersion is the oldest fusekit-holder release cc-pool will host on.
// It carries the NFS kernel-panic mitigations (no namedattr, AppleDouble "._"
// blocking, attribute stabilization) AND the single-mount mux surface
// (HolderSpec.MuxRoot / Request.mux_root): an older holder silently ignores
// mux_root and would mount each account dir per-dir, so it is the fail-loud gate
// against a pre-mux holder. Hosting on an older holder is refused (routed to
// healDeferredUnmitigated) until `brew upgrade --cask fusekit-holder`.
const MinHolderVersion = "v0.29.0"

// HolderVersionMitigated reports whether a holder's reported version carries
// the kernel-panic mitigations (>= MinHolderVersion). The holder reports
// fusekit's version.String(), which may append a commit ("v0.23.0 (abc1234)");
// only the first field is compared. "dev", empty, or otherwise unparseable
// versions pass: a locally-built holder is current-source, therefore mitigated.
func HolderVersionMitigated(version string) bool {
	v, _, _ := strings.Cut(strings.TrimSpace(version), " ")
	if !semver.IsValid(v) {
		return true
	}
	return semver.Compare(v, MinHolderVersion) >= 0
}

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
// commit paren is dropped like HolderVersionMitigated. "dev", empty, or
// otherwise unparseable versions pass: a locally-built app is current-source.
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

// ErrHolderUnmitigated means the shared mount holder predates MinHolderVersion:
// hosting fuse mirrors on it can panic macOS (nfs_vinvalbuf2), so every fuse
// mount entry point refuses it until the cask is upgraded.
var ErrHolderUnmitigated = errors.New("mount holder predates the NFS kernel-panic mitigations")

// holderHealth reports the shared holder's version via its Health RPC; a var so
// tests can fake an old, current, or absent holder.
var holderHealth = func() (string, error) {
	return mountd.NewClient(mountd.DefaultHolderSocket()).Health()
}

// mitigationGate wraps the remote fuse provider so every fuse mount entry point
// (`ccp add`/`ccp init`, migrate's conversion, the daemon's heal loop) refuses
// to host mirrors on a holder that predates the NFS kernel-panic mitigations —
// not just handleMigrate's fuseGate. Only Setup is gated: Teardown, Health, and
// Sync must keep working against any holder.
type mitigationGate struct {
	fkoverlay.Provider
	health func() (string, error)
}

// Setup refuses before mounting when a serving holder reports an unmitigated
// version, and re-verifies after mounting — Setup may have just spawned the
// holder from an old cask binary, and its first Health answer is the earliest
// the version is observable. A mount that cannot be vouched mitigated is torn
// down (gracefully; it is seconds old, nothing rides it yet) before anything
// writes through it.
func (g mitigationGate) Setup(base, dir string) error {
	if ver, err := g.health(); err == nil && !HolderVersionMitigated(ver) {
		return unmitigatedHolderError(ver)
	}
	if err := g.Provider.Setup(base, dir); err != nil {
		return err
	}
	var cause error
	switch ver, err := g.health(); {
	case err != nil:
		// The holder answered the mount RPC moments ago; an unanswerable Health
		// now means it died — the fresh mount cannot be vouched mitigated.
		cause = fmt.Errorf("verify holder mitigations after mount: %w", err)
	case HolderVersionMitigated(ver):
		return nil
	default:
		cause = unmitigatedHolderError(ver)
	}
	if terr := g.Teardown(base, dir); terr != nil {
		// terr rides as text (terr.Error(), not %w): the failure class callers
		// match on (errors.Is) must stay the cause, never the teardown's wire error.
		return fmt.Errorf("%w (and tearing the unverified fresh mount down failed: %s)", cause, terr.Error())
	}
	return cause
}

func unmitigatedHolderError(ver string) error {
	return fmt.Errorf("%w: holder %s needs %s or newer; run `brew upgrade --cask fusekit-holder`", ErrHolderUnmitigated, ver, MinHolderVersion)
}

// overlaySpec builds cc-pool's fusekit/overlay Spec. PassthroughOnly is false
// because cc-pool serves synthetic content (the merged /.claude.json), forcing
// fuse-t's NFS backend.
func overlaySpec() fkoverlay.Spec {
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
			// tears down another tenant's mounts (the cask's launchd owns holder
			// upgrades).
			BridgeSocket:    BridgeSocketPath(),
			ContentMode:     "source",
			ProbePath:       "/" + overlay.ProbeFileName,
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
// wrapped in the mitigation gate — this is the one choke point every real
// mount rides through.
func OverlayProviderFor(b fkoverlay.Backend) (fkoverlay.Provider, error) {
	prov, err := fkoverlay.ProviderFor(b, overlaySpec())
	if err != nil {
		return nil, err
	}
	if b.IsFuse() {
		return mitigationGate{Provider: prov, health: holderHealth}, nil
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
