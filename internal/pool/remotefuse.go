package pool

import (
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/mountd"
)

// remoteFuse adapts fusekit's mountd.RemoteHost to cc-pool's overlay.Provider.
// RemoteHost is the wire/lifecycle half of the old internal/mountd
// RemoteProvider — Setup/Teardown/Sync/Health, all inherited here via embedding
// — and this adapter adds only the consumer-specific Kind and PrivateRoot. It
// drives the detached mount holder over its socket, so the mirrors outlive the
// daemon and CLI processes that ask for them, and it compiles in every build
// variant (a running holder is usable by any build; only the spawn path needs
// the fuse build).
type remoteFuse struct {
	*mountd.RemoteHost
}

var _ overlay.Provider = remoteFuse{}

// Kind always reports KindFuse: the adapter stands in for the fuse overlay
// regardless of whether this build could host the mounts itself, so every
// stored-kind fence keyed on KindFuse stays honest.
func (remoteFuse) Kind() overlay.Kind { return overlay.KindFuse }

// PrivateRoot returns the fuse provider's per-account private backing dir.
func (remoteFuse) PrivateRoot(accountDir string) string {
	return overlay.FusePrivateRoot(accountDir)
}

// newRemoteFuse builds the holder-backed fuse provider for the pool's mount
// socket and holder log, carrying cc-pool's holder argv and pure-build brew
// hint. SpawnTimeout is left zero (DefaultSpawnTimeout).
func newRemoteFuse() remoteFuse {
	return remoteFuse{&mountd.RemoteHost{
		Socket:         MountsSocketPath(),
		LogPath:        MountHolderLogPath(),
		Args:           []string{"mount-holder", "--socket", MountsSocketPath()},
		CannotHostHint: cannotHostHint,
	}}
}
