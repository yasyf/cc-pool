// Package mountd is the mount-holder subsystem: a tiny detached process that
// hosts fuse-t mounts so daemon restarts never disturb live claude sessions.
// The daemon and CLI drive it over its own 0600 unix socket using the
// newline-delimited JSON protocol defined here — one request and one response
// per connection, exactly like the daemon socket.
//
// The holder is stateless mechanism: no sqlite, no Keychain, no pool imports.
// Desired state lives elsewhere; the holder only mounts and unmounts what it
// is told, and reports kernel truth.
//
// Compatibility policy: proto-1 ops are FROZEN — their names, fields, and
// semantics never change. New capability means a new op or a new optional
// field, never a rename, repurpose, or retype of an existing one. Drivers
// treat ok=false with an "unknown op" error as not-supported, never as holder
// failure. Bidirectional version skew is routine and must keep working: an
// old daemon drives a new holder (brew upgrades swap the opt_bin binary under
// a running daemon), and a new daemon drives an old holder (the holder
// outlives daemon restarts by design).
package mountd

// MountProtoVersion stamps every request and response. It is bumped only on
// incompatible wire changes — which the compatibility policy above rules out,
// so in practice it stays 1.
const MountProtoVersion = 1

// Op is a request operation.
type Op string

const (
	OpHealth   Op = "health"   // liveness + version probe
	OpProbe    Op = "probe"    // throwaway in-process fuse capability mount
	OpMount    Op = "mount"    // ensure a live mirror of base at dir (a held-but-dead mirror is remounted)
	OpUnmount  Op = "unmount"  // unmount the mirror at dir
	OpList     Op = "list"     // snapshot the mounts this holder owns
	OpShutdown Op = "shutdown" // unmount everything, reply, exit
)

// Request is one client request (one JSON object per line). Base and Dir are
// required by mount AND unmount: Teardown refuses base==dir, so even a
// carcass unmount (a mountpoint this holder has no registry row for) needs
// the caller to name the base.
type Request struct {
	Proto int    `json:"proto"`
	Op    Op     `json:"op"`
	Base  string `json:"base,omitempty"`
	Dir   string `json:"dir,omitempty"`
}

// MountInfo is one mount in a list or shutdown response. Live is shallow
// kernel truth, not registry membership: dir is currently a mountpoint (the
// device-id check, overlay.Mounted) AND base's contents are visible through
// it (overlay.MountAlive). Either half alone can lie — a dead mirror exposes
// the underlying dir, whose leftover entries can shadow base's. Live does NOT
// reflect a partial wedge (shallow-alive but bulk reads hang): the holder
// ships no deep verdict — the daemon deep-probes through the kernel mount and
// keeps its own per-dir verdict. A lingering pre-upgrade holder may still send
// a "wedged" field; a current daemon ignores it (it uses its own probe), so
// the field is gone from the wire entirely.
type MountInfo struct {
	Dir  string `json:"dir"`
	Base string `json:"base"`
	Live bool   `json:"live"`
	// Epoch increments each time this holder (re)mounts Dir, starting at 1
	// for the holder's first mount of it. Zero means the holder predates the
	// field.
	Epoch uint64 `json:"epoch,omitempty"`
	// MountedAt is the unix-seconds timestamp of the current mount of Dir.
	// Zero means the holder predates the field.
	MountedAt int64 `json:"mounted_at,omitempty"`
}

// Error classes, sent alongside Error so drivers classify failures without
// string matching. The values are frozen; new failure modes get new classes,
// and a driver that predates a class treats it as retryable
// (ErrUnknownClass) — never as a mount failure, exactly as an unknown op
// reads as not-supported.
const (
	// ClassTCC: the mount was issued but never came live — almost always the
	// one-time macOS "Network Volumes" TCC grant. The Error text carries the
	// user-facing walkthrough.
	ClassTCC = "tcc"
	// ClassMountTimeout: the mount was issued but did not come live within
	// the holder's bounded wait, in a process whose "Network Volumes" grant
	// is already proven by an earlier live mount or probe — NOT the TCC
	// condition. Transient; drivers retry and never surface TCC guidance.
	// Drivers that predate this class degrade to ErrUnknownClass, which the
	// additive policy routes to retry as well.
	ClassMountTimeout = "mount-timeout"
	// ClassMountFailed: the mount failed outright.
	ClassMountFailed = "mount-failed"
	// ClassWedged: the unmount did not take; the dir is still a mountpoint
	// and must not be treated as torn down.
	ClassWedged = "wedged"
	// ClassForeignMount: dir is a mountpoint this holder does not own; the
	// caller must unmount it first — the holder never stacks mounts.
	ClassForeignMount = "foreign-mount"
	// ClassBusy: another operation is in flight on the same dir. Transient;
	// the caller may retry once it completes.
	ClassBusy = "busy"
	// ClassBaseMismatch: dir is already mounted by this holder but mirrors a
	// different base; the caller must unmount it first.
	ClassBaseMismatch = "base-mismatch"
)

// Response is one server reply (one JSON object per line).
type Response struct {
	Proto    int    `json:"proto"`
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
	ErrClass string `json:"err_class,omitempty"`
	Version  string `json:"version,omitempty"` // health
	FuseOK   bool   `json:"fuse_ok,omitempty"` // probe
	// Mounts: list returns every owned mount; shutdown returns only the dirs
	// that FAILED to unmount (empty means a clean sweep).
	Mounts []MountInfo `json:"mounts,omitempty"`
}
