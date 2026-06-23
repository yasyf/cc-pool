//go:build fuse && cgo && darwin

package overlay

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"golang.org/x/sys/unix"
)

// sharedLinkInoBase is the first synthetic inode handed to a carve-out symlink.
// 1<<62 keeps the synthetic fileids clear of the real backing inode numbers the
// mirror serves for /.claude.json and the private entries, so the NFS client
// never aliases a symlink with a real object.
const sharedLinkInoBase = uint64(1) << 62

// sharedEntry is a precomputed live-symlink presentation for a shared top-level
// entry: the absolute base target and a synthetic S_IFLNK stat. Both are fixed
// for the mount's life — the carve-out targets are immutable links the kernel
// resolves OUTSIDE the mount — so Getattr/Readlink serve them with zero syscalls.
type sharedEntry struct {
	target string
	stat   fuse.Stat_t
}

// mirrorFS is a passthrough filesystem: most operations are applied directly to
// the corresponding path under root (~/.claude), so reads and writes are shared
// with plain `claude`. There is NO copy-up. The exception is the account-local
// PrivateEntry names (daemon/, ide/, backups/, .claude.json and its atomic-write
// temp files): their whole subtrees are redirected to a private per-account
// backing dir so concurrent sessions don't fight over one supervisor/IDE
// registry — matching the symlink provider's layout. Identity and the rest of
// ClaudeJSONPrivateKeys never land in the shared base; the SHAREABLE subset of
// .claude.json, however, flows both ways through cj: read opens of
// /.claude.json serve base's shareable keys merged over the private file, and
// committed writes split the shareable keys back through to the base sibling
// ~/.claude.json (see fuse_claudejson.go).
type mirrorFS struct {
	fuse.FileSystemBase
	root        string          // ~/.claude
	privateRoot string          // per-account backing for ExcludedEntries
	cj          *claudeJSONView // merged read view + base write-through for /.claude.json
	settings    *settingsView   // injected read view + base write-through for /settings.json
	probe       *probeView      // virtual read-only /.ccp-probe for deep wedge probing
	// sharedStat maps each shared top-level base name present at mount time to its
	// precomputed live-symlink presentation (absolute target + synthetic S_IFLNK
	// stat). Only these present as live symlinks into base (sharedLink), carving
	// claude's bulk transcript/history I/O out of fuse-t's NFS path. It is nil
	// until Setup snapshots it, so the method-level tests (which never mount) see
	// pure passthrough. Snapshotting at mount time — rather than carving any name
	// that merely exists in base — is what keeps a create-through-mount safe: were
	// a brand-new top-level file to flip to a symlink the instant CREATE lands it
	// in base, the kernel's immediate post-CREATE Getattr would race it into a
	// symlink and the in-flight NFS write would fail EIO. Entries born after the
	// mount stay passthrough; the next mount snapshots and carves them.
	sharedStat map[string]sharedEntry
}

// FusePassthroughOnly reports false: the mirror is NOT pure passthrough. It
// serves synthetic, handler-generated content keyed on fuse file handles — the
// merged /.claude.json (per-account identity), the injected /settings.json
// (plansDirectory), and the virtual /.ccp-probe — which fuse-t's FSKit backend
// does not honor (it ignores fi->fh and tears those reads, as a Phase-0 spike
// confirmed). So fusekit keeps fuse-t's NFS backend, which preserves fi->fh.
// See fusekit.PassthroughOnly.
func (fs *mirrorFS) FusePassthroughOnly() bool { return false }

func newMirrorFS(root, privateRoot, baseClaudeJSON string) *mirrorFS {
	absRoot, _ := filepath.Abs(root)
	absPriv, _ := filepath.Abs(privateRoot)
	absBase, _ := filepath.Abs(baseClaudeJSON)
	return &mirrorFS{
		root:        absRoot,
		privateRoot: absPriv,
		cj:          newClaudeJSONView(filepath.Join(absPriv, ".claude.json"), absBase),
		settings:    newSettingsView(filepath.Join(absRoot, "settings.json"), filepath.Join(absRoot, "plans")),
		probe:       newProbeView(),
	}
}

// real maps a fuse path ("/foo/bar") to its backing path: under privateRoot for
// a private top-level component, else under root.
func (fs *mirrorFS) real(path string) string {
	rel := filepath.FromSlash(path)
	if privateName(topComponent(path)) {
		return filepath.Join(fs.privateRoot, rel)
	}
	return filepath.Join(fs.root, rel)
}

// privateName reports whether a top-level name backs onto privateRoot:
// PrivateEntry names plus their AppleDouble "._<name>" sidecars. A sidecar
// must colocate with its parent — xnu writes "._<name>" beside "<name>" when
// setxattr fails ENOTSUP (pre-namedattr mounts, or any fuse-t named-attribute
// regression), and routing a private file's sidecar into the shared base
// litters it with orphans once claude's tmp→rename commit moves on. TrimPrefix
// on a non-sidecar name is a no-op, so normal names route exactly as before.
// The ONE predicate is shared by real and Readdir's private-merge filter so
// the two can never disagree about where a name lives.
func privateName(name string) bool {
	return PrivateEntry(strings.TrimPrefix(name, "._"))
}

// topComponent returns the first path component of a fuse path ("/daemon/x" -> "daemon").
func topComponent(path string) string {
	p := strings.TrimPrefix(path, "/")
	if i := strings.IndexByte(p, '/'); i >= 0 {
		p = p[:i]
	}
	return p
}

// isTopLevel reports whether path names exactly one component ("/projects" yes,
// "/projects/p.json" and "/" no).
func isTopLevel(path string) bool {
	p := strings.TrimPrefix(path, "/")
	return p != "" && !strings.ContainsRune(p, '/')
}

// snapshotShared records the top-level names present in base at mount time and
// precomputes each one's live-symlink presentation (target + synthetic S_IFLNK
// stat); only these present as live symlinks (see sharedLink and the sharedStat
// field). Called once from Setup before the mount serves, so no client op can
// race the snapshot. A read failure leaves the map nil (pure passthrough) rather
// than guessing. The private names, the merged /.claude.json, the injected
// /settings.json, and the virtual probe are excluded here once, so the per-call
// sharedLink needs no syscall and no re-check.
func (fs *mirrorFS) snapshotShared() {
	entries, err := os.ReadDir(fs.root)
	if err != nil {
		return
	}
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid()) //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
	now := time.Now()
	ts := fuse.Timespec{Sec: now.Unix(), Nsec: int64(now.Nanosecond())}
	ino := sharedLinkInoBase
	fs.sharedStat = make(map[string]sharedEntry, len(entries))
	for _, e := range entries {
		name := e.Name()
		if privateName(name) || "/"+name == claudeJSONFusePath || "/"+name == settingsJSONFusePath || name == ProbeFileName {
			continue
		}
		target := filepath.Join(fs.root, name)
		fs.sharedStat[name] = sharedEntry{
			target: target,
			stat: fuse.Stat_t{
				Ino:      ino,
				Mode:     fuse.S_IFLNK | 0o777,
				Nlink:    1,
				Uid:      uid,
				Gid:      gid,
				Size:     int64(len(target)),
				Atim:     ts,
				Mtim:     ts,
				Ctim:     ts,
				Birthtim: ts,
			},
		}
		ino++
	}
}

// sharedEntryFor returns the precomputed live-symlink presentation for a shared
// top-level entry, or false for everything else (nested paths, private names,
// /.claude.json, /settings.json, the probe, and any name not present in base at
// mount time).
// It is a pure map lookup: snapshotShared already applied the exclusions and
// captured the target + synthetic stat, so no syscall and no re-check is needed.
// A target deleted from base after the mount is not pruned here — the entry then
// presents as a dangling symlink (the kernel resolves it OUTSIDE the mount and
// gets ENOENT there), exactly as a real on-disk symlink to a deleted file would.
func (fs *mirrorFS) sharedEntryFor(path string) (sharedEntry, bool) {
	if !isTopLevel(path) {
		return sharedEntry{}, false
	}
	e, ok := fs.sharedStat[strings.TrimPrefix(path, "/")]
	return e, ok
}

// sharedLink reports whether path is a shared top-level entry the mirror should
// present as a LIVE SYMLINK into base, returning the absolute base target. The
// shared entries (projects/, history, todos/, shell-snapshots/, statsig/, …) are
// pure passthrough — presenting them as symlinks lets the kernel resolve them
// OUTSIDE the mount, so all of claude's bulk transcript and history I/O bypasses
// fuse-t's chunked NFS layer entirely, exactly as the on-disk symlink provider
// already does. The mirror keeps doing real work only for the carve-outs this
// excludes: /.claude.json (merged read + write-through), /settings.json (injected
// read + write-through), private names (privateRoot redirect) and /.ccp-probe
// (virtual). Only names in the mount-time snapshot (fs.sharedStat) qualify — an
// entry born after the mount stays passthrough so a create-through-mount never
// flips type mid-write; its content is still served live through the symlink once
// a later mount carves it.
func (fs *mirrorFS) sharedLink(path string) (string, bool) {
	e, ok := fs.sharedEntryFor(path)
	return e.target, ok
}

func errno(err error) int {
	if err == nil {
		return 0
	}
	var e syscall.Errno
	if errors.As(err, &e) {
		return -int(e)
	}
	return -int(syscall.EIO)
}

func (fs *mirrorFS) Statfs(path string, stat *fuse.Statfs_t) int {
	if path == probeFusePath {
		// The virtual probe lives on root's filesystem: answer with root's
		// stats instead of ENOENT (no shadow entry) or a passthrough to a
		// shadowed base entry's path.
		path = "/"
	}
	var s syscall.Statfs_t
	if err := syscall.Statfs(fs.real(path), &s); err != nil {
		return errno(err)
	}
	stat.Bsize = uint64(s.Bsize)
	stat.Frsize = uint64(s.Bsize)
	stat.Blocks = s.Blocks
	stat.Bfree = s.Bfree
	stat.Bavail = s.Bavail
	stat.Files = s.Files
	stat.Ffree = s.Ffree
	stat.Namemax = 255
	return 0
}

func (fs *mirrorFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	if probeFh(fh) || path == probeFusePath {
		// Probe interception precedes the real() mapping everywhere: the file
		// is purely virtual and shadows any real base entry of the same name.
		return fs.probe.getattr(stat)
	}
	if settingsFh(fh) {
		// Settings synthetic handles share the merged-view range, so route them
		// BEFORE cj (see settingsFhBase).
		return fs.settings.getattrSnapshot(fh, stat)
	}
	if syntheticFh(fh) {
		return fs.cj.getattrSnapshot(fh, stat)
	}
	var st syscall.Stat_t
	if fh != ^uint64(0) {
		// A real handle at /.claude.json is a write handle (read opens are
		// synthetic): raw Fstat, no merged-view override.
		if err := syscall.Fstat(int(fh), &st); err != nil { //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
			return errno(err)
		}
		copyStat(stat, &st)
		return 0
	}
	if e, ok := fs.sharedEntryFor(path); ok {
		// A shared top-level entry presents as a live symlink to the absolute base
		// path; the kernel follows it OUTSIDE the mount, so the carved bulk I/O
		// never traverses fuse-t's NFS layer (see sharedLink). The synthetic stat
		// is precomputed at mount time (snapshotShared), so this costs no syscall.
		*stat = e.stat
		return 0
	}
	if err := syscall.Lstat(fs.real(path), &st); err != nil {
		return errno(err)
	}
	copyStat(stat, &st)
	if path == claudeJSONFusePath {
		return fs.cj.overrideMergedAttr(stat)
	}
	if path == settingsJSONFusePath {
		return fs.settings.overrideMergedAttr(stat)
	}
	return 0
}

func (fs *mirrorFS) Open(path string, flags int) (int, uint64) {
	if path == probeFusePath {
		return fs.probe.open(flags)
	}
	if path == claudeJSONFusePath && flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		return fs.cj.openSnapshot()
	}
	if path == settingsJSONFusePath && flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		return fs.settings.openSnapshot()
	}
	fd, err := syscall.Open(fs.real(path), flags, 0)
	if err != nil {
		return errno(err), ^uint64(0)
	}
	if path == settingsJSONFusePath {
		// A writable open of the shared settings.json reads/writes base directly
		// (no private redirect). Mark the real fd dirty at open: unlike cj, the
		// settings write-through only ever STRIPS our own injected key by
		// value-equality (it never copies stale content across), so a write-through
		// scheduled by an open that turns out not to write is a harmless no-op
		// (writeThroughBase's bytes.Equal short-circuit). The baseline guarantees
		// no write path (an O_TRUNC at open, an in-place rewrite) can leave an
		// injected plansDirectory committed into base.
		fs.settings.markDirty(uint64(fd)) //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
	}
	return 0, uint64(fd) //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
}

func (fs *mirrorFS) Create(path string, flags int, mode uint32) (int, uint64) {
	if path == probeFusePath {
		return -int(syscall.EPERM), ^uint64(0)
	}
	fd, err := syscall.Open(fs.real(path), flags|syscall.O_CREAT, mode)
	if err != nil {
		return errno(err), ^uint64(0)
	}
	return 0, uint64(fd) //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
}

func (fs *mirrorFS) Read(_ string, buff []byte, ofst int64, fh uint64) int {
	if probeFh(fh) {
		return fs.probe.read(fh, buff, ofst)
	}
	if settingsFh(fh) {
		return fs.settings.readSnapshot(fh, buff, ofst)
	}
	if syntheticFh(fh) {
		return fs.cj.readSnapshot(fh, buff, ofst)
	}
	n, err := syscall.Pread(int(fh), buff, ofst) //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
	if err != nil {
		return errno(err)
	}
	return n
}

func (fs *mirrorFS) Write(path string, buff []byte, ofst int64, fh uint64) int {
	if probeFh(fh) {
		// Probe handles are read-only; without this guard the huge handle ID
		// would be passed to pwrite as a bogus fd int.
		return -int(syscall.EBADF)
	}
	if syntheticFh(fh) {
		// Synthetic merged-view handles (cj AND settings) are read-only; without
		// this guard the huge handle ID would be passed to pwrite as a bogus fd
		// int. settingsFh ⊂ syntheticFh, so this one check covers both.
		return -int(syscall.EBADF)
	}
	n, err := syscall.Pwrite(int(fh), buff, ofst) //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
	if err != nil {
		return errno(err)
	}
	if path == claudeJSONFusePath {
		fs.cj.markDirty(fh)
	}
	if path == settingsJSONFusePath {
		fs.settings.markDirty(fh)
	}
	return n
}

func (fs *mirrorFS) Truncate(path string, size int64, fh uint64) int {
	if path == probeFusePath || probeFh(fh) {
		return -int(syscall.EPERM)
	}
	if syntheticFh(fh) {
		// Synthetic merged-view handles (cj AND settings) are read-only; without
		// this guard the huge handle ID would be passed to ftruncate as a bogus fd
		// int. settingsFh ⊂ syntheticFh, so this one check covers both.
		return -int(syscall.EINVAL)
	}
	var err error
	if fh != ^uint64(0) {
		err = syscall.Ftruncate(int(fh), size) //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
		if err == nil && path == claudeJSONFusePath {
			fs.cj.markDirty(fh)
		}
		if err == nil && path == settingsJSONFusePath {
			fs.settings.markDirty(fh)
		}
	} else {
		err = syscall.Truncate(fs.real(path), size)
	}
	return errno(err)
}

func (fs *mirrorFS) Fsync(_ string, _ bool, fh uint64) int {
	if probeFh(fh) || syntheticFh(fh) {
		return 0 // probe bytes and merged snapshots are memory; nothing to sync
	}
	return errno(syscall.Fsync(int(fh))) //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
}

func (fs *mirrorFS) Release(path string, fh uint64) int {
	if probeFh(fh) {
		fs.probe.release(fh)
		return 0
	}
	if settingsFh(fh) {
		// Settings synthetic handles share the merged-view range, so route them
		// BEFORE cj (see settingsFhBase).
		fs.settings.closeSnapshot(fh)
		return 0
	}
	if syntheticFh(fh) {
		fs.cj.closeSnapshot(fh)
		return 0
	}
	st := errno(syscall.Close(int(fh))) //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
	if path == claudeJSONFusePath && fs.cj.takeDirty(fh) {
		// The fd actually wrote into the private /.claude.json (an in-place
		// commit): propagate the shareable keys to base after the close. A
		// write-capable fd that never wrote stays clean — write-through would
		// push possibly-stale private shareable keys over a newer base. The
		// propagation runs off this handler (scheduleWriteThrough) so the close
		// never blocks on base I/O and stalls the mount.
		fs.cj.scheduleWriteThrough()
	}
	if path == settingsJSONFusePath && fs.settings.takeDirty(fh) {
		// A writable settings.json fd is closing: strip our injected
		// plansDirectory back out of base so the real file stays pristine. The fd
		// is marked dirty at open (writable opens read/write base directly), so
		// every writable close runs the strip; it is a no-op when nothing was
		// injected (writeThroughBase's bytes.Equal short-circuit). Off this
		// handler (scheduleWriteThrough) so the close never blocks on base I/O.
		fs.settings.scheduleWriteThrough()
	}
	return st
}

func (fs *mirrorFS) Opendir(path string) (int, uint64) {
	if path == probeFusePath {
		// The virtual probe is a regular file (and unreachable here while
		// Getattr says so); never open a shadowed base directory through it.
		return -int(syscall.ENOTDIR), ^uint64(0)
	}
	fd, err := syscall.Open(fs.real(path), syscall.O_RDONLY, 0)
	if err != nil {
		return errno(err), ^uint64(0)
	}
	return 0, uint64(fd) //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
}

func (fs *mirrorFS) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, _ int64, _ uint64) int {
	dir, err := os.Open(fs.real(path))
	if err != nil {
		return errno(err)
	}
	defer func() { _ = dir.Close() }()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return errno(err)
	}
	// Invariant: entries are filled with nil stats so the kernel issues a
	// per-name Getattr — the path-based Getattr is where /.claude.json's
	// merged size/mtime override applies. Filling real stats here would
	// bypass it and pin the private file's raw size.
	fill(".", nil, 0)
	fill("..", nil, 0)
	seen := map[string]bool{}
	for _, name := range names {
		seen[name] = true
		if path == "/" && name == ProbeFileName {
			// A shadowed base .ccp-probe must not surface its NAME either:
			// the probe is purely virtual and never listed (its content and
			// attrs are already shadowed by the path intercepts). Root only —
			// a nested .ccp-probe is an ordinary real file.
			continue
		}
		if !fill(name, nil, 0) {
			return 0
		}
	}
	if path == "/" {
		// Private files (e.g. a seeded .claude.json) live only in privateRoot;
		// merge them into the root listing or they'd be stat-able but unlisted.
		priv, err := os.ReadDir(fs.privateRoot)
		if err != nil {
			return 0 // listing base succeeded; a missing private root is not an error
		}
		for _, e := range priv {
			if seen[e.Name()] || !privateName(e.Name()) {
				continue
			}
			if !fill(e.Name(), nil, 0) {
				return 0
			}
		}
	}
	return 0
}

func (fs *mirrorFS) Releasedir(_ string, fh uint64) int {
	return errno(syscall.Close(int(fh))) //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
}

func (fs *mirrorFS) Mkdir(path string, mode uint32) int {
	if path == probeFusePath {
		return -int(syscall.EPERM)
	}
	return errno(syscall.Mkdir(fs.real(path), mode))
}

func (fs *mirrorFS) Unlink(path string) int {
	if path == probeFusePath {
		return -int(syscall.EPERM)
	}
	return errno(syscall.Unlink(fs.real(path)))
}

func (fs *mirrorFS) Rmdir(path string) int {
	if path == probeFusePath {
		// Unreachable today (Getattr presents S_IFREG, so the client VFS fails
		// rmdir with ENOTDIR first), but guarded like every other mutation op:
		// a shadowed base DIRECTORY named .ccp-probe must never be deletable
		// through the probe name.
		return -int(syscall.EPERM)
	}
	return errno(syscall.Rmdir(fs.real(path)))
}

func (fs *mirrorFS) Link(oldpath string, newpath string) int {
	if oldpath == probeFusePath || newpath == probeFusePath {
		// Either side would reach the backing dir's real .ccp-probe path; with
		// a shadowed base entry present, the oldpath side would hardlink that
		// entry to a new name inside base. Same both-sides posture as Rename.
		return -int(syscall.EPERM)
	}
	return errno(syscall.Link(fs.real(oldpath), fs.real(newpath)))
}

func (fs *mirrorFS) Symlink(target string, newpath string) int {
	if newpath == probeFusePath {
		return -int(syscall.EPERM)
	}
	return errno(syscall.Symlink(target, fs.real(newpath)))
}

func (fs *mirrorFS) Readlink(path string) (int, string) {
	if path == probeFusePath {
		// The virtual probe is a regular file; answering here keeps a shadowed
		// base symlink's target from leaking through the probe name.
		return -int(syscall.EINVAL), ""
	}
	if target, ok := fs.sharedLink(path); ok {
		// The live symlink Getattr synthesized for a shared top-level entry: its
		// target is the absolute base path, which the kernel resolves outside the
		// mount (see sharedLink).
		return 0, target
	}
	buf := make([]byte, 4096)
	n, err := syscall.Readlink(fs.real(path), buf)
	if err != nil {
		return errno(err), ""
	}
	return 0, string(buf[:n])
}

func (fs *mirrorFS) Rename(oldpath string, newpath string) int {
	if oldpath == probeFusePath || newpath == probeFusePath {
		// Renaming the virtual probe away, or a real file onto it, would reach
		// the backing dir's real .ccp-probe path — never touched for probing.
		return -int(syscall.EPERM)
	}
	st := errno(syscall.Rename(fs.real(oldpath), fs.real(newpath)))
	if st == 0 && newpath == claudeJSONFusePath {
		// claude's atomic save (tmp + rename) just committed the private file;
		// propagate its shareable keys to base. The private rename's status is
		// ALWAYS returned — the commit durably happened, so a write-through
		// failure must not fail the save; it goes sticky and surfaces via
		// Health. Propagation runs off this handler (scheduleWriteThrough) so
		// the rename never blocks on base I/O and stalls the mount.
		fs.cj.scheduleWriteThrough()
	}
	if st == 0 && newpath == settingsJSONFusePath {
		// claude's atomic save (tmp + rename) just committed settings.json into
		// base; strip our injected plansDirectory back out so the real file stays
		// pristine. The rename's status is ALWAYS returned — the commit durably
		// happened, so a write-through failure must not fail the save; it goes
		// sticky and surfaces via Health. Off this handler (scheduleWriteThrough)
		// so the rename never blocks on base I/O and stalls the mount.
		fs.settings.scheduleWriteThrough()
	}
	return st
}

func (fs *mirrorFS) Chmod(path string, mode uint32) int {
	if path == probeFusePath {
		return -int(syscall.EPERM)
	}
	return errno(syscall.Chmod(fs.real(path), mode))
}

func (fs *mirrorFS) Chown(path string, uid uint32, gid uint32) int {
	if path == probeFusePath {
		return -int(syscall.EPERM)
	}
	return errno(syscall.Lchown(fs.real(path), int(uid), int(gid)))
}

func (fs *mirrorFS) Utimens(path string, tmsp []fuse.Timespec) int {
	if path == probeFusePath {
		return -int(syscall.EPERM)
	}
	if len(tmsp) < 2 {
		return errno(syscall.EINVAL)
	}
	tv := []syscall.Timeval{
		{Sec: tmsp[0].Sec, Usec: int32(tmsp[0].Nsec / 1000)}, //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
		{Sec: tmsp[1].Sec, Usec: int32(tmsp[1].Nsec / 1000)}, //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
	}
	return errno(syscall.Utimes(fs.real(path), tv))
}

// The xattr ops pass through to fs.real(path) via x/sys/unix's L-variants —
// never following symlinks, matching the mirror's Lstat/Readlink posture. They
// exist because the mount runs with namedattr: implementing them (rather than
// inheriting FileSystemBase's -ENOSYS) is what keeps xnu's AppleDouble
// fallback from littering ._ sidecars, and fuse-t requires Listxattr or
// Getxattr fails outright (fuse-t issue #62). /.claude.json gets no merged-view
// carve-out: the merge covers file CONTENT only, so its xattrs live on the
// private backing file like any other private path. /.ccp-probe, by contrast,
// IS carved out like its other ops: it is answered virtually (no xattrs;
// mutations EPERM) so a shadowed base entry can never be read or modified
// through the probe name.

// Setxattr passes through, translating ENOTSUP to EPERM: ENOTSUP from setxattr
// is the one status that trips xnu's AppleDouble ._ fallback — the exact bug
// this layer exists to prevent — while EPERM refuses cleanly. Every other
// error (notably EPERM for the SIP-protected com.apple.provenance) passes
// through unchanged.
func (fs *mirrorFS) Setxattr(path string, name string, value []byte, flags int) int {
	if path == probeFusePath {
		return -int(syscall.EPERM)
	}
	return setxattrErrno(unix.Lsetxattr(fs.real(path), name, value, flags))
}

// setxattrErrno maps a setxattr passthrough error to the fuse status,
// translating ENOTSUP to EPERM (see Setxattr). Split from Setxattr so the
// translation is testable without a filesystem that rejects xattrs.
func setxattrErrno(err error) int {
	if errors.Is(err, unix.ENOTSUP) {
		return -int(syscall.EPERM)
	}
	return errno(err)
}

func (fs *mirrorFS) Getxattr(path string, name string) (int, []byte) {
	if path == probeFusePath {
		return -int(syscall.ENOATTR), nil // the virtual probe has no xattrs
	}
	backing := fs.real(path)
	for {
		sz, err := unix.Lgetxattr(backing, name, nil)
		if err != nil {
			return errno(err), nil
		}
		if sz == 0 {
			return 0, []byte{}
		}
		buf := make([]byte, sz)
		n, err := unix.Lgetxattr(backing, name, buf)
		if errors.Is(err, unix.ERANGE) {
			continue // grew between the size probe and the fetch; re-probe
		}
		if err != nil {
			return errno(err), nil
		}
		return 0, buf[:n]
	}
}

func (fs *mirrorFS) Listxattr(path string, fill func(name string) bool) int {
	if path == probeFusePath {
		return 0 // the virtual probe has no xattrs
	}
	backing := fs.real(path)
	var buf []byte
	for {
		sz, err := unix.Llistxattr(backing, nil)
		if err != nil {
			return errno(err)
		}
		if sz == 0 {
			return 0
		}
		buf = make([]byte, sz)
		n, err := unix.Llistxattr(backing, buf)
		if errors.Is(err, unix.ERANGE) {
			continue // grew between the size probe and the fetch; re-probe
		}
		if err != nil {
			return errno(err)
		}
		buf = buf[:n]
		break
	}
	for _, name := range strings.Split(string(buf), "\x00") {
		if name == "" {
			continue // trailing NUL terminator
		}
		if !fill(name) {
			return 0
		}
	}
	return 0
}

func (fs *mirrorFS) Removexattr(path string, name string) int {
	if path == probeFusePath {
		return -int(syscall.EPERM)
	}
	return errno(unix.Lremovexattr(fs.real(path), name))
}

// copyStat converts a Go syscall.Stat_t (darwin) into a fuse.Stat_t.
func copyStat(dst *fuse.Stat_t, src *syscall.Stat_t) {
	dst.Dev = uint64(src.Dev) //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
	dst.Ino = uint64(src.Ino)
	dst.Mode = uint32(src.Mode)
	dst.Nlink = uint32(src.Nlink)
	dst.Uid = src.Uid
	dst.Gid = src.Gid
	dst.Rdev = uint64(src.Rdev) //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
	dst.Size = src.Size
	dst.Atim = fuse.Timespec{Sec: src.Atimespec.Sec, Nsec: src.Atimespec.Nsec}
	dst.Mtim = fuse.Timespec{Sec: src.Mtimespec.Sec, Nsec: src.Mtimespec.Nsec}
	dst.Ctim = fuse.Timespec{Sec: src.Ctimespec.Sec, Nsec: src.Ctimespec.Nsec}
	dst.Birthtim = fuse.Timespec{Sec: src.Birthtimespec.Sec, Nsec: src.Birthtimespec.Nsec}
	dst.Blksize = int64(src.Blksize)
	dst.Blocks = src.Blocks
	dst.Flags = src.Flags
}
