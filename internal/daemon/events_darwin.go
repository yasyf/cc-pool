//go:build darwin

package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/yasyf/cc-pool/internal/overlay"
	"golang.org/x/sys/unix"
)

type fileIdentity struct {
	exists                    bool
	device                    int32
	inode                     uint64
	size                      int64
	modifiedSec, modifiedNano int64
	changedSec, changedNano   int64
}

type vnodeTarget struct {
	path     string
	cause    dirtyCause
	identity fileIdentity
	fd       int
}

func watchSemanticInputs(ctx context.Context, paths overlay.SemanticInputPaths, mark func(dirtyCause)) error {
	kq, err := unix.Kqueue()
	if err != nil {
		return fmt.Errorf("create kqueue: %w", err)
	}
	closeDone := make(chan struct{})
	stopClose := context.AfterFunc(ctx, func() {
		_ = unix.Close(kq)
		close(closeDone)
	})
	defer func() {
		if stopClose() {
			_ = unix.Close(kq)
			return
		}
		<-closeDone
	}()

	canonical := vnodeTarget{path: paths.Canonical, cause: dirtyCanonical, fd: -1}
	settings := vnodeTarget{path: paths.Settings, cause: dirtySettings, fd: -1}
	app := vnodeTarget{path: paths.AppBuild, cause: dirtyApp, fd: -1}
	canonical.identity, err = statIdentity(canonical.path)
	if err != nil {
		return err
	}
	settings.identity, err = statIdentity(settings.path)
	if err != nil {
		return err
	}
	app.identity, err = statIdentity(app.path)
	if err != nil {
		return err
	}
	watchers := map[int]string{}
	canonicalParentFD, err := addVnode(kq, paths.CanonicalParent)
	if err != nil {
		return fmt.Errorf("watch canonical parent %s: %w", paths.CanonicalParent, err)
	}
	defer func() { _ = unix.Close(canonicalParentFD) }()
	watchers[canonicalParentFD] = "canonical-parent"
	claudeDirFD, err := addVnode(kq, paths.ClaudeDir)
	if err != nil {
		return fmt.Errorf("watch claude dir %s: %w", paths.ClaudeDir, err)
	}
	defer func() { _ = unix.Close(claudeDirFD) }()
	watchers[claudeDirFD] = "claude-dir"
	appParentFD, err := addVnode(kq, paths.AppParent)
	if err != nil {
		return fmt.Errorf("watch app parent %s: %w", paths.AppParent, err)
	}
	defer func() { _ = unix.Close(appParentFD) }()
	watchers[appParentFD] = "app-parent"
	if err := rearmTarget(kq, &canonical, watchers); err != nil {
		return err
	}
	defer closeTarget(&canonical)
	if err := rearmTarget(kq, &settings, watchers); err != nil {
		return err
	}
	defer closeTarget(&settings)
	if err := rearmTarget(kq, &app, watchers); err != nil {
		return err
	}
	defer closeTarget(&app)
	// All parent and exact-file subscriptions are live before the catch-up mark,
	// so changes racing startup or a watcher restart cannot fall into a blind gap.
	mark(dirtyStartup)

	events := make([]unix.Kevent_t, 8)
	for {
		n, err := unix.Kevent(kq, nil, events, nil)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, unix.EBADF) {
				return nil
			}
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("wait for vnode event: %w", err)
		}
		for _, event := range events[:n] {
			role := watchers[int(event.Ident)] //nolint:gosec // Ident originated from a nonnegative file descriptor.
			switch role {
			case "canonical-parent":
				if err := checkParentTarget(kq, &canonical, watchers, mark); err != nil {
					return err
				}
			case "claude-dir":
				mark(dirtyStructure)
				if err := checkParentTarget(kq, &settings, watchers, mark); err != nil {
					return err
				}
			case "app-parent":
				if err := checkParentTarget(kq, &app, watchers, mark); err != nil {
					return err
				}
			case "canonical":
				mark(dirtyCanonical)
				if err := refreshExactTarget(kq, &canonical, watchers); err != nil {
					return err
				}
			case "settings":
				mark(dirtySettings)
				if err := refreshExactTarget(kq, &settings, watchers); err != nil {
					return err
				}
			case "app":
				mark(dirtyApp)
				if err := refreshExactTarget(kq, &app, watchers); err != nil {
					return err
				}
			}
		}
	}
}

func addVnode(kq int, path string) (int, error) {
	fd, err := unix.Open(path, unix.O_EVTONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	flags := uint32(unix.NOTE_WRITE | unix.NOTE_EXTEND | unix.NOTE_ATTRIB | unix.NOTE_RENAME | unix.NOTE_DELETE)
	change := unix.Kevent_t{Ident: uint64(fd), Filter: unix.EVFILT_VNODE, Flags: unix.EV_ADD | unix.EV_ENABLE | unix.EV_CLEAR, Fflags: flags} //nolint:gosec // fd is nonnegative after unix.Open succeeds.
	if _, err := unix.Kevent(kq, []unix.Kevent_t{change}, nil, nil); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

func rearmTarget(kq int, target *vnodeTarget, watchers map[int]string) error {
	closeTargetMapped(target, watchers)
	if !target.identity.exists {
		return nil
	}
	fd, err := addVnode(kq, target.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ENOENT) {
			target.identity.exists = false
			return nil
		}
		return fmt.Errorf("watch semantic input %s: %w", target.path, err)
	}
	target.fd = fd
	watchers[fd] = targetRole(target.cause)
	return nil
}

func targetRole(cause dirtyCause) string {
	switch cause {
	case dirtyCanonical:
		return "canonical"
	case dirtyApp:
		return "app"
	default:
		return "settings"
	}
}

func checkParentTarget(kq int, target *vnodeTarget, watchers map[int]string, mark func(dirtyCause)) error {
	identity, err := statIdentity(target.path)
	if err != nil {
		return err
	}
	if identity == target.identity {
		return nil
	}
	target.identity = identity
	mark(target.cause)
	return rearmTarget(kq, target, watchers)
}

func refreshExactTarget(kq int, target *vnodeTarget, watchers map[int]string) error {
	identity, err := statIdentity(target.path)
	if err != nil {
		return err
	}
	replaced := identity.exists != target.identity.exists || identity.device != target.identity.device || identity.inode != target.identity.inode
	target.identity = identity
	if replaced || !identity.exists {
		return rearmTarget(kq, target, watchers)
	}
	return nil
}

func statIdentity(path string) (fileIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Lstat(path, &stat); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return fileIdentity{}, nil
		}
		return fileIdentity{}, fmt.Errorf("lstat semantic input %s: %w", path, err)
	}
	return fileIdentity{
		exists: true, device: stat.Dev, inode: stat.Ino, size: stat.Size,
		modifiedSec: stat.Mtim.Sec, modifiedNano: stat.Mtim.Nsec,
		changedSec: stat.Ctim.Sec, changedNano: stat.Ctim.Nsec,
	}, nil
}

func closeTarget(target *vnodeTarget) {
	if target.fd >= 0 {
		_ = unix.Close(target.fd)
		target.fd = -1
	}
}

func closeTargetMapped(target *vnodeTarget, watchers map[int]string) {
	if target.fd >= 0 {
		delete(watchers, target.fd)
	}
	closeTarget(target)
}
