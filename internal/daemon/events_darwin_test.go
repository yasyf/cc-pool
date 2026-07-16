//go:build darwin

package daemon

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestParentEventMarksOnlyWhenExactAppBuildChanges(t *testing.T) {
	parent := t.TempDir()
	appBuild := filepath.Join(parent, "CCPoolStatus.app", "Contents", "Info.plist")
	if err := os.MkdirAll(filepath.Dir(appBuild), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(appBuild, []byte("build-1"), 0o600); err != nil {
		t.Fatal(err)
	}
	identity, err := statIdentity(appBuild)
	if err != nil {
		t.Fatal(err)
	}
	target := vnodeTarget{path: appBuild, cause: dirtyApp, identity: identity, fd: -1}
	kq, err := unix.Kqueue()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(kq) })
	watchers := map[int]string{}
	if err := rearmTarget(kq, &target, watchers); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTarget(&target) })
	marks := 0
	mark := func(cause dirtyCause) {
		if cause != dirtyApp {
			t.Fatalf("cause = %v, want dirtyApp", cause)
		}
		marks++
	}

	if err := os.WriteFile(filepath.Join(parent, "unrelated"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkParentTarget(kq, &target, watchers, mark); err != nil {
		t.Fatal(err)
	}
	if marks != 0 {
		t.Fatalf("unrelated parent churn marked app dirty %d times", marks)
	}

	replacement := filepath.Join(parent, "replacement.plist")
	if err := os.WriteFile(replacement, []byte("build-2"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, appBuild); err != nil {
		t.Fatal(err)
	}
	if err := checkParentTarget(kq, &target, watchers, mark); err != nil {
		t.Fatal(err)
	}
	if marks != 1 {
		t.Fatalf("app build replacement marked dirty %d times, want 1", marks)
	}
}
