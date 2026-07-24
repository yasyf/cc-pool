package pool

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
)

const (
	firstTestInstance  = "00112233445566778899aabbccddeeff"
	secondTestInstance = "ffeeddccbbaa99887766554433221100"
)

func TestAccountConfigIdentityIsInstanceStable(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	first, err := AccountConfigDir(firstTestInstance)
	if err != nil {
		t.Fatal(err)
	}
	second, err := AccountConfigDir(secondTestInstance)
	if err != nil {
		t.Fatal(err)
	}
	if first != filepath.Join(home, ".cc-pool", "config", firstTestInstance) || first == second {
		t.Fatalf("unexpected stable paths: first=%q second=%q", first, second)
	}
	service, err := AccountKeychainService(firstTestInstance)
	if err != nil {
		t.Fatal(err)
	}
	if service != creds.ServiceName(first) {
		t.Fatalf("service = %q, want Claude path-derived %q", service, creds.ServiceName(first))
	}
}

func TestAccountConfigIdentityRejectsNonCanonicalInstanceID(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	for _, instanceID := range []string{"", "abc", "00112233445566778899AABBCCDDEEFF", "00112233445566778899aabbccddeefg"} {
		if _, err := AccountConfigDir(instanceID); err == nil {
			t.Fatalf("AccountConfigDir(%q) succeeded", instanceID)
		}
	}
}

func TestAccountConfigLinkCreateRetargetAndRemove(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	first := filepath.Join(home, "Library", "CloudStorage", "First")
	second := filepath.Join(home, "Library", "CloudStorage", "Second")
	if err := EnsureAccountConfigDir(firstTestInstance, first); err != nil {
		t.Fatal(err)
	}
	link, _ := AccountConfigDir(firstTestInstance)
	assertLinkTarget(t, link, first)
	if err := EnsureAccountConfigDir(firstTestInstance, first); err != nil {
		t.Fatalf("idempotent ensure: %v", err)
	}
	if err := RetargetAccountConfigDir(firstTestInstance, first, second); err != nil {
		t.Fatal(err)
	}
	assertLinkTarget(t, link, second)
	if err := RemoveAccountConfigDir(firstTestInstance, second); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(link); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired link still exists: %v", err)
	}
	if err := RemoveAccountConfigDir(firstTestInstance, second); err != nil {
		t.Fatalf("idempotent remove: %v", err)
	}
}

func TestAccountConfigLinkRemovalPreservesTarget(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	target := filepath.Join(home, "Library", "CloudStorage", "Target")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(target, "marker")
	if err := os.WriteFile(marker, []byte("preserved"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := EnsureAccountConfigDir(firstTestInstance, target); err != nil {
		t.Fatal(err)
	}
	if err := RemoveAccountConfigDir(firstTestInstance, target); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(marker); err != nil || string(got) != "preserved" {
		t.Fatalf("target marker = %q err=%v", got, err)
	}
}

func TestAccountConfigLinkConcurrentSameTargetIsIdempotent(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	const workers = 16
	var group sync.WaitGroup
	errorsCh := make(chan error, workers)
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			errorsCh <- EnsureAccountConfigDir(firstTestInstance, "/verified/target")
		}()
	}
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	link, _ := AccountConfigDir(firstTestInstance)
	assertLinkTarget(t, link, "/verified/target")
}

func TestAccountConfigIdentitySurvivesRepeatedRetarget(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	configDir, err := AccountConfigDir(firstTestInstance)
	if err != nil {
		t.Fatal(err)
	}
	service, err := AccountKeychainService(firstTestInstance)
	if err != nil {
		t.Fatal(err)
	}
	if err := EnsureAccountConfigDir(firstTestInstance, "/verified/one"); err != nil {
		t.Fatal(err)
	}
	if err := RetargetAccountConfigDir(firstTestInstance, "/verified/one", "/verified/two"); err != nil {
		t.Fatal(err)
	}
	if err := RetargetAccountConfigDir(firstTestInstance, "/verified/two", "/verified/three"); err != nil {
		t.Fatal(err)
	}
	afterDir, _ := AccountConfigDir(firstTestInstance)
	afterService, _ := AccountKeychainService(firstTestInstance)
	if afterDir != configDir || afterService != service {
		t.Fatalf("identity drifted: %q/%q -> %q/%q", configDir, service, afterDir, afterService)
	}
	assertLinkTarget(t, configDir, "/verified/three")
}

func TestAccountConfigLinkFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name   string
		create func(string) error
	}{
		{name: "relative", create: func(link string) error { return os.Symlink("relative", link) }},
		{name: "unclean", create: func(link string) error { return os.Symlink("/tmp/../tmp/target", link) }},
		{name: "regular", create: func(link string) error { return os.WriteFile(link, nil, 0o600) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			link, _ := AccountConfigDir(firstTestInstance)
			if err := os.MkdirAll(filepath.Dir(link), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := test.create(link); err != nil {
				t.Fatal(err)
			}
			if err := EnsureAccountConfigDir(firstTestInstance, "/verified/target"); !errors.Is(err, ErrAccountConfigLinkConflict) {
				t.Fatalf("EnsureAccountConfigDir error = %v", err)
			}
		})
	}
}

func TestAccountConfigLinkRefusesWrongTarget(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := EnsureAccountConfigDir(firstTestInstance, "/verified/first"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureAccountConfigDir(firstTestInstance, "/verified/second"); !errors.Is(err, ErrAccountConfigLinkConflict) {
		t.Fatalf("wrong ensure error = %v", err)
	}
	if err := RetargetAccountConfigDir(firstTestInstance, "/foreign/target", "/verified/second"); !errors.Is(err, ErrAccountConfigLinkConflict) {
		t.Fatalf("foreign retarget error = %v", err)
	}
	if err := RemoveAccountConfigDir(firstTestInstance, "/verified/second"); !errors.Is(err, ErrAccountConfigLinkConflict) {
		t.Fatalf("wrong remove error = %v", err)
	}
}

func TestAccountConfigParentMustBeReal0700Directory(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	realParent := filepath.Join(home, "real")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(home, ".cc-pool"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realParent, filepath.Join(home, ".cc-pool", "config")); err != nil {
		t.Fatal(err)
	}
	if err := EnsureAccountConfigDir(firstTestInstance, "/verified/target"); err == nil {
		t.Fatal("EnsureAccountConfigDir accepted symlinked parent")
	}
}

func assertLinkTarget(t *testing.T, link, want string) {
	t.Helper()
	got, err := os.Readlink(link)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("link target = %q, want %q", got, want)
	}
}
