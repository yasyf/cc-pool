package statusapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
)

type recordingRunner struct {
	calls  [][]string
	output []byte
	err    error
}

func (r *recordingRunner) Run(_ context.Context, name string, arguments ...string) error {
	r.calls = append(r.calls, append([]string{name}, arguments...))
	return r.err
}

func (r *recordingRunner) Output(_ context.Context, name string, arguments ...string) ([]byte, error) {
	r.calls = append(r.calls, append([]string{name}, arguments...))
	return r.output, r.err
}

func TestReleasePinsExactVersionURLAndDigest(t *testing.T) {
	setRelease(t, "v0.63.0", "0.63.0", strings.Repeat("ab", 32))
	got, err := release()
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != "0.63.0" || got.SHA256.String() != strings.Repeat("ab", 32) {
		t.Fatalf("release = %+v", got)
	}
	if got.URL != "https://github.com/yasyf/cc-pool/releases/download/v0.63.0/cc-pool-status-v0.63.0-darwin.zip" {
		t.Fatalf("URL = %q", got.URL)
	}
}

func TestReleaseAcceptsPrereleaseTagWithNumericBundleVersion(t *testing.T) {
	setRelease(t, "v0.63.0-rc.1", "0.63.0", strings.Repeat("ab", 32))
	got, err := release()
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != "0.63.0" || got.URL != "https://github.com/yasyf/cc-pool/releases/download/v0.63.0-rc.1/cc-pool-status-v0.63.0-rc.1-darwin.zip" {
		t.Fatalf("release = %+v", got)
	}
}

func TestReleaseRejectsMissingOrInexactMetadata(t *testing.T) {
	for _, test := range []struct{ appVersion, digest string }{
		{"", strings.Repeat("ab", 32)},
		{" 0.63.0", strings.Repeat("ab", 32)},
		{"0.64.0", strings.Repeat("ab", 32)},
		{"0.63.0", ""},
	} {
		setRelease(t, "v0.63.0", test.appVersion, test.digest)
		if _, err := release(); err == nil {
			t.Fatalf("release accepted version=%q digest=%q", test.appVersion, test.digest)
		}
	}
}

func TestProveElectionRegistersExactFixedFileProvider(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	appPath := filepath.Join(home, "Applications", "CCPoolStatus.app")
	extension := filepath.Join(appPath, "Contents", "PlugIns", "CCPoolFileProvider.appex")
	commands := &recordingRunner{output: []byte("+    " + fileProviderBundleID + "(0.63.0)\tUUID\tdate\t" + extension + "\n (1 plug-in)\n")}
	if err := proveElection(t.Context(), appPath, commands); err != nil {
		t.Fatal(err)
	}
	wantCalls := [][]string{
		{"/usr/bin/pluginkit", "-a", extension},
		{"/usr/bin/pluginkit", "-e", "use", "-i", fileProviderBundleID},
		{"/usr/bin/pluginkit", "-m", "-v", "-i", fileProviderBundleID},
	}
	if !reflect.DeepEqual(commands.calls, wantCalls) {
		t.Fatalf("commands = %#v, want %#v", commands.calls, wantCalls)
	}
}

func TestProveElectionRejectsUnexpectedInstallPathBeforeRegistration(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	commands := &recordingRunner{}
	if err := proveElection(t.Context(), filepath.Join(t.TempDir(), "CCPoolStatus.app"), commands); err == nil {
		t.Fatal("proof accepted an unexpected installation path")
	}
	if len(commands.calls) != 0 {
		t.Fatalf("registration ran after path mismatch: %#v", commands.calls)
	}
}

func TestEnsureInstallDirectoryRejectsSymlink(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.Symlink(t.TempDir(), pool.WidgetAppDir()); err != nil {
		t.Fatal(err)
	}
	if err := ensureInstallDirectory(pool.WidgetAppDir()); err == nil {
		t.Fatal("install directory accepted a symlink")
	}
}

func TestProveElectionSurfacesRegistrationFailure(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	want := errors.New("failure")
	if err := proveElection(t.Context(), pool.WidgetAppPath(), &recordingRunner{err: want}); !errors.Is(err, want) {
		t.Fatalf("registration error = %v", err)
	}
}

func TestVerifyElectionRequiresOneExactEnabledPath(t *testing.T) {
	expected := "/Users/test/Applications/CCPoolStatus.app/Contents/PlugIns/CCPoolFileProvider.appex"
	valid := "+    " + fileProviderBundleID + "(0.63.0)\tUUID\tdate\t" + expected + "\n (1 plug-in)\n"
	if err := verifyElection([]byte(valid), expected); err != nil {
		t.Fatalf("exact election rejected: %v", err)
	}
	for name, output := range map[string]string{
		"absent":     " (0 plug-ins)\n",
		"wrong path": strings.ReplaceAll(valid, expected, "/Applications/CCPoolStatus.app/Contents/PlugIns/CCPoolFileProvider.appex"),
		"duplicates": valid + valid,
		"malformed":  "+ " + fileProviderBundleID + "\n",
		"disabled":   "-    " + fileProviderBundleID + "(0.63.0)\tUUID\tdate\t" + expected + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			if err := verifyElection([]byte(output), expected); err == nil {
				t.Fatalf("verifyElection accepted %q", output)
			}
		})
	}
}

func setRelease(t *testing.T, releaseVersion, appVersion, digest string) {
	t.Helper()
	oldVersion, oldAppVersion, oldDigest := version.Version, version.StatusAppVersion, version.StatusAppSHA256
	version.Version, version.StatusAppVersion, version.StatusAppSHA256 = releaseVersion, appVersion, digest
	t.Cleanup(func() {
		version.Version, version.StatusAppVersion, version.StatusAppSHA256 = oldVersion, oldAppVersion, oldDigest
	})
}
