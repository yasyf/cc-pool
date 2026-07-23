package statusapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit/fetch"
)

type recordingFetcher struct {
	installation fetch.Installation
	err          error
	calls        int
	config       fetch.Config
}

func (f *recordingFetcher) Fetch(_ context.Context, config fetch.Config) (fetch.Installation, error) {
	f.calls++
	f.config = config
	return f.installation, f.err
}

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

func TestReconcileFetchesFixedAppAndRegistersFileProvider(t *testing.T) {
	setRelease(t, "v0.63.0", "0.63.0", strings.Repeat("cd", 32))
	home := t.TempDir()
	t.Setenv("HOME", home)
	appPath := filepath.Join(home, "Applications", "CCPoolStatus.app")
	fetcher := &recordingFetcher{installation: fetch.Installation{Path: appPath}}
	extension := filepath.Join(appPath, "Contents", "PlugIns", "CCPoolFileProvider.appex")
	commands := &recordingRunner{output: []byte("+    " + fileProviderBundleID + "(0.63.0)\tUUID\tdate\t" + extension + "\n (1 plug-in)\n")}
	got, err := reconcile(t.Context(), fetcher, commands)
	if err != nil {
		t.Fatal(err)
	}
	if got != appPath || fetcher.calls != 1 {
		t.Fatalf("reconcile = %q, fetch calls = %d", got, fetcher.calls)
	}
	if fetcher.config.Dir != filepath.Join(home, "Applications") || fetcher.config.AppName != appName {
		t.Fatalf("fetch config = %+v", fetcher.config)
	}
	if fetcher.config.Identity.TeamID != holderbridge.TeamID ||
		fetcher.config.Identity.SigningIdentifier != holderbridge.BundleID {
		t.Fatalf("fetch identity = %+v", fetcher.config.Identity)
	}
	wantCalls := [][]string{
		{"/usr/bin/pluginkit", "-a", extension},
		{"/usr/bin/pluginkit", "-e", "use", "-i", fileProviderBundleID},
		{"/usr/bin/pluginkit", "-m", "-v", "-i", fileProviderBundleID},
	}
	if !reflect.DeepEqual(commands.calls, wantCalls) {
		t.Fatalf("commands = %#v, want %#v", commands.calls, wantCalls)
	}
	info, err := os.Lstat(filepath.Join(home, "Applications"))
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		t.Fatalf("Applications mode = %v", info.Mode())
	}
}

func TestReconcileRejectsUnexpectedInstallPathBeforeRegistration(t *testing.T) {
	setRelease(t, "v0.63.0", "0.63.0", strings.Repeat("cd", 32))
	t.Setenv("HOME", t.TempDir())
	fetcher := &recordingFetcher{installation: fetch.Installation{Path: filepath.Join(t.TempDir(), "CCPoolStatus.app")}}
	commands := &recordingRunner{}
	if _, err := reconcile(t.Context(), fetcher, commands); err == nil {
		t.Fatal("reconcile accepted an unexpected installation path")
	}
	if len(commands.calls) != 0 {
		t.Fatalf("registration ran after path mismatch: %#v", commands.calls)
	}
}

func TestReconcileRejectsSymlinkedApplicationsDirectory(t *testing.T) {
	setRelease(t, "v0.63.0", "0.63.0", strings.Repeat("cd", 32))
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.Symlink(t.TempDir(), pool.WidgetAppDir()); err != nil {
		t.Fatal(err)
	}
	fetcher := &recordingFetcher{}
	if _, err := reconcile(t.Context(), fetcher, &recordingRunner{}); err == nil {
		t.Fatal("reconcile accepted a symlinked Applications directory")
	}
	if fetcher.calls != 0 {
		t.Fatalf("fetch calls = %d, want 0", fetcher.calls)
	}
}

func TestReconcileSurfacesFetchAndRegistrationFailures(t *testing.T) {
	setRelease(t, "v0.63.0", "0.63.0", strings.Repeat("cd", 32))
	t.Setenv("HOME", t.TempDir())
	want := errors.New("failure")
	if _, err := reconcile(t.Context(), &recordingFetcher{err: want}, &recordingRunner{}); !errors.Is(err, want) {
		t.Fatalf("fetch error = %v", err)
	}
	appPath := pool.WidgetAppPath()
	if _, err := reconcile(t.Context(), &recordingFetcher{installation: fetch.Installation{Path: appPath}}, &recordingRunner{err: want}); !errors.Is(err, want) {
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
