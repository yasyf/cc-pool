package pool

import (
	"os"
	"path/filepath"
	"testing"
)

// TestConfigDirForMount pins the single wire→ConfigDir translation: a mux subtree
// (a direct child of MuxRootDir()) maps to its account ConfigDir, and any other
// served path passes through unchanged.
func TestConfigDirForMount(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	cases := map[string]struct {
		mountDir string
		want     string
	}{
		"mux subtree maps to the account ConfigDir": {
			mountDir: filepath.Join(MuxRootDir(), "acct-01"),
			want:     filepath.Join(AccountsDir(), "acct-01"),
		},
		"a legacy per-dir mount (Dir == ConfigDir) passes through": {
			mountDir: filepath.Join(AccountsDir(), "acct-02"),
			want:     filepath.Join(AccountsDir(), "acct-02"),
		},
		"an unrelated path passes through": {
			mountDir: "/some/other/mount",
			want:     "/some/other/mount",
		},
		"the mux root itself is not a subtree, passes through": {
			mountDir: MuxRootDir(),
			want:     MuxRootDir(),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := ConfigDirForMount(tc.mountDir); got != tc.want {
				t.Fatalf("ConfigDirForMount(%q) = %q, want %q", tc.mountDir, got, tc.want)
			}
		})
	}
}

// TestFPBridgeSocketPathFitsSunPath pins the short-leaf choice: the FP bridge
// socket in cc-pool's own state dir must fit AF_UNIX's 104-byte sun_path for
// this home.
func TestFPBridgeSocketPathFitsSunPath(t *testing.T) {
	p := FPBridgeSocketPath()
	if len(p) >= 104 {
		t.Fatalf("FPBridgeSocketPath() = %q (%d bytes), exceeds the 104-byte sun_path limit", p, len(p))
	}
	home, err := Home()
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(mustHome(), "Library", "Group Containers", AppGroupID, "b.sock"); p != want {
		t.Fatalf("FPBridgeSocketPath() = %q, want %q", p, want)
	}
	// Home-independent budget: the fixed suffix below is the part
	// cc-pool controls, so a fattened leaf fails on any runner, however short its
	// home.
	const suffixBudget = len("/Library/Group Containers/" + AppGroupID + "/b.sock")
	if overhead := len(p) - len(home); overhead > suffixBudget {
		t.Fatalf("FPBridgeSocketPath() fixed suffix = %d bytes, want <= %d (leaves %d bytes of sun_path for $HOME)", overhead, suffixBudget, 103-suffixBudget)
	}
}

// TestIsBridgeSymlink pins bridge detection: only a symlink whose target is a
// child of MuxRootDir() reads true. A real dir, a symlink elsewhere, and an
// absent path all read false — nothing is traversed.
func TestIsBridgeSymlink(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(AccountsDir(), 0o700); err != nil {
		t.Fatal(err)
	}

	bridge := filepath.Join(AccountsDir(), "acct-01")
	if err := os.Symlink(filepath.Join(MuxRootDir(), "acct-01"), bridge); err != nil {
		t.Fatal(err)
	}
	realDir := filepath.Join(AccountsDir(), "acct-02")
	if err := os.MkdirAll(realDir, 0o700); err != nil {
		t.Fatal(err)
	}
	elsewhere := filepath.Join(AccountsDir(), "acct-03")
	if err := os.Symlink(filepath.Join(home, "not-the-mux"), elsewhere); err != nil {
		t.Fatal(err)
	}

	cases := map[string]struct {
		path string
		want bool
	}{
		"bridge symlink into the mux root":  {bridge, true},
		"a real directory":                  {realDir, false},
		"a symlink pointing elsewhere":      {elsewhere, false},
		"an absent path":                    {filepath.Join(AccountsDir(), "acct-99"), false},
		"the mux root itself (not a child)": {MuxRootDir(), false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := IsBridgeSymlink(tc.path); got != tc.want {
				t.Fatalf("IsBridgeSymlink(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}
