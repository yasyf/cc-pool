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

// TestClassifyAccountDir pins the Lstat/Readlink-only classification: a real
// dir, an absent path, a mux bridge, an FP domain bridge, and any other symlink
// each land in their DirKind, with the target riding back only for links.
// Classification is by the raw stored target, so a relative target never matches
// the absolute bridge-root prefixes — it reads foreign even when it resolves into
// the mux root.
func TestClassifyAccountDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(AccountsDir(), 0o700); err != nil {
		t.Fatal(err)
	}

	realDir := filepath.Join(AccountsDir(), "acct-01")
	if err := os.MkdirAll(realDir, 0o700); err != nil {
		t.Fatal(err)
	}
	link := func(name, target string) string {
		p := filepath.Join(AccountsDir(), name)
		if err := os.Symlink(target, p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	muxTarget := filepath.Join(MuxRootDir(), "acct-02")
	fpTarget := filepath.Join(FPCloudStorageDir(), FPDomainFolderPrefix+"acct-03")
	foreignTarget := filepath.Join(home, "somewhere-else")
	relTarget := filepath.Join("..", "mnt", "acct-05") // resolves into the mux root, but is not absolute

	cases := map[string]struct {
		path       string
		wantKind   DirKind
		wantTarget string
	}{
		"a real directory":                  {realDir, DirReal, ""},
		"an absent path":                    {filepath.Join(AccountsDir(), "acct-99"), DirAbsent, ""},
		"a mux bridge symlink":              {link("acct-02", muxTarget), DirMuxBridge, muxTarget},
		"a file provider domain bridge":     {link("acct-03", fpTarget), DirFPBridge, fpTarget},
		"a foreign symlink":                 {link("acct-04", foreignTarget), DirForeignLink, foreignTarget},
		"a relative target is not a bridge": {link("acct-05", relTarget), DirForeignLink, relTarget},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			kind, target := ClassifyAccountDir(tc.path)
			if kind != tc.wantKind || target != tc.wantTarget {
				t.Fatalf("ClassifyAccountDir(%q) = (%v, %q), want (%v, %q)", tc.path, kind, target, tc.wantKind, tc.wantTarget)
			}
		})
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

// TestParseFPDomainFolder pins the ~/Library/CloudStorage folder parse: only a
// name whose suffix round-trips AccountDirName(n) exactly is a pool domain; an
// unpadded index, junk, a foreign app's folder, or the empty string is rejected.
func TestParseFPDomainFolder(t *testing.T) {
	cases := map[string]struct {
		name   string
		wantID int
		wantOK bool
	}{
		"two-digit index":      {"CCPoolStatus-acct-14", 14, true},
		"padded single-digit":  {"CCPoolStatus-acct-01", 1, true},
		"unpadded index":       {"CCPoolStatus-acct-1", 0, false},
		"over-padded index":    {"CCPoolStatus-acct-014", 0, false},
		"non-numeric suffix":   {"CCPoolStatus-foo", 0, false},
		"zero index":           {"CCPoolStatus-acct-00", 0, false},
		"foreign app prefix":   {"OtherApp-acct-2", 0, false},
		"empty string":         {"", 0, false},
		"prefix only":          {"CCPoolStatus-", 0, false},
		"missing acct segment": {"CCPoolStatus-14", 0, false},
		"trailing junk":        {"CCPoolStatus-acct-14x", 0, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			id, ok := ParseFPDomainFolder(tc.name)
			if id != tc.wantID || ok != tc.wantOK {
				t.Fatalf("ParseFPDomainFolder(%q) = (%d, %v), want (%d, %v)", tc.name, id, ok, tc.wantID, tc.wantOK)
			}
		})
	}
}
