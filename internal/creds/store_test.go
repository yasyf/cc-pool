package creds

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	// loginSearchList is what `security list-keychains -d user` prints in a
	// normal GUI session: the login keychain is searchable.
	loginSearchList = "    \"/Users/tester/Library/Keychains/login.keychain-db\"\n"
	// headlessSearchList is the headless (e.g. SSH) shape: no login keychain in
	// the user search list, so a find miss proves nothing.
	headlessSearchList = "    \"/Library/Keychains/System.keychain\"\n"
)

// fakeKeychain wires the fake security(1) into securityBin for one test and
// returns the store dir holding its item files, calls.log, and keychains.txt.
func fakeKeychain(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	storeDir := filepath.Join(dir, "items")
	if err := os.MkdirAll(storeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	old := securityBin
	securityBin = writeFakeSecurity(t, dir, storeDir)
	t.Cleanup(func() { securityBin = old })
	return storeDir
}

// setSearchList writes the `list-keychains -d user` output the fake will print.
func setSearchList(t *testing.T, storeDir, list string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(storeDir, "keychains.txt"), []byte(list), 0o600); err != nil {
		t.Fatal(err)
	}
}

// writeBrokenSecurity writes a stub security(1) that always fails with a
// non-not-found error, emulating a locked/broken Keychain backend.
func writeBrokenSecurity(t *testing.T) string {
	t.Helper()
	script := "#!/bin/bash\necho 'security: SecKeychainItemCopyContent: User interaction is not allowed.' >&2\nexit 1\n"
	path := filepath.Join(t.TempDir(), "security")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil { //nolint:gosec // G306: executable test stub
		t.Fatal(err)
	}
	return path
}

// TestKeychainItemRead pins the miss-path triage: a readable item wins; a miss
// is ErrNotFound only when the login keychain is searchable and ErrUnavailable
// when it is not; a broken security(1) surfaces as neither sentinel. The
// availability exec runs only on the miss path.
func TestKeychainItemRead(t *testing.T) {
	const svc = "Claude Code-credentials-deadbeef"
	const acct = "tester"
	item := KeychainItem{Service: svc, Account: acct}

	if got := item.Source(); got != SourceKeychain {
		t.Errorf("Source() = %v, want SourceKeychain", got)
	}
	if got, want := item.String(), `keychain item "Claude Code-credentials-deadbeef"`; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}

	cases := []struct {
		name        string
		searchList  string
		seedToken   string // non-empty: item pre-seeded with this access token
		breakBin    bool
		wantErr     error
		wantNeither bool // error that is neither ErrNotFound nor ErrUnavailable
	}{
		{name: "item present", searchList: loginSearchList, seedToken: "at-item"},
		{name: "missing with login keychain searchable", searchList: loginSearchList, wantErr: ErrNotFound},
		{name: "missing without login keychain", searchList: headlessSearchList, wantErr: ErrUnavailable},
		{name: "security bin failure", breakBin: true, wantNeither: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var storeDir string
			if tc.breakBin {
				old := securityBin
				securityBin = writeBrokenSecurity(t)
				t.Cleanup(func() { securityBin = old })
			} else {
				storeDir = fakeKeychain(t)
				setSearchList(t, storeDir, tc.searchList)
			}
			if tc.seedToken != "" {
				seed := &Credential{ClaudeAiOauth: OAuth{AccessToken: tc.seedToken, RefreshToken: "rt"}}
				if err := item.Write(seed); err != nil {
					t.Fatal(err)
				}
			}

			got, err := item.Read()
			switch {
			case tc.wantNeither:
				if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrUnavailable) {
					t.Fatalf("Read with broken security = (%v, %v), want a non-sentinel error", got, err)
				}
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Read = (%v, %v), want %v", got, err, tc.wantErr)
				}
			default:
				if err != nil {
					t.Fatalf("Read: %v", err)
				}
				if got.ClaudeAiOauth.AccessToken != tc.seedToken {
					t.Fatalf("Read token = %q, want %q", got.ClaudeAiOauth.AccessToken, tc.seedToken)
				}
				log, lerr := os.ReadFile(filepath.Join(storeDir, "calls.log")) //nolint:gosec // G304: storeDir is a test-owned temp dir, not user input.
				if lerr != nil {
					t.Fatal(lerr)
				}
				if strings.Contains(string(log), "list-keychains") {
					t.Fatal("availability check ran on the hit path; it must exec only on a miss")
				}
			}
		})
	}
}

// TestKeychainItemReassert pins the read-then-write-back ACL adoption cycle
// and that its read goes through the miss-path triage.
func TestKeychainItemReassert(t *testing.T) {
	const svc = "Claude Code-credentials-deadbeef"
	const acct = "tester"
	item := KeychainItem{Service: svc, Account: acct}
	storeDir := fakeKeychain(t)
	setSearchList(t, storeDir, loginSearchList)

	seed := &Credential{ClaudeAiOauth: OAuth{AccessToken: "at-1", RefreshToken: "rt-1", ExpiresAt: 1700000000000}}
	if err := item.Write(seed); err != nil {
		t.Fatal(err)
	}
	got, err := item.Reassert()
	if err != nil {
		t.Fatalf("Reassert: %v", err)
	}
	if got.ClaudeAiOauth.AccessToken != "at-1" || got.ClaudeAiOauth.RefreshToken != "rt-1" {
		t.Fatalf("Reassert returned wrong credential: %+v", got.ClaudeAiOauth)
	}
	log, err := os.ReadFile(filepath.Join(storeDir, "calls.log")) //nolint:gosec // G304: storeDir is a test-owned temp dir, not user input.
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(log), "add-generic-password"); n != 2 {
		t.Fatalf("add-generic-password ran %d times, want 2 (seed + write-back)", n)
	}

	if err := item.Delete(); err != nil {
		t.Fatal(err)
	}
	setSearchList(t, storeDir, headlessSearchList)
	if _, err := item.Reassert(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Reassert on unsearchable miss = %v, want ErrUnavailable", err)
	}
}

// TestFileStore pins the plaintext-file Store: 0600 round-trip, Delete
// idempotence, and its Source/String identity.
func TestFileStore(t *testing.T) {
	dir := t.TempDir()
	st := FileStore{ConfigDir: dir}

	if got := st.Source(); got != SourceFile {
		t.Errorf("Source() = %v, want SourceFile", got)
	}
	if got, want := st.String(), FileCredentialPath(dir); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}

	if _, err := st.Read(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Read on empty dir = %v, want ErrNotFound", err)
	}
	if err := st.Delete(); err != nil {
		t.Fatalf("Delete on missing = %v, want nil", err)
	}

	cred := &Credential{ClaudeAiOauth: OAuth{AccessToken: "at-1", RefreshToken: "rt-1", ExpiresAt: 1700000000000}}
	if err := st.Write(cred); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(FileCredentialPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("mode = %v, want 0600", fi.Mode().Perm())
	}
	got, err := st.Read()
	if err != nil {
		t.Fatal(err)
	}
	if got.ClaudeAiOauth.AccessToken != "at-1" || got.ClaudeAiOauth.RefreshToken != "rt-1" {
		t.Fatalf("round-trip mismatch: %+v", got.ClaudeAiOauth)
	}

	if err := st.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Read(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Read after Delete = %v, want ErrNotFound", err)
	}
	if err := st.Delete(); err != nil {
		t.Fatalf("second Delete = %v, want nil", err)
	}
}

// TestClassifyRead pins the shared owned-precedence read taxonomy: nil is
// present, the two empty sentinels collapse to ReadEmpty, ErrUnavailable is its
// own unsearchable arm, and every other error (including wrapped sentinels)
// fails closed as ReadFatal.
func TestClassifyRead(t *testing.T) {
	cases := map[string]struct {
		err  error
		want ReadState
	}{
		"nil is present":                   {nil, ReadPresent},
		"not found is empty":               {ErrNotFound, ReadEmpty},
		"no tokens is empty":               {ErrNoTokens, ReadEmpty},
		"wrapped not found is empty":       {fmt.Errorf("read: %w", ErrNotFound), ReadEmpty},
		"unavailable is unsearchable":      {ErrUnavailable, ReadUnsearchable},
		"wrapped unavailable unsearchable": {fmt.Errorf("probe: %w", ErrUnavailable), ReadUnsearchable},
		"other error fails closed":         {errors.New("security: user interaction not allowed"), ReadFatal},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := ClassifyRead(tc.err); got != tc.want {
				t.Fatalf("ClassifyRead(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}
