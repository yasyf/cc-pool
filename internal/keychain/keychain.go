// Package keychain is the linchpin: it derives the per-config-dir Keychain
// service name exactly as Claude Code does, and reads/writes the credential
// item by shelling out to /usr/bin/security.
//
// Why shell out instead of using a native Keychain API? Items created or
// updated through the stable Apple `security` binary are later readable by
// `security` PROMPT-FREE, because the item ACL trusts that signed Apple binary
// rather than ours. This sidesteps every ad-hoc-signing / TCC concern. Claude
// Code itself reads and writes via `security`, so we share its trust domain.
//
// The canonical unsuffixed item plain `claude` owns is never named by cc-pool:
// ServiceName always emits a hash-suffixed name, so no code path here can read,
// write, or delete plain claude's credential.
package keychain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"golang.org/x/text/unicode/norm"
)

// securityBin is the path to Apple's security(1). Overridable in tests via a
// PATH shim by setting the CLAUDE_POOL_SECURITY_BIN env var.
var securityBin = func() string {
	if v := os.Getenv("CLAUDE_POOL_SECURITY_BIN"); v != "" {
		return v
	}
	return "/usr/bin/security"
}()

// baseService is the un-suffixed service used for the default ~/.claude item.
const baseService = "Claude Code-credentials"

// usernameRE matches Claude's own username validation (regex Eq5 in the
// binary). A username failing this falls back to fallbackAccount.
var usernameRE = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

const fallbackAccount = "claude-code-user"

// ErrNotFound is returned when the requested Keychain item does not exist.
var ErrNotFound = errors.New("keychain item not found")

// ServiceName derives the Keychain service name Claude Code uses for a given
// explicit CLAUDE_CONFIG_DIR value. The derivation, verbatim from the binary:
//
//	service = "Claude Code-credentials" + "-" + sha256(NFC(configDir)).hex[:8]
//
// The hash is taken over the RAW config-dir string (only NFC-normalized) — not
// its realpath and not trailing-slash-normalized. Callers must therefore pass
// exactly the string that will be exported as CLAUDE_CONFIG_DIR.
func ServiceName(configDir string) string {
	k := norm.NFC.String(configDir)
	sum := sha256.Sum256([]byte(k))
	suffix := hex.EncodeToString(sum[:])[:8]
	return baseService + "-" + suffix
}

// AccountLabel returns the Keychain account (-a) label Claude uses: $USER, or
// the OS username, validated against usernameRE, else a fixed fallback.
func AccountLabel() string {
	u := os.Getenv("USER")
	if u == "" {
		if name, err := currentUsername(); err == nil {
			u = name
		}
	}
	if !usernameRE.MatchString(u) {
		return fallbackAccount
	}
	return u
}

// Read fetches and parses the credential stored under (service, account).
// account may be empty, in which case AccountLabel() is used.
func Read(service, account string) (*Credential, error) {
	if account == "" {
		account = AccountLabel()
	}
	raw, err := readRaw(service, account)
	if err != nil {
		return nil, err
	}
	return parseCredential(raw)
}

// readRaw returns the raw secret bytes for (service, account).
func readRaw(service, account string) ([]byte, error) {
	//nolint:gosec // G204: securityBin is the fixed /usr/bin/security path; account/service are cc-pool-derived keychain identifiers
	cmd := exec.Command(securityBin,
		"find-generic-password", "-a", account, "-s", service, "-w")
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		if isNotFound(errb.String()) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("security find-generic-password: %w: %s", err, strings.TrimSpace(errb.String()))
	}
	// `security -w` prints the password followed by a trailing newline.
	return bytes.TrimRight(out.Bytes(), "\n"), nil
}

// Write stores cred under (service, account), creating or updating the item
// in place (-U). The secret is hex-encoded and passed via -X, mirroring Claude
// Code's own write path. NOTE: -X places the value in the spawned process's
// argv, which is briefly visible to same-user `ps -Eww`. This matches Claude
// Code's behavior and the same-user trust model (a same-user process can read
// the item outright via `security ... -w`), but it is not argv-private;
// eliminating it would require the native SecItemAdd API.
func Write(service, account string, cred *Credential) error {
	if account == "" {
		account = AccountLabel()
	}
	blob, err := cred.Marshal()
	if err != nil {
		return err
	}
	return writeRaw(service, account, blob)
}

func writeRaw(service, account string, blob []byte) error {
	hexed := hex.EncodeToString(blob)
	//nolint:gosec // G204: securityBin is the fixed /usr/bin/security path; account/service/hexed are cc-pool-derived, not external input
	cmd := exec.Command(securityBin,
		"add-generic-password", "-U", "-a", account, "-s", service, "-X", hexed)
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("security add-generic-password: %w: %s", err, strings.TrimSpace(errb.String()))
	}
	return nil
}

// Delete removes the item under (service, account). Missing is not an error.
func Delete(service, account string) error {
	if account == "" {
		account = AccountLabel()
	}
	//nolint:gosec // G204: securityBin is the fixed /usr/bin/security path; account/service are cc-pool-derived keychain identifiers
	cmd := exec.Command(securityBin,
		"delete-generic-password", "-a", account, "-s", service)
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		if isNotFound(errb.String()) {
			return nil
		}
		return fmt.Errorf("security delete-generic-password: %w: %s", err, strings.TrimSpace(errb.String()))
	}
	return nil
}

// Reassert reads the item under (service, account) and writes it straight back
// via our `security` invocation, so subsequent reads/writes by our tooling are
// prompt-free regardless of which process originally created it. Used at add
// time after the user's interactive /login.
func Reassert(service, account string) (*Credential, error) {
	cred, err := Read(service, account)
	if err != nil {
		return nil, err
	}
	if err := Write(service, account, cred); err != nil {
		return nil, err
	}
	return cred, nil
}

// isNotFound recognizes security(1)'s "item could not be found" error text.
// The numeric SecKeychain error for this is 44 (errSecItemNotFound shown as
// "The specified item could not be found in the keychain.").
func isNotFound(stderr string) bool {
	s := strings.ToLower(stderr)
	return strings.Contains(s, "could not be found") ||
		strings.Contains(s, "the specified item could not be found")
}

// currentUsername returns the OS account username.
func currentUsername() (string, error) {
	// os/user requires cgo on some platforms; on macOS it is available and
	// matches what Claude derives from os.userInfo().username.
	u, err := userCurrent()
	if err != nil {
		return "", err
	}
	return u, nil
}
