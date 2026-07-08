// Package creds owns Claude Code credentials: the Credential blob format, the
// per-config-dir Keychain service-name derivation (exactly as Claude Code
// does), and both storage backends — the macOS Keychain item accessed via
// /usr/bin/security, and claude's plaintext .credentials.json fallback.
//
// Shelling out to Apple's signed security(1) keeps items prompt-free on later
// reads: the item ACL trusts that binary, not ours, sidestepping ad-hoc-signing
// and TCC. claude reads/writes the same way, so we share its trust domain.
//
// ServiceName always emits a hash-suffixed name, so no code path here can name
// the canonical unsuffixed item plain claude owns.
package creds

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

// securityBin is Apple's security(1), overridable for tests.
var securityBin = func() string {
	if v := os.Getenv("CLAUDE_POOL_SECURITY_BIN"); v != "" {
		return v
	}
	return "/usr/bin/security"
}()

// baseService is the un-suffixed service used for the default ~/.claude item.
const baseService = "Claude Code-credentials"

// usernameRE matches Claude's own username validation (regex Eq5 in the binary).
var usernameRE = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

const fallbackAccount = "claude-code-user"

// ErrNotFound is returned when a store holds no credential; both the Keychain
// and file backends share it, so its text names neither.
var ErrNotFound = errors.New("credential not found")

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

// Write upserts cred under (service, account) via security -U, the secret
// hex-encoded through -X. -X leaves the value in same-user-ps-visible argv by
// design (matches claude's trust model), not a leak to eliminate.
func Write(service, account string, cred *Credential) error {
	if err := cred.validateForWrite(); err != nil {
		return err
	}
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

// isNotFound recognizes security(1)'s errSecItemNotFound ("could not be found") text.
func isNotFound(stderr string) bool {
	s := strings.ToLower(stderr)
	return strings.Contains(s, "could not be found") ||
		strings.Contains(s, "the specified item could not be found")
}

func currentUsername() (string, error) {
	// Matches what Claude derives from os.userInfo().username.
	u, err := userCurrent()
	if err != nil {
		return "", err
	}
	return u, nil
}
