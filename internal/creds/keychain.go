// Package creds owns Claude Code credentials: the Credential blob format, the
// per-config-dir Keychain service-name derivation (exactly as Claude Code
// does), and the macOS Keychain item accessed via /usr/bin/security.
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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit/worker"
	"golang.org/x/text/unicode/norm"
)

// securityBin is Apple's security(1), overridable for tests.
var securityBin = "/usr/bin/security"

func securityExecutable() string {
	if executable := os.Getenv("CLAUDE_POOL_SECURITY_BIN"); executable != "" {
		return executable
	}
	return securityBin
}

// baseService is the un-suffixed service used for the default ~/.claude item.
const baseService = "Claude Code-credentials"

// usernameRE matches Claude's own username validation (regex Eq5 in the binary).
var usernameRE = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

const fallbackAccount = "claude-code-user"

const maxSecurityOutput = 1 << 20
const keychainTaskTimeout = 30 * time.Second

type boundedBuffer struct {
	bytes.Buffer
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	original := len(p)
	remaining := maxSecurityOutput - b.Len()
	if remaining <= 0 {
		return original, nil
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	_, err := b.Buffer.Write(p)
	return original, err
}

// ErrNotFound is returned when the Keychain item holds no credential.
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
func Read(ctx context.Context, runner TaskRunner, service, account string) (*Credential, error) {
	if account == "" {
		account = AccountLabel()
	}
	raw, err := readRaw(ctx, runner, service, account)
	if err != nil {
		return nil, err
	}
	return parseCredential(raw)
}

func readRaw(ctx context.Context, runner TaskRunner, service, account string) ([]byte, error) {
	var out, errb boundedBuffer
	if err := runKeychainTask(
		ctx,
		runner,
		[]string{"find-generic-password", "-a", account, "-s", service, "-w"},
		&out,
		&errb,
	); err != nil {
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
func Write(ctx context.Context, runner TaskRunner, service, account string, cred *Credential) error {
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
	return writeRaw(ctx, runner, service, account, blob)
}

func writeRaw(ctx context.Context, runner TaskRunner, service, account string, blob []byte) error {
	hexed := hex.EncodeToString(blob)
	var errb boundedBuffer
	if err := runKeychainTask(
		ctx,
		runner,
		[]string{"add-generic-password", "-U", "-a", account, "-s", service, "-X", hexed},
		nil,
		&errb,
	); err != nil {
		return fmt.Errorf("security add-generic-password: %w: %s", err, strings.TrimSpace(errb.String()))
	}
	return nil
}

// Delete removes the item under (service, account). Missing is not an error.
func Delete(ctx context.Context, runner TaskRunner, service, account string) error {
	if account == "" {
		account = AccountLabel()
	}
	var errb boundedBuffer
	if err := runKeychainTask(
		ctx,
		runner,
		[]string{"delete-generic-password", "-a", account, "-s", service},
		nil,
		&errb,
	); err != nil {
		if isNotFound(errb.String()) {
			return nil
		}
		return fmt.Errorf("security delete-generic-password: %w: %s", err, strings.TrimSpace(errb.String()))
	}
	return nil
}

func runKeychainTask(
	ctx context.Context,
	runner TaskRunner,
	args []string,
	stdout, stderr *boundedBuffer,
) error {
	if runner == nil {
		return errors.New("credential keychain worker runner is required")
	}
	result, err := runner.Run(ctx, worker.CommandRequest{
		Path: securityExecutable(), Dir: workerexec.TempDir(), Args: args,
		TotalTimeout: keychainTaskTimeout,
	})
	if stdout != nil {
		_, _ = stdout.Write(result.Stdout)
	}
	if stderr != nil {
		_, _ = stderr.Write(result.Stderr)
	}
	return err
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
