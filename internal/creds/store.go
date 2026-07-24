package creds

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/yasyf/daemonkit/supervise"
)

// ErrUnavailable means a Keychain item's state could not be determined: in a
// headless (e.g. SSH) session security(1)'s user search list lacks the login
// keychain, so item state is unknowable — absence proves nothing.
var ErrUnavailable = errors.New("login keychain not in the security search list")

// TaskRunner executes one durably tracked disposable process group.
type TaskRunner interface {
	Run(context.Context, supervise.Task) error
}

// keychainPathRE captures each quoted keychain path in `security list-keychains` output.
var keychainPathRE = regexp.MustCompile(`"([^"]+)"`)

// Store is one concrete Keychain credential location.
type Store interface {
	// Source identifies the backend kind.
	Source() Source
	// Read returns the stored credential, or ErrNotFound when absent. The
	// Keychain backend returns ErrUnavailable instead when absence cannot be
	// proven (login keychain unsearchable).
	Read(context.Context) (*Credential, error)
	// Write upserts the credential.
	Write(context.Context, *Credential) error
	// Delete removes the credential; missing is not an error.
	Delete(context.Context) error
	// String is the human-readable location for error messages.
	String() string
}

// ReadState is the owned-precedence taxonomy of a Store.Read outcome, shared by
// every credential install and CAS write so the sentinel handling never drifts.
type ReadState int

const (
	// ReadPresent means Read returned a credential (err == nil).
	ReadPresent ReadState = iota
	// ReadEmpty means the slot is provably empty: absent (ErrNotFound) or a
	// tombstone (ErrNoTokens).
	ReadEmpty
	// ReadUnsearchable means the login keychain is unsearchable (ErrUnavailable),
	// so absence cannot be proven.
	ReadUnsearchable
	// ReadFatal means the read failed for any other reason; owned-state cannot be
	// established, so callers fail closed.
	ReadFatal
)

// ClassifyRead maps a Store.Read error to a ReadState.
func ClassifyRead(err error) ReadState {
	switch {
	case err == nil:
		return ReadPresent
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrNoTokens):
		return ReadEmpty
	case errors.Is(err, ErrUnavailable):
		return ReadUnsearchable
	default:
		return ReadFatal
	}
}

// KeychainItem is the Store for one macOS Keychain generic-password item,
// addressed by service and account labels and accessed via security(1). An
// empty Account means AccountLabel(), matching the package-level helpers.
type KeychainItem struct {
	Service string
	Account string
	Runner  TaskRunner
}

// Source returns SourceKeychain.
func (k KeychainItem) Source() Source { return SourceKeychain }

// Read fetches and parses the item. On a miss it distinguishes true absence
// (ErrNotFound) from an unsearchable login keychain (ErrUnavailable) with one
// extra list-keychains exec, run only on the miss path.
func (k KeychainItem) Read(ctx context.Context) (*Credential, error) {
	cred, err := Read(ctx, k.Runner, k.Service, k.Account)
	if !errors.Is(err, ErrNotFound) {
		return cred, err
	}
	searchable, err := loginKeychainSearchable(ctx, k.Runner)
	if err != nil {
		return nil, err
	}
	if !searchable {
		return nil, ErrUnavailable
	}
	return nil, ErrNotFound
}

// Write upserts cred under the item.
func (k KeychainItem) Write(ctx context.Context, cred *Credential) error {
	return Write(ctx, k.Runner, k.Service, k.Account, cred)
}

// WriteCanonical writes a credential produced by
// CanonicalizeCredentialForWrite without rendering it again.
func (k KeychainItem) WriteCanonical(ctx context.Context, credential CanonicalCredential) error {
	if len(credential.blob) == 0 {
		return errors.New("canonical credential is empty")
	}
	account := k.Account
	if account == "" {
		account = AccountLabel()
	}
	return writeRaw(ctx, k.Runner, k.Service, account, credential.blob)
}

// Delete removes the item; missing is not an error.
func (k KeychainItem) Delete(ctx context.Context) error {
	return Delete(ctx, k.Runner, k.Service, k.Account)
}

// Reassert reads then rewrites the item through our security(1) so our later
// access is prompt-free (ACL ownership) whatever process created it.
func (k KeychainItem) Reassert(ctx context.Context) (*Credential, error) {
	cred, err := k.Read(ctx)
	if err != nil {
		return nil, err
	}
	if err := k.Write(ctx, cred); err != nil {
		return nil, err
	}
	return cred, nil
}

// String names the Keychain item for error messages.
func (k KeychainItem) String() string {
	return fmt.Sprintf("keychain item %q", k.Service)
}

// loginKeychainSearchable reports whether security(1)'s user search list
// contains the login keychain; without it a find miss proves nothing.
func loginKeychainSearchable(ctx context.Context, runner TaskRunner) (bool, error) {
	var out, errb boundedBuffer
	if err := runKeychainTask(
		ctx,
		runner,
		[]string{"list-keychains", "-d", "user"},
		&out,
		&errb,
	); err != nil {
		return false, fmt.Errorf("security list-keychains: %w: %s", err, strings.TrimSpace(errb.String()))
	}
	for _, m := range keychainPathRE.FindAllStringSubmatch(out.String(), -1) {
		if strings.HasPrefix(filepath.Base(m[1]), "login.keychain") {
			return true, nil
		}
	}
	return false, nil
}

var _ Store = KeychainItem{}
