package pool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/overlay"
)

// ErrNoIdentity means a .claude.json has no usable "oauthAccount" identity.
var ErrNoIdentity = errors.New("no oauthAccount identity in .claude.json")

// Identity is the "oauthAccount" object claude writes into .claude.json at /login.
type Identity struct {
	AccountUUID  string
	EmailAddress string
}

func privateClaudeJSONPath(backingDir string) string {
	return filepath.Join(backingDir, ".claude.json")
}

// AccountIdentity reads one account identity through a killable backing worker.
func (m *Manager) AccountIdentity(
	ctx context.Context,
	accountID int,
	_ string,
) (*Identity, error) {
	response, err := m.runBackingWorker(ctx, backingWorkerRequest{
		Operation: backingWorkerReadIdentity,
		AccountID: accountID,
	})
	if err != nil {
		return nil, err
	}
	if response.Identity == nil {
		return nil, errors.New("account backing worker returned no identity")
	}
	return response.Identity, nil
}

// AccountOAuth reads one account's byte-exact OAuth identity through a killable worker.
func (m *Manager) AccountOAuth(
	ctx context.Context,
	accountID int,
	_ string,
) (json.RawMessage, *Identity, error) {
	response, err := m.runBackingWorker(ctx, backingWorkerRequest{
		Operation: backingWorkerReadOAuth,
		AccountID: accountID,
	})
	if err != nil {
		return nil, nil, err
	}
	if response.Identity == nil || len(response.OAuthAccount) == 0 {
		return nil, nil, errors.New("account backing worker returned incomplete OAuth identity")
	}
	return response.OAuthAccount, response.Identity, nil
}

// WriteIdentity writes one account's OAuth identity through a killable worker.
func (m *Manager) WriteIdentity(
	ctx context.Context,
	accountID int,
	_ string,
	oauthAccount json.RawMessage,
) error {
	_, err := m.runBackingWorker(ctx, backingWorkerRequest{
		Operation:    backingWorkerWriteIdentity,
		AccountID:    accountID,
		OAuthAccount: oauthAccount,
	})
	return err
}

func accountIdentityDirect(backingDir string) (*Identity, error) {
	_, identity, err := accountOAuthDirect(backingDir)
	return identity, err
}

func accountOAuthDirect(backingDir string) (json.RawMessage, *Identity, error) {
	return readIdentityRaw(privateClaudeJSONPath(backingDir))
}

func writeIdentityDirect(backingDir string, oauthAccount json.RawMessage) error {
	path := privateClaudeJSONPath(backingDir)
	src, err := os.ReadFile(path) //nolint:gosec // G304: path is a cc-pool-managed account .claude.json under the state dir
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(src, &top); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	if top == nil {
		return fmt.Errorf("parse %s: not a JSON object", path)
	}
	top["oauthAccount"] = oauthAccount
	out, err := json.Marshal(top)
	if err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}
	if err := overlay.WriteAtomic0600(path, out); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func readIdentityRaw(path string) (json.RawMessage, *Identity, error) {
	src, err := os.ReadFile(path) //nolint:gosec // G304: path is a cc-pool-managed account .claude.json under the state dir
	if os.IsNotExist(err) {
		return nil, nil, ErrNoIdentity
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", path, err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(src, &top); err != nil {
		return nil, nil, fmt.Errorf("parse %s: %w", path, err)
	}
	raw, ok := top["oauthAccount"]
	if !ok {
		return nil, nil, ErrNoIdentity
	}
	var fields struct {
		AccountUUID  string `json:"accountUuid"`
		EmailAddress string `json:"emailAddress"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, nil, fmt.Errorf("parse oauthAccount in %s: %w", path, err)
	}
	if fields.AccountUUID == "" {
		return nil, nil, ErrNoIdentity
	}
	return raw, &Identity{
		AccountUUID:  fields.AccountUUID,
		EmailAddress: fields.EmailAddress,
	}, nil
}
