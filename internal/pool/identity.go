package pool

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/state"
)

// ErrNoIdentity means a .claude.json has no usable "oauthAccount" identity.
var ErrNoIdentity = errors.New("no oauthAccount identity in .claude.json")

// Identity is the "oauthAccount" object claude writes into .claude.json at /login.
type Identity struct {
	AccountUUID  string
	EmailAddress string
}

// privateClaudeJSONPath is the account's private .claude.json — pure path
// math off the backend (see AccountIdentity for why the account dir is never
// traversed).
func privateClaudeJSONPath(backend fkoverlay.Backend, configDir string) string {
	priv := configDir
	if backend != fkoverlay.BackendSymlink {
		priv = fkoverlay.FusePrivateRoot(configDir)
	}
	return filepath.Join(priv, ".claude.json")
}

// AccountIdentity returns a pool account's identity from its private .claude.json.
// The path is pure math off the backend, so a read works whether or not a mount,
// holder, or domain is up. Only a symlink row keeps its identity in the account dir;
// fuse and fileprovider rows hold it in the shared private backing root, and their
// account dir is a bridge symlink a read must never traverse. See ccn doc d1ab40f.
func AccountIdentity(backend fkoverlay.Backend, configDir string) (*Identity, error) {
	_, id, err := readIdentityRaw(privateClaudeJSONPath(backend, configDir))
	return id, err
}

// AccountOAuth returns the verbatim oauthAccount object plus the parsed
// identity from a pool account's private .claude.json — the byte-exact
// payload a sync-registry publish carries.
func AccountOAuth(backend fkoverlay.Backend, configDir string) (json.RawMessage, *Identity, error) {
	return readIdentityRaw(privateClaudeJSONPath(backend, configDir))
}

// WriteIdentity injects oauthAccount verbatim into a pool account's private
// .claude.json, preserving sibling keys; a missing or unparseable file fails
// loud — it never seeds a fresh document.
func WriteIdentity(backend fkoverlay.Backend, configDir string, oauthAccount json.RawMessage) error {
	path := privateClaudeJSONPath(backend, configDir)
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
	if err := state.AtomicWrite(path, out, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func readIdentity(path string) (*Identity, error) {
	_, id, err := readIdentityRaw(path)
	return id, err
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
