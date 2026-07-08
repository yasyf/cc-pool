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

// AccountIdentity returns a pool account's identity from its private
// .claude.json. The path is pure math off the backend, so a read works whether
// or not a mount, holder, or domain is up. Only a symlink row keeps its
// identity in the account dir: fuse AND fileprovider rows hold it in the
// shared private backing root, and their account dir is a bridge symlink a
// read must never traverse (unbounded through a mirror or a materializing
// domain, and the domain serves the MERGED .claude.json — base identity, not
// the account's own).
func AccountIdentity(backend fkoverlay.Backend, configDir string) (*Identity, error) {
	priv := configDir
	if backend != fkoverlay.BackendSymlink {
		priv = fkoverlay.FusePrivateRoot(configDir)
	}
	return readIdentity(filepath.Join(priv, ".claude.json"))
}

// WriteIdentity injects oauthAccount verbatim into a pool account's private
// .claude.json, the inverse of AccountIdentity: same private-root path math,
// merged into the existing document (seeded by PrepareAdd) preserving every
// sibling key, written atomically. A missing or unparseable file fails loud —
// it never seeds a fresh document.
func WriteIdentity(backend fkoverlay.Backend, configDir string, oauthAccount json.RawMessage) error {
	priv := configDir
	if backend != fkoverlay.BackendSymlink {
		priv = fkoverlay.FusePrivateRoot(configDir)
	}
	path := filepath.Join(priv, ".claude.json")
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
	src, err := os.ReadFile(path) //nolint:gosec // G304: path is a cc-pool-managed account .claude.json under the state dir
	if os.IsNotExist(err) {
		return nil, ErrNoIdentity
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(src, &top); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	raw, ok := top["oauthAccount"]
	if !ok {
		return nil, ErrNoIdentity
	}
	var fields struct {
		AccountUUID  string `json:"accountUuid"`
		EmailAddress string `json:"emailAddress"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("parse oauthAccount in %s: %w", path, err)
	}
	if fields.AccountUUID == "" {
		return nil, ErrNoIdentity
	}
	return &Identity{
		AccountUUID:  fields.AccountUUID,
		EmailAddress: fields.EmailAddress,
	}, nil
}
