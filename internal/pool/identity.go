package pool

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	fkoverlay "github.com/yasyf/fusekit/overlay"
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
// or not a mount or holder is up.
func AccountIdentity(backend fkoverlay.Backend, configDir string) (*Identity, error) {
	priv := configDir
	if backend.IsFuse() {
		priv = fkoverlay.FusePrivateRoot(configDir)
	}
	return readIdentity(filepath.Join(priv, ".claude.json"))
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
