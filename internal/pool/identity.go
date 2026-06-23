package pool

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// ErrNoIdentity means a .claude.json has no usable top-level "oauthAccount"
// identity (file missing, key absent, or accountUuid empty).
var ErrNoIdentity = errors.New("no oauthAccount identity in .claude.json")

// Identity is the top-level "oauthAccount" object claude writes into its
// .claude.json after /login.
type Identity struct {
	AccountUUID  string
	EmailAddress string
}

// AccountIdentity returns a pool account's identity from its private
// .claude.json, written by that account's own login. The private path is pure
// path math off the recorded backend — no provider is resolved, because reading
// the file must work whether or not a mount or holder is up: fuse keeps it in
// the private backing dir beside the mountpoint, every other backend in the
// account dir itself.
func AccountIdentity(backend fkoverlay.Backend, configDir string) (*Identity, error) {
	priv := configDir
	if backend.IsFuse() {
		priv = fkoverlay.FusePrivateRoot(configDir)
	}
	return readIdentity(filepath.Join(priv, ".claude.json"))
}

// readIdentity parses the top-level "oauthAccount" object out of a
// .claude.json. Missing file or missing/empty identity → ErrNoIdentity;
// unparseable JSON is a real error.
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
