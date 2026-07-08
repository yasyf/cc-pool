// Command seed fabricates and mutates file-backend pool accounts for the
// two-host sync sim (scripts/sync-sim/run.sh). It drives cc-pool's own
// packages — store.Open, creds.FileStore, the pool identity writers, and
// hostsync.Service — so the fabricated state matches what a real `ccp add`
// plus `claude /login` would leave on disk, minus any real token or Keychain
// item. Every path resolves off $HOME, so the caller points HOME at
// /tmp/ccp-sim/{a,b}. It never talks to the network or the Keychain.
//
// Subcommands:
//
//	seed init                     set the initialized + symlink-overlay meta, make ~/.claude base
//	seed account --id N ...       fabricate a logged-in file-backend account (row uuid left empty)
//	seed rotate  --id N ...       rotate the chain: new tokens, parent = hash(current)
//	seed publish --id N           force-publish (PublishAccount) — the tombstone-override re-add intent
//	seed hash    --id N           print the current credential's CredentialHash
//	seed rowuuid --id N           print the account row's stored account_uuid
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/synckit/hostregistry"
)

func main() {
	if len(os.Args) < 2 {
		fatal(fmt.Errorf("usage: seed <init|account|rotate|publish|hash|rowuuid> [flags]"))
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "init":
		err = cmdInit()
	case "account":
		err = cmdAccount(args)
	case "rotate":
		err = cmdRotate(args)
	case "publish":
		err = cmdPublish(args)
	case "hash":
		err = cmdHash(args)
	case "rowuuid":
		err = cmdRowUUID(args)
	default:
		err = fmt.Errorf("unknown subcommand %q", cmd)
	}
	if err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "seed:", err)
	os.Exit(1)
}

// cmdInit sets the pool up as an initialized, symlink-overlay pool and lays a
// minimal ~/.claude base so the symlink provider and the materializer have a
// source. Overlay is pinned to symlink so the sim never mounts fuse.
func cmdInit() error {
	m, err := pool.Open()
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()
	if err := pool.EnsureStateDir(); err != nil {
		return err
	}
	if err := pool.EnsureAccountsDir(); err != nil {
		return err
	}
	if err := os.MkdirAll(pool.SyncStampsDir(), 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(pool.ClaudeDir(), 0o700); err != nil {
		return fmt.Errorf("make ~/.claude: %w", err)
	}
	if _, err := os.Stat(pool.ClaudeJSONPath()); os.IsNotExist(err) {
		if err := os.WriteFile(pool.ClaudeJSONPath(), []byte("{}\n"), 0o600); err != nil {
			return fmt.Errorf("write ~/.claude.json: %w", err)
		}
	}
	if err := m.Store.SetMeta("overlay_kind", "symlink"); err != nil {
		return err
	}
	if err := m.Store.SetMeta("initialized", "1"); err != nil {
		return err
	}
	return nil
}

func cmdAccount(args []string) error {
	fs := flag.NewFlagSet("account", flag.ExitOnError)
	id := fs.Int("id", 1, "account index")
	uuid := fs.String("uuid", "", "Claude accountUuid (identity)")
	email := fs.String("email", "", "account email")
	label := fs.String("label", "", "account label")
	access := fs.String("access", "", "access token")
	refresh := fs.String("refresh", "", "refresh token")
	expiresMS := fs.Int64("expires-ms", 0, "access-token expiry, unix millis")
	setRowUUID := fs.Bool("set-row-uuid", false, "stamp the account_uuid column now (else left empty for backfill)")
	_ = fs.Parse(args)
	if *uuid == "" || *access == "" || *refresh == "" || *expiresMS == 0 {
		return fmt.Errorf("account needs --uuid, --access, --refresh, --expires-ms")
	}

	m, err := pool.Open()
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	configDir := pool.AccountDir(*id)
	prov, err := pool.OverlayProviderFor(fkoverlay.BackendSymlink)
	if err != nil {
		return err
	}
	if err := prov.Setup(pool.ClaudeDir(), configDir); err != nil {
		return fmt.Errorf("set up symlink overlay: %w", err)
	}

	// Write the private identity exactly as `claude /login` would: a .claude.json
	// object carrying oauthAccount. Bootstrap an empty object, then inject.
	identPath := filepath.Join(configDir, ".claude.json")
	if err := os.WriteFile(identPath, []byte("{}"), 0o600); err != nil {
		return fmt.Errorf("bootstrap identity file: %w", err)
	}
	oauthAccount, err := json.Marshal(map[string]string{
		"accountUuid":  *uuid,
		"emailAddress": *email,
	})
	if err != nil {
		return err
	}
	if err := pool.WriteIdentity(fkoverlay.BackendSymlink, configDir, oauthAccount); err != nil {
		return fmt.Errorf("write identity: %w", err)
	}

	cred := makeCred(*access, *refresh, *expiresMS)
	if err := (creds.FileStore{ConfigDir: configDir}).Write(cred); err != nil {
		return fmt.Errorf("write file credential: %w", err)
	}

	rowUUID := ""
	if *setRowUUID {
		rowUUID = *uuid
	}
	acct := store.Account{
		ID:              *id,
		ConfigDir:       configDir,
		KeychainService: creds.ServiceName(configDir),
		KeychainAccount: creds.AccountLabel(),
		Label:           *label,
		OverlayKind:     string(fkoverlay.BackendSymlink),
		AccountUUID:     rowUUID,
		CreatedAt:       time.Now(),
	}
	if err := m.Store.UpsertAccount(acct); err != nil {
		return fmt.Errorf("upsert account row: %w", err)
	}
	fmt.Printf("seeded acct-%02d uuid=%s hash=%s\n", *id, *uuid, creds.CredentialHash(cred))
	return nil
}

// cmdRotate rotates an account's chain in place: parent = hash(current cred),
// new tokens/expiry written to the file store, and the chain-hash columns
// updated — the same lineage bookkeeping writeCred does for a real refresh.
func cmdRotate(args []string) error {
	fs := flag.NewFlagSet("rotate", flag.ExitOnError)
	id := fs.Int("id", 1, "account index")
	access := fs.String("access", "", "new access token")
	refresh := fs.String("refresh", "", "new refresh token")
	expiresMS := fs.Int64("expires-ms", 0, "new access-token expiry, unix millis")
	_ = fs.Parse(args)
	if *access == "" || *refresh == "" || *expiresMS == 0 {
		return fmt.Errorf("rotate needs --access, --refresh, --expires-ms")
	}

	m, err := pool.Open()
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	configDir := pool.AccountDir(*id)
	fstore := creds.FileStore{ConfigDir: configDir}
	cur, err := fstore.Read()
	if err != nil {
		return fmt.Errorf("read current credential: %w", err)
	}
	parent := creds.CredentialHash(cur)

	next := makeCred(*access, *refresh, *expiresMS)
	if err := fstore.Write(next); err != nil {
		return fmt.Errorf("write rotated credential: %w", err)
	}
	if err := m.Store.SetChainHashes(*id, creds.CredentialHash(next), parent); err != nil {
		return fmt.Errorf("record chain hashes: %w", err)
	}
	fmt.Printf("rotated acct-%02d hash=%s parent=%s\n", *id, creds.CredentialHash(next), parent)
	return nil
}

// cmdPublish force-publishes an account to the shared registry, mirroring
// cli.syncPublisher.Publish: PublishAccount (the explicit re-add intent that
// overrides a tombstone) then a stamp touch. Stands in for `ccp add`'s publish
// hook, which the sim cannot reach without an interactive `claude /login`.
func cmdPublish(args []string) error {
	fs := flag.NewFlagSet("publish", flag.ExitOnError)
	id := fs.Int("id", 1, "account index")
	_ = fs.Parse(args)

	m, err := pool.Open()
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	a, err := m.Store.GetAccount(*id)
	if err != nil {
		return err
	}
	backend, err := fkoverlay.Parse(a.OverlayKind)
	if err != nil {
		return err
	}
	raw, ident, err := pool.AccountOAuth(backend, a.ConfigDir)
	if err != nil {
		return fmt.Errorf("read identity: %w", err)
	}
	cred, _, err := m.ReadCredential(a)
	if err != nil {
		return fmt.Errorf("read credential: %w", err)
	}
	hash := creds.CredentialHash(cred)
	parent := a.CredParentHash
	if a.CredHash != "" && a.CredHash != hash {
		parent = a.CredHash
	}

	self, err := meshSelf()
	if err != nil {
		return err
	}
	rf := hostsync.NewRegistryFile(pool.SyncDir())
	svc := &hostsync.Service{Registry: rf, StampDir: pool.SyncStampsDir()}
	v := hostsync.AccountValue{
		UUID:         ident.AccountUUID,
		Email:        ident.EmailAddress,
		Label:        a.Label,
		OAuthAccount: raw,
		Chain: hostsync.ChainStamp{
			ExpiresAt:  cred.ClaudeAiOauth.ExpiresAt,
			Hash:       hash,
			Holder:     self,
			ParentHash: parent,
			RotatedAt:  time.Now().UnixMilli(),
		},
	}
	if err := svc.PublishAccount(context.Background(), v); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	if a.AccountUUID != ident.AccountUUID {
		if err := m.Store.SetAccountUUID(a.ID, ident.AccountUUID); err != nil {
			return fmt.Errorf("tag row uuid: %w", err)
		}
	}
	fmt.Printf("published acct-%02d uuid=%s holder=%s\n", *id, ident.AccountUUID, self)
	return nil
}

func cmdHash(args []string) error {
	fs := flag.NewFlagSet("hash", flag.ExitOnError)
	id := fs.Int("id", 1, "account index")
	_ = fs.Parse(args)
	cred, err := (creds.FileStore{ConfigDir: pool.AccountDir(*id)}).Read()
	if err != nil {
		return err
	}
	fmt.Print(creds.CredentialHash(cred))
	return nil
}

func cmdRowUUID(args []string) error {
	fs := flag.NewFlagSet("rowuuid", flag.ExitOnError)
	id := fs.Int("id", 1, "account index")
	_ = fs.Parse(args)
	m, err := pool.Open()
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()
	a, err := m.Store.GetAccount(*id)
	if err != nil {
		return err
	}
	fmt.Print(a.AccountUUID)
	return nil
}

func makeCred(access, refresh string, expiresMS int64) *creds.Credential {
	return &creds.Credential{ClaudeAiOauth: creds.OAuth{
		AccessToken:      access,
		RefreshToken:     refresh,
		ExpiresAt:        expiresMS,
		SubscriptionType: "max",
	}}
}

// meshSelf resolves this host's registry identity from the hand-written synckit
// state.json (self), the same string peers dial as the chain holder.
func meshSelf() (string, error) {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return "", fmt.Errorf("load mesh: %w", err)
	}
	if reg.Self == "" {
		return "", fmt.Errorf("mesh self is empty (write state.json first)")
	}
	return reg.Self, nil
}
