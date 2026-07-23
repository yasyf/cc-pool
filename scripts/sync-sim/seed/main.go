// Command seed fabricates and mutates Keychain-backed pool accounts for the
// two-host sync sim (scripts/sync-sim/run.sh). It drives cc-pool's own
// packages — store.Open, creds.KeychainItem, the pool identity writers, and
// hostsync — so the fabricated state matches what a real `ccp add` plus
// `claude /login` would leave on disk, using the sim's isolated fake Keychain.
// Every path resolves off $HOME, so the caller points HOME at /tmp/ccp-sim/{a,b}.
// It never talks to the real network or login Keychain.
//
// Subcommands:
//
//	seed init                     set the initialized + symlink-overlay meta, make ~/.claude base
//	seed account --id N ...       fabricate one exactly admitted logged-in Keychain account
//	seed rotate  --id N ...       write a fresh OWNED chain in place (models a login/rotation on this host)
//	seed setexp  --id N ...       rewrite the credential's expiresAt in place, tokens preserved
//	seed hash    --id N           print the current credential's AccessHash
//	seed rowuuid --id N           print the account row's stored account_uuid
//	seed wirecap --peer P --uuid U  fetch U's stripped envelope from peer P and print the raw wire bytes
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

type directTaskRunner struct{}

func (directTaskRunner) Run(ctx context.Context, task supervise.Task) error {
	command := exec.CommandContext(ctx, task.Path, task.Args...) //nolint:gosec // sim controls the exact security shim path
	command.Dir = task.Dir
	command.Env = task.Env
	command.Stdin = task.Stdin
	command.Stdout = task.Stdout
	command.Stderr = task.Stderr
	return command.Run()
}

func simCredentialStore(configDir string) creds.KeychainItem {
	return creds.KeychainItem{
		Service: creds.ServiceName(configDir), Account: creds.AccountLabel(), Runner: directTaskRunner{},
	}
}

func main() {
	if len(os.Args) < 2 {
		fatal(fmt.Errorf("usage: seed <init|account|rotate|setexp|hash|rowuuid|wirecap> [flags]"))
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
	case "setexp":
		err = cmdSetExp(args)
	case "hash":
		err = cmdHash(args)
	case "rowuuid":
		err = cmdRowUUID(args)
	case "wirecap":
		err = cmdWireCap(args)
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

func openStore() (*store.Store, error) {
	if err := pool.EnsureStateDir(); err != nil {
		return nil, err
	}
	return store.Open(pool.DBPath())
}

// cmdInit sets up the pool's source-of-truth state without starting FuseKit.
func cmdInit() error {
	db, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if err := os.MkdirAll(pool.FuseKitBackingRoot(), 0o700); err != nil {
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
	if err := db.SetMeta("initialized", "1"); err != nil {
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
	_ = fs.Parse(args)
	if *uuid == "" || *access == "" || *refresh == "" || *expiresMS == 0 {
		return fmt.Errorf("account needs --uuid, --access, --refresh, --expires-ms")
	}

	db, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	backingDir := pool.AccountBackingDir(*id)
	if err := os.MkdirAll(backingDir, 0o700); err != nil {
		return fmt.Errorf("create private account source: %w", err)
	}
	configDir := filepath.Join(
		os.Getenv("HOME"), "Library", "CloudStorage", fmt.Sprintf("CCPool-sim-acct-%02d", *id),
	)

	// Write the private identity exactly as the source authority expects.
	identPath := filepath.Join(backingDir, ".claude.json")
	identity, err := json.Marshal(map[string]any{
		"oauthAccount": map[string]string{
			"accountUuid":  *uuid,
			"emailAddress": *email,
		},
	})
	if err != nil {
		return err
	}
	if err := os.WriteFile(identPath, identity, 0o600); err != nil {
		return fmt.Errorf("write identity: %w", err)
	}

	cred := makeCred(*access, *refresh, *expiresMS)
	if err := simCredentialStore(configDir).Write(context.Background(), cred); err != nil {
		return fmt.Errorf("write Keychain credential: %w", err)
	}

	owner := proc.Record{
		RecoveryClass: proc.RecoveryTask,
		PID:           os.Getpid(), StartTime: "sync-sim", Boot: "sync-sim",
		Comm: "seed", Generation: *uuid,
	}
	reservation, err := db.ReserveAccountIndex(owner)
	if err != nil {
		return fmt.Errorf("reserve account row: %w", err)
	}
	if reservation.ID != *id {
		return fmt.Errorf("reserved account %d, want requested account %d", reservation.ID, *id)
	}
	acct := store.Account{
		ID:              reservation.ID,
		InstanceID:      reservation.InstanceID,
		Generation:      reservation.Generation,
		ConfigDir:       configDir,
		KeychainService: creds.ServiceName(configDir),
		KeychainAccount: creds.AccountLabel(),
		Label:           *label,
		AccountUUID:     *uuid,
		CreatedAt:       time.Now(),
	}
	proof := simPresentationProof(acct, "sync-sim-promotion")
	if err := db.PromoteReservedSyncedAccount(reservation, acct, proof); err != nil {
		return fmt.Errorf("promote account row: %w", err)
	}
	freshProof := proof
	freshProof.FileProvider.ActivationGeneration = "sync-sim-admitted"
	admitted, err := db.AdmitSyncedAccount(acct, proof, freshProof)
	if err != nil {
		return fmt.Errorf("admit account row: %w", err)
	}
	if !admitted {
		return errors.New("admit account row: awaiting-origin state was not cleared")
	}
	fmt.Printf("seeded acct-%02d uuid=%s hash=%s\n", *id, *uuid, creds.AccessHash(cred))
	return nil
}

func simPresentationProof(account store.Account, activation string) store.PresentationPreparationProof {
	tenantID := "account-" + account.InstanceID
	return store.PresentationPreparationProof{
		CatalogTenantID: tenantID, CatalogGeneration: account.Generation,
		Requested: 1, Desired: 1, Observed: 1, Verified: 1, Applied: 1,
		SourceAuthority: "sync-sim", SourceRevision: 1, CatalogRevision: 1,
		ChangeID: "sync-sim-change", OperationID: "sync-sim-operation",
		PresentationKind: store.PresentationKindFileProvider,
		FileProvider: store.FileProviderPreparationProof{
			TenantID: tenantID, DomainID: "domain-" + account.InstanceID,
			Generation: account.Generation, ActivationGeneration: activation,
			PublicPath: account.ConfigDir,
		},
	}
}

// cmdRotate writes a fresh OWNED chain in place: new tokens/expiry to the
// isolated Keychain, refresh token present. On a host that currently holds a synced (peer)
// copy this models `ccp login` minting the host its own origin chain; on the
// origin it models an out-of-band rotation.
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

	configDir, err := accountConfigDir(*id)
	if err != nil {
		return err
	}
	credentialStore := simCredentialStore(configDir)
	if _, err := credentialStore.Read(context.Background()); err != nil {
		return fmt.Errorf("read current credential: %w", err)
	}
	next := makeCred(*access, *refresh, *expiresMS)
	if err := credentialStore.Write(context.Background(), next); err != nil {
		return fmt.Errorf("write rotated credential: %w", err)
	}
	fmt.Printf("rotated acct-%02d hash=%s\n", *id, creds.AccessHash(next))
	return nil
}

// cmdSetExp rewrites the credential's expiresAt in place, preserving both
// tokens: near-expiry to force the origin's refresh, or past to expire a
// synced peer copy. A synced blob stays synced (no refresh token reappears).
func cmdSetExp(args []string) error {
	fs := flag.NewFlagSet("setexp", flag.ExitOnError)
	id := fs.Int("id", 1, "account index")
	expiresMS := fs.Int64("expires-ms", 0, "new expiresAt, unix millis")
	_ = fs.Parse(args)
	if *expiresMS == 0 {
		return fmt.Errorf("setexp needs --expires-ms")
	}
	configDir, err := accountConfigDir(*id)
	if err != nil {
		return err
	}
	credentialStore := simCredentialStore(configDir)
	cur, err := credentialStore.Read(context.Background())
	if err != nil {
		return fmt.Errorf("read current credential: %w", err)
	}
	cur.ClaudeAiOauth.ExpiresAt = *expiresMS
	if err := credentialStore.Write(context.Background(), cur); err != nil {
		return fmt.Errorf("write re-expired credential: %w", err)
	}
	fmt.Printf("setexp acct-%02d expiresAt=%d hash=%s\n", *id, *expiresMS, creds.AccessHash(cur))
	return nil
}

func cmdHash(args []string) error {
	fs := flag.NewFlagSet("hash", flag.ExitOnError)
	id := fs.Int("id", 1, "account index")
	_ = fs.Parse(args)
	configDir, err := accountConfigDir(*id)
	if err != nil {
		return err
	}
	cred, err := simCredentialStore(configDir).Read(context.Background())
	if err != nil {
		return err
	}
	fmt.Print(creds.AccessHash(cred))
	return nil
}

func accountConfigDir(id int) (string, error) {
	db, err := openStore()
	if err != nil {
		return "", err
	}
	defer func() { _ = db.Close() }()
	account, err := db.GetAccount(id)
	if err != nil {
		return "", err
	}
	return account.ConfigDir, nil
}

func cmdRowUUID(args []string) error {
	fs := flag.NewFlagSet("rowuuid", flag.ExitOnError)
	id := fs.Int("id", 1, "account index")
	_ = fs.Parse(args)
	db, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	a, err := db.GetAccount(*id)
	if err != nil {
		return err
	}
	fmt.Print(a.AccountUUID)
	return nil
}

// cmdWireCap issues the raw credential-fetch RPC to a peer and prints the exact
// envelope bytes that crossed the wire, so the harness can prove the origin's
// stripped envelope carries no refresh token. It bypasses FetchCredential's
// verification on purpose — the goal is the unfiltered wire payload.
func cmdWireCap(args []string) error {
	fs := flag.NewFlagSet("wirecap", flag.ExitOnError)
	peer := fs.String("peer", "", "peer transport string (exec:... or ssh target)")
	uuid := fs.String("uuid", "", "account uuid to fetch")
	_ = fs.Parse(args)
	if *peer == "" || *uuid == "" {
		return fmt.Errorf("wirecap needs --peer and --uuid")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return syncservice.WithTransportRunner(ctx, func(runner syncservice.TransportRunner) error {
		tx := hostsync.PeerTransport(runner, *peer)
		defer func() { _ = tx.Close() }()
		resp, err := tx.Do(ctx, &rpc.Request{Method: hostsync.MethodFetchCredential, Params: map[string]any{"uuid": *uuid}})
		if err != nil {
			return fmt.Errorf("fetch %s from peer: %w", *uuid, err)
		}
		if !resp.OK {
			return fmt.Errorf("peer refused fetch: %s", resp.Error)
		}
		_, _ = os.Stdout.Write(resp.Result)
		fmt.Println()
		return nil
	})
}

func makeCred(access, refresh string, expiresMS int64) *creds.Credential {
	return &creds.Credential{ClaudeAiOauth: creds.OAuth{
		AccessToken:      access,
		RefreshToken:     refresh,
		ExpiresAt:        expiresMS,
		SubscriptionType: "max",
	}}
}
