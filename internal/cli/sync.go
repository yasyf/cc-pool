package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// syncMetaKey is the store-meta flag host sync keys on; the daemon reads the
// same key per request, so enable/disable needs no daemon restart.
const syncMetaKey = "sync_enabled"

// syncWatchDebounce batches one converge pass's burst of stamp touches into a
// single synckitd notification.
const syncWatchDebounce = 2 * time.Second

// syncProbeTimeout bounds the status command's sync-socket capabilities probe.
const syncProbeTimeout = 2 * time.Second

// syncDaemonSpawnTimeout bounds how long rpc-serve/converge wait for the
// daemon socket after spawning it.
const syncDaemonSpawnTimeout = 5 * time.Second

// synckitdLookPath resolves synckitd on PATH; a var so tests fake absence.
var synckitdLookPath = func() (string, error) { return exec.LookPath("synckitd") }

// synckitdRun best-effort execs synckitd; a var so tests record instead of exec.
var synckitdRun = func(ctx context.Context, args ...string) error {
	return exec.CommandContext(ctx, "synckitd", args...).Run() //nolint:gosec // G204: synckitd is a fixed cc-pool-managed binary; args are fixed subcommands
}

// syncEnsureDaemon reports the daemon reachable, spawning it if needed; a var
// so tests never spawn a real daemon.
var syncEnsureDaemon = func() bool {
	return daemon.NewClient().EnsureRunning(syncDaemonSpawnTimeout)
}

func newSyncCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Share the pool across hosts via synckit",
		Long: `sync makes this host's pool part of a synckit mesh: accounts, labels,
removals, and credential freshness converge across every enabled host.

    ccp sync enable
    ccp sync status

Requires synckitd (brew install yasyf/tap/synckit) and a mesh
(synckitd host add <user@host>). The shared registry is secretless; credentials
transit peer RPC only during a pull and land directly in the local Keychain.`,
	}
	cmd.AddCommand(
		newSyncEnableCmd(),
		newSyncDisableCmd(),
		newSyncStatusCmd(),
		newSyncRPCServeCmd(),
		newSyncConvergeCmd(),
	)
	return cmd
}

func newSyncEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "enable",
		Short: "Enable host sync: publish local accounts and register with synckitd",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				return runSyncEnable(cmd, m)
			})
		},
	}
}

func newSyncDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable",
		Short: "Disable host sync and unregister from synckitd",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				return runSyncDisable(cmd, m)
			})
		},
	}
}

func newSyncStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show mesh peers, sync socket health, and the shared account registry",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				return runSyncStatus(cmd, m)
			})
		},
	}
}

// newSyncRPCServeCmd is the stdio bridge synckitd and ssh peers drive; stdout
// is the framing channel, so nothing else may write there.
func newSyncRPCServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "rpc-serve",
		Short:  "Bridge sync RPC frames from stdin/stdout to the daemon's sync socket (used by synckitd)",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSyncRPCServe(cmd.Context(), os.Stdin, os.Stdout, syncEnsureDaemon, pool.SyncSocketPath())
		},
	}
}

func newSyncConvergeCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "converge",
		Short:  "Run one converge pass through the daemon's sync socket (debug)",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				return runSyncConverge(cmd.Context(), cmd.OutOrStdout(), syncEnsureDaemon, pool.SyncSocketPath())
			})
		},
	}
}

func runSyncEnable(cmd *cobra.Command, m *pool.Manager) error {
	out := cmd.OutOrStdout()
	if _, err := synckitdLookPath(); err != nil {
		return fmt.Errorf("synckitd is not on PATH; install it with `brew install yasyf/tap/synckit` and re-run `ccp sync enable`: %w", err)
	}
	if err := os.MkdirAll(pool.SyncStampsDir(), 0o700); err != nil {
		return fmt.Errorf("create sync dirs: %w", err)
	}
	if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
		return fmt.Errorf("enable sync: %w", err)
	}

	if err := syncBackfillUUIDs(out, m); err != nil {
		return err
	}

	mesh, err := hostregistry.Mesh.Load()
	if err != nil {
		warn(out, "mesh state unreadable: %v", err)
		mesh = &hostregistry.Registry{}
	}
	self, err := syncSelf(mesh)
	if err != nil {
		return err
	}
	published, err := syncScanPublish(cmd.Context(), m, self)
	if err != nil {
		return err
	}
	success(out, "Published %s to the shared registry.", plural(published, "account"))

	path, err := writeSyncManifest()
	if err != nil {
		return err
	}
	success(out, "Wrote synckit manifest %s.", path)

	if err := synckitdRun(cmd.Context(), "register", path); err != nil {
		warn(out, "synckitd register failed (%v); run `synckitd register %s` manually", err, path)
	}

	printMesh(out, mesh)
	return nil
}

func runSyncDisable(cmd *cobra.Command, m *pool.Manager) error {
	out := cmd.OutOrStdout()
	if err := m.Store.SetMeta(syncMetaKey, "0"); err != nil {
		return fmt.Errorf("disable sync: %w", err)
	}
	path, err := hostsync.ManifestPath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove sync manifest: %w", err)
	}
	if err := synckitdRun(cmd.Context(), "unregister", "cc-pool"); err != nil {
		warn(out, "synckitd unregister failed (%v); run `synckitd unregister cc-pool` manually", err)
	}
	success(out, "Host sync disabled; manifest removed.")
	note(out, "The shared registry and its tombstones are kept under %s; `ccp sync enable` re-joins.", pool.SyncDir())
	return nil
}

func runSyncStatus(cmd *cobra.Command, m *pool.Manager) error {
	out := cmd.OutOrStdout()
	enabled, _, err := m.Store.GetMeta(syncMetaKey)
	if err != nil {
		return err
	}
	if enabled == "1" {
		step(out, "Host sync: enabled")
	} else {
		step(out, "Host sync: disabled (run `ccp sync enable`)")
	}

	mesh, err := hostregistry.Mesh.Load()
	if err != nil {
		warn(out, "mesh state unreadable: %v", err)
	} else {
		printMesh(out, mesh)
	}

	probeSyncSocket(cmd.Context(), out)

	reg, err := syncRegistryFile().Load()
	if err != nil {
		warn(out, "shared registry unreadable: %v (see `ccp doctor`)", err)
	} else if len(reg) > 0 {
		printRegistryTable(out, reg, m)
	} else {
		note(out, "Shared registry is empty.")
	}

	return syncFileFallbackWarnings(out, m)
}

// runSyncRPCServe bridges frames between in/out and the daemon's sync socket.
// out is the framing channel: rpc.Proxy owns it exclusively.
func runSyncRPCServe(ctx context.Context, in io.Reader, out io.Writer, ensure func() bool, sock string) error {
	if !ensure() {
		return fmt.Errorf("cc-pool daemon is not reachable and could not be started; check `ccp doctor`")
	}
	return rpc.Proxy(ctx, in, out, sock)
}

// runSyncConverge asks the daemon for one converge pass over the sync socket.
func runSyncConverge(ctx context.Context, out io.Writer, ensure func() bool, sock string) error {
	if !ensure() {
		return fmt.Errorf("cc-pool daemon is not reachable and could not be started; check `ccp doctor`")
	}
	cl := syncservice.NewClient(syncservice.Socket(sock))
	defer func() { _ = cl.Close() }()
	res, err := cl.Sync(ctx, "")
	if err != nil {
		return fmt.Errorf("converge via %s: %w", sock, err)
	}
	success(out, "Converged %d item(s), %d deferred busy.", res.Converged, res.SkippedBusy)
	return nil
}

// syncBackfillUUIDs stamps each local row missing its Claude accountUuid from
// the account's own .claude.json identity; unreadable identities are skipped loudly.
func syncBackfillUUIDs(out io.Writer, m *pool.Manager) error {
	accts, err := m.Store.ListAccounts()
	if err != nil {
		return err
	}
	for _, a := range accts {
		if a.AccountUUID != "" {
			continue
		}
		backend, err := fkoverlay.Parse(a.OverlayKind)
		if err != nil {
			note(out, "acct-%02d: unparseable overlay backend; not published", a.ID)
			continue
		}
		ident, err := pool.AccountIdentity(backend, a.ConfigDir)
		if errors.Is(err, pool.ErrNoIdentity) {
			note(out, "acct-%02d: no readable identity; not published (finish `ccp login %d` first)", a.ID, a.ID)
			continue
		}
		if err != nil {
			return fmt.Errorf("read acct-%02d identity: %w", a.ID, err)
		}
		if err := m.Store.SetAccountUUID(a.ID, ident.AccountUUID); err != nil {
			return fmt.Errorf("backfill acct-%02d uuid: %w", a.ID, err)
		}
	}
	return nil
}

// syncSelf names this host as chain holder: the mesh ssh target when joined
// (peers dial the holder by it), else the hostname — the daemon's
// resolveSyncSelf must resolve identically. A host that cannot name itself
// must not publish: an empty Origin would corrupt chain ownership.
func syncSelf(mesh *hostregistry.Registry) (string, error) {
	if mesh.Self != "" {
		return mesh.Self, nil
	}
	host, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("resolve sync self: %w", err)
	}
	if host == "" {
		return "", fmt.Errorf("resolve sync self: kernel hostname is empty")
	}
	return host, nil
}

// syncRegistryFile is the shared-registry handle every CLI sync surface uses;
// same paths as the daemon's hostsync.NewRegistryFile(pool.SyncDir()).
func syncRegistryFile() hostsync.RegistryFile {
	return hostsync.RegistryFile{
		Path:     pool.SyncRegistryPath(),
		LockPath: pool.SyncRegistryLockPath(),
	}
}

// syncScanPublish folds local accounts into the shared registry, then touches
// the changed stamps. ScanPublish, never PublishAccount — a bulk publish would
// resurrect peers' removals — see ccn 10bf17d.
func syncScanPublish(ctx context.Context, m *pool.Manager, self string) (int, error) {
	rf := syncRegistryFile()
	svc := &hostsync.Service{
		Registry: &rf,
		StampDir: pool.SyncStampsDir(),
		Locals:   hostsync.ManagerLocals(m, self, time.Now),
	}
	changed := map[string]bool{}
	err := rf.Update(ctx, func(reg hostsync.Registry) error {
		before := make(map[string]string, len(reg))
		for id, entry := range reg {
			before[id] = hostsync.Fingerprint(entry)
		}
		if _, err := svc.ScanPublish(ctx, reg); err != nil {
			return err
		}
		for id, entry := range reg {
			if hostsync.Fingerprint(entry) != before[id] {
				changed[id] = true
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("publish local accounts: %w", err)
	}
	for uuid := range changed {
		if err := svc.TouchStamp(uuid); err != nil {
			return len(changed), err
		}
	}
	return len(changed), nil
}

// ccpoolManifest is the synckit manifest synckitd drives cc-pool with: fsnotify
// on the stamp dirs, the typed service on the sync socket, the rpc-serve stdio
// bridge. No launchd/helper blocks — cc-pool's daemon owns its own lifecycle.
func ccpoolManifest() manifest.Manifest {
	return manifest.Manifest{
		Name:   "cc-pool",
		Binary: "cc-pool",
		Brew:   "yasyf/tap/cc-pool",
		Watch: manifest.WatchSpec{
			Backend:  "fsnotify",
			Debounce: codec.Duration(syncWatchDebounce),
		},
		Service: manifest.ServiceSpec{
			Transport: "socket",
			ServeArgs: []string{"sync", "rpc-serve"},
			Sock:      pool.SyncSocketPath(),
		},
	}
}

// writeSyncManifest validates and writes the manifest (0600), returning its path.
func writeSyncManifest() (string, error) {
	m := ccpoolManifest()
	if err := m.Validate(); err != nil {
		return "", fmt.Errorf("build cc-pool manifest: %w", err)
	}
	path, err := hostsync.ManifestPath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("create manifests dir: %w", err)
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode cc-pool manifest: %w", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		return "", fmt.Errorf("write cc-pool manifest %s: %w", path, err)
	}
	return path, nil
}

func printMesh(out io.Writer, mesh *hostregistry.Registry) {
	if mesh.Self != "" {
		step(out, "Mesh self: %s", mesh.Self)
	}
	if len(mesh.Hosts) == 0 {
		note(out, "No mesh peers yet; add one with `synckitd host add <user@host>`.")
		return
	}
	step(out, "Mesh peers: %s", strings.Join(mesh.Hosts, ", "))
}

// probeSyncSocket reports the daemon's sync-socket health via svc.capabilities.
func probeSyncSocket(ctx context.Context, out io.Writer) {
	ctx, cancel := context.WithTimeout(ctx, syncProbeTimeout)
	defer cancel()
	cl := syncservice.NewClient(syncservice.Socket(pool.SyncSocketPath()))
	defer func() { _ = cl.Close() }()
	caps, err := cl.Capabilities(ctx)
	if err != nil {
		warn(out, "sync socket %s not answering (%v); is the daemon running with sync enabled?", pool.SyncSocketPath(), err)
		return
	}
	success(out, "Sync socket healthy: %s (protocol %d, %s).", caps.Name, caps.ProtocolVersion, plural(len(caps.Methods), "method"))
}

func printRegistryTable(out io.Writer, reg hostsync.Registry, m *pool.Manager) {
	uuids := make([]string, 0, len(reg))
	for uuid := range reg {
		uuids = append(uuids, uuid)
	}
	sort.Strings(uuids)
	local := localOwnershipByUUID(out, m)
	tw := tabwriter.NewWriter(out, 0, 2, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "UUID\tLABEL\tCHAIN EXPIRY\tORIGIN\tLOCAL\tSTATE")
	for _, uuid := range uuids {
		entry := reg[uuid]
		v := entry.Value
		state := "present"
		if !entry.Present() {
			state = "removed"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			uuid, orDash(v.Label), formatMillis(v.Chain.ExpiresAt), orDash(v.Chain.Origin), orDash(local[uuid]), state)
	}
	_ = tw.Flush()
}

// localOwnershipByUUID classifies each locally-held account's credential as
// "owned" (a refresh token present) or "synced" (a peer copy, none), keyed by
// account UUID — a refresh-free read, so it never spends a refresh token.
// Accounts with no local credential are simply absent (rendered "-"); a load
// failure warns to w rather than silently blanking the column.
func localOwnershipByUUID(w io.Writer, m *pool.Manager) map[string]string {
	out := map[string]string{}
	accts, err := m.Store.ListAccounts()
	if err != nil {
		warn(w, "listing accounts for the LOCAL column: %v", err)
		return out
	}
	for _, a := range accts {
		if a.AccountUUID == "" {
			continue
		}
		cred, _, err := m.ReadCredential(a)
		switch creds.ClassifyRead(err) {
		case creds.ReadPresent:
			if cred.HasRefreshToken() {
				out[a.AccountUUID] = "owned"
			} else {
				out[a.AccountUUID] = "synced"
			}
		case creds.ReadFatal:
			warn(w, "acct-%02d reading credential for the LOCAL column: %v", a.ID, err)
		}
	}
	return out
}

// syncFileFallbackWarnings flags accounts whose credential lives in the
// plaintext file store instead of the Keychain.
func syncFileFallbackWarnings(out io.Writer, m *pool.Manager) error {
	accts, err := m.Store.ListAccounts()
	if err != nil {
		return err
	}
	for _, a := range accts {
		_, src, err := m.ReadCredential(a)
		if err != nil {
			continue
		}
		if src == creds.SourceFile {
			warn(out, "acct-%02d credential is in the plaintext file store, not the Keychain (headless fallback)", a.ID)
		}
	}
	return nil
}

func formatMillis(ms int64) string {
	if ms == 0 {
		return "-"
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
