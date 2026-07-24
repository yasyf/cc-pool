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
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
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

// synckitdLookPath resolves synckitd on PATH; a var so tests fake absence.
var synckitdLookPath = func() (string, error) { return exec.LookPath("synckitd") }

// synckitdRun best-effort execs synckitd; a var so tests record instead of exec.
var synckitdRun = func(ctx context.Context, args ...string) error {
	return exec.CommandContext(ctx, "synckitd", args...).Run() //nolint:gosec // G204: synckitd is a fixed cc-pool-managed binary; args are fixed subcommands
}

// syncEnsureDaemon reports whether the daemonkit-managed service is reachable;
// a var so tests never dial a real daemon.
var syncEnsureDaemon = func(ctx context.Context) bool {
	cl := daemon.NewClient()
	defer func() { _ = cl.Close() }()
	health, err := cl.HealthContext(ctx)
	return err == nil && health.RuntimeBuild == version.String()
}

var syncConverge = runSyncConverge

func newSyncCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Share the pool across hosts via synckit",
		Long: `sync makes this host's pool part of a synckit mesh: accounts, labels,
removals, and credential freshness converge across every enabled host.

    ccp sync enable
    ccp sync status

Requires synckitd (brew install yasyf/tap/synckit) and a mesh
(synckitd host add <user@host>). Synckit delivers immutable revisioned snapshots;
refresh tokens never leave their origin, and access-only credentials land directly
in the local Keychain.`,
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
			return withManager(cmd.Context(), func(m *pool.Manager) error {
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
			return withManager(cmd.Context(), func(m *pool.Manager) error {
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
			return withManager(cmd.Context(), func(m *pool.Manager) error {
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
			return withManager(cmd.Context(), func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				ensureDaemon(cmd)
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
	if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
		return fmt.Errorf("enable sync: %w", err)
	}
	ensureDaemon(cmd)

	mesh, err := hostregistry.Mesh.Load()
	if err != nil {
		warn(out, "mesh state unreadable: %v", err)
		mesh = &hostregistry.Registry{}
	}
	if err := syncConverge(cmd.Context(), out, syncEnsureDaemon, pool.SyncSocketPath()); err != nil {
		return err
	}

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

	reg, err := syncRegistryState(cmd.Context(), pool.SyncSocketPath())
	if err != nil {
		warn(out, "shared registry unreadable: %v", err)
	} else if len(reg) > 0 {
		printRegistryTable(cmd.Context(), out, reg, m)
	} else {
		note(out, "Shared registry is empty.")
	}

	return nil
}

func syncRegistryState(ctx context.Context, sock string) (hostsync.Registry, error) {
	cl := syncservice.NewClient(syncservice.Socket(sock))
	defer func() { _ = cl.Close() }()
	change, err := cl.Export(ctx, syncservice.ExportRequest{
		ServiceID: hostsync.SyncServiceID, SchemaFingerprint: hostsync.SyncSchemaFingerprint,
		SinceRevision: syncservice.NewRevision(0),
	})
	if err != nil {
		return nil, err
	}
	return hostsync.RegistryFromExport(change)
}

// runSyncRPCServe bridges frames between in/out and the daemon's sync socket.
// out is the framing channel: rpc.Proxy owns it exclusively.
func runSyncRPCServe(ctx context.Context, in io.Reader, out io.Writer, ensure func(context.Context) bool, sock string) error {
	if !ensure(ctx) {
		return fmt.Errorf("cc-pool daemon is not reachable; run `ccp service install`")
	}
	return rpc.Proxy(ctx, in, out, sock)
}

// runSyncConverge asks the daemon for one converge pass over the sync socket.
func runSyncConverge(ctx context.Context, out io.Writer, ensure func(context.Context) bool, sock string) error {
	if !ensure(ctx) {
		return fmt.Errorf("cc-pool daemon is not reachable; run `ccp service install`")
	}
	cl := syncservice.NewClient(syncservice.Socket(sock))
	defer func() { _ = cl.Close() }()
	res, err := cl.Reconcile(ctx, "")
	if err != nil {
		return fmt.Errorf("converge via %s: %w", sock, err)
	}
	success(out, "Converged %d item(s), %d deferred busy.", res.Converged, res.SkippedBusy)
	return nil
}

// ccpoolManifest is the synckit manifest synckitd drives cc-pool with: stamp
// directory watches, the typed service on the sync socket, and the rpc-serve
// stdio bridge. No helper blocks; cc-pool's daemon owns its own lifecycle.
func ccpoolManifest() manifest.Manifest {
	return manifest.Manifest{
		Name:   "cc-pool",
		Binary: "cc-pool",
		Brew:   "yasyf/tap/cc-pool",
		Watch: manifest.WatchSpec{
			Debounce: codec.Duration(syncWatchDebounce),
		},
		Service: manifest.ServiceSpec{
			Kind: "resident", Socket: pool.SyncSocketPath(),
			SchemaFingerprint: hostsync.SyncSchemaFingerprint,
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
	success(out, "Sync socket healthy: %s (%s).", caps.Name, plural(len(caps.Methods), "method"))
}

func printRegistryTable(
	ctx context.Context,
	out io.Writer,
	reg hostsync.Registry,
	m *pool.Manager,
) {
	uuids := make([]string, 0, len(reg))
	for uuid := range reg {
		uuids = append(uuids, uuid)
	}
	sort.Strings(uuids)
	local := localOwnershipByUUID(ctx, out, m)
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

// localOwnershipByUUID reports local registry presence without opening a
// credential store. Credential inspection is daemon-only.
func localOwnershipByUUID(
	_ context.Context,
	w io.Writer,
	m *pool.Manager,
) map[string]string {
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
		out[a.AccountUUID] = "present"
	}
	return out
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
