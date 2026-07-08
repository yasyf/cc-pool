package cli

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// reportSync runs the host-sync doctor section; silent unless sync is enabled.
// Warnings (warnf) never flip the doctor verdict — mesh reachability is
// best-effort, and uuid dupes wedge only removals.
func reportSync(ctx context.Context, m *pool.Manager, accts []store.Account, report func(string, bool, string), warnf func(string, string)) error {
	v, ok, err := m.Store.GetMeta(syncMetaKey)
	if err != nil {
		return fmt.Errorf("read %s meta: %w", syncMetaKey, err)
	}
	if !ok || v != "1" {
		return nil
	}
	reportSyncUUIDDupes(accts, m.Store.AccountsByUUID, report, warnf)
	reportSyncSocket(ctx, pool.SyncSocketPath(), report)
	reportSyncMesh(ctx, report, warnf)
	reportSyncRegistry(syncRegistryFile(), report)
	return nil
}

// reportSyncUUIDDupes warns on pool accounts sharing one Claude accountUuid:
// sync teardown refuses ambiguous uuids by design (a tombstone must never
// serially destroy every row sharing a uuid — ccn 10bf17d), so a pool-wide
// removal wedges until the duplicates are resolved.
func reportSyncUUIDDupes(accts []store.Account, byUUID func(string) ([]store.Account, error), report func(string, bool, string), warnf func(string, string)) {
	seen := make(map[string]bool, len(accts))
	for _, a := range accts {
		uuid := a.AccountUUID
		if uuid == "" || seen[uuid] {
			continue
		}
		seen[uuid] = true
		rows, err := byUUID(uuid)
		if err != nil {
			report("sync uuids", false, fmt.Sprintf("resolve uuid %s: %v", uuid, err))
			continue
		}
		if len(rows) < 2 {
			continue
		}
		names := make([]string, len(rows))
		for i, r := range rows {
			names[i] = fmt.Sprintf("acct-%02d", r.ID)
		}
		warnf("sync uuids", fmt.Sprintf(
			"%s share account uuid %s — sync teardown refuses ambiguous uuids by design, so a pool-wide removal of this account wedges until resolved (`ccp remove` the duplicate rows)",
			strings.Join(names, ", "), uuid))
	}
}

// syncSockCapabilities probes a sync socket for its svc.capabilities answer —
// a seam so doctor tests fake the daemon.
var syncSockCapabilities = func(ctx context.Context, sock string) (syncservice.Capabilities, error) {
	ctx, cancel := context.WithTimeout(ctx, syncProbeTimeout)
	defer cancel()
	cl := syncservice.NewClient(syncservice.Socket(sock))
	defer func() { _ = cl.Close() }()
	return cl.Capabilities(ctx)
}

// reportSyncSocket verifies the daemon's sync socket exists and answers
// svc.capabilities — the surface synckitd and peers pull from.
func reportSyncSocket(ctx context.Context, sock string, report func(string, bool, string)) {
	if _, err := os.Stat(sock); err != nil {
		report("sync socket", false, abbreviateHome(sock)+" missing — the daemon binds it at startup; is the daemon running? (`ccp service status`)")
		return
	}
	caps, err := syncSockCapabilities(ctx, sock)
	if err != nil {
		report("sync socket", false, fmt.Sprintf("%s not answering svc.capabilities: %v — restart the daemon (`brew services restart cc-pool`)", abbreviateHome(sock), err))
		return
	}
	report("sync socket", true, fmt.Sprintf("%s protocol v%d", caps.Name, caps.ProtocolVersion))
}

// loadMeshRegistry reads the shared synckit mesh identity — a seam so doctor
// tests feed a canned mesh.
var loadMeshRegistry = func() (*hostregistry.Registry, error) {
	return hostregistry.Mesh.Load()
}

// synckitdLive probes the synckitd daemon socket — a seam so doctor tests fake it.
var synckitdLive = func(ctx context.Context) bool {
	sock, err := hostregistry.Mesh.SockPath()
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, syncProbeTimeout)
	defer cancel()
	resp, err := rpc.Call(ctx, sock, &rpc.Request{Method: "status"})
	return err == nil && resp.OK
}

// reportSyncMesh checks the synckit mesh best-effort: an absent or stalled
// synckitd is a warning, never a doctor failure — notify fan-out and the
// reconcile tick live there, but single-host operation stays healthy.
func reportSyncMesh(ctx context.Context, report func(string, bool, string), warnf func(string, string)) {
	if _, err := synckitdLookPath(); err != nil {
		warnf("sync mesh", "synckitd is not installed — peer changes cannot propagate; brew install yasyf/tap/synckit")
		return
	}
	reg, err := loadMeshRegistry()
	if err != nil {
		warnf("sync mesh", fmt.Sprintf("cannot read the synckit mesh state: %v", err))
		return
	}
	if len(reg.Hosts) == 0 {
		warnf("sync mesh", "no peers registered — this host syncs with nobody; add one with `synckitd host add <peer>`")
		return
	}
	if !synckitdLive(ctx) {
		warnf("sync mesh", fmt.Sprintf("%s registered but synckitd is not running — notify fan-out and the reconcile tick are stalled; run `synckitd install`", plural(len(reg.Hosts), "peer")))
		return
	}
	report("sync mesh", true, fmt.Sprintf("self %s; %s", reg.Self, plural(len(reg.Hosts), "peer")))
}

// reportSyncRegistry loads the registry: a corrupt file is loud — the refresh
// gate reads a load failure as "no entry" and fails open, so every host may
// refresh this pool's chains (token double-spend) until it is fixed — ccn 10bf17d.
func reportSyncRegistry(rf hostsync.RegistryFile, report func(string, bool, string)) {
	reg, err := rf.Load()
	if err != nil {
		report("sync registry", false, fmt.Sprintf(
			"%v — while this persists the refresh gate FAILS OPEN (every host refreshes, double-spending token chains); repair or remove %s, then re-run `ccp sync enable`",
			err, abbreviateHome(rf.Path)))
		return
	}
	report("sync registry", true, plural(len(reg.Present()), "account"))
}
