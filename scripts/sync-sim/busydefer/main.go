// Command busydefer runs ONE converge pass for host B entirely in-process, with
// an injectable "busy" verdict, for the sim's busy-defer scenario. A live
// session on B is not fakeable from outside the daemon (serverSessions.Busy
// reads procscan live processes, in-memory reservations, and convert claims),
// so this runner rebuilds the same hostsync.Service the daemon's setupSync
// wires — real Manager, registry, driver, fetcher, and mesh — but swaps in a
// fake Sessions seam. It is the sanctioned substitution the sim plan calls out.
//
// Everything else is the real path: it fetches the peer registry over the
// exec: transport (the peer's own daemon), merges, reconciles, and runs the
// teardown pass, so a --busy=true run must defer (SkippedBusy) and a
// --busy=false run must tear the tombstoned account down for real.
//
//	busydefer --busy=true|false
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/synckit/converge"
)

// fakeSessions is the injected liveness seam: it reports every account busy
// when busy is set, so the teardown pass defers instead of removing.
type fakeSessions struct{ busy bool }

func (f fakeSessions) Busy(context.Context, string) (bool, string, error) {
	if f.busy {
		return true, "sim: fake live session on host B", nil
	}
	return false, "", nil
}

// allowClaims stands in for the daemon's convert-claim seam: with no live
// selects in-process, every teardown claim succeeds.
type allowClaims struct{}

func (allowClaims) TryClaim(string) (func(), bool) { return func() {}, true }

func main() {
	busy := flag.Bool("busy", false, "report every account busy (defer teardown)")
	flag.Parse()

	if err := run(*busy); err != nil {
		fmt.Fprintln(os.Stderr, "busydefer:", err)
		os.Exit(1)
	}
}

func run(busy bool) error {
	m, err := pool.Open()
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	self, _, err := (hostsync.SynckitMesh{}).Resolve(context.Background())
	if err != nil {
		return fmt.Errorf("resolve mesh self: %w", err)
	}
	manifestPath, err := hostsync.ManifestPath()
	if err != nil {
		return err
	}

	logger := log.New(io.Discard, "", 0)
	if os.Getenv("BUSYDEFER_VERBOSE") != "" {
		logger = log.New(os.Stderr, "[busydefer] ", 0)
	}

	svc := &hostsync.Service{
		M:        m,
		Registry: hostsync.NewRegistryFile(pool.SyncDir()),
		StampDir: pool.SyncStampsDir(),
		Log:      logger,
		Locals:   hostsync.ManagerLocals(m, self, time.Now),
		Mesh:     hostsync.SynckitMesh{},
		Sessions: fakeSessions{busy: busy},
		Claims:   allowClaims{},
		Status:   converge.NewPeerStatus(),
		Fetcher:  hostsync.NewSSHFetcher(),
	}
	pull := func(ctx context.Context, uuid string, chain hostsync.ChainStamp, localExpiresAt int64, peers []string) (*creds.Credential, error) {
		return hostsync.FetchCredential(ctx, hostsync.PeerTransport, uuid, chain, localExpiresAt, peers)
	}
	svc.Driver = hostsync.NewDriver(svc, hostsync.DriverDeps{
		Store:      m.Store,
		Cred:       m,
		LocalIndex: hostsync.ManagerLocalIndex(m),
		Materialize: func(ctx context.Context, v hostsync.AccountValue, peers []string) (hostsync.MaterializeResult, error) {
			noLocal := func(ctx context.Context, uuid string, chain hostsync.ChainStamp, peers []string) (*creds.Credential, error) {
				return pull(ctx, uuid, chain, 0, peers)
			}
			return svc.Materialize(ctx, v, peers, noLocal, manifestPath)
		},
		Pull: pull,
	})

	res, err := svc.Converge(context.Background(), "")
	if err != nil {
		return fmt.Errorf("converge: %w", err)
	}
	accts, err := m.Store.ListAccounts()
	if err != nil {
		return err
	}
	out, _ := json.Marshal(map[string]int{
		"converged":   res.Converged,
		"skippedBusy": res.SkippedBusy,
		"accounts":    len(accts),
	})
	fmt.Println(string(out))
	return nil
}
