package hostsync

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/yasyf/synckit/hostregistry"
)

// SynckitMesh resolves the host mesh from the shared synckit host registry
// (state.json under the synckit config dir): self is this host's ssh target —
// the name peers dial, so it doubles as the chain-holder identity — and peers
// are the other registered hosts. An unjoined host is a hard error, never an
// empty mesh that silently syncs nothing.
type SynckitMesh struct{}

// Resolve implements Mesh from the shared synckit host registry.
func (SynckitMesh) Resolve(context.Context) (string, []string, error) {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return "", nil, fmt.Errorf("read synckit host mesh: %w", err)
	}
	if reg.Self == "" {
		return "", nil, fmt.Errorf("this host has not joined the synckit mesh (run `synckitd register`)")
	}
	return reg.Self, reg.Hosts, nil
}

var _ Mesh = SynckitMesh{}

// ManifestPath is synckitd's discovery path for cc-pool's manifest,
// <synckit config dir>/manifests/cc-pool.json.
func ManifestPath() (string, error) {
	dir, err := hostregistry.Mesh.Dir()
	if err != nil {
		return "", fmt.Errorf("resolve synckit config dir: %w", err)
	}
	return filepath.Join(dir, "manifests", "cc-pool.json"), nil
}
