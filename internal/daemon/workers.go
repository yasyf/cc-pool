package daemon

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/daemonkit/proc"
)

func newProcessReaper() (*proc.Reaper, error) {
	return newProcessReaperAt(pool.DisposableWorkerStorePath())
}

func newProcessReaperAt(path string) (*proc.Reaper, error) {
	var generation [16]byte
	if _, err := rand.Read(generation[:]); err != nil {
		return nil, fmt.Errorf("generate process generation: %w", err)
	}
	return &proc.Reaper{
		Store: &proc.FileStore{Path: path}, Generation: hex.EncodeToString(generation[:]),
	}, nil
}
