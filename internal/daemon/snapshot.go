package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
)

// writeStatusSnapshot failure must not stall polling.
func (s *Server) writeStatusSnapshot(ctx context.Context) error {
	accts, err := s.statuses(ctx)
	if err != nil {
		return fmt.Errorf("assemble status snapshot: %w", err)
	}
	data, err := json.Marshal(NewStatusSnapshot(accts, time.Now()))
	if err != nil {
		return fmt.Errorf("encode status snapshot: %w", err)
	}
	if err := overlay.WriteAtomic0600(s.snapshot, data); err != nil {
		return fmt.Errorf("write status snapshot %s: %w", s.snapshot, err)
	}
	return nil
}
