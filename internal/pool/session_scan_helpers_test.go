package pool

import (
	"context"

	"github.com/yasyf/cc-pool/internal/procscan"
)

func noPoolSessions(context.Context) ([]procscan.Session, error) {
	return nil, nil
}
