//go:build !darwin

package daemon

import (
	"context"

	"github.com/yasyf/cc-pool/internal/overlay"
)

func watchSemanticInputs(ctx context.Context, _ overlay.SemanticInputPaths, mark func(dirtyCause)) error {
	mark(dirtyStartup)
	<-ctx.Done()
	return nil
}
