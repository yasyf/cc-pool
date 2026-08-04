package daemon

import (
	"fmt"

	"github.com/yasyf/cc-pool/internal/accountterminal"
	"github.com/yasyf/daemonkit"
)

// openAccountTerminals is the one seam blocked on the accountterminal
// successor (lane ccpool-migration, task #104): the manager's spawn substrate
// moves from the deleted daemonkit/proc reaper onto Serve's process scope, so
// crashed generations come back as Ctx.Reclaimed and the old Recover pass has
// no successor.
func (s *Server) openAccountTerminals(scope daemonkit.Ctx) (*accountterminal.Manager, error) {
	terminals, err := accountterminal.NewManager(accountTerminalLimit, scope)
	if err != nil {
		return nil, fmt.Errorf("open account terminals: %w", err)
	}
	return terminals, nil
}
