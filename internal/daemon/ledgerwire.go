package daemon

import "sort"

// ledgersWire assembles the daemon-owned self-heal ledger block.
func (s *Server) ledgersWire() []LedgerState {
	s.ledMu.Lock()
	rows := s.led.snapshot()
	s.ledMu.Unlock()
	return composeLedgersWire(rows)
}

func composeLedgersWire(rows []ledgerSnapshot) []LedgerState {
	if len(rows) == 0 {
		return nil
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Policy != rows[j].Policy {
			return rows[i].Policy < rows[j].Policy
		}
		return rows[i].Resource < rows[j].Resource
	})
	out := make([]LedgerState, len(rows))
	for i, r := range rows {
		out[i] = LedgerState(r)
	}
	return out
}
