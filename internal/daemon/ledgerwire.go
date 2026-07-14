package daemon

import "sort"

// ledgersWire assembles the composed Ledgers block for the on-disk snapshot: the
// Server-owned store's rows under ledMu plus the holder cache's fuse verdict rows
// under the holder's own mu — the two locks taken and released separately, never
// nested; bookkeeping only, no I/O under either.
func (s *Server) ledgersWire() []LedgerState {
	s.ledMu.Lock()
	rows := s.led.snapshot()
	s.ledMu.Unlock()
	return composeLedgersWire(rows, s.holder.ledgersSnapshot())
}

// composeLedgersWire folds the Server-owned store's snapshot and the holder cache's
// fuse-verdict rows into the sorted wire block. The stores stay separate by design
// (the holder rows reset in lockstep with its mount cache); only this composes them.
// Rows sort by policy then resource so the wire view is deterministic.
func composeLedgersWire(ledRows, holderRows []ledgerSnapshot) []LedgerState {
	rows := append(ledRows, holderRows...)
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
