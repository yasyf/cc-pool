package daemon

import "sort"

// ledgersWire assembles the composed Ledgers block for the status wire and the
// on-disk snapshot: the Server-owned store's rows under ledMu plus the holder
// cache's fuse verdict rows under the holder's own mu — the two locks taken and
// released separately, never nested; bookkeeping only, no I/O under either. The
// stores stay separate by design (the holder rows reset in lockstep with its
// mount cache); only this read composes them. Rows are sorted by policy then
// resource so the wire view is deterministic.
func (s *Server) ledgersWire() []LedgerState {
	s.ledMu.Lock()
	rows := s.led.snapshot()
	s.ledMu.Unlock()
	rows = append(rows, s.holder.ledgersSnapshot()...)
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
