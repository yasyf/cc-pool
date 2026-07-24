package pool

import (
	"strings"
	"testing"
)

func TestHostSyncWorkersUseDistinctV1ProcessLedger(t *testing.T) {
	if HostSyncWorkerStorePath() == DisposableWorkerStorePath() {
		t.Fatal("host-sync and daemon workers share a process ledger")
	}
	if got := HostSyncWorkerStorePath(); !strings.HasSuffix(got, "hostsync-workers-v1.json") {
		t.Fatalf("host-sync worker ledger = %q, want v1 path", got)
	}
}
