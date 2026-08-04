package pool

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/testhome"
)

// TestCredentialLockRecoveryAdoptsVZeroTwentyNineJournal is §B's arm for the
// one durable file that decodes its worker field: a journal and marker left by
// a crashed v0.20.9 holder mid-acquisition — worker a literal proc.Record
// object — recover under the standard crash path, and acquisition unblocks.
func TestCredentialLockRecoveryAdoptsVZeroTwentyNineJournal(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	configDir := prepareTestAccountConfigDir(t, 1)
	paths, err := credentialRefreshLockPaths(configDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(paths[0], 0o700); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := credentialLockFingerprintForPath(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	workerRaw := json.RawMessage(poolUpgradeGoldenOwner)
	nonce := "00112233445566778899aabbccddeeff"
	marker, err := json.Marshal(credentialLockMarker{
		Schema: credentialLockJournalSchema, AccountID: 1, Nonce: nonce,
		Worker: workerRaw, Target: paths[0], Fingerprint: fingerprint,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(paths[0], lockMarkerName), marker, 0o600,
	); err != nil {
		t.Fatal(err)
	}
	journal, err := json.Marshal(credentialLockJournal{
		Schema: credentialLockJournalSchema, AccountID: 1, Nonce: nonce, Worker: workerRaw,
		Targets: []credentialLockTarget{
			{
				Path: paths[0], Stage: credentialLockStagePath(paths[0], nonce, 0),
				Phase: credentialLockAcquired, Fingerprint: fingerprint,
			},
			{
				Path: paths[1], Stage: credentialLockStagePath(paths[1], nonce, 1),
				Phase: credentialLockIntended,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	journalPath := credentialLockJournalPath(1)
	if err := os.MkdirAll(filepath.Dir(journalPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(journalPath, journal, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	lease, err := acquireCredentialRefreshLocks(ctx, mintCredentialLockOwner(t), 1, configDir)
	if err != nil {
		t.Fatalf("acquisition over a v0.20.9 journal = %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	assertCredentialLockResidueGone(t, 1, configDir)
}
