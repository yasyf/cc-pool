package daemon

import (
	"errors"
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/yasyf/cc-pool/internal/pool"
)

// sweepLegacyAccountTerminals refuses the boot while the v0.20.9
// account-terminal ledger still holds records. A record means that era's
// daemon was tracking an interactive `claude auth login` when it died, and a
// surviving login would overwrite credentials this daemon manages.
//
// The check reads only the ledger — deliberately. Three review rounds of
// process-table inspection each produced a new race (check-then-kill,
// fork-during-snapshot, zombies defeating getsid): the machine cannot
// reliably prove a login session is empty, and the human can just look at
// their screen. The refusal therefore names one command to clear the ledger
// once the user has confirmed no login is open, and the daemon's restart
// policy retries until then. A ledger with no records — the ordinary upgrade,
// where every terminal settled before the old daemon exited — archives
// silently and the boot proceeds.
//
// One transition cycle only — ships in v0.21.x, deleted in v0.22, the same
// lifespan as the cross-era gate.
func sweepLegacyAccountTerminals() error {
	path := pool.AccountTerminalProcessStorePath()
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect legacy account-terminal ledger: %w", err)
	}
	count, err := countLegacyTerminalRecords(path)
	if err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf(
			"cc-pool refused to start: %d interactive login terminal(s) from before the upgrade are "+
				"still recorded, and a surviving login would overwrite credentials this daemon manages. "+
				"Confirm no `claude auth login` window is open, then run `rm '%s'`; the daemon retries "+
				"automatically and starts once the ledger is cleared",
			count, path,
		)
	}
	if err := os.Rename(path, path+".archived"); err != nil {
		return fmt.Errorf("archive legacy account-terminal ledger: %w", err)
	}
	return nil
}

func countLegacyTerminalRecords(path string) (int, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
	if err != nil {
		return 0, fmt.Errorf("open legacy account-terminal ledger: %w", err)
	}
	defer func() { _ = db.Close() }()
	count := 0
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("records"))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(_, _ []byte) error {
			count++
			return nil
		})
	})
	if err != nil {
		return 0, fmt.Errorf("read legacy account-terminal ledger: %w", err)
	}
	return count, nil
}
