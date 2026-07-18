package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
)

func newStoreCutoverCmd() *cobra.Command {
	var database, backup string
	cmd := &cobra.Command{
		Use:   "store-cutover",
		Short: "Replace the stopped v1 state database with the exact v2 schema",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if database == "" {
				database = pool.DBPath()
			}
			defaultDB, err := filepath.Abs(pool.DBPath())
			if err != nil {
				return err
			}
			targetDB, err := filepath.Abs(database)
			if err != nil {
				return err
			}
			isDefault, err := sameDatabaseFile(targetDB, defaultDB)
			if err != nil {
				return err
			}
			var socketLock *proc.FileLockHandle
			if isDefault {
				database = defaultDB
				socketLock, err = proc.FileLockSpec{
					Path: filepath.Clean(pool.SocketPath() + ".lock"),
					Mode: proc.FileLockExclusive, Deadline: time.Second,
				}.TryAcquire()
				if err != nil {
					return fmt.Errorf("store cutover requires the cc-pool service to be stopped: %w", err)
				}
				defer func() { _ = socketLock.Close() }()
			}
			result, err := store.Cutover(database, backup)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Cut over %d accounts; archived the v1 database at %s.\n", result.Accounts, result.Backup)
			return nil
		},
	}
	cmd.Flags().StringVar(&database, "database", "", "state database path (defaults to cc-pool's pool.db)")
	cmd.Flags().StringVar(&backup, "backup", "", "archive path (defaults to DATABASE.pre-v2)")
	return cmd
}

func sameDatabaseFile(path, expected string) (bool, error) {
	pathInfo, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("inspect cutover database: %w", err)
	}
	expectedInfo, err := os.Stat(expected)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("inspect default database: %w", err)
	}
	return os.SameFile(pathInfo, expectedInfo), nil
}
