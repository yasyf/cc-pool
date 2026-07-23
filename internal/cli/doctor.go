package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
)

func newDoctorCmd() *cobra.Command {
	var fix bool
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check the account store, daemon, credentials, and FuseKit readiness",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(cmd.Context(), func(manager *pool.Manager) error {
				if fix {
					ensureDaemon(cmd)
				}
				return runDoctor(cmd, manager)
			})
		},
	}
	cmd.Flags().BoolVar(&fix, "fix", false, "start the current daemon before checking")
	return cmd
}

func runDoctor(cmd *cobra.Command, manager *pool.Manager) error {
	initialized, err := manager.Initialized()
	if err != nil {
		return err
	}
	if !initialized {
		return pool.ErrNotInitialized
	}
	accounts, err := manager.Store.ListAccounts()
	if err != nil {
		return fmt.Errorf("list accounts: %w", err)
	}
	client := daemon.NewClient()
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(cmd.Context(), 3*time.Second)
	defer cancel()
	health, err := client.HealthContext(ctx)
	if err != nil {
		fail(cmd.ErrOrStderr(), "daemon/FuseKit readiness: %v", err)
		return fmt.Errorf("doctor found unhealthy state")
	} else if health.RuntimeBuild != version.String() {
		fail(cmd.ErrOrStderr(), "daemon version %s does not match client %s", health.RuntimeBuild, version.String())
		return fmt.Errorf("doctor found unhealthy state")
	}
	success(cmd.OutOrStdout(),
		"Daemon and FuseKit runtime are ready (%s; active reservations=%d sessions=%d exclusive claims=%d).",
		health.RuntimeBuild, health.ActiveReservations, health.ActiveSessions, health.ExclusiveClaims)
	failed := false
	for _, account := range accounts {
		if err := client.AccountHealth(cmd.Context(), account.ID); err != nil {
			fail(cmd.ErrOrStderr(), "acct-%02d backing or credential: %v", account.ID, err)
			failed = true
			continue
		}
		success(cmd.OutOrStdout(), "acct-%02d backing identity and credential stores are healthy.", account.ID)
	}
	if failed {
		return fmt.Errorf("doctor found unhealthy state")
	}
	return nil
}
