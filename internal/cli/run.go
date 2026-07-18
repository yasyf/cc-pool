package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/execguard"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/fileproviderd"
	"github.com/yasyf/fusekit/lease"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// ccpAccountEnv forces a specific account for `ccp run`, which parses no flags
// (every arg passes through to claude) — hence an env var, not a flag.
const ccpAccountEnv = "CCP_ACCOUNT"

// settingsJSONName is the per-account settings file the exec-time guard materializes.
const settingsJSONName = "settings.json"

// fpLaunchPrepareDeadline bounds the pre-launch File Provider materialization.
const fpLaunchPrepareDeadline = 30 * time.Second

// runLaunchPrepareBudget bounds selection plus account-local launch gates across retries.
const runLaunchPrepareBudget = 60 * time.Second

// fpDomainPreparer is the launch-time materialization op the File Provider provider
// exposes (satisfied by *fusekit/overlay.FileProviderProvider).
type fpDomainPreparer interface {
	PrepareDomain(ctx context.Context, accountDir string, deadline time.Duration) error
}

// fpLaunchPreparer resolves the File Provider domain preparer; a var so tests inject
// a fake without a live companion app.
var fpLaunchPreparer = func() (fpDomainPreparer, error) {
	prov, err := pool.OverlayProviderFor(fkoverlay.BackendFileProvider)
	if err != nil {
		return nil, err
	}
	preparer, ok := prov.(fpDomainPreparer)
	if !ok {
		return nil, fmt.Errorf("file provider provider %T cannot prepare its domain", prov)
	}
	return preparer, nil
}

// prepareFPForLaunch force-materializes a File Provider account's computed
// settings.json before a launch commits, so claude's first read never blocks on a
// cold fetch. A non-FP account is a no-op. forced (CCP_ACCOUNT) names the account in
// a not-serving failure for a retry; an automatic pick is excluded and re-ranked by
// the launch loop. An ErrOpUnsupported app is a loud cask-upgrade error, never a silent
// unready launch; a busy/unreachable app fails THIS launch without wedging the domain.
func prepareFPForLaunch(ctx context.Context, a store.Account, forced bool) error {
	if !isFPRow(a.OverlayKind) {
		return nil
	}
	preparer, err := fpLaunchPreparer()
	if err != nil {
		return fmt.Errorf("acct-%02d resolve file provider: %w", a.ID, err)
	}
	switch err := preparer.PrepareDomain(ctx, a.ConfigDir, fpLaunchPrepareDeadline); {
	case err == nil:
		return nil
	case errors.Is(err, fileproviderd.ErrOpUnsupported):
		return err // the provider already prefixed the cask-upgrade hint
	case errors.Is(err, fileproviderd.ErrDomainNotServing):
		if forced {
			return fmt.Errorf("acct-%02d's file provider domain is not serving; retry once it recovers: %w", a.ID, err)
		}
		return fmt.Errorf("acct-%02d's file provider domain is not serving: %w", a.ID, err)
	case errors.Is(err, fileproviderd.ErrBusy), errors.Is(err, fileproviderd.ErrAppUnavailable):
		return fmt.Errorf("acct-%02d's companion app is busy or unreachable; retry this launch: %w", a.ID, err)
	default:
		return fmt.Errorf("acct-%02d's file provider domain is not ready; retry: %w", a.ID, err)
	}
}

func newRunCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run [claude args...]",
		Short: "Select an account and exec `claude`, passing every arg through",
		Long: `run picks the best account and replaces itself with ` + "`claude`" + ` (via exec)
with CLAUDE_CONFIG_DIR set, so once claude starts cc-pool is gone from the
process tree — signals, the controlling terminal, and the exit code are all claude's.

Every argument is forwarded verbatim, with no ` + "`--`" + ` separator (e.g.
` + "`ccp run --resume`" + `). Set ` + ccpAccountEnv + `=<id> to force a specific account
instead of auto-selecting. This is the only supported pooled launch path; it
records the exact process identity before replacing itself with Claude Code.`,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withManager(func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				return runClaude(cmd, m, args)
			})
		},
	}
	return cmd
}

// runClaude resolves the account selection (resolveSelection runs the daemon-response
// validator before any client-side effect), then commits the pick to a launch.
func runClaude(cmd *cobra.Command, m *pool.Manager, args []string) error {
	account, err := ccpAccountFromEnv()
	if err != nil {
		return err
	}
	identity, err := procscan.Identity(os.Getpid())
	if err != nil {
		return fmt.Errorf("identify launch process: %w", err)
	}
	process := store.ProcessIdentity{PID: identity.PID, StartedAt: identity.StartedAt}
	cwd, _ := os.Getwd() // best-effort: an unreadable cwd just disables stickiness
	base := cmd.Context()
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithTimeout(base, runLaunchPrepareBudget)
	defer cancel()
	return runLaunchCandidates(ctx, account != nil,
		func(excluded []int) (*selectionTxn, error) {
			return resolveSelectionTxn(ctx, cmd, m, selectReq{
				account: account, cwd: cwd, process: process, excludeIDs: excluded,
			})
		},
		func(selection *selectionTxn) error {
			return runLaunchSelection(ctx, cmd, selection, args, account != nil)
		},
	)
}

func runLaunchCandidates(ctx context.Context, forced bool, resolve func([]int) (*selectionTxn, error), launch func(*selectionTxn) error) error {
	var attempts []launchAttempt
	var excluded []int
	for {
		if err := ctx.Err(); err != nil {
			return launchAttemptError(attempts, err)
		}
		selection, err := resolve(excluded)
		if err != nil {
			return launchAttemptError(attempts, err)
		}
		if err := ctx.Err(); err != nil {
			selection.Abort()
			return launchAttemptError(attempts, err)
		}
		err = launch(selection)
		if err == nil || forced || !errors.Is(err, fileproviderd.ErrDomainNotServing) {
			return launchAttemptError(attempts, err)
		}
		attempts = append(attempts, launchAttempt{accountID: selection.acct.ID, err: err})
		excluded = append(excluded, selection.acct.ID)
		if ctx.Err() != nil {
			return launchAttemptError(attempts, ctx.Err())
		}
	}
}

type launchAttempt struct {
	accountID int
	err       error
}

func launchAttemptError(attempts []launchAttempt, final error) error {
	if final == nil || len(attempts) == 0 {
		return final
	}
	parts := make([]string, 0, len(attempts))
	for _, attempt := range attempts {
		parts = append(parts, fmt.Sprintf("acct-%02d: %v", attempt.accountID, attempt.err))
	}
	return fmt.Errorf("launch preparation exhausted candidates (%s): %w", strings.Join(parts, "; "), final)
}

// runAcquireLease and runExecClaude are the run pipeline's lease-acquire and exec
// seams — package vars so a pipeline test can assert the launch ordering (a failed
// prepare gate leaves them uncalled) without a live holder or a real exec.
var (
	runAcquireLease = acquireAndProbeSessionLeaseContext
	runPrimeForExec = execguard.PrimeForExecContext
	runExecClaude   = execClaude
)

// runLaunch commits a resolved selection to a claude exec. The order is load-bearing:
// the FP prepare gate (P7) runs first, so a failure aborts with NO lease, NO banner,
// and NO exec; only then the session lease, the settings.json prime (P8, FP only), the
// pick banner, and exec. A non-FP account skips prepare and priming.
func runLaunch(cmd *cobra.Command, acct store.Account, dir, line string, args []string, forced bool) error {
	selection := &selectionTxn{acct: acct, dir: dir, line: line, commit: func(context.Context) error { return nil }, abort: func() {}}
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	return runLaunchSelection(ctx, cmd, selection, args, forced)
}

func runLaunchSelection(ctx context.Context, cmd *cobra.Command, selection *selectionTxn, args []string, forced bool) error {
	defer selection.Abort()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := prepareFPForLaunch(ctx, selection.acct, forced); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Session lease BEFORE exec: its non-CLOEXEC fd rides into claude, pinning the
	// mount; the post-Acquire probe refuses a dead/wedged mount.
	h, err := runAcquireLease(ctx, selection.acct)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		closeRunLease(h)
		return err
	}
	if isFPRow(selection.acct.OverlayKind) {
		if err := runPrimeForExec(ctx, filepath.Join(selection.dir, settingsJSONName)); err != nil {
			closeRunLease(h)
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		closeRunLease(h)
		return err
	}
	if err := selection.Commit(ctx); err != nil {
		closeRunLease(h)
		return fmt.Errorf("commit selection: %w", err)
	}
	step(cmd.ErrOrStderr(), "%s", selection.line)
	return runExecClaude(h, selection.dir, args)
}

func closeRunLease(h *lease.Handle) {
	if h != nil {
		_ = h.Close()
	}
}

func ccpAccountFromEnv() (*int, error) {
	v := os.Getenv(ccpAccountEnv)
	if v == "" {
		return nil, nil
	}
	id, err := strconv.Atoi(v)
	if err != nil {
		return nil, fmt.Errorf("%s must be an account id, got %q: %w", ccpAccountEnv, v, err)
	}
	return &id, nil
}

func execClaude(h *lease.Handle, configDir string, args []string) error {
	bin, err := exec.LookPath("claude")
	if err != nil {
		return fmt.Errorf("`claude` not found on PATH: %w", err)
	}
	// The utility QoS clamp keeps a busy pool from starving fseventsd and
	// interactive processes of CPU.
	argv := append([]string{"taskpolicy", "-c", "utility", bin}, args...)
	//nolint:gosec // G204: bin is the resolved claude executable; argv are this CLI's own passthrough args
	err = syscall.Exec("/usr/sbin/taskpolicy", argv, execEnv(os.Environ(), configDir))
	// The lease fd must stay open at exec time for the lock to survive into claude;
	// KeepAlive holds h live across the (on success, non-returning) Exec call.
	keepLeaseAlive(h)
	if err != nil {
		return fmt.Errorf("exec claude: %w", err)
	}
	return nil // unreachable: a successful Exec never returns
}

// execEnv returns environ with exactly one CLAUDE_CONFIG_DIR (duplicate keys have
// platform-dependent getenv precedence). Unless already set, it pins
// CLAUDE_CODE_PLUGIN_CACHE_DIR to canonical ~/.claude/plugins (claude's marketplace
// validator string-compares unresolved paths, rejecting account-anchored ones as
// "corrupted installLocation") and CLAUDE_CODE_DEBUG_LOGS_DIR to ~/.claude/debug
// (keeping DEBUG=1's bulk log off the fuse-t mirror it would wedge; claude's
// per-session UUID log names keep pooled sessions from colliding in that shared dir).
func execEnv(environ []string, configDir string) []string {
	const cfgKey = "CLAUDE_CONFIG_DIR="
	const pluginKey = "CLAUDE_CODE_PLUGIN_CACHE_DIR="
	const debugKey = "CLAUDE_CODE_DEBUG_LOGS_DIR="
	out := make([]string, 0, len(environ)+3)
	havePlugin := false
	haveDebug := false
	for _, e := range environ {
		if strings.HasPrefix(e, cfgKey) {
			continue
		}
		havePlugin = havePlugin || strings.HasPrefix(e, pluginKey)
		haveDebug = haveDebug || strings.HasPrefix(e, debugKey)
		out = append(out, e)
	}
	out = append(out, cfgKey+configDir)
	if !havePlugin {
		out = append(out, pluginKey+filepath.Join(pool.ClaudeDir(), "plugins"))
	}
	if !haveDebug {
		out = append(out, debugKey+filepath.Join(pool.ClaudeDir(), "debug"))
	}
	return out
}
