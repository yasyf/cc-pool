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
// a not-serving failure for a retry; an auto pick fails the launch too (the daemon's
// select-time probe already excludes genuinely-non-serving domains, so a retry picks
// another). An ErrOpUnsupported app is a loud cask-upgrade error, never a silent
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
		return fmt.Errorf("acct-%02d's file provider domain is not serving; re-run to pick another account: %w", a.ID, err)
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
instead of auto-selecting.

This is the imperative equivalent of:

    CLAUDE_CODE_PLUGIN_CACHE_DIR="$HOME/.claude/plugins" CLAUDE_CODE_DEBUG_LOGS_DIR="$HOME/.claude/debug" CLAUDE_CONFIG_DIR=$(ccp select) claude ...

(The plugin var keeps the session writing canonical ~/.claude plugin paths into
the shared plugin state; the debug var keeps DEBUG=1's verbose per-session log
off the fuse-t mirror, where the bulk write would wedge it. ` + "`ccp run`" + ` sets both for you.)`,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withManager(func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				account, err := ccpAccountFromEnv()
				if err != nil {
					return err
				}
				cwd, _ := os.Getwd() // best-effort: an unreadable cwd just disables stickiness
				// exec replaces this process in place, so os.Getpid() IS the future
				// claude pid — the pick registers as a real session checkout.
				acct, dir, line, err := resolveSelection(cmd, m, selectReq{account: account, cwd: cwd, pid: os.Getpid()})
				if err != nil {
					return err
				}
				// P7: materialize the File Provider domain before committing the launch;
				// a definitive not-serving fails loud, never a silent unready launch.
				if err := prepareFPForLaunch(cmd.Context(), acct, account != nil); err != nil {
					return err
				}
				// Session lease BEFORE exec: its non-CLOEXEC fd rides into claude, pinning
				// the mount; the post-Acquire probe refuses a dead/wedged mount.
				h, err := acquireAndProbeSessionLease(acct)
				if err != nil {
					return err
				}
				// P8: turn on dataless-file materialization (inherited by claude) and read
				// settings.json fully before exec; a failure aborts the launch.
				if isFPRow(acct.OverlayKind) {
					if err := execguard.PrimeForExec(filepath.Join(dir, settingsJSONName)); err != nil {
						_ = h.Close()
						return err
					}
				}
				step(cmd.ErrOrStderr(), "%s", line)
				return execClaude(h, dir, args)
			})
		},
	}
	return cmd
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
	argv := append([]string{"claude"}, args...)
	//nolint:gosec // G204: bin is the resolved claude executable; argv are this CLI's own passthrough args
	err = syscall.Exec(bin, argv, execEnv(os.Environ(), configDir))
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
