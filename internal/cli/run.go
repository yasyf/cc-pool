package cli

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/lease"
)

// ccpAccountEnv forces a specific account for `ccp run`, which parses no flags
// (every arg passes through to claude) — hence an env var, not a flag.
const ccpAccountEnv = "CCP_ACCOUNT"

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
				return runClaude(cmd, m, args, acquireAndProbeSessionLease, execClaude)
			})
		},
	}
	return cmd
}

func runClaude(cmd *cobra.Command, m *pool.Manager, args []string, acquire func(store.Account) (*lease.Handle, error), execFn func(*lease.Handle, string, []string) error) error {
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
	step(cmd.ErrOrStderr(), "%s", line)
	// Take the session lease BEFORE exec: its non-CLOEXEC fd rides through
	// into claude, pinning the account's mount against holder teardown for
	// the whole session, and the post-Acquire probe refuses to exec into a
	// dead or partially-wedged mount.
	h, err := acquire(acct)
	if err != nil {
		return err
	}
	return execFn(h, dir, args)
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
