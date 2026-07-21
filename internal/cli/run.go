package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
)

// ccpAccountEnv forces a specific account for `ccp run`, which parses no flags
// (every arg passes through to claude) — hence an env var, not a flag.
const ccpAccountEnv = "CCP_ACCOUNT"

// runLaunchPrepareBudget bounds selection and exact tenant preparation.
const runLaunchPrepareBudget = 60 * time.Second

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
			return withManager(cmd.Context(), func(m *pool.Manager) error {
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
	selection, err := resolveSelectionTxn(ctx, cmd, m, selectReq{
		account: account, cwd: cwd, process: process,
	})
	if err != nil {
		return err
	}
	return runLaunchSelection(ctx, cmd, selection, args)
}

var runExecClaude = execClaude

// runLaunchSelection atomically activates the already-prepared daemon selection,
// then replaces the client with Claude using FuseKit's presentation path.
func runLaunchSelection(ctx context.Context, cmd *cobra.Command, selection *selectionTxn, args []string) error {
	defer selection.Abort()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := selection.Commit(ctx); err != nil {
		return fmt.Errorf("commit selection: %w", err)
	}
	step(cmd.ErrOrStderr(), "%s", selection.line)
	return runExecClaude(selection.dir, args)
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

func execClaude(configDir string, args []string) error {
	bin, err := exec.LookPath("claude")
	if err != nil {
		return fmt.Errorf("`claude` not found on PATH: %w", err)
	}
	// The utility QoS clamp keeps a busy pool from starving fseventsd and
	// interactive processes of CPU.
	argv := append([]string{"taskpolicy", "-c", "utility", bin}, args...)
	//nolint:gosec // G204: bin is the resolved claude executable; argv are this CLI's own passthrough args
	err = syscall.Exec("/usr/sbin/taskpolicy", argv, execEnv(os.Environ(), configDir))
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
