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
)

// ccpAccountEnv forces a specific pool account for `ccp run`. The command
// parses no flags of its own (every argument passes through to claude), so the
// account override travels out-of-band in the environment.
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
		// Pass every argument straight through to claude; ccp owns no flags here.
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
				// `ccp run` is the imperative form of `ccp select | claude`: it shares
				// the exact selection pipeline, then execs instead of printing the dir.
				// Our pid IS the future claude pid (exec replaces in place), so the
				// pick is marked as a real session checkout.
				dir, line, err := resolveSelection(cmd, m, selectReq{account: account, cwd: cwd, pid: os.Getpid()})
				if err != nil {
					return err
				}
				step(cmd.ErrOrStderr(), "%s", line)
				return execClaude(dir, args)
			})
		},
	}
	return cmd
}

// ccpAccountFromEnv reads the CCP_ACCOUNT override, returning nil when it is
// unset and an error when it is not a valid account id.
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

// execClaude replaces this process with `claude`, forwarding args verbatim and
// pointing it at configDir via CLAUDE_CONFIG_DIR. It does not return on success.
func execClaude(configDir string, args []string) error {
	bin, err := exec.LookPath("claude")
	if err != nil {
		return fmt.Errorf("`claude` not found on PATH: %w", err)
	}
	argv := append([]string{"claude"}, args...)
	//nolint:gosec // G204: bin is the resolved claude executable; argv are this CLI's own passthrough args
	if err := syscall.Exec(bin, argv, execEnv(os.Environ(), configDir)); err != nil {
		return fmt.Errorf("exec claude: %w", err)
	}
	return nil // unreachable: a successful Exec never returns
}

// execEnv returns environ with any existing CLAUDE_CONFIG_DIR dropped and
// CLAUDE_CONFIG_DIR=configDir appended, so the launched claude sees exactly one
// (a duplicate key has platform-dependent getenv precedence).
//
// It also pins CLAUDE_CODE_PLUGIN_CACHE_DIR to the shared base's plugins dir.
// claude otherwise derives its plugin root from CLAUDE_CONFIG_DIR and stamps
// account-anchored installLocation/installPath strings into the SHARED plugin
// state files (acct-NN/plugins is a whole-dir share of ~/.claude/plugins), and
// its marketplace validator string-compares stored paths against the canonical
// root without resolving symlinks — so every such entry is later rejected as
// "corrupted installLocation". Pinning the root makes pooled sessions write
// the same canonical spellings as plain claude. A value already present in
// environ is respected: a user-set global plugin root applies to plain claude
// too, and overriding it here would split the roots this pin exists to unify.
//
// It likewise pins CLAUDE_CODE_DEBUG_LOGS_DIR to the shared base's debug dir.
// claude otherwise derives that dir from CLAUDE_CONFIG_DIR and, under DEBUG=1,
// streams a verbose per-session log into it — a high-volume sequential write
// that, through the pooled CLAUDE_CONFIG_DIR's fuse-t mirror, hits the bulk-I/O
// path that wedges the mount and hangs the session forever (see
// internal/overlay/probe.go). Pinning the canonical ~/.claude/debug keeps that
// write off the mirror entirely — exactly where plain claude already logs; the
// per-session UUID filenames mean pooled sessions never collide. A pre-set
// value is respected, like the plugin root.
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
