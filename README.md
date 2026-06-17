# cc-pool

![cc-pool banner](docs/assets/readme-banner.webp)

[![CI](https://img.shields.io/github/actions/workflow/status/yasyf/cc-pool/ci.yml?branch=main&label=CI)](https://github.com/yasyf/cc-pool/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/yasyf/cc-pool)](https://github.com/yasyf/cc-pool/releases)
[![License: PolyForm Noncommercial](https://img.shields.io/badge/License-PolyForm--Noncommercial--1.0.0-blue.svg)](LICENSE)

**Predictive, start-of-session load-balancing across multiple Claude subscriptions — for macOS.**

Run several Claude Max/Pro subscriptions and you hit the same wall every week:
one account pegged at its 5-hour or weekly limit while another sits idle.
cc-pool launches every Claude Code session on the **emptiest** account, picked
from live 5-hour / 7-day usage *before* the session starts — predictive, not
reactive. No proxy in the request path, no manual switching, no waiting for a
rate-limit error to learn you picked wrong.

After setup, `claude` is an alias for `ccp run` and the pooling disappears
into the background. Two hard guarantees underwrite the design: plain `claude`
on `~/.claude` keeps working untouched — the pool can never log it out — and
secrets stay in the macOS Keychain, never in cc-pool's database. [How it
works](#how-it-works) has the details.

## Install

```sh
brew tap yasyf/cc-pool https://github.com/yasyf/cc-pool
brew install yasyf/cc-pool/cc-pool
```

macOS only. The binary installs as `cc-pool` with a `ccp` symlink; the default
build is pure Go.

## Quickstart

Pool two subscriptions and launch Claude on the emptiest one, in about five
minutes.

**1. Run `ccp`.** On an empty pool it walks you through logging in each
subscription — every account gets its own `claude /login`:

```text
$ ccp
✓ Set up cc-pool on this machine.

How do you want to log in?
> Log in now, in this terminal

# claude opens its /login flow right here; finish it and ccp closes claude for you

Name for this account (optional)
> work@example.com

✓ Added work@example.com.

Add another account?
> Yes

# the second login runs the same way
✓ Added personal@example.com.

Your pool now has 2 accounts.

Launch Claude on the emptiest account:

    ccp run

Wrap `claude` to always launch on the emptiest account?
> Yes

✓ Wrapped `claude` — added an alias to ~/.zshrc.
Restart your shell or run `source ~/.zshrc` to use it now.
Run `command claude` for plain ~/.claude.
```

**2. Check the pool.** `ccp status --plain` prints the table once; bare
`ccp status` (or bare `ccp`, now that accounts exist) opens a live TUI with
per-account score breakdowns:

```text
$ ccp status --plain
  ACCOUNT                     SCORE  5h used  7d used  LIVE RESETS
▸ work@example.com             68.1      22%      46%     0 6:00 PM
  personal@example.com         34.8      61%      70%     0 4:30 PM
▸ = next pick · score higher = emptier · 5h/7d = % used
updated 3:58 PM
```

**3. Launch.** Run `claude` (now wrapped). It announces its pick on stderr,
then execs the real `claude` — cc-pool is gone from the process tree before
Claude Code draws its first frame:

```text
$ claude
Selected work@example.com · 5h 22% used · 7d 46% used
```

The `Selected` line names the account marked `▸` in step 2 — that match is
your verification. From here on, every `claude` lands on whichever account
has the most headroom.

## Day-to-day use

### The alias, and escaping it

The quickstart wraps `claude` with `alias claude='ccp run'`. `command claude`
bypasses it — plain claude on `~/.claude`, one keystroke away. Prefer to
leave `claude` untouched? Decline the prompt (or pass `ccp add --no-alias`)
and pick your own name:

```sh
alias cl='ccp run'
```

### Passing arguments

`ccp run` forwards every argument to `claude` verbatim — no `--` separator
needed. Bare `ccp` with flags is shorthand for the same thing:

```sh
ccp run --resume
ccp -p "summarize this repo"   # auto-converts to `ccp run -p ...`
```

### Forcing and pinning

`CCP_ACCOUNT=2 ccp run` forces account 2 instead of auto-selecting. Repeated
launches from the same directory stick to one account for prompt-cache
continuity; those announce `Reusing work@example.com (pinned)` instead of
`Selected`.

### When every account is full

Selection refuses to burn a nearly-reset window: an account with an exhausted
plan window is never picked while any account has headroom, and
`ccp select --wait` blocks until one does. If the whole pool is exhausted, the
launch falls back to the least-bad account and warns loudly on stderr —
that session bills pay-as-you-go credits if extra usage is enabled, or
rate-limits until the window resets.

### Composing the launch yourself

`ccp select` prints the chosen config dir on stdout and nothing else. Set the
plugin root too, so the session writes canonical plugin paths into the shared
`~/.claude/plugins` (`ccp run` and `ccp env` both do this for you):

```sh
CLAUDE_CODE_PLUGIN_CACHE_DIR="$HOME/.claude/plugins" CLAUDE_CONFIG_DIR=$(ccp select) claude
```

### The Notification Center widget

<img src="docs/assets/widget-medium.png" width="377"
     alt="cc-pool medium widget: pool mascot and headline above per-account 5h/7d usage bars">

`ccp status` without the terminal: per-account 5h/7d usage bars, live-session
counts, and a pool mascot whose mood tracks how fast the pool is draining.

```sh
ccp widget
```

That installs the widget app from a Homebrew cask and prints the steps to add
it from the widget gallery. Data flow, build-from-source, and troubleshooting
live in [widget/README.md](widget/README.md).

## How it works

### `~/.claude` is sacred

`~/.claude` is **never moved** and never registered as a pool account. It
stays the canonical config dir, so plain `claude` keeps working exactly as
before, and is the **shared base** every pooled account mirrors. The
pool never touches plain claude's credential or login identity. Every account,
including your main subscription, joins with its own `claude /login`, so its
token chain is fully independent of plain claude's.

### One real config dir per account

Claude Code namespaces its Keychain credential **per config dir**: the default
`~/.claude` uses the item `Claude Code-credentials`; a custom
`CLAUDE_CONFIG_DIR` gets a suffixed item `Claude Code-credentials-<hash>`.
cc-pool gives each account a real, unique dir (`~/.cc-pool/accounts/acct-NN`)
so each gets its own Keychain item, its own independent OAuth grant (its own
refresh-token chain), and runs on its own **subscription** — never API
billing. Each account dir is seeded with a copy of your `~/.claude.json` with
the identity stripped (the account's own login writes its identity), so pooled
sessions inherit your settings, MCP servers, and per-project tool approvals
instead of running first-run onboarding.

### Shared overlay

Each account dir presents **all of `~/.claude`** — `projects/`, `skills/`,
`plans/`, `settings.json`, `history.jsonl`, the lot — with writes passing
straight back, so every session shares the same workspace and plan-mode plans
persist across pooled sessions. Two providers:

- **symlink** (default, zero-dependency): symlinks each top-level entry of
  `~/.claude` into the account dir. New top-level entries are picked up
  automatically at launch, by the daemon, and by `ccp doctor --fix`.
- **fuse** (optional, live mirror): a passthrough mirror mounted via
  [fuse-t](https://github.com/macos-fuse-t/fuse-t) — kext-less, mounted as
  you, no root — and hosted by a detached cc-pool **mount-holder** process, so
  daemon restarts and upgrades never disturb live sessions' mounts. Requires a
  `-tags fuse` build (cgo) and a one-time *Network Volumes* privacy grant.

A few entries stay per-account instead of shared: `daemon/` and `ide/`
(Claude's PID-keyed supervisor and IDE lock/socket files, which would collide
across concurrent sessions), `backups/` (rotating backups of each account's
`.claude.json`), the identity and credential files `.claude.json` and
`.credentials.json`, `.last-update-result.json` (instance-local auto-update
state), and `remote-settings.json` (claude's cached per-subscription
settings).

Per-account `.claude.json` doesn't mean settings fork, though: its shareable
top-level keys — everything except identity, per-project state, and startup
counters — flow from `~/.claude.json` into pooled sessions, so a setting you
change in vanilla `claude` reaches every account. One caveat: under the
default symlink overlay the flow is one-way (merged in at launch, base wins),
so changing a shareable setting *inside* a pooled session reverts at the next
launch — manage shared settings in vanilla `claude`, or use the fuse overlay,
whose live merged view writes shareable changes back to `~/.claude.json`
(two-way).

### Scoring

The baseline — exact when windows are far from a reset — is:

```text
score = 0.70·(100−util_5h) + 0.25·(100−util_7d)
      − 2·active_sessions − 100·rate_limited − 20·stale_or_refresh_failed
```

Three terms keep the ranking honest near the edges: an **imminent reset**
earns credit in proportion to how soon the window resets (a 90%-used window
resetting in 10 minutes ranks *up*, not down); a **low-headroom barrier**
stops a nearly-exhausted 7-day window from being masked by 5-hour headroom;
and a **burn-rate** term downranks an account being actively drained.
`select` picks argmax. Usage comes from Claude's own `/api/oauth/usage`
endpoint.

### The daemon

`brew services start cc-pool` (Homebrew installs) or `ccp service install`
(source builds) runs a **user LaunchAgent** — a root daemon couldn't read your
login Keychain. It polls usage every ~3 min with exponential backoff,
refreshes **idle** accounts' tokens before they expire (a checked-out session
owns its own refresh; the daemon adopts whatever token it rotated to on
check-in), caches scores, and — with the fuse overlay — supervises the
detached mount holder, which owns the mounts (so daemon restarts and upgrades
never disturb them). `ccp add` and `ccp init` start it automatically; if it
isn't running, `ccp select` auto-spawns it or samples live.

No secrets are ever stored in cc-pool's database — the macOS Keychain is the
only secret store.

## Commands

These two tables cover the full user-facing surface; `ccp help <command>`
prints the same per command.

| Command | What it does |
|---|---|
| `ccp` | On a terminal — empty pool: guided onboarding; populated pool: status. With flags: shorthand for `ccp run` |
| `ccp add` | Pool a subscription via its own `claude /login` (auto-inits the pool, starts the daemon) |
| `ccp run [claude args…]` | Select the emptiest account and exec `claude`, forwarding every arg |
| `ccp status` | Per-account usage, score, and sessions — TUI on a terminal, plain table when piped |
| `ccp select` | Print the chosen account's config dir on stdout — the composable hot path |
| `ccp env` | Print shell `export` lines to launch an account by hand |
| `ccp list` | Static account list: ids, paths, Keychain items |
| `ccp doctor` | Check accounts' Keychain items and overlays; repair drift |
| `ccp remove <id>` | Remove an account from the pool |
| `ccp rename <id> <name>` | Rename an account; `--auto` derives names from account emails |
| `ccp init` | Set up the pool and start the daemon (optional — `ccp add` does this) |
| `ccp service install\|uninstall\|status` | Manage the daemon and mount holder (delegates to `brew services` on Homebrew installs) |
| `ccp widget` | Install the Notification Center status widget (Homebrew cask) and show how to enable it |

Flags, by command:

| Command | Flag | Effect |
|---|---|---|
| `add` | `--label <name>` | Name for the first account |
| `add` | `--count <n>` | Add exactly N accounts, no continue prompt |
| `add` | `--yes`, `-y` | Add one account and log in right away |
| `add` | `--run-login` | Log in immediately instead of asking how |
| `add` | `--no-alias` | Don't add a `claude` shell alias |
| `run` | `CCP_ACCOUNT=<id>` (env) | Force a specific account instead of auto-selecting |
| `select` | `--wait` | Wait for an account with headroom instead of failing or using an exhausted one |
| `select` | `--account <id>` | Force a specific account id |
| `select` | `--no-daemon` | Don't use the daemon; sample usage live |
| `select` | `--fresh <dur>` | Reuse cached usage newer than this (live mode) |
| `status` | `--plain` | Print the plain table instead of the interactive TUI |
| `status` | `--watch`, `-w` | Refresh continuously (plain mode) |
| `status` | `--live` | Force live sampling even if the daemon is running |
| `env` | `--account <id>` | Account id (defaults to the best account) |
| `doctor` | `--fix` | Attempt to repair detected drift |
| `remove` | `--keep-credential` | Keep the account's Keychain item |
| `rename` | `--auto` | Derive labels from account emails (skips custom labels) |
| `rename` | `--force` | With `--auto`, overwrite custom labels too |
| `init` | `--no-service` | Don't start the daemon now; `ccp add` starts it |
| `service uninstall` | `--force` | Skip the live-session gate (sessions on unmounted dirs will break) |
| `service uninstall` | `--purge` | Also remove all pool accounts and state; never touches `~/.claude` |

## Uninstall

```sh
ccp service uninstall            # stop the daemon + mount holder, unmount fuse overlays
                                 # (refuses under live sessions; --force overrides)
ccp service uninstall --purge    # ...and remove all pool accounts/dirs/state
brew uninstall cc-pool
```

`~/.claude` and its credential are never touched.

## Development

Build with `CGO_ENABLED=0 go build ./cmd/cc-pool`; `go test ./...` passes with
no network, Keychain, or daemon. The manual end-to-end test matrix lives in
[docs/VERIFICATION.md](docs/VERIFICATION.md), release history in
[CHANGELOG.md](CHANGELOG.md), and conventions in [AGENTS.md](AGENTS.md).

## License

PolyForm-Noncommercial-1.0.0 © Yasyf Mohamedali — free for noncommercial use.
See [LICENSE](LICENSE) or the [license text
online](https://polyformproject.org/licenses/noncommercial/1.0.0).
