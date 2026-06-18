# cc-pool

![cc-pool banner](docs/assets/readme-banner.webp)

[![CI](https://img.shields.io/github/actions/workflow/status/yasyf/cc-pool/ci.yml?branch=main&label=CI)](https://github.com/yasyf/cc-pool/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/yasyf/cc-pool)](https://github.com/yasyf/cc-pool/releases)
[![License: PolyForm Noncommercial](https://img.shields.io/badge/License-PolyForm--Noncommercial--1.0.0-blue.svg)](LICENSE)

**Start-of-session load-balancing across multiple Claude subscriptions — for macOS.**

Run several Claude Max/Pro subscriptions and you hit the same wall every week: one account
pegged at its 5-hour or weekly limit while another sits idle. cc-pool launches every Claude
Code session on the **emptiest** account, picked from live 5-hour / 7-day usage *before* the
session starts — no proxy in the request path, no manual switching, no waiting for a
rate-limit error to learn you picked wrong.

After setup, `claude` is an alias for `ccp run` and pooling disappears into the background.
Two guarantees hold: plain `claude` on `~/.claude` keeps working untouched (the pool can
never log it out), and secrets stay in the macOS Keychain, never in cc-pool's database. See
[How it works](docs/ARCHITECTURE.md) for the design.

## Install

```sh
brew tap yasyf/cc-pool https://github.com/yasyf/cc-pool
brew install yasyf/cc-pool/cc-pool
```

macOS only. The binary installs as `cc-pool` with a `ccp` symlink; the default build is
pure Go.

## Quickstart

Pool two subscriptions and launch Claude on the emptiest one, in about five minutes.

**1. Run `ccp`.** On an empty pool it walks you through logging in each subscription — every
account gets its own `claude /login`:

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
✓ Added personal@example.com.

Wrap `claude` to always launch on the emptiest account?
> Yes
✓ Wrapped `claude` — added an alias to ~/.zshrc. Run `command claude` for plain ~/.claude.
```

**2. Check the pool.** `ccp status` opens a live TUI; add `--plain` for a one-shot table:

```text
$ ccp status --plain
  ACCOUNT                     SCORE  5h used  7d used  LIVE RESETS
▸ work@example.com             68.1      22%      46%     0 6:00 PM
  personal@example.com         34.8      61%      70%     0 4:30 PM
▸ = next pick · score higher = emptier
```

**3. Launch.** Run `claude` (now wrapped). It announces its pick on stderr, then execs the
real `claude` — cc-pool is gone from the process tree before Claude Code draws its first
frame:

```text
$ claude
Selected work@example.com · 5h 22% used · 7d 46% used
```

The `Selected` account matches the `▸` row from step 2. From here, every `claude` lands on
whichever account has the most headroom.

## Day-to-day use

**The alias, and escaping it.** The quickstart sets `alias claude='ccp run'`. `command
claude` bypasses it — plain claude on `~/.claude`, one keystroke away. To leave `claude`
untouched, decline the prompt (or pass `ccp add --no-alias`) and pick your own name, e.g.
`alias cl='ccp run'`.

**Passing arguments.** `ccp run` forwards every argument to `claude` verbatim — no `--`
needed. Bare `ccp` with flags is shorthand for the same thing:

```sh
ccp run --resume
ccp -p "summarize this repo"   # auto-converts to `ccp run -p ...`
```

**Forcing and pinning.** `CCP_ACCOUNT=2 ccp run` forces account 2 instead of auto-selecting.
Repeated launches from the same directory stick to one account for prompt-cache continuity
and announce `Reusing … (pinned)` instead of `Selected`.

**When every account is full.** Selection never picks an exhausted window while any account
has headroom, and `ccp select --wait` blocks until one frees up. If the whole pool is
exhausted, the launch falls back to the least-bad account and warns loudly on stderr — that
session then bills pay-as-you-go credits (if extra usage is enabled) or rate-limits until
the window resets.

**Composing it yourself.** `ccp select` prints the chosen config dir on stdout; `ccp env`
prints the matching `export` lines. A bare-`ccp select` launch also needs the plugin root
set, so the session writes canonical paths into the shared `~/.claude/plugins` (`ccp run`
and `ccp env` do this for you):

```sh
CLAUDE_CODE_PLUGIN_CACHE_DIR="$HOME/.claude/plugins" CLAUDE_CONFIG_DIR=$(ccp select) claude
```

**The widget.** `ccp widget` installs a Notification Center widget — per-account 5h/7d usage
bars, live-session counts, and a pool mascot whose mood tracks how fast the pool is draining.
Details in [widget/README.md](widget/README.md).

## Commands

| Command | What it does |
|---|---|
| `ccp` | Empty pool: guided onboarding. Populated pool: status. With flags: shorthand for `ccp run` |
| `ccp add` | Pool a subscription via its own `claude /login` (auto-inits the pool, starts the daemon) |
| `ccp run [claude args…]` | Select the emptiest account and exec `claude`, forwarding every arg |
| `ccp status` | Per-account usage, score, and sessions — TUI on a terminal, plain table when piped |
| `ccp select` | Print the chosen account's config dir on stdout — the composable hot path |
| `ccp doctor` | Check accounts' Keychain items and overlays; `--fix` repairs drift |
| `ccp service` | Manage the daemon and mount holder (`install`/`uninstall`/`status`) |

Run `ccp help <command>` for the rest (`env`, `list`, `rename`, `remove`, `init`) and every
flag.

## How it works

`~/.claude` is **never touched** — plain `claude` keeps working and can't be logged out by
the pool. Secrets live only in the macOS Keychain, never in cc-pool's database. Selection is
**predictive**: it scores each account's live 5h/7d usage before launch and picks the
emptiest. The full design — per-account config dirs, the shared overlay, the scoring formula,
and the daemon — is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Uninstall

```sh
ccp service uninstall            # stop the daemon + mount holder, unmount fuse overlays
                                 # (refuses under live sessions; --force overrides)
ccp service uninstall --purge    # ...and remove all pool accounts/dirs/state
brew uninstall cc-pool
```

`~/.claude` and its credential are never touched.

## Development

Build with `CGO_ENABLED=0 go build ./cmd/cc-pool`; `go test ./...` passes with no network,
Keychain, or daemon. The manual end-to-end test matrix lives in
[docs/VERIFICATION.md](docs/VERIFICATION.md), release history in [CHANGELOG.md](CHANGELOG.md),
and conventions in [AGENTS.md](AGENTS.md).

## License

PolyForm-Noncommercial-1.0.0 © Yasyf Mohamedali — free for noncommercial use. See
[LICENSE](LICENSE) or the [license text
online](https://polyformproject.org/licenses/noncommercial/1.0.0).
