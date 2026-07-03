# ![cc-pool](docs/assets/readme-banner.webp)

**Make Claude's 5-hour limit your other account's problem.** cc-pool reads live 5-hour and 7-day usage and execs every claude session on the account with the most headroom; no proxy ever touches the request path.

[![CI](https://img.shields.io/github/actions/workflow/status/yasyf/cc-pool/ci.yml?branch=main&label=CI)](https://github.com/yasyf/cc-pool/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/yasyf/cc-pool)](https://github.com/yasyf/cc-pool/releases)
[![License: PolyForm Noncommercial](https://img.shields.io/badge/License-PolyForm--Noncommercial--1.0.0-blue.svg)](LICENSE)

## Get started

```sh
brew install yasyf/tap/cc-pool
ccp
```

<img src="docs/assets/demo.png" alt="Terminal running 'ccp --help' — the command list from add to widget, plus the guarantee that plain claude is never part of the pool" width="700">

macOS only; the binary installs as `cc-pool` with a `ccp` symlink. On an empty pool, `ccp` walks you through logging in each subscription — every account gets its own `claude /login` — then offers to wrap `claude` so every session lands on the account with the most headroom.

Driving with an agent? Paste this:

```text
Install cc-pool with `brew install yasyf/tap/cc-pool`, then run `ccp` and walk me through pooling my Claude subscriptions.
Add each account via its own `claude /login` flow and accept the alias so `claude` becomes `ccp run`.
Finish by running `ccp status --plain` and tell me which account my next session will land on and why.
```

---

## Use cases

### Stop hitting the 5-hour limit while another account sits idle

Run two subscriptions and you hit the same wall every week: one account pegged at its window while the other holds a week of headroom. With the alias in place, launching is unchanged:

```sh
claude
```

The wrapper announces its pick on stderr, then execs the real `claude` — cc-pool is gone from the process tree before Claude Code draws its first frame (emails here and below are normalized):

```text
Selected work@example.com · 5h 22% used · 7d 46% used
```

### See every account's 5-hour and 7-day headroom before you launch

A rate-limit error is a terrible way to learn which account is full. Ask the pool first:

```sh
ccp status --plain
```

```text
  ACCOUNT                     SCORE  5h used  7d used  LIVE RESETS
▸ work@example.com             68.1      22%      46%     0 6:00 PM
  personal@example.com         34.8      61%      70%     0 4:30 PM
▸ = next pick · score higher = emptier
```

On a terminal, `ccp status` renders the same table as a live TUI.

### Keep prompt caches warm by pinning a directory to one account

Bouncing a long-running project between accounts throws away its prompt caches on every switch. Repeated launches from one directory already stick to a single account automatically; to force it, pin the directory from the TUI:

```sh
ccp status   # highlight an account, press p to pin the current directory
```

Every launch from that directory then announces `Reusing work@example.com (pinned)` instead of `Selected`, whatever the scores say.

## Day-to-day use

**Escaping the alias.** The onboarding alias is `alias claude='ccp run'`, and `command claude` bypasses it — plain claude on `~/.claude`, one keystroke away. To leave `claude` untouched, decline the prompt (or pass `ccp add --no-alias`) and pick your own name, e.g. `alias cl='ccp run'`.

**Passing arguments.** `ccp run` forwards every argument to `claude` verbatim — no `--` needed. Bare `ccp` with flags is shorthand for the same thing:

```sh
ccp run --resume
ccp -p "summarize this repo"   # auto-converts to `ccp run -p ...`
```

**Forcing an account.** `CCP_ACCOUNT=2 ccp run` forces account 2 instead of auto-selecting.

**When every account is full.** Selection never picks an exhausted window while any account has headroom, and `ccp select --wait` blocks until one frees up. If the whole pool is exhausted, the launch falls back to the least-bad account and warns loudly on stderr — that session then bills pay-as-you-go credits (if extra usage is enabled) or rate-limits until the window resets.

**Composing it yourself.** `ccp select` prints the chosen config dir on stdout; `ccp env` prints the matching `export` lines. A bare-`ccp select` launch also needs the plugin root set, so the session writes canonical paths into the shared `~/.claude/plugins` (`ccp run` and `ccp env` do this for you):

```sh
CLAUDE_CODE_PLUGIN_CACHE_DIR="$HOME/.claude/plugins" CLAUDE_CONFIG_DIR=$(ccp select) claude
```

**The widget.** `ccp widget` installs a Notification Center widget (a separate cask, `yasyf/tap/cc-pool-status`) — per-account 5h/7d usage bars, live-session counts, and a pool mascot whose mood tracks how fast the pool is draining. Details in [widget/README.md](widget/README.md).

<img src="docs/assets/widget-medium.png" alt="The cc-pool Notification Center widget — per-account 5-hour and 7-day usage bars, live-session counts, and a worried mascot at 62% pool usage" width="450">

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

Run `ccp help <command>` for the rest (`env`, `list`, `login`, `rename`, `remove`, `init`, `migrate`, `cred`, `fuse`) and every flag.

## How it works

`~/.claude` is **never touched** — plain `claude` keeps working and can't be logged out by the pool. Secrets live only in the macOS Keychain, never in cc-pool's database. Selection is **predictive**: it scores each account's live 5h/7d usage before launch and picks the emptiest, instead of waiting for a rate-limit error to tell you the pick was wrong. The full design — per-account config dirs, the shared overlay, the scoring formula, and the daemon — is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Uninstall

```sh
ccp service uninstall            # stop the daemon + mount holder, unmount fuse overlays
                                 # (refuses under live sessions; --force overrides)
ccp service uninstall --purge    # ...and remove all pool accounts/dirs/state
brew uninstall cc-pool
```

Licensed under [PolyForm Noncommercial 1.0.0](LICENSE) — free for noncommercial use.
