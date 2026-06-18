# How cc-pool works

cc-pool runs several Claude subscriptions and launches each Claude Code session on the
emptiest one. It does this without a proxy in the request path and without ever touching
plain `claude`'s login. This page is the full design; the [README](../README.md) is the
front door.

Two hard guarantees underwrite everything below:

1. **Plain `claude` on `~/.claude` keeps working, untouched.** The pool can never log it
   out.
2. **Secrets live only in the macOS Keychain**, never in cc-pool's database.

## `~/.claude` is sacred

`~/.claude` is **never moved** and never registered as a pool account. It stays the
canonical config dir, so plain `claude` keeps working exactly as before, and it's the
**shared base** every pooled account mirrors. The pool never touches plain claude's
credential or login identity. Every account — including your main subscription — joins with
its own `claude /login`, so its token chain is fully independent of plain claude's.

This is why there's no credential "adoption": forking a pool account off plain claude's
login would spend plain claude's single-use refresh token, and the rotation would sign it
out. A fresh login per account is the only safe path.

## One real config dir per account

Claude Code namespaces its Keychain credential **per config dir**. The default `~/.claude`
uses the item `Claude Code-credentials`; a custom `CLAUDE_CONFIG_DIR` gets a suffixed item
`Claude Code-credentials-<hash>`. cc-pool gives each account a real, unique dir
(`~/.cc-pool/accounts/acct-NN`), so each gets its own Keychain item, its own independent
OAuth grant (its own refresh-token chain), and runs on its own **subscription** — never API
billing.

The pool's service-name derivation always emits a hash-suffixed name and can never name the
canonical unsuffixed item, so plain claude's credential is unreachable from pool code.

Each account dir is seeded with a copy of your `~/.claude.json` with the identity stripped
(the account's own login writes its identity), so pooled sessions inherit your settings, MCP
servers, and per-project tool approvals instead of running first-run onboarding.

## Shared overlay

Each account dir presents **all of `~/.claude`** — `projects/`, `skills/`, `plans/`,
`settings.json`, `history.jsonl`, the lot — with writes passing straight back, so every
session shares one workspace and plan-mode plans persist across pooled sessions. Two
providers:

- **symlink** (default, zero-dependency): symlinks each top-level entry of `~/.claude` into
  the account dir. New top-level entries are picked up automatically at launch, by the
  daemon, and by `ccp doctor --fix`.
- **fuse** (optional, live mirror): a passthrough mirror mounted via
  [fuse-t](https://github.com/macos-fuse-t/fuse-t) — kext-less, mounted as you, no root —
  and hosted by a detached cc-pool **mount-holder** process, so daemon restarts and upgrades
  never disturb live sessions' mounts. Requires a `-tags fuse` build (cgo) and a one-time
  *Network Volumes* privacy grant.

A few entries stay per-account instead of shared: `daemon/` and `ide/` (Claude's PID-keyed
supervisor and IDE lock/socket files, which would collide across concurrent sessions),
`backups/` (rotating backups of each account's `.claude.json`), the identity and credential
files `.claude.json` and `.credentials.json`, `.last-update-result.json` (instance-local
auto-update state), and `remote-settings.json` (claude's cached per-subscription settings).

Per-account `.claude.json` doesn't mean settings fork. Its shareable top-level keys —
everything except identity, per-project state, and startup counters — flow from
`~/.claude.json` into pooled sessions, so a setting you change in vanilla `claude` reaches
every account. One caveat: under the default symlink overlay the flow is one-way (merged in
at launch, base wins), so changing a shareable setting *inside* a pooled session reverts at
the next launch. Manage shared settings in vanilla `claude`, or use the fuse overlay, whose
live merged view writes shareable changes back to `~/.claude.json` (two-way).

## Scoring

Selection is predictive: cc-pool scores live 5-hour and 7-day usage *before* the session
starts and picks the emptiest account, rather than waiting for a rate-limit error to learn
it picked wrong. The baseline — exact when windows are far from a reset — is:

```text
score = 0.70·(100−util_5h) + 0.25·(100−util_7d)
      − 2·active_sessions − 100·rate_limited − 20·stale
```

Three terms keep the ranking honest near the edges: an **imminent reset** earns credit in
proportion to how soon the window resets (a 90%-used window resetting in 10 minutes ranks
*up*, not down); a **low-headroom barrier** stops a nearly-exhausted 7-day window from being
masked by 5-hour headroom; and a **burn-rate** term downranks an account being actively
drained. A signed-out account is excluded from selection entirely rather than ranked.
`select` picks argmax. Usage comes from Claude's own `/api/oauth/usage` endpoint.

## The daemon

`brew services start cc-pool` (Homebrew installs) or `ccp service install` (source builds)
runs a **user LaunchAgent** — a root daemon couldn't read your login Keychain. It polls
usage every ~3 min with exponential backoff, refreshes **idle** accounts' tokens before they
expire (a checked-out session owns its own refresh; the daemon adopts whatever token it
rotated to on check-in), caches scores, and — with the fuse overlay — supervises the
detached mount holder, which owns the mounts so daemon restarts and upgrades never disturb
them. `ccp add` and `ccp init` start it automatically; if it isn't running, `ccp select`
auto-spawns it or samples live.

No secrets are ever stored in cc-pool's database — the macOS Keychain is the only secret
store.
