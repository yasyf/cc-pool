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
- **fuse** (optional, live mirror): a passthrough mirror served via
  [fuse-t](https://github.com/macos-fuse-t/fuse-t) — kext-less, mounted as you, no root —
  and hosted by the external **fusekit-holder** cask app, a Developer ID-signed bundle at a
  fixed `/Applications` path, so daemon restarts and cc-pool upgrades never disturb live
  sessions' mounts (cc-pool itself stays pure Go). The pool rides **one native mount** at
  `~/.cc-pool/mnt`: each account is a subtree (`mnt/acct-NN`) of that single mount — one
  fuse-t NFS server process (`go-nfsv4`) total, not one per account — and each
  `accounts/acct-NN` dir is a fail-closed symlink onto its subtree, so account-dir paths
  (which feed Keychain service-name hashing and session detection) never change. Attaching
  or detaching an account is a holder-side map operation with no kernel unmount; the only
  whole-mount operations are holder replacement and wedge recovery, both gated on a
  pool-wide zero-session window. Requires fusekit-holder ≥ v0.29.0 (`ccp` refuses older
  holders loudly and keeps accounts on symlink until upgraded).

Privacy (TCC) grants across this stack are engineered to be one-time, and durability
depends on how tccd keys each grant. **fusekit-holder** (`com.yasyf.fusekit-holder`)
performs the NFS mounts, so a single *Network Volumes* grant covers every consumer; it
survives upgrades because the holder is an app bundle at a fixed `/Applications` path,
which tccd keys by identifier. cc-pool is a bare executable, and tccd keys a bare
executable's grants by its **resolved path** — the dotted signing identifier lands in the
grant's code requirement (any Developer ID release satisfies it) but not in the lookup
key — so a grant made against a per-version Homebrew keg path dies on every upgrade. The
daemon therefore runs from a signed **CCPoolDaemon.app** bundle inside the Homebrew keg's
`libexec` (`<keg>/libexec/CCPoolDaemon.app/Contents/MacOS/cc-pool`): its app-group
entitlement plus an embedded Developer ID provisioning profile grant prompt-free
group-container access for the File Provider bridge, so tccd keys it by identifier like the
holder app rather than by keg path — the durable replacement for the retired
`~/.cc-pool/bin` stable-path re-exec. The daemon is the only process that touches the group
container — the CLI reads bridge health from the daemon. Two residuals
still re-prompt: the interactive CLI runs from the keg path, so *Network Volumes* grants
for its deep-probe reads through fuse mounts stay per-version, and unsigned local builds
carry a per-build cdhash requirement.

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
external **fusekit-holder** app, which owns the mounts so daemon restarts and upgrades never
disturb them. `ccp add` and `ccp init` start it automatically; if it isn't running, `ccp select`
auto-spawns it or samples live.

No secrets are ever stored in cc-pool's database — the macOS Keychain is the only secret
store.

## Host sync

`ccp sync enable` makes the pool span several Macs over a
[synckit](https://github.com/yasyf/synckit) mesh: accounts, labels, removals, and
credential freshness converge across every enabled host, so `ccp add` on one Mac
materializes the account on the rest with no extra login.

**The shared registry is secretless.** Hosts converge on
`~/.cc-pool/sync/registry.json`, a last-writer-wins CRDT keyed by each account's Claude
`accountUuid`. An entry carries metadata plus a chain stamp — the origin host, token
expiry, a one-way hash of the access token, the rotation time — never a token. Merging
is pull-only and order-independent, and a removal is a tombstone that outlives the
entry, so `ccp remove` on any host tears the account down everywhere. synckitd watches
per-account stamp files and nudges peers on change; its periodic reconcile tick is the
floor when a notify is missed.

**One secret path.** The daemon serves synckit's consumer contract on a second socket,
`~/.cc-pool/sync.sock`, and that dispatcher carries exactly one custom method:
`ccp.fetch_stripped_credential`. It serves the credential **stripped** — the refresh
token is removed at the origin, so that secret never leaves the origin process.
Stripped credentials transit peer RPC — over SSH via the hidden `ccp sync rpc-serve`
stdio bridge — only during a pull, and land directly in the receiving host's Keychain.
A host whose login Keychain is unsearchable (headless SSH) falls back to the plaintext
file store; `ccp sync status` flags the exposure.

**Refresh discipline: one origin per chain.** Claude refresh tokens are single-use, so
two hosts refreshing one chain double-spend it and the loser gets signed out. Each
chain therefore has exactly one **origin** — the host whose login minted it — and only
the origin refreshes. Origin is a static fact, not a leased role, and the enforcement
is structural: a peer holds only the access token and its expiry, never the refresh
token, so it has nothing to double-spend no matter how it races. The origin's idle
refresh rotates the chain as usual, and each new access token propagates through the
registry and pull machinery, keeping peers usable indefinitely while the origin is
alive. A peer whose synced copy expires with nothing fresher available reports
needs-login — log in there, or wait for the origin — and sinks in scoring; `ccp login`
mints that host its own chain, making the account dual-origin. On `invalid_grant`, the
daemon strips the spent refresh token from its own blob and pulls the winner's
stripped chain, so a chain owned by two hosts self-heals into origin plus peer on the
first double-spend.

**Freshness is owned-first.** Ownership is refresh-token presence: a blob holding a
refresh token is owned, one holding only an access token is synced. Sync never
replaces an owned blob, and a synced blob advances only to a strictly later expiry. A
host never publishes a chain stamp for an account it doesn't own, so an empty chain
can never clobber a live origin's in the merge. The origin is authoritative for its
own chain — a self-consistent answer ahead of the registry stamp is mirror lag, not
corruption — while a relayed answer must match the registry's hash and expiry exactly,
so a tampered stamp stays uninstallable. The full rationale, including the
adversarial-review record behind it, is in the design note (`ccn doc show 4dce1ad`).

**Teardown fails closed.** A tombstoned account is removed locally only when the host
can prove it idle — no live session, no launch reservation, no overlay conversion in
flight; anything unprovable defers to the next pass. After claiming the removal, the
registry is re-checked so an account re-added in the window is spared, and a uuid shared
by several local rows is refused outright, never guessed at. `ccp doctor` reports the
resulting wedge, along with sync-socket health, mesh reachability, and registry
corruption — which would otherwise fail the refresh gate open.
