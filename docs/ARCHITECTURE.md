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
`Claude Code-credentials-<hash>`. cc-pool gives each account a real, unique File Provider
root selected by macOS, so each gets its own Keychain item, its own independent
OAuth grant (its own refresh-token chain), and runs on its own **subscription** — never API
billing.

The pool's service-name derivation always emits a hash-suffixed name and can never name the
canonical unsuffixed item, so plain claude's credential is unreachable from pool code.

Each account's private FuseKit backing is seeded with a copy of your `~/.claude.json` with
the identity stripped (the account's own login writes its identity), so pooled sessions
inherit your settings, MCP servers, and per-project tool approvals instead of running
first-run onboarding.

## Revisioned tenant presentation

Each account has one immutable FuseKit tenant generation. Its private backing directory
keeps Claude identity and credential state, while the revisioned FuseKit catalog presents
the effective shared configuration through File Provider. Tenant
and domain identifiers derive from the account instance ID, never a path, filename, or
private/computed classification.

cc-pool publishes one authoritative source revision. FuseKit computes the effective object
fingerprint per tenant, suppresses unchanged tenants, journals the exact catalog delta, and
notifies only domains that are live and materialized for an affected object. Inactive
tenants remain logically stale until `PrepareTenant` catches up that one selected tenant.
Atomic replacement is one catalog transaction: the source object keeps its identity, the
replaced target is tombstoned once, and old handles retain their pinned snapshot.

The fixed Developer ID-signed `~/Applications/CCPoolStatus.app` embeds the File Provider
catalog runtime in the same Mach-O as the application. Account presentation uses only File
Provider and requires no Network Volumes authorization.

The release formula carries the exact notarized application as a verified Cellar resource;
it never writes an Applications directory from the Homebrew sandbox. `ccp package install`
passes that resource and its source-bound service policy to daemonkit's sealed deployment
controller. Daemonkit alone stages it on the destination filesystem, proves the prior
generation stopped, atomically replaces the fixed per-user application, activates the exact
new generation, and recovers or rolls back every durable phase. `ccp package uninstall`
likewise delegates quiescence and canonical removal to daemonkit's sealed transaction.

The same app owns the File Provider broker endpoint in App Group `SXKCTF23Q2.ccp`.
`CCPoolFileProvider.appex` reaches only that broker. The app forwards catalog traffic to
the ordinary `~/.cc-pool/fusekit/fusekit.sock` endpoint; the pure-Go cc-pool account daemon
never resolves, names, binds, dials, or traverses the Group Container. Host and extension
signatures, Team ID, hardened runtime, provisioning, and entitlements are release gates.
There is no second signed daemon identity and no direct home-directory exception in the
File Provider extension.

A few entries stay per-account instead of shared: `daemon/` and `ide/` (Claude's PID-keyed
supervisor and IDE lock/socket files, which would collide across concurrent sessions),
`backups/` (rotating backups of each account's `.claude.json`), the identity and credential
files `.claude.json` and `.credentials.json`, `.last-update-result.json` (instance-local
auto-update state), and `remote-settings.json` (claude's cached per-subscription settings).

Per-account `.claude.json` doesn't mean settings fork. Its shareable top-level keys —
everything except identity, per-project state, and startup counters — flow from
`~/.claude.json` into pooled sessions, so a setting changed in vanilla `claude` reaches
every affected tenant as one source revision. A shareable mutation made inside a pooled
session commits through the same catalog/source transaction; identity and instance-local
keys remain in that tenant's private backing state.

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

`ccp service install` installs and starts the daemonkit-owned **user LaunchAgent** — a root
daemon couldn't read your login Keychain. Homebrew installs the binary but does not own a
second service lifecycle. The daemon polls
usage every ~3 min with exponential backoff, refreshes **idle** accounts' tokens before they
expire (a checked-out session owns its own refresh; the daemon adopts whatever token it
rotated to on check-in), caches scores, publishes authoritative source revisions, and asks
FuseKit to provision or prepare exact tenant generations. It does not supervise presentations,
File Provider extensions, or App Group listeners. The fixed signed app owns the FuseKit
runtime through daemonkit; the account daemon connects over the exact private session and
fails closed on a missing or mismatched runtime. `ccp add` and `ccp init` start the account
daemon automatically. `ccp select` starts the exact matching daemon but remains metadata-only:
it neither prepares a tenant nor returns a runnable path. `ccp run` refuses launch until the
requested tenant revision and its File Provider presentation are proven ready.

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

**One delivery path.** The daemon exposes only Synckit's exact Export/Apply contract.
Each immutable snapshot contains the secretless CRDT plus delivery-only credentials
owned by that source host. Those credentials are **stripped** before export: the access
token is bound to the registry chain hash and expiry, while the refresh token never
leaves the origin process. Apply validates the canonical envelope and chain before
making the access-only credential available to that one local convergence call. cc-pool
does not persist delivery material in its registry or outbox, and there is no custom
credential-fetch RPC, peer dialer, or compatibility transport.

**Refresh discipline: one origin per chain.** Claude refresh tokens are single-use, so
two hosts refreshing one chain double-spend it and the loser gets signed out. Each
chain therefore has exactly one **origin** — the host whose login minted it — and only
the origin refreshes. Origin is a static fact, not a leased role, and the enforcement
is structural: a peer holds only the access token and its expiry, never the refresh
token, so it has nothing to double-spend no matter how it races. The origin's idle
refresh rotates the chain as usual, and each new access token propagates through the
registry snapshot delivery, keeping peers usable indefinitely while the origin is
alive. A peer whose synced copy expires with nothing fresher available reports
needs-login — log in there, or wait for the origin — and sinks in scoring; `ccp login`
mints that host its own chain, making the account dual-origin. On `invalid_grant`, the
daemon strips the spent refresh token from its own blob and accepts the winner's
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
can prove it idle — no live session, no launch reservation, no tenant preparation or
durable source mutation in flight, and exact File Provider domain absence; anything
unprovable defers to the next pass. After claiming the removal, the
registry is re-checked so an account re-added in the window is spared, and a uuid shared
by several local rows is refused outright, never guessed at. `ccp doctor` reports the
resulting wedge, along with sync-socket health, mesh reachability, and registry
corruption — which would otherwise fail the refresh gate open.
