# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.69.0] - 2026-08-30

### Removed
- The v0.20.9 cross-era migration: the boot-time gate that booted out an
  unmarked pre-daemonkit LaunchAgent and held that era's listener lock, the
  legacy account-terminal ledger sweep that refused a boot while a
  pre-upgrade `claude auth login` was still recorded, and the direct legacy
  bootout `ccp service install`/`stop` led with. It was written for one
  transition cycle out of v0.21.x and has run on every boot since; no v0.20.9
  incumbent survives to gate. A machine still carrying
  `~/.cc-pool/account-terminals-v1.db` or a stale `daemon.sock.lock` loses both
  to `ccp service uninstall --purge`.
### Added

- `ccp package reset` retires the installed application's launchd agents and
  deployment records through daemonkit's `Deployment.Reset`; the installed bytes
  and the FuseKit catalog under `~/.cc-pool/fusekit/` stay where they are.
  It is the way out of a deployment state no other verb accepts — a sealed
  activation whose daemon has since restarted, a removal proof for an app that
  was put back by hand, a plain file planted at a generation slot — where
  `ccp package install` refuses and `ccp package uninstall` would throw away a
  working installation. The verb binds the deployment from the compiled service
  contract alone, never from the installed bundle, so a missing or mangled
  application cannot refuse the reset that exists to clean it up.

### Fixed

- **`CCPoolStatus` no longer crash-loops on `catalog: tenant generation
  mismatch`.** Every runtime start republished cc-pool's v1 source-authority
  topology unconditionally, always at the hard-cut generation 1. fusekit's
  catalog admits a publication only when the generation it expects matches the
  one already stored, so the first start stored generation 1 and every start
  after it was refused:

  ```
  CCPoolStatus: FuseKit runtime start failed: publish exact source fleet: catalog: tenant generation mismatch
  ```

  Both machines running 0.68.0 wedged this way, and removing
  `~/.cc-pool/fusekit/catalog.sqlite` bought one boot before the same refusal
  came back. A start now reads the stored desired fleet first and publishes
  nothing when its owner, authority count, and both digests already match the
  topology this build compiles. A start that loses the publication race to a
  concurrent start re-reads once and accepts the winner's topology, so two
  starts from an empty catalog no longer leave one of them refused.

  The generation stays cc-pool's hard-cut v1 rather than advancing from the one
  just read. `resolveClaudePhysicalPolicy` refuses a stored source-task identity
  whose `FleetGeneration` is not `SourceAuthorityFleetGeneration`, so a fleet
  advanced to generation 2 would leave a daemon that cannot start at all — a
  worse wedge than the one this fixes. A stored topology that genuinely differs
  from the compiled one still surfaces the catalog's refusal rather than being
  swallowed; it already fails earlier, inside the driver factory, and making
  that case converge is a separate change.

- Requires fusekit v1.20.0 for `LocalTenantController.DesiredSourceFleet`,
  which reads an owner's published fleet back without publishing; an owner that
  has never published reads as absent. The widget's Swift `FuseKit` pin moves to
  1.20.0 with it — `verify-suite-pins.sh` refuses the skew.

## [0.68.0] - 2026-08-27

### Fixed

- **Requires fusekit v1.19.0, which stops the FuseKit holder wedging itself.**
  fusekit drew every reap receipt's sequence from one counter shared by all
  eight recovery IDs, while the catalog's source-owner floor demands strict
  contiguity. One unclean holder exit with the catalog worker still tracked left
  a hole in the source-owner sequence that nothing could close, and
  `CCPoolStatus` then failed the same way on every start:

  ```
  CCPoolStatus: FuseKit runtime start failed: FuseKit runtime: daemon returned before readiness
  FuseKit runtime: recover source-owner receipts: catalog: mutation id reused with different request
  catalog: reap receipt acknowledgement is out of order
  ```

  Because `ccp daemon` requires a live tenants runtime and `CCPoolStatus`
  provides it, the pool daemon cannot come up behind a wedged holder. One
  machine crash-looped this way for six days, `~/.cc-pool/fusekit/holder.log`
  reaching 108,651 lines.

  v1.19.0 numbers each recovery ID separately and migrates an existing
  `~/.cc-pool/fusekit/processes.db` in place. It does not repair a ledger that
  already holds a gap: a holder still failing on `reap receipt acknowledgement
  is out of order` after this upgrade needs `catalog.sqlite` and `processes.db`
  removed together from `~/.cc-pool/fusekit/`, which rebuilds both from the
  pool's own account state. Deleting either alone re-wedges on the other, and
  neither file holds account data — accounts live in `~/.cc-pool/pool-v1.db`
  and the Keychain.

- **Requires fusekit v1.19.0's boot-session fix.** fusekit identified a boot by
  rendering `kern.boottime`, which darwin slews as the clock is adjusted, so a
  process record captured before an adjustment could be declared cross-boot and
  retired without its PID ever being probed. A `CCPoolStatus` generation that
  was still running could be retired under a live owner.

## [0.67.0] - 2026-08-26

### Changed

- Requires daemonkit v0.23.0 and fusekit v1.18.0, which move every daemon's
  private state from `~/.daemonkit/agents/<Label>` to `~/.daemonkit/a/<Label>`.
  daemonkit v0.22.0's root spent 18 of the 103 bytes darwin's `sun_path` leaves
  a socket path, which put cc-orchestrate's pty daemons one byte over the limit
  under a long account name; the byte comes out of the root rather than out of
  any consumer's label.

  Both of cc-pool's daemons — `com.yasyf.cc-pool` and the FuseKit status holder
  `com.yasyf.cc-pool.status.fusekit` — come up fresh under the new root. There
  is no migration and no fallback read, so a `~/<Label>` directory left by a
  0.65.x build stays where it lies and is deleted by hand once the daemon is
  confirmed running under `~/.daemonkit/a/`. Upgrading straight from 0.65.x
  skips the intermediate root entirely, leaving one stale directory per daemon
  rather than two; 0.66.0 is the only release that creates the intermediate one.

  `~/.cc-pool` is unaffected — that is cc-pool's own application directory, not
  daemon state.

## [0.66.0] - 2026-08-17

### Fixed

- `ccp add` provisions an account again. Since the daemonkit v0.21.3 migration
  every add failed with `daemonkit: business session: context deadline
  exceeded`, and no timeout was ever going to be long enough: the File Provider
  broker could not start at all. The signed app was launched with the daemon's
  socket path in its argv and compared it against a constant of its own, and
  when that migration moved the daemon's socket the two stopped agreeing. The
  app refused every broker it was handed — `daemonSocketMismatch` in
  `~/.cc-pool/fusekit/broker.log` — so provisioning waited on a runtime that
  was never coming up.

  The broker now takes its sessions over the channel it inherits at spawn
  (fusekit v1.17.0, daemonkit v0.22.0), so there is no path for the two sides
  to disagree about, and none for another process on the account to bind first.
  `holderSocketPath` is gone from the widget's configuration, as is
  `pool.FuseKitSocketPath`, which nothing had called since the holder stopped
  binding that path.

- A rollback no longer buries the failure that caused it. Retiring a reserved
  FuseKit tenant that provisioning never got around to creating reported
  not-found, so the add reported its cleanup error alongside the real one —
  `retire reserved FuseKit tenant: cc-pool holder: not_found: catalog: not
  found: catalog: not found`. Already absent is the end state a retirement
  wants, so it now succeeds, and `ControlRemoteError` carries the holder's
  classification through `Unwrap` rather than only in its text.

## [0.65.2] - 2026-08-16

### Changed
- Builds now use Go 1.26.6, which patches five standard-library advisories
  reachable from cc-pool's own call graph: GO-2026-5972 (`encoding/asn1`),
  GO-2026-6088 (`net/url`), GO-2026-6089 (`crypto/tls`), GO-2026-6090
  (`net/http`), and GO-2026-6218 (`encoding/xml`). All five were published
  against 1.26.5 after `0.65.0` shipped. The `go` directive stays at 1.26.5 so
  consumers are not forced up.

## [0.65.1] - 2026-08-16

### Fixed
- `ccp package install` no longer fails with `peer failed trust verification:
  wire: untrusted peer`. The FuseKit runtime admitted only the status app's
  signing identity on its control lane, but the app drives its runtime in
  process and never opens a wire session — the lane's only peer is the Homebrew
  CLI, signed `com.yasyf.cc-pool`, so the runtime rejected the one caller the
  requirement exists to protect. On a machine with no app installed this
  surfaced late, at `activate installed application`, after the app, the
  LaunchAgent, and the running job had all landed correctly; `ccp init` and
  `ccp doctor` then reported a healthy pool, because nothing outside deploy's
  own idempotency check reads the activation record that was never sealed. The
  same rejection blocks every subsequent install outright, at
  `land packaged candidate`: superseding an installed app drains the running
  daemon over that same refused lane, and deploy's session-less fallback covers
  only a draining or absent peer. Upgrades were unreachable on any machine that
  had installed `0.65.0`.

  Installing `0.65.1` over `0.65.0` needs the old runtime booted out first,
  since the `0.65.0` daemon still refuses the drain:
  `launchctl bootout gui/$(id -u)/com.yasyf.cc-pool.status.fusekit`, then
  `ccp package install`.

## [0.65.0] - 2026-08-04

cc-pool moves to daemonkit v0.21. Credential ownership is fenced on an opaque
per-generation owner record, the store accepts exactly one schema fingerprint,
and the daemon's control surface revs its wire schema. Downgrading after the
first post-upgrade boot is unsupported: a `0.64.x` daemon fails loudly on the
new owner records and stays broken until upgraded again.

### Changed
- Credential ownership fencing now keys on an opaque per-generation owner
  record across all five owner-carrying store tables (active credential
  operations, account mutations, pending account reservations, and both
  receipt tables) instead of a decoded process identity. A newly started
  daemon claims every active row a prior generation left behind, daemonkit
  `v0.20.9`-era rows included byte-for-byte as stored, and honors that
  generation's receipts through evidence replay, pending publication
  delivery, and acknowledgement, so interrupted credential operations,
  account mutations, and pending account reservations resume or settle at
  boot instead of orphaning. Downgrading after the first post-upgrade boot
  is unsupported; a `0.64.x` daemon fails loudly on the new owner records
  and stays broken until upgraded again.
- The store accepts exactly one schema fingerprint per binary and refuses
  any other database outright. Upgrading from `0.64.x` keeps the existing
  database, whose fingerprint is unchanged; in-flight rows are adopted as
  above.
- Account terminals now settle their children through daemonkit `v0.21.4`
  process scopes. A controller attach that races terminal cancellation now
  fails with daemonkit's "closed before Release" refusal instead of
  silently succeeding on a dying terminal.
- Crashed terminal children are now reclaimed when the daemon's process
  scope opens, before any work is admitted; recovery no longer runs as a
  separate admission gate inside the terminal manager.
- Daemon and worker lifecycle now runs on daemonkit `v0.21.4`.
- The daemon's control surface is a unary request and reply protocol whose
  account-mutation output is polled rather than streamed, and its wire schema
  revs to match. A CLI and a daemon built from different revisions now refuse
  each other by exact build identity instead of attempting to interoperate.
- The first launch after upgrading from `0.64.x` retires the previous service
  registration and takes over the legacy socket lock before claiming any work,
  so it replaces a running legacy daemon rather than racing it. If a crashed
  `0.64.x` daemon left tracked interactive `claude auth login` terminals
  behind, the first launch refuses to start rather than risk a surviving login
  overwriting credentials it manages: the log names the exact command that
  clears the old terminal ledger once you have confirmed no login window is
  open, and the daemon retries automatically and starts once it is cleared.
  It never ends a login itself — you may be in the middle of one. Both checks
  happen once per machine.
- The sync helper runs as a resident service on its own socket, addressed by
  the service it registers rather than by a path the pool computes.

### Fixed
- Confirming that the helper service is active now also confirms the process
  answering is the installed application's own binary: the connection itself
  requires the installed application's signing identity, and the serving
  process is additionally pinned to one live instance of the installed bundle
  across the observation. Previously any process running as the same user
  satisfied the check by listening on the socket.
- `ccp service install` succeeds again. It compared the daemon's reported build
  against this CLI's version, but those name different things — the former is
  daemonkit's own digest of the running executable, the latter cc-pool's
  release version — so the check could never hold. Every successful install
  reported a build mismatch and rolled its own holder installation back.
- Shutting the daemon down no longer releases the credential store while an
  account mutation is still settling. A drain whose budget ran out left its
  worker join behind and closed the store anyway, and the settling mutation
  then read through the freed handle. Teardown now waits for that join, and on
  a spent budget it keeps everything held rather than releasing it early.
- `ccp select` no longer accepts a daemon that has begun draining. Readiness
  came from a status the daemon last published while serving, which stayed
  behind once shutdown started; it now follows the daemon's live phase, so a
  draining daemon is refused instead of failing the next call.
- Removing an account no longer gives up after 30 seconds. The tenant call it
  makes imposed its own half-minute limit on top of the caller's, so the
  four-minute budget the command asks for was cut short with time to spare.
  A caller that states its own deadline now keeps it.
- Host sync no longer fails on a large but legitimate worker response. Replies
  were capped at 4 MiB — a default that applied because no limit was stated —
  while the protocol admits 12 MiB, so a valid larger reply was truncated and
  reported as an error.
- Starting the daemon no longer retires a reservation its own sync helper is
  still holding. Startup claimed abandoned work from earlier runs while the
  helper was already live, and every helper records a distinct owner, so its
  in-flight reservation was indistinguishable from one left behind by a dead
  predecessor. The claim now completes before the helper starts.
- The bundled runtime reports itself installed only once its tenant lane
  answers, rather than as soon as the primary holder starts — activation could
  previously succeed and then unwind when a later startup step failed. If
  either embedded lane stops, the application now stops with it instead of
  running on with one lane missing.
- The command the daemon prints for clearing a pre-upgrade login ledger now
  quotes the path, so it still runs when the home directory contains an
  apostrophe. It was the only documented way past that refusal.

## [0.64.9] - 2026-07-27

### Fixed
- Helper installation now resolves the real home directory from the passwd
  database through daemonkit `v0.20.9`, so a helper installed under Homebrew
  postinstall's sandboxed temporary `HOME` still lands in the invoking user's
  launchd domain.
- A launchctl exit status of 5 is no longer retried as a transient bootstrap
  failure; it surfaces immediately as the permanent domain rejection it is.
- Recovery-mode reconciliation now clears a self-wedged install instead of
  leaving a half-registered service the next bootstrap cannot replace.
- The Go runtime and the packaged status application now share FuseKit
  `v1.15.5`, whose DaemonKit requirement matches the pinned `v0.20.9`. The
  widget's Swift package graph could not resolve at all against FuseKit
  `v1.15.3`, which held DaemonKit at `0.20.6`.

## [0.64.8] - 2026-07-25

### Fixed
- Homebrew publication now verifies the exact installed CLI and signed
  application without attempting GUI runtime activation on a headless release
  runner. Live helper activation and FuseKit readiness remain mandatory at the
  GUI fleet cutover.
- Signed FUSE-T libraries with no entitlements now use FuseKit's canonical
  empty entitlement attestation instead of failing helper activation.
- Daemon and worker lifecycle now runs on daemonkit `v0.20.6`, whose durable
  untrack lands once a reaped owner has settled.
- The Go runtime and the packaged status application now share FuseKit
  `v1.15.3`.

## [0.64.6] - 2026-07-24

### Fixed
- Release preflight now rejects lightweight, unsigned, or unverified tags
  before building or publishing any artifact.
- Signed-helper entitlement attestation now uses daemonkit's canonical plist
  digest through FuseKit `v1.15.1`, eliminating representation-dependent
  mismatches during a clean package install.
- Daemon and worker shutdown now uses daemonkit `v0.19.1` terminal runtime
  claims so failed owners settle and reaping completes before replacement.
- Fleet synchronization now pins synckit `v0.35.2`, keeping its daemonkit
  runtime dependency explicit across the hard-cut release line.

## [0.64.5] - 2026-07-24

### Fixed
- Clean installations now derive the canonical signed-helper service policy
  from the packaged application before the fixed user application target
  exists. Release publication proves the real absent-target package install,
  runtime readiness, signature, notarization, and uninstall path.

## [0.64.4] - 2026-07-24

### Fixed
- Homebrew installation now keeps the signed status application archive
  opaque until the formula extracts it explicitly, preserving the exact
  `CCPoolStatus.app` bundle root and signed Mach-O load commands through
  resource staging and linkage repair.

## [0.64.3] - 2026-07-24

### Fixed
- The packaged status application resource now renders the literal release
  version into its download URL instead of resolving an unset resource-local
  Homebrew version.

## [0.64.2] - 2026-07-24

### Fixed
- Homebrew formula dependencies now precede packaged application resources,
  and the exact rendered formula is rejected before publication when this
  ordering regresses.

## [0.64.1] - 2026-07-24

### Fixed
- Homebrew publication now permits signed helper applications packaged inside
  formulae while continuing to reject retired standalone helper casks.

## [0.64.0] - 2026-07-24

### Changed
- Each account now has an immutable, path-independent instance identity. Claude
  runs through a private stable execution symlink under
  `~/.cc-pool/config/<instance-id>`, while File Provider keeps the separate
  public `~/Library/CloudStorage/CCPoolStatus-acct-NN` presentation. Keychain
  services derive only from the stable execution path.
- The signed `CCPoolStatus.app` embeds the FuseKit runtime and installs from the
  same Homebrew formula at `~/Applications/CCPoolStatus.app`; there is no
  standalone holder or cask.
- The final runtime stack pins daemonkit `v0.18.0` and FuseKit `v1.14.0`
  exactly in both Go and Swift.

### Fixed
- Daemon bootstrap now provisions every desired account presentation before
  readiness, waits through the bounded starting state, and rejects draining
  generations. Account selection no longer fails fleet-wide with
  `runtime presentations are starting`, `wire: server is draining`, or holder
  readiness timeouts during a normal startup.
- Pending and committed account removal now durably records authorization,
  retires the exact File Provider presentation, removes the stable execution
  link, and settles credential/backing state before an account index can be
  reused. Lost responses, owner death, restart replay, and ambiguous external
  state all fail closed without deleting a live credential or presentation.
- Credential access validates the exact account generation, stable execution
  path, Keychain locator, and real owned `0700` public target before any
  Keychain I/O. Foreign links and unverifiable external state remain
  quarantined instead of being silently repaired.
- Release packaging now verifies the exact signed, notarized, stapled app
  digest carried by the formula and invokes the installed formula's `ccp`
  binary for package activation.

## [0.62.0] - 2026-07-23

### Changed
- cc-pool now uses daemonkit for exact persistent-session transport, peer
  identity, listener takeover, admission and draining, launchd policy,
  disposable process groups, and durable worker reaping. The daemon no longer
  owns a parallel LF-delimited protocol, process supervisor, socket lifecycle,
  or generated-plist rewrite path.
- Account selection now acquires a short, expiring reservation, releases the
  account transaction, asks FuseKit to `PrepareTenant` at an exact revision,
  and activates the session lease only if the reservation and account
  generation still match. Account claims are never held across filesystem,
  provider, socket, process, Keychain, or OAuth I/O.
- Filesystem presentation now uses FuseKit's revisioned tenant catalog and
  authoritative source fleet. Opaque object identity survives atomic
  replacement, catalog deltas drive incremental File Provider enumeration, and
  demand-aware convergence targets only live, materialized domains whose
  effective content changed.
- Credential writes, moves, refreshes, divergent-copy cleanup, and recovery are
  generation-fenced durable operations. Context-unaware Keychain and file work
  runs in disposable workers under Claude's credential locks, verifies exact
  before/after observations, and is terminated and reaped before a timed-out
  lane can be reused.
- Credential settlement now records explicit result categories, failure
  classes, slot states (`empty`, `present`, `unsearchable`, and `unreadable`),
  and terminal success, failure, or quarantine instead of collapsing uncertain
  external state into a retryable write. Retained provider failures never
  masquerade as new outage probes or repeat OAuth work; internal ambiguity
  stays unselectable until an exact readable credential replacement resolves
  its receipt and quarantine fence.
- The fixed Developer ID-signed `CCPoolStatus.app` now embeds the FuseKit
  runtime and App Group broker alongside its File Provider extension and widget.
  The unsigned Go daemon uses only its private socket and never resolves,
  names, binds, dials, or traverses the App Group container.
- Runtime state and every cc-pool-owned protocol start at one fresh epoch:
  `pool-v1.db` with SQLite schema version 1, `cc-pool.rpc.v1`, status snapshot
  protocol 1, and v1 operation-ID domains. Exact equality is required before
  mutation; old databases and clients fail closed.

### Added
- Durable account-mutation, credential-operation, source-authority, and removal
  journals with generation fences, exact worker authority, crash recovery, and
  causal publication evidence.
- A single signed runtime/broker topology and release/VM assertions for code
  identity, entitlements, App Group isolation, worker deadline enforcement,
  atomic replacement, and reconciliation amplification.

### Removed
- The cc-pool-owned mount runtime, overlay conversion and probing, custom File
  Provider bridge/controller/enumerator, per-domain notification fan-out,
  session-lease files, PTY relay, migration commands, and old daemon wire.
- All state and wire migration readers, import/export cutover tools, protocol
  negotiation, compatibility aliases, and mixed-version operation. The hard
  cut rebuilds derived runtime state from the account source of truth; it does
  not teach the new binary to read an old epoch.

### Fixed
- A timed-out content catch-up can no longer retain an account claim or leave
  abandoned filesystem/provider work running after the caller returns.
- One semantic Claude configuration change no longer causes global File
  Provider fan-out, private-to-computed identity changes, full-root incremental
  rebuilds, or cc-pool-amplified reconciliation retries.

## [0.28.0] - 2026-06-16

### Changed
- The widget header shows **pool pace** in place of the old "burn N%/h". That
  phrase summed every account's window drain but measured it against nothing, so
  it read as most dire when the pool was healthiest — "burn 58%/h" looks fatal,
  yet a six-account pool regenerates ~120%/h and holds that line indefinitely.
  Pace divides the gross burn by the pool's regeneration rate: below 1.0 the
  pool refills forever (`pace 48%`), above 1.0 a wall is coming. The binding
  window is shown, labeled when the week is the constraint (`7d pace 130%`). The
  "refilling N%/h" and dry-clock captions are unchanged.
- The pool dry-out clock is refill-aware. It used to disappear whenever any
  account's reset fell before the projected wall — even when burn kept
  outpacing refills past that reset — so a genuine deadline could read as "no
  wall." It now forward-simulates the staggered resets and reports the first
  moment combined capacity actually reaches zero.
- The large widget's per-account line is tighter: the projection and its 5h
  reset collapse into one clause (`→37% by 5:20 AM`), and the weekly reset shows
  only once the week is meaningfully spent (7d ≥ 40% used).

### Added
- The `ccp status --json` pool block gains `pace_5h` and `pace_7d` (gross burn
  over regeneration rate; 1.0 is sustainable), and each account gains
  `burn_7d_per_hour`. The existing `burn_5h_per_hour` and `net_burn_5h_per_hour`
  keys are untouched and the protocol version is unchanged, so a widget built
  before this release keeps decoding the snapshot and simply ignores the new
  keys. A widget from this release talking to an older daemon re-derives pace
  locally from the gross burn, so it shows pace either way.

## [0.19.0] - 2026-06-11

### Added
- New accounts get a friendly default name derived from their logged-in
  email instead of the raw address: consumer providers keep the username
  (`yasyfm@gmail.com` → `yasyfm`), org domains become the org name
  (`rebecca.fang@ucsf.edu` → `UCSF`, `yasyf@aneta.company` → `Aneta`).
- `ccp rename <account> <new-label>` renames an account, and
  `ccp rename --auto` applies the derived names to every account whose label
  is empty or still the raw email (`--force` overwrites custom labels too) —
  the migration path for pools named under the old default. Renames are live:
  the daemon and widget pick them up on their next read, no restarts.
- `ccp remove` and `ccp rename` both accept `acct-NN` account references as
  printed by `ccp list`, not just bare ids.

## [0.18.2] - 2026-06-11

### Fixed
- The widget no longer dims as "stale" while the daemon is healthy. The
  timeline planted a future-dated pre-dimmed entry 7 minutes after each
  snapshot, assuming reloads land every poll — but WidgetKit budget-throttles
  reloads, so the widget spent most of its life dimmed over fresh data.
  Staleness is now judged when the timeline is rebuilt, and the self-updating
  "updated … ago" footer always carries the true age.

## [0.18.1] - 2026-06-11

### Fixed
- `ccp widget` works on Homebrew 5, which removed the `--no-quarantine`
  install flag: quarantine is now disabled via `HOMEBREW_CASK_OPTS`, and any
  quarantine attribute that slips through anyway is stripped after install
  (Gatekeeper blocks the ad-hoc-signed app otherwise). The first launch also
  opens the app by path — LaunchServices hasn't indexed a freshly installed
  app yet, so the by-name lookup failed on exactly the registering launch.

## [0.18.0] - 2026-06-11

### Added
- `ccp widget` installs the Notification Center widget with one command: it
  pulls the prebuilt app from the new `cc-pool-status` Homebrew cask with
  quarantine disabled (which the ad-hoc-signed app needs), launches it so
  macOS discovers the widget, and walks through enabling it. Releases now
  build and publish the app zip, and the formula-bump job keeps the cask
  current.

## [0.17.0] - 2026-06-11

### Added
- The daemon now mirrors account status to `~/.cc-pool/status.json` after every
  completed poll (atomic temp+rename, same per-account shape as the socket
  `status` op plus a `generated_at` heartbeat), so out-of-process readers can
  render status without the socket.
- `ccp status --json` prints that same snapshot schema — the daemon's cached
  view when available, live sampling otherwise.
- A macOS Notification Center widget (`widget/`, Swift/WidgetKit via XcodeGen)
  that visualizes the snapshot: per-account 5h/7d usage bars, live-session
  counts, and stale/rate-limited/exhausted/overage flags in the CLI's display
  order. See `widget/README.md` for build and install steps.

## [0.16.0] - 2026-06-11

### Added
- Manual pin/unpin from the status TUI: the new `p` toggle pins the launch
  directory to an account, badged in the list, detail pane, and `--plain`
  table. A manual pin always binds while the account can serve; when it
  cannot, selection ranks freely, keeps the pin, and reports the bypass.

### Changed
- Pin expiry is activity-based instead of a flat hour after the last select:
  a pin survives while a tracked session runs in its directory and dies one
  TTL after the later of its last select and last session end, so long
  sessions and the post-session warm-cache hour stay protected.

### Fixed
- Status never sorts an unusable account above a usable one: display order
  now mirrors selection preference (available, then exhausted, then
  rate-limited; score deciding only within a tier), so the ▸ next-pick marker
  always points at an account that can actually serve.

## [0.15.1] - 2026-06-11

### Fixed
- The daemon no longer logs a `cannot link .../remote-settings.json` error on
  every overlay sync. claude's new `remote-settings.json` file caches
  per-subscription settings from claude.ai, so each account now keeps its own
  private copy instead of sharing one; a leftover shared link from an earlier
  version is cleaned up automatically.
- An overlay sync that hits a conflict on one entry now still repairs every
  other entry and reports all conflicts at once, instead of stopping at the
  first one.

## [0.15.0] - 2026-06-10

### Changed
- The generated shell alias (and docs) wrap `ccp run` instead of the
  `ccp select` compose form, so the alias always launches with the full
  environment cc-pool sets up, even as future versions add to it.

### Fixed
- Pooled sessions stamped account-anchored install paths into the shared
  `~/.claude/plugins` state, which claude's marketplace validator rejected as
  a "corrupted installLocation". cc-pool now pins the plugin root by setting
  `CLAUDE_CODE_PLUGIN_CACHE_DIR=~/.claude/plugins` everywhere it launches or
  instructs a launch — `ccp run`, the `ccp add` login, and the `ccp env`
  export lines; a user-set value is respected.

## [0.14.0] - 2026-06-10

### Added
- When every account is exhausted, `ccp select` and `ccp run` fall back to
  the least-bad account and print a loud billing warning to stderr naming the
  binding reset window, so extra-usage billing is never silent; `ccp env`
  prints the same warning.
- `ccp status` shows exhausted and overage `$X/$Y` badges, backed by the
  extra-usage overage block now decoded and persisted from the usage API.
- The daemon logs every selection (pick kind, score, usage, runner-up) so
  surprising picks can be audited after the fact.

### Changed
- The format of the internal usage database changed incompatibly, and
  cc-pool does not migrate old databases. If you upgraded from an earlier
  version and see database errors, delete `~/.cc-pool/pool.db*` and restart
  the daemon; it rebuilds the database from scratch. The database holds only
  regenerable cache and state, never secrets.

### Fixed
- `ccp` could launch a session on an account whose 5-hour window was 100%
  used — imminent-reset credit forgave total exhaustion and a sticky session
  pin never let go — silently billing extra-usage credits. Accounts with a
  pegged window and a pending reset are now unavailable, and the headroom
  barrier, runway, and sticky floor score raw current remaining, so an
  imminent reset can no longer mask zero headroom.
- `ccp select --wait` now actually waits instead of returning immediately.
  It refuses exhausted-pool fallback picks and keeps waiting for headroom,
  and a pick it discards while waiting no longer counts against the account.

## [0.13.2] - 2026-06-08

### Fixed
- Account scoring forgave far too much weekly usage when the 7-day reset was
  days away — an account at 73% weekly usage with a reset ~2.5 days out could
  rank first over fresher peers. The reset-credit horizon is now capped at
  one 5-hour session, so a distant weekly reset earns no credit and accounts
  rank by true weekly headroom.
- The `ccp select`/`ccp run` heading and the `ccp status` detail panel
  labeled the reset-aware "effective" figure as `free`/`remaining` next to
  the raw `used` bars, producing contradictions like 73% used / 74% free.
  The heading now shows raw % used (health-tinted), and the panel labels its
  score term `effective` so it reconciles with the usage bars.

## [0.13.1] - 2026-06-08

### Fixed
- `ccp add` no longer hangs silently before the login prompt: the daemon now
  answers readiness checks immediately on startup, a restart race that could
  kill the fresh daemon and start a second instance is gone, and the
  remaining startup waits show a spinner so they read as progress.

## [0.13.0] - 2026-06-08

### Changed
- Plan-mode plans are shared across the whole pool: the overlay links each
  account's `plans` dir to the single `~/.claude/plans` store, so plans
  written in one pooled session are visible from every account and from
  plain `claude` instead of being scattered per account.

## [0.12.4] - 2026-06-08

### Fixed
- `ccp add` no longer mistakes plain claude's startup-adopted session secret
  for a completed login. Over headless SSH, `claude /login` copies the global
  session secret into the account's credential store before any login
  happens; the add watcher treated that copy as "login done", killed claude
  mid-menu, and registered a duplicate of the main subscription. Completion
  is now gated on the account's own `.claude.json` identity, and finalize
  refuses accounts without one — console/third-party API logins, which write
  no identity, are rejected: cc-pool pools Max/Pro OAuth subscriptions only.

## [0.12.3] - 2026-06-08

### Fixed
- Upgrading no longer prints a `pkill -f 'cc-pool daemon'` instruction when
  an old daemon is slow to release the socket: `ccp` now evicts the stale
  daemon itself, asking launchd first and, for a true orphan, stopping only
  the exact process holding `~/.cc-pool/daemon.sock`, never anything matched
  by name.

## [0.12.2] - 2026-06-08

### Fixed
- The shared overlay no longer links or mirrors `~/.claude/.credentials.json`
  into pool account dirs. claude writes its OAuth credential to that
  plaintext file when the macOS Keychain is unavailable (e.g. a headless SSH
  session), and sharing it meant `ccp add` ran `claude /login` against plain
  claude's live credential — adopting that login instead of prompting for a
  new account, and mutating plain claude's credential on refresh, the one
  thing the pool must never touch. Each account now keeps its own.
- Credentials resolve Keychain-first with a plaintext-file fallback, so the
  login watcher, add finalization, the daemon's idle token refresh, and
  `ccp doctor` work whether claude stored the credential in the Keychain or
  in `.credentials.json` — headless setups no longer break the Keychain-only
  assumptions.

## [0.12.1] - 2026-06-08

### Fixed
- The daemon no longer logs a relink error on every overlay sync poll:
  claude rewrites `.last-update-result.json` atomically, replacing the
  overlay's symlink with a real file that sync refused to relink. The
  auto-update result is now a private per-account entry — it stays
  instance-local and never lands in the shared `~/.claude` base.
- Starting the daemon via brew (`ccp service install`) now makes sure it is
  actually running: `brew services start` only loads the launchd job, and a
  stop/start race could leave it loaded but never executing.

## [0.12.0] - 2026-06-08

### Changed
- Rate-limit reset windows show as absolute local times — `3:58 PM`,
  `tomorrow 3:58 PM`, `Tue 3:58 PM`, `Jun 15, 3:58 PM` — instead of relative
  durations like `Resets 45h50m`, across the `ccp status` table, the TUI
  5h/7d rows, and the `ccp select` waiting-until lines.

### Fixed
- The daemon converges to the installed version after an upgrade. Previously,
  when an orphaned old-version daemon held the socket but launchd no longer
  tracked it, the new daemon refused to bind and `ccp status` restarted the
  daemon on every run without converging. A starting daemon now evicts a
  version-skewed runtime (refusing to bind only against a genuine same-version
  peer), and `ccp` asks the old daemon to step down before restarting it.
  Along the way: an exiting daemon can no longer delete its successor's
  socket, health checks respond immediately on boot even while fuse mounts
  are still coming up, and fuse unmounts can no longer hang shutdown.

## [0.11.0] - 2026-06-08

### Changed
- `ccp run` and `ccp select` name the chosen account and show its effective
  5-hour / 7-day headroom instead of a generic "Selected the emptiest
  account" line; an account the daemon has never sampled degrades to a
  name-only line.

## [0.10.0] - 2026-06-08

### Added
- Bare `ccp` invocations that lead with a claude flag (e.g. `ccp --resume`,
  `ccp -p "hi"`) forward to `ccp run` instead of failing with "unknown
  flag". `ccp`'s own `-h`/`--help` and `-v`/`--version`, its subcommands,
  bare `ccp`, and unknown-command suggestions all behave as before.

## [0.9.1] - 2026-06-08

### Fixed
- `ccp status` no longer mislabels stale accounts as "no-data": the daemon
  reports whether usage data actually exists instead of inferring it from
  staleness, so an account whose poll is merely overdue keeps showing its
  real utilization.
- Accounts no longer read "stale" for most of every poll cycle: the
  staleness label used a 90-second threshold against a ~3-minute poll
  interval, and now allows 5 minutes before flagging an account.
- `ccp status`, `ccp select`, and `ccp run` detect and reject a daemon left
  running from a previous version instead of rendering garbage; the status
  TUI restarts the skewed daemon automatically.

## [0.9.0] - 2026-06-08

### Changed
- Renamed the short command from `clp` to `ccp` (the symlink beside the
  `cc-pool` binary), along with all help/error text, shell-alias generation,
  and docs. Everything named `cc-pool` is unchanged — the binary, Go module,
  Homebrew formula, launchd label, FUSE volname, and the `~/.cc-pool` state
  dir — so account dirs, Keychain service names, and stored state need no
  migration.

## [0.8.0] - 2026-06-08

### Changed
- `ccp run` is a transparent launcher: it replaces itself with `claude` via
  exec instead of supervising a subprocess, so once claude starts cc-pool is
  gone from the process tree and signals, the controlling terminal, and the
  exit code are all claude's. Arguments pass through verbatim with no `--`
  separator needed (e.g. `ccp run --resume`).
- `ccp run` picks its account the same way `ccp select` does — taking the
  daemon's reserved pick and falling back to a live selection only when the
  daemon is unreachable — and the daemon keeps the chosen account's token
  fresh in the background.

### Removed
- The `--account` flag on `ccp run`, which cannot coexist with literal
  argument passthrough; force a specific account with `CCP_ACCOUNT=<id>`
  instead.

## [0.7.0] - 2026-06-08

### Added
- Interactive `ccp status` TUI: a live-refreshing account list plus a detail
  pane that explains the selected account's score factor by factor, with a
  footer legend. Piped or non-TTY output falls back to the plain table with a
  one-line legend; `--plain` forces the table on a terminal.

### Changed
- Status 5h/7d columns show % used (100% = exhausted), matching Anthropic's
  framing, with usage bars; the `SESS` column is renamed `LIVE`, and flags
  appear inline only when present.
- `ccp select` is quieter: it names the chosen account instead of printing a
  generic reuse line, and prints only when stdout is a terminal — silent
  under pipes and command substitution.

## [0.6.1] - 2026-06-08

### Fixed
- Two processes refreshing the same account's token at once — the daemon's
  poll loop racing a short-lived `ccp` invocation — could double-spend the
  single-use OAuth refresh token, leaving the loser with `invalid_grant` and
  the account revoked until an interactive re-login. Token refresh is now
  serialized across processes with a per-account lock held over the whole
  read–refresh–write cycle; a waiter that acquires the lock after a peer
  already rotated the token re-reads the fresh credential instead of spending
  its own refresh.

## [0.6.0] - 2026-06-08

### Added
- `ccp add` no longer ends silently: it prints the canonical launch command
  and offers to wrap `claude` so plain `claude` routes through the emptiest
  account, forwarding all arguments. The wrapper is idempotent, never
  clobbers a user-defined `claude`, `-y` consents, and `--no-alias` opts out.
- `ccp add` detects when a login is the same subscription already in the pool
  (by accountUuid) and, on a TTY, asks before adding it again; non-interactive
  runs warn and proceed so automation is never blocked.

### Changed
- Trimmed the add flow to what you can act on: internal setup chatter and
  the redundant "closed claude" line are gone. "Add another account?" now
  defaults to yes after the first account, and the closing line just reports
  the pool total.

### Fixed
- Usage headroom was inflated 100x: the `/api/oauth/usage` endpoint reports
  each window's `utilization` as a percent (0–100), but the client treated it
  as a 0..1 fraction, so `ccp status` showed nonsense like -2600% headroom
  and negative scores. The value is now read verbatim, and the usage
  model matches the current API shape (the stored-but-never-scored seven-day
  Opus window is dropped).

## [0.5.0] - 2026-06-08

### Changed
- Reworked the CLI output: a shared styled output layer with consistent,
  flush-left formatting and a single huh form theme. Human flows (`add`,
  `status`, `select`, `run`) no longer leak internal ids, Keychain service
  names, paths, or overlay kind; those stay in the inspection commands
  (`ccp list`, `ccp doctor`). Copy across every command rewritten for clarity.

### Removed
- Credential adoption. `ccp add` no longer copies plain claude's current login
  into a pool account. Adopting meant refreshing plain claude's single-use
  OAuth refresh token, which rotation invalidated — silently logging plain
  claude out. Every account now logs in with its own `claude /login`, so plain
  claude's token is never read or spent; no code path in cc-pool can even
  name plain claude's Keychain item anymore.

### Fixed
- Usage decode crashed when the API returned `resets_at` as a string. The field
  now accepts a JSON number, a numeric string, or an RFC3339 timestamp.

## [0.4.1] - 2026-06-08

### Added
- Bare `ccp` routes by pool state: an empty or uninitialized pool on a
  terminal runs the add flow, a populated pool shows status, and an empty
  pool without a terminal errors loudly.
- Credential adoption in `ccp add`: when plain claude's current login isn't
  pooled yet, offer to copy its credential (read-only) into the new account's
  own Keychain item and refresh the copy onto its own token chain — no new
  login (removed again in 0.5.0; see above).
- Watched login: `ccp add` owns the `claude /login` it spawns and closes it
  for you once the credential lands, with full terminal restoration; the
  manual other-terminal path polls with a spinner. Account labels prefill
  from the logged-in email.

### Changed
- `ccp remove --keep-credential` followed by a re-add no longer resurrects
  the kept credential, and `ccp add -y` no longer prompts for a label.

### Fixed
- A usage-validation blip during the final add step keeps the registered
  account with a warning instead of offering a rollback that destroyed a
  live credential.

## [0.4.0] - 2026-06-06

### Added
- `ccp add` — pool a Claude Max/Pro subscription (interactive `claude /login`),
  each account in its own `~/.cc-pool/accounts/acct-NN` config dir with its own
  independent OAuth grant and Keychain credential. Auto-initializes the pool
  and starts the background daemon on first run; loops to onboard several
  subscriptions in one go. Plain `claude` on `~/.claude` is never part of the
  pool and can never be logged out by it.
- New account dirs are seeded from `~/.claude.json` (top-level `oauthAccount`
  stripped; `claude /login` writes the account's own), so pooled sessions
  inherit settings, MCP servers, and per-project tool approvals instead of
  hitting the first-run onboarding wizard.
- `ccp select` — print the emptiest account's config dir, scored from live
  5-hour / 7-day usage with imminent-reset credit, low-headroom barrier, and
  burn-rate terms; cwd-keyed session stickiness for prompt-cache continuity.
- `ccp run` / `ccp status` / `ccp list` / `ccp env` / `ccp doctor` /
  `ccp remove` — session lifecycle, live usage table, account inspection,
  drift repair, and account removal.
- Shared overlay so every account presents all of `~/.claude`: symlink
  provider (default, pure Go) and optional fuse-t live mirror (`-tags fuse`).
  Per-account private entries: `daemon/`, `ide/`, `backups/`, and
  `.claude.json`.
- User LaunchAgent daemon (`ccp service`, `brew services`): usage polling with
  backoff, idle-account token refresh, score caching, fuse mount lifecycle.
  `ccp add`/`ccp init` start it automatically and restart it after upgrades.
- `ccp init` — optional explicit pool setup (state dir + daemon); `ccp add`
  does the same automatically.
- Homebrew formula with `ccp` symlink (prebuilt universal binary; fuse build
  picked automatically when fuse-t is present); CI and release workflows.
- License: PolyForm Noncommercial 1.0.0.

[Unreleased]: https://github.com/yasyf/cc-pool/compare/v0.69.0...HEAD
[0.69.0]: https://github.com/yasyf/cc-pool/compare/v0.68.0...v0.69.0
[0.68.0]: https://github.com/yasyf/cc-pool/compare/v0.67.0...v0.68.0
[0.67.0]: https://github.com/yasyf/cc-pool/compare/v0.66.0...v0.67.0
[0.66.0]: https://github.com/yasyf/cc-pool/compare/v0.65.2...v0.66.0
[0.65.2]: https://github.com/yasyf/cc-pool/compare/v0.65.1...v0.65.2
[0.65.1]: https://github.com/yasyf/cc-pool/compare/v0.65.0...v0.65.1
[0.65.0]: https://github.com/yasyf/cc-pool/compare/v0.64.9...v0.65.0
[0.64.9]: https://github.com/yasyf/cc-pool/compare/v0.64.8...v0.64.9
[0.64.8]: https://github.com/yasyf/cc-pool/compare/v0.64.6...v0.64.8
[0.64.6]: https://github.com/yasyf/cc-pool/compare/v0.64.5...v0.64.6
[0.64.5]: https://github.com/yasyf/cc-pool/compare/v0.64.4...v0.64.5
[0.64.4]: https://github.com/yasyf/cc-pool/compare/v0.64.3...v0.64.4
[0.64.3]: https://github.com/yasyf/cc-pool/compare/v0.64.2...v0.64.3
[0.64.2]: https://github.com/yasyf/cc-pool/compare/v0.64.1...v0.64.2
[0.64.1]: https://github.com/yasyf/cc-pool/compare/v0.64.0...v0.64.1
[0.64.0]: https://github.com/yasyf/cc-pool/compare/v0.63.3...v0.64.0
[0.62.0]: https://github.com/yasyf/cc-pool/compare/v0.61.7...v0.62.0
[0.18.2]: https://github.com/yasyf/cc-pool/compare/v0.18.1...v0.18.2
[0.18.1]: https://github.com/yasyf/cc-pool/compare/v0.18.0...v0.18.1
[0.18.0]: https://github.com/yasyf/cc-pool/compare/v0.17.0...v0.18.0
[0.17.0]: https://github.com/yasyf/cc-pool/compare/v0.16.0...v0.17.0
[0.16.0]: https://github.com/yasyf/cc-pool/compare/v0.15.1...v0.16.0
[0.15.1]: https://github.com/yasyf/cc-pool/compare/v0.15.0...v0.15.1
[0.15.0]: https://github.com/yasyf/cc-pool/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/yasyf/cc-pool/compare/v0.13.2...v0.14.0
[0.13.2]: https://github.com/yasyf/cc-pool/compare/v0.13.1...v0.13.2
[0.13.1]: https://github.com/yasyf/cc-pool/compare/v0.13.0...v0.13.1
[0.13.0]: https://github.com/yasyf/cc-pool/compare/v0.12.4...v0.13.0
[0.12.4]: https://github.com/yasyf/cc-pool/compare/v0.12.3...v0.12.4
[0.12.3]: https://github.com/yasyf/cc-pool/compare/v0.12.2...v0.12.3
[0.12.2]: https://github.com/yasyf/cc-pool/compare/v0.12.1...v0.12.2
[0.12.1]: https://github.com/yasyf/cc-pool/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/yasyf/cc-pool/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/yasyf/cc-pool/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/yasyf/cc-pool/compare/v0.9.1...v0.10.0
[0.9.1]: https://github.com/yasyf/cc-pool/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/yasyf/cc-pool/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/yasyf/cc-pool/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/yasyf/cc-pool/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/yasyf/cc-pool/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/yasyf/cc-pool/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/yasyf/cc-pool/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/yasyf/cc-pool/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/yasyf/cc-pool/compare/v0.3.0...v0.4.0
