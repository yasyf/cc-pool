# vmctl — a disposable tart VM harness for the File Provider incident replay

`vmctl` builds a throwaway macOS VM and replays the 2026-07-05 File Provider
wedge incident inside it: a 10-account pool on symlink overlays, live read
pressure, then the exact fleet command that triggered the incident —
`ccp migrate --to fileprovider --force`. On a fixed build the migration settles
account-by-account and every domain serves reads; on a regressed build the
domains materialize simultaneously, `fileproviderd` wedges, and the companion
app's control server mints `domain … is busy with another operation`. The whole
footprint lives under `/tmp/ccpool-vm` (VM disk, ssh key, results), so a wedged
guest is disposable and `vmctl destroy` reclaims everything.

> **Why a VM.** The migration drives real `NSFileProviderManager` domain
> registration and the same macOS overlay stack that has kernel-panicked bare
> Macs. `vmctl` refuses any ssh target that is not a VM (`kern.hv_vmm_present`
> must be 1), and a guest panic is caught by the watcher and mapped to a
> verdict rather than taking down a real machine.

## Provenance

`vmctl` and `lib.sh` are adapted from
[`fusekit/scripts/vm`](../../../fusekit/scripts/vm) per that README's "Reusing
the harness in another repo". The panic watcher, evidence scrape, exit-code
mapping, `meta.json`, cc-notes archive flow, and the `/tmp`-only lifecycle come
from there unchanged. cc-pool-specific: the `/tmp/ccpool-vm` namespace, the
File-Provider provisioning (no fuse-t / holder), `push.sh` (builds the cc-pool
binary, the Developer ID-signed `CCPoolStatus.app`, and the Developer ID
`CCPoolDaemon.app` daemon bundle), and the `scenarios/replay-fp-migrate-storm.sh`
and `scenarios/verify-appgroup-noprompt.sh` scenarios.

## Requirements

- An Apple Silicon Mac (tart is arm64-only; the builds `push` ships are arm64).
- Homebrew. `vmctl create` runs `brew install cirruslabs/cli/tart` if tart is
  missing — the only host mutation outside `/tmp/ccpool-vm`, announced first.
- Xcode 26 and `xcodegen` on the host (the widget SDK-skew constraint; `push`
  builds `CCPoolStatus.app` on the host and installs it into the guest).
- A **Developer ID Application** signing identity for team `SXKCTF23Q2` in the
  login keychain. The File Provider appex will not register with `pluginkit`
  ad-hoc-signed, so this is mandatory; `push` stops with options if none is
  found. Pin one explicitly with `VMCTL_SIGN_IDENTITY` + `VMCTL_SIGN_TEAM`.
- About 90 GB free in `/tmp` at peak, unless the image cache is shared (below).

## Quickstart

Run everything from the repo root, on a build that carries the Phase 2 fixes:

```sh
scripts/vm/vmctl create      # install tart if needed, clone the image (first pull: 20-60 min)
scripts/vm/vmctl provision   # boot, ssh key, App-Group TCC, (FP extension enabled after push)
scripts/vm/vmctl push        # build cc-pool + Developer ID CCPoolStatus.app + CCPoolDaemon.app, install, selftest
scripts/vm/vmctl run replay-fp-migrate-storm   # exit 0 == the incident does NOT reproduce
scripts/vm/vmctl destroy     # delete the VM, then rm -rf /tmp/ccpool-vm
```

`run` prints a verdict, leaves evidence under
`/tmp/ccpool-vm/results/<ts>-replay-fp-migrate-storm/`, and archives it to the
"cc-pool vm-repro chronology" cc-notes log (created on first archive).

## Sharing the tart image cache

The base image is a multi-GB pull. To reuse a sibling harness's already-warm
`macos-tahoe-base` image instead of re-pulling, point the cache — and only the
cache — at it:

```sh
VMCTL_TART_HOME=/tmp/fusekit-vm/tart scripts/vm/vmctl create
```

Keep `VMCTL_TART_HOME` set for every command in that session. A shared cache
lives outside `/tmp/ccpool-vm`, so `vmctl destroy` leaves it intact (it only
`tart delete`s this harness's own `ccpool-test` clone). Keep `VMCTL_NAME`
distinct from any sibling harness sharing the same cache.

## One-time provisioning (the two GUI-consent gates)

Two macOS consents cannot always be granted headlessly. On the SIP-disabled
cirruslabs image `provision`/`push` insert or script them; if either does not
take, boot once with a window (`VMCTL_GRAPHICS=1 scripts/vm/vmctl provision`)
and grant it by hand — the grant persists for the VM's lifetime.

1. **App-Group-data TCC** (the daemon's File Provider bridge). The non-sandboxed
   `cc-pool` daemon binds a socket inside the `CCPoolStatus` App Group container,
   which macOS gates behind a one-time prompt. `provision` inserts a
   `kTCCServiceSystemPolicyAppData` grant for the daemon binary directly into the
   guest's TCC.db (SIP-disabled only). The service is a best-guess: if it does
   not take, the daemon logs `awaiting the one-time app-group-container consent`
   and the replay's bridge-socket wait fails with that hint — boot
   `VMCTL_GRAPHICS=1` and click **Allow** when the daemon first binds.
2. **File Provider extension enabled** (`pluginkit`). Installing `CCPoolStatus.app`
   is not enough: LaunchServices must scan it and the extension must be marked
   `use`d. `push` runs `lsregister` + `pluginkit -a` + `pluginkit -e use`, then
   the selftest asserts `pluginkit -m -i com.yasyf.cc-pool.status.fileprovider`
   leads with `+`. The **System Settings > General > Login Items & Extensions >
   File Provider** toggle is a *separate* gate from `pluginkit`; if the headless
   enable does not stick, flip it in `VMCTL_GRAPHICS=1`.

Either way the proof is end-to-end: the replay's through-domain reads only
succeed when both gates are granted.

## The replay scenario

`scenarios/replay-fp-migrate-storm.sh` (`EXPECT=clean`) runs in five phases,
all bounded by the run window:

1. **seed** — 10 synthetic accounts: `~/.cc-pool/accounts/acct-01..10` config
   dirs each with a fake `.claude.json` (a real `oauthAccount` identity so the
   conversion's identity checks pass), plus matching `symlink` rows written into
   `~/.cc-pool/pool.db` via `sqlite3` (schema mirrors `internal/store/store.go`).
   Migration never touches the Keychain, so no real logins are needed.
2. **daemon-start** — launch `CCPoolStatus.app` (serves the FP control socket),
   start `cc-pool daemon` (logs to a file the scenario scrapes), and wait for the
   daemon socket, the control socket, and the App-Group bridge socket.
3. **live-sessions** — per account, a process holding an open fd on its
   `.claude.json` plus a 1s read loop through the account dir: the
   materialization read pressure that crushed `fileproviderd` in the incident.
4. **migrate-storm** — fire the exact incident command,
   `ccp migrate --to fileprovider --force`.
5. **assert** — from the scraped daemon log and through-domain reads:
   - all 10 accounts migrated `symlink -> fileprovider`;
   - no `busy with another operation` / wedge / recovery / reconcile-defer
     storm lines at all;
   - every account's through-domain `.claude.json` read returns its
     `oauthAccount` within 5s;
   - the migrated lines appear in strict `acct-01..10` order, non-interleaved —
     proof each domain served before the next conversion started.

Any failed assertion exits nonzero (mapped to infra/workload failure `1`); a
clean settle exits `0`; a kernel panic exits `2`.

> **Re-runs.** The scenario assumes a clean File Provider state. It is designed
> for a fresh guest (or one whose domains were retreated with
> `ccp migrate --to symlink` while the daemon was up). Re-running against a guest
> that still holds registered cc-pool domains from a prior run can collide on
> domain registration; `vmctl destroy && vmctl create` is the clean reset.

## The App-Group no-prompt scenario

`scenarios/verify-appgroup-noprompt.sh` (`EXPECT=clean`) proves the packaging
contract: shipping the daemon as `CCPoolDaemon.app` — a Developer ID bundle
carrying the `com.apple.security.application-groups` entitlement (and, when
profiled, an embedded provisioning profile) — makes the daemon's first bind of
the File Provider bridge socket inside the App Group container a **silent,
no-prompt grant**, TCC-keyed by the durable `CFBundleIdentifier`
(`com.yasyf.cc-pool.daemon`) rather than a per-version keg path.

Run it against a guest provisioned **without** the pre-seeded grant:

```sh
VMCTL_SKIP_TCC=1 scripts/vm/vmctl provision
# profiled (the shipping config): supply the daemon's provisioning profile
VMCTL_PROFILE_DAEMON="$(base64 -i daemon.provisionprofile)" scripts/vm/vmctl push
VMCTL_PROFILE_DAEMON="$(base64 -i daemon.provisionprofile)" scripts/vm/vmctl run verify-appgroup-noprompt
# unprofiled control (does the Team-ID-prefixed entitlement alone suffice?)
scripts/vm/vmctl push
scripts/vm/vmctl run verify-appgroup-noprompt
```

Set `VMCTL_DAEMON_MODE=both` to A/B both arms in one run. For every selected
mode the scenario locally re-wraps + Developer ID-signs the daemon bundle (via
`vm_build_daemon_bundle`, the same path `push` uses), installs it, and asserts:

1. **cold start, no grant** — the FP bridge socket accepts within 3s, the daemon
   log carries no `awaiting the one-time app-group-container consent` line, both
   the user and system `TCC.db` hold **zero** cc-pool `kTCCServiceSystemPolicyAppData`
   rows, and `launchctl procinfo` on the running daemon shows the App Group
   entitlement validated;
2. **upgrade-replay** — a second build installed at a fresh keg path adds zero
   new TCC rows and re-prompts not at all (proof the grant is identifier-keyed,
   not path-keyed).

> **Run-from-bundle dependency.** The entitlement-validated assertion requires
> the daemon to keep running from the bundle. On a pre-rewire build that still
> re-execs the daemon into a bare `~/.cc-pool/bin/cc-pool` copy (the retired
> stable-bin mechanism; only the `doctor` cleanup rung and
> `pool.LegacyStableBinDir()` reference that path now) the
> `assert_runs_from_bundle` check fails by design — run the scenario on a build
> that carries the rewire.

## Environment

cc-pool-specific variables (the fusekit README documents the shared ones —
`VMCTL_IMAGE`, `VMCTL_CPUS`, `VMCTL_MEMORY_MB`, `VMCTL_DISK_GB`,
`VMCTL_RUN_TIMEOUT_MIN`, `VMCTL_GRAPHICS`):

| Variable | Default | Meaning |
|---|---|---|
| `VMCTL_NAME` | `ccpool-test` | tart VM name. Keep distinct from siblings sharing a cache. |
| `VMCTL_TART_HOME` | `$VM_ROOT/tart` | Repoint ONLY the image cache to share a warm pull (e.g. `/tmp/fusekit-vm/tart`). |
| `VMCTL_SIGN_IDENTITY` | auto-discovered | Pin the Developer ID signing identity (sha1 or name). |
| `VMCTL_SIGN_TEAM` | auto-discovered | Pin the signing team id (must be `SXKCTF23Q2` to match `paths.go`). |
| `VMCTL_TCC_APPDATA_CLIENTS` | sshd-wrapper + daemon path | App-Group-data TCC grantees (ignored when `VMCTL_SKIP_TCC` is set). |
| `VMCTL_SKIP_TCC` | unset | Set (non-empty) to make `provision` NOT pre-seed the App-Group grant — required for `verify-appgroup-noprompt`. |
| `VMCTL_PROFILE_DAEMON` | unset | The daemon's App-Group provisioning profile (a `.provisionprofile` path or its base64). Set ⇒ push builds a **profiled** `CCPoolDaemon.app`; unset ⇒ **unprofiled** (app-group entitlement alone). |
| `VMCTL_DAEMON_MODE` | `auto` | `verify-appgroup-noprompt` arm(s): `auto` (profiled iff a profile is set), `profiled`, `unprofiled`, or `both`. |
| `VMCTL_CHRONOLOGY_LOG` | resolved by name | Pin the cc-notes log id to archive into. |
| `BUILD_REV` | short `git` HEAD (`-dirty`) | The revision recorded in the guest and `meta.json`. |

## Commands, exit codes, scenario contract

`vmctl` keeps fusekit's command set (`create`, `provision`, `push`, `run`,
`shell`, `collect`, `archive`, `status`, `destroy`), its `run` exit codes
(`0` expectation met, `1` infra failure, `2` panicked under `EXPECT=clean`,
`3` no repro under `EXPECT=panic`), and its scenario contract (`EXPECT=` on its
own line, `vm_phase`/`vm_seconds_left` helpers, host-side helpers only). See the
[fusekit README](../../../fusekit/scripts/vm/README.md) for the full contract,
the panic watcher, and the `meta.json` fields — all inherited unchanged.
