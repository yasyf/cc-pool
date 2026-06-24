# INCIDENT — mount-holder fork storm froze the machine (2026-06-24 ~02:17–02:59)

**Severity: critical (whole-machine freeze).** This is a HANDOFF doc. A fresh session or the
user can execute it. **Read the "Safety rules" section before running anything.**

## What happened

An autonomous Claude workflow (`fp-phase1-fusekit`, Phase 1 of the File-Provider-overlay-backend
work) ran `go test ./...` in `/Users/yasyf/Code/fusekit` **on the user's live primary Mac**. The
fusekit test suite — specifically the `overlay` FP / holder-spawn tests — spawns the **real**
`mount-holder` binary through `mountd.Spawn` + `proc.Supervisor`. A holder that could never bind its
socket (`/tmp/cc-test/mounts.sock`, which never existed) was **respawned with no global ceiling**.
The respawn loop self-replicated (holders observed parenting holders; 655 reparented to launchd) into
a **fork+exec storm**: ~4,718 → 5,401 `mount-holder` processes, **load average 188 → 214**, the
process table was exhausted, `sysmond` crashed, and the machine froze.

A second session (mobile, over SSH) mitigated it: `chmod 000` the holder binary to break respawn,
then `killall mount-holder` until `POST_COUNT=0`. Load is now falling.

This is a **recurrence of the known class** documented in the `claude-pool-holder-kill-meltdown`
memory ("wedged fuse-t mounts / runaway holder processes exhaust the process table and freeze the
whole machine"). The new trigger is **autonomous test execution on the host**.

## Current containment state (verified read-only, 2026-06-24 ~03:06)

- Load average: `11.29 / 34.98 / 41.76` (1/5/15-min) — **falling** from the 214 peak. Recovering.
- Storm binary `/tmp/cc-test/bin/mount-holder` (4.4 MB, built 02:17): permissions `----------`
  (chmod 000) — **disabled, cannot respawn.**
- No live `mount-holder` processes respawning. `pgrep` itself errors with
  `sysmond service not found` — residual damage from the process-table exhaustion; clears on reboot.
- Killed workflow is **not resumable** (TaskStop reports "no task found" for both its handles). Good —
  a resume would re-run `go test` and rebuild a fresh holder binary in a fresh temp dir, re-storming.
- Leaked test scratch: **2,496** `/tmp/ccp-ov-fp*` dirs (overlay FP tests) + 1 `/tmp/ccp-fpd*`.
- fusekit working tree holds the completed Phase-1 edits (uncommitted): full `fileproviderd/` package,
  `overlay/{backend,select,provider,spec}.go` modified, FP test files, `overlay/fpavail_*.go`,
  **and a modified `go.mod`** (review this — an agent may have bumped the toolchain/deps).

## Safety rules for whoever picks this up — READ FIRST

1. **DO NOT run `go test ./...` (or any spawn-capable test/build) for fusekit or cc-pool on this
   machine or ANY machine the user cares about.** fusekit's `mountd`, `overlay` holder-path, and
   `proc.Supervisor` tests spawn the real `mount-holder`. Until the spawn-ceiling fix below lands,
   a single bad run can fork-bomb the host again.
2. **A git worktree is NOT isolation** — same kernel, same process table. Isolation means a disposable
   VM/container, or a remote/cloud sandbox with a process cap (`ulimit -u`). Nothing on the host.
3. **Do not `git checkout`/discard the fusekit Phase-1 work.** The code is real and mostly fine; the
   failure is the *missing spawn ceiling* + *running spawn tests on the host*, not the FP code itself.
4. **Reboot is the clean way to restore `sysmond`/process accounting** once evidence is preserved.

## Diagnosis (read-only forensics — confirm the exact self-replication mechanism)

All of these are read-only. None spawn or compile. Do them in a normal shell.

1. **Preserve evidence first** (copy out before any cleanup/reboot):
   - `/tmp/cc-test/holder.log` (816 lines of `PASS` — the holder's probe/health loop; confirms the
     holder kept "passing" a probe while never serving, i.e. the respawn never reached a terminal
     classification).
   - `/tmp/cc-test/bin/mount-holder` (the disabled storm binary — keep its build timestamp).
   - The workflow transcript dir:
     `/Users/yasyf/.cc-pool/.../subagents/workflows/wf_dd8238f5-70a/` (what each agent ran).
2. **Find the spawner test.** Identify which `overlay/fileprovider_*_test.go` / `select_test.go`
   path reaches the holder spawn, and whether it (a) loops (2,496 iterations ≈ 2,496 leaked dirs),
   (b) uses a fixed `/tmp/cc-test` path instead of `t.TempDir()`, (c) builds/launches the real holder
   instead of a fake. Grep (quote globs in fish): `grep -rln 'cc-test\|mount-holder\|MkdirTemp' /Users/yasyf/Code/fusekit/overlay /Users/yasyf/Code/fusekit/mountd`
3. **Confirm the no-ceiling respawn** in `fusekit/proc/supervisor.go` + `proc/spawn.go` +
   `proc/backoff.go`: does a holder that exits/fails-to-bind get respawned unconditionally? Is
   `backoff` actually consulted, and is there ANY cap on total restarts within a window? (Expected
   finding: no global StartLimit-style ceiling → unbounded respawn.)
4. **Confirm the self-parenting**: how can a `mount-holder` spawn another `mount-holder`? Check
   whether the holder runs its own Supervisor, or whether `proc.Spawn` re-execs. The "holders parent
   holders + reparent to launchd" pattern is what turned a respawn loop into exponential growth — this
   is the highest-priority mechanism to understand and kill.

## Fix — defense in depth (no single layer is trusted)

### Layer 1 — code: bound the blast radius even if a spawn loop is triggered (fusekit/proc)

This is generic + safety-critical, so it belongs in `fusekit/proc` (every consumer benefits — see the
`push-generic-mechanism-to-fusekit` memory), not siloed in a test or in cc-pool.

- **Global restart ceiling (systemd `StartLimitBurst`/`StartLimitIntervalSec` analog).** The Supervisor
  tracks restarts in a sliding window; if a child restarts more than `N` times in `T` seconds, it
  **aborts loud** (`ErrRestartStorm`) and stops spawning — it does NOT keep looping. Pick conservative
  defaults (e.g. 5 restarts / 60 s).
- **Terminal-failure classification.** A holder that exits because it cannot bind/serve its socket is
  **terminal**, not a transient crash → no respawn. Only genuine crashes (with backoff) respawn.
  Verify `proc/backoff.go` is on the respawn path and has a hard cap, not just a delay.
- **`RLIMIT_NPROC` on spawned children.** Set a child process-count rlimit in `proc.Spawn` so a runaway
  hits `EAGAIN` and fails loud long before it exhausts the *system* process table.
- **Holder self-defense (`mount-holder` main).** On startup: if it can't bind its socket within a few
  attempts, exit with a terminal (non-respawn) code; and it must **never** spawn another holder. Add a
  cheap "am I in a storm?" check (e.g. many sibling `mount-holder` PIDs, or `RLIMIT_NPROC` near the
  limit) → abort.

### Layer 2 — tests: make spawn-capable tests impossible to run by accident

- **Gate every real-holder-spawning test behind a build tag + env guard** (e.g. `//go:build live`
  and require `FUSEKIT_LIVE=1`). Default `go test ./...` must be **provably non-spawning** — it uses
  fakes only. This is the single most important change: it makes the default command an autonomous
  agent runs safe.
- **Hermetic discipline:** `t.TempDir()` only (never a fixed `/tmp/cc-test`), build the holder once,
  bounded iterations, a hard per-test `-timeout`, and guaranteed teardown (`t.Cleanup` kills any
  spawned holder). Add a meta-test / CI lint that fails if a test spawns the holder outside the live
  guard.

### Layer 3 — operational: never run spawn-capable work on the host

- Autonomous workflows **must not** run `go test ./...` for these repos on the user's machine. Either
  scope to specific non-spawning packages, or run in a disposable VM/container/remote sandbox with
  `ulimit -u` set. Encode this as a standing rule (memory + AGENTS.md).

### Layer 4 — recovery: a one-command, documented kill-switch

- Wrap the mitigation the second session improvised into a script / `ccp panic`:
  `chmod 000 <holder-binary>` (break respawn first) → `killall mount-holder` → verify count 0 →
  confirm load dropping. Document it so recovery under load needs no improvisation.

## Cleanup (only AFTER evidence is preserved)

- Remove leaked scratch: the **2,496** `/tmp/ccp-ov-fp*` dirs + `/tmp/ccp-fpd*` + `/tmp/cc-test`.
  Do it gently (it's a lot of inodes; `find /tmp -maxdepth 1 -name 'ccp-ov-fp*' -print0 | xargs -0 rm -rf`)
  and ideally after a reboot when the machine is idle.
- Re-enable or delete the disabled holder binary after the fix lands.
- **Reboot** to restore `sysmond` and clear residual process-accounting damage.

## Verification (isolated environment ONLY)

- After Layers 1–2 land: in a VM/container, prove (a) default `go test ./...` spawns **zero** real
  holders, (b) a deliberately-wedged holder hits the restart ceiling and aborts loud instead of
  storming, (c) `RLIMIT_NPROC` caps a runaway. Never validate this on the host.

## Status of the FP feature work (separate from this incident)

- Phase 1 fusekit code (the `fileproviderd` package + overlay FP arms) is written and uncommitted in
  `/Users/yasyf/Code/fusekit`. It has NOT been verified green (Stage C never finished — it caused the
  storm). **Do not run its tests on the host.** Resume the feature only after Layers 1–3 are in place,
  and only against an isolated test environment.
- The approved plan lives at
  `/Users/yasyf/.cc-pool/accounts/acct-05/plans/when-on-macos-fusekit-cc-pool-hazy-rabin.md`.
