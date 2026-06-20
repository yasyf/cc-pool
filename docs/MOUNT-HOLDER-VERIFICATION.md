# Mount-holder end-to-end verification

A manual on-device checklist for the detached mount-holder architecture: fuse
mirrors live in a `cc-pool mount-holder` process (socket
`~/.cc-pool/mounts.sock`), the daemon supervises it, and daemon restarts and
upgrades never disturb mounts. Every step needs a real fuse setup — a `-tags
fuse` build, fuse-t installed, the "Network Volumes" grant, and at least one
fuse account (`ccp migrate`).

Steps 5–7 cover the follow-ups to the two 2026-06-12 incidents: mount-up
timeouts are no longer presumed to be the missing TCC grant, and a partially
wedged mirror (metadata fine, bulk reads hang) is detected and remounted
automatically.

> **One-time transition note.** Upgrading from a pre-mount-holder release
> (v0.24.x and earlier) drops mounts ONCE: the OLD daemon hosts its mounts
> in-process and still unmounts them when the upgrade restarts it. Close
> fuse-account sessions for that single upgrade. Every restart and upgrade
> after this version is mount-safe.

## 1. Daemon restart under a live session

```sh
ccp run                                   # (human) keep this session open on a fuse account
mount | grep -c cc-pool                   # note the mount count
brew services restart cc-pool
mount | grep -c cc-pool                   # unchanged — mirrors never came down
# the session keeps working: type into it; no fs errors
tail ~/.cc-pool/daemon.log
```

Expect in `daemon.log`: a fresh `daemon <ver> started; socket=…` and per-account
`acct-NN adopted live mount`. Must NOT appear: `mount holder respawned`,
`replacing version-skewed mount holder`, or any unmount of the account dirs.

## 2. Holder crash → respawn + remount

```sh
kill -9 "$(pgrep -f 'mount-holder --socket')"
sleep 12                                  # one supervision tick is 10s
mount | grep -c cc-pool                   # mounts are back
tail ~/.cc-pool/daemon.log
```

Expect, within ~10s, in order:

```
mount holder unreachable
mount holder respawned
acct-NN cleared a dead mount before remounting   # carcass swept, per account
```

With several fuse accounts the carcass-clear + remount churn can push a
sibling past the 8s mount wait. Since the holder process has just mounted its
siblings, the grant is proven and the timeout reads as `mount-timeout` (step
5): expect `acct-NN fuse mount did not come up within the mount wait;
retrying` and a retry under the supervision backoff, all mirrors back within
~30s, and `ccp status` / `ccp doctor` quiet throughout. If any "Network
Volumes" copy shows up during this storm, the original misclassification bug
is back.

## 3. Version skew → deferred under a session, heals when idle

```sh
# Upgrade the binary (or reinstall a different build) and restart the daemon
# while a claude session is live on a fuse account.
brew upgrade cc-pool && brew services restart cc-pool
tail -f ~/.cc-pool/daemon.log
```

Expect while the session lives (logged once per reason, not per tick):

```
deferring replacement of version-skewed mount holder (<old>): N live session(s) on <dir>
```

`ccp status` shows the footer `mount holder <old> skewed; will be replaced when
idle`; `ccp service status` shows `version skew — will be replaced when idle`.
Close the session; within a tick expect:

```
replacing version-skewed mount holder (<old>) with <new>
mount holder replaced at <new>
```

…and the skew footer disappears from `ccp status`.

## 4. Uninstall gates

```sh
# (a) gate: with a live session on a fuse account
ccp service uninstall            # refuses: "live claude sessions are using pool
                                 #  accounts: acct-NN (pid …) — close them or pass --force"
# (b) --force overrides
ccp service uninstall --force    # proceeds: "Stopped the daemon." then
                                 # "Stopped the mount holder."; mount | grep cc-pool is empty
# (c) purge aborts on a survivor mount: wedge a mirror (e.g. hold a cwd inside
#     it from another shell so the unmount fails), then
ccp service uninstall --purge    # "refusing to purge: acct-NN is still a live
                                 #  mountpoint; unmount it first …"; ~/.cc-pool survives
```

## 5. Mount-up timeout under a proven grant (`mount-timeout`)

TCC is now a deduction, not a presumption. The first mount in a holder
process waits 14s; once any mount in that process has come live (a fuse
capability probe counts), the "Network Volumes" grant is proven for the rest
of the process lifetime, later mounts wait 8s, and a timeout is classified as
the wire class `mount-timeout` (`overlay.ErrMountTimeout`): transient fuse-t
slowness, never the TCC condition. The Setup error reads:

```
fuse mount did not come up in time: <dir> after <wait>; this process already hosts live mounts, so the Network Volumes grant is proven — transient fuse-t slowness, retrying
```

and the daemon logs, without recording a TCC error:

```
acct-NN fuse mount did not come up within the mount wait; retrying
```

No action needed: supervision retries the row on its 10s tick under
per-account backoff (10s doubling to a 2 min cap, deliberately under the 180s
scheduler period), so the mirror is back within 10s–2min of the timeout. To
force an immediate startup reconcile instead of waiting out the backoff:

```sh
brew services restart cc-pool             # mounts survive; the restart re-runs the reconcile
```

Verify:

```sh
mount | grep cc-pool                      # the mirror is back
ccp doctor                                # all green; no "mount holder TCC" failure
```

Must NOT appear: any "Network Volumes" copy in `daemon.log`, the `ccp status`
footer `mount holder: TCC blocked — …`, or a failing `ccp doctor`
`mount holder TCC` check. A daemon older than this class degrades it to
unknown-class handling, which also retries — never a mount-failure verdict.

## 6. Genuine TCC: fresh machine or new binary path

With zero proven mounts in the holder process (a fresh machine, or a holder
running from a path macOS has not granted), the first mount sits on the
one-time grant and times out after the 14s first-mount wait. The failed
attempt is itself the fix's first step — macOS only creates the System
Settings toggle after a mount has been denied. The Setup error reads:

```
fuse mount did not come up: <dir> (presumed missing macOS TCC grant: this failed attempt is what creates the toggle under System Settings ▸ Privacy & Security ▸ Network Volumes — grant Network Volumes access once and mounts retry automatically)
```

Until granted, expect in `daemon.log`:

```
acct-NN fuse mount blocked pending the macOS "Network Volumes" grant, retrying next poll
```

plus the `ccp status` footer `mount holder: TCC blocked — …` and a failing
`ccp doctor` `mount holder TCC` check.

Grant it: System Settings ▸ Privacy & Security ▸ Network Volumes, enable
cc-pool. Supervision re-probes the blocked row under the same backoff as
step 5, so the mount lands within 10s–2min of the grant — no waiting on the
3-minute scheduler poll — and all three surfaces clear.

Two research conclusions are recorded here so they are not re-litigated. The
Network Volumes TCC grant is not queryable on macOS: there is no public
preflight API for the service, `tccutil` only resets grants, and TCC.db is
private and gated behind Full Disk Access. Attempting a mount is the only
observable, which is why classification is a deduction from the process's own
mount history. The honest gap in that deduction: a grant revoked mid-process
reads as `mount-timeout` until the holder restarts, because established NFS
mounts survive revocation, so the proven bit stays set and no probe can
detect the revocation. The next holder restart resets the deduction and
re-classifies correctly.

### TCC across a binary swap

tccd keys the grant on the holder's RESOLVED executable path (`client_type=1`
in TCC.db). Homebrew installs each version at a new versioned Cellar path, so a
holder spawned from there would re-prompt on every upgrade. cc-pool sidesteps
this by spawning the holder from a stable copy at `~/.cc-pool/bin/mount-holder`
(fusekit `mountd.Spawn.StableExecDir`): the path is constant across versions,
and the copy's embedded Developer-ID designated requirement survives the byte
copy, so tccd matches the same grant and never re-prompts.

```sh
# (a) upgrade via brew, then kill the holder so the new binary respawns it from
#     the same ~/.cc-pool/bin/mount-holder copy: expect NO new TCC prompt and a
#     clean remount (step 2 lines). TCC.db holds a single client_type=1 row for
#     ~/.cc-pool/bin/mount-holder, unchanged by the upgrade.
# (b) run a holder from a NEW path (e.g. a source build in /tmp without the
#     stable copy): expect the full step-6 sequence — blocked copy, footer,
#     doctor failure — until the new path is granted.
```

Still unverified on-device: whether a new Developer-ID signature whose
designated requirement still matches keeps the grant after the stable copy is
refreshed with a fresh build. To be settled in the on-device verification
phase; record the result here either way.

## 7. Partial wedge: deep probe, auto remount, orphaned sessions

Symptom (confirmed live on acct-02, 2026-06-12): the mirror serves small
stats and reads instantly but hangs forever on large reads — the confirmed
data point was a 1.5 MB transcript — and freshly launched sessions block in
uninterruptible `openat`. Shallow liveness probes pass, so before this fix
the holder reported the mount live, selection kept assigning sessions, and no
heal path ever fired.

**Detection.** Every fuse mirror serves a synthetic read-only file at
`/.ccp-probe`: 2 MiB, purely virtual (it never exists in `~/.claude`, is
hidden from Readdir, and rejects writes). The holder pulls it in full through
the kernel mount every 30s, each read bounded at 5s, and 2 consecutive
failures mark the mirror wedged — within ~60–90s of onset. Expect in
`~/.cc-pool/mount-holder.log`:

```
deep probe <dir>: 2 consecutive failures; marking the mirror wedged (serves metadata but hangs bulk reads)
```

and on recovery:

```
deep probe <dir>: recovered; marking the mirror live again
```

**Heal.** The holder folds the verdict into `Live` (selection stops assigning
the account, even under an old daemon), and the daemon's supervision remounts
the row on its next 10s tick — about two minutes worst case from wedge onset
to fresh mirror. Expect in `daemon.log`:

```
acct-NN wedged mirror (serves metadata but hangs reads); remounting under N live session(s) — relaunch them
```

A mirror that fails reads outright (an out-of-band `umount -f`, a dead fuse-t
worker) takes the same path, logged as
`dead mirror (fails reads outright; unmounted out of band or its fuse worker died?)`.

**Sessions.** Sessions that were live on the wedged mirror are unrecoverable:
they are parked in uninterruptible syscalls against a mount that no longer
exists, and fuse-t/NFS has no fd-stable remount. Relaunch them. Doctor names
both the mirror and the stale sessions:

```
✗ acct-NN mirror: wedged (serves metadata but hangs reads) — daemon will remount; relaunch its sessions
✗ acct-NN session: pid N predates the current mirror (remounted HH:MM:SS) — it is bound to a yanked mount; relaunch it
```

and `ccp status` shows the footer
``mount holder: 1 wedged mirror — run `ccp doctor` ``.

**Manual escape hatches — only when the automation above fails.**

- The holder's own forced teardown can itself wedge. If `daemon.log` keeps
  repeating `acct-NN mount blocked by a wedged unmount, retrying next poll`,
  break the cycle by hand; supervision mounts fresh within its backoff:

  ```sh
  umount -f ~/.cc-pool/accounts/acct-NN
  ```

- Last resort: `kill -9` the holder. Supervision respawns it and remounts ALL
  mirrors (step 2), which orphans every live session on every fuse account,
  not just the wedged one.

Explicitly NOT a fix: `brew services restart cc-pool` does not remount
anything — the holder survives daemon restarts by design (step 1), so a
restart re-adopts the same wedged mirror. And every holder respawn or remount
orphans the sessions that predate it; relaunching them is always the final
step.
