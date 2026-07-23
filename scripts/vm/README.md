# vmctl — disposable macOS gates for the hard-cut FuseKit stack

`vmctl` owns a throwaway tart VM for cc-pool's live File Provider, TCC, native
runtime, and process-kill verification. The VM disk, ssh key, logs, and results
live below `/tmp/ccpool-vm`; `vmctl destroy` removes that state. The harness
refuses any ssh target whose `kern.hv_vmm_present` is not `1`.

The permanent topology under test is:

```text
CCPoolFileProvider.appex
        | App Group socket
        v
~/Applications/CCPoolStatus.app
        | ordinary private socket
        v
cc-pool account daemon
```

The fixed signed `CCPoolStatus.app` embeds the FuseKit runtime and broker. The
ordinary Go account daemon never resolves or traverses the App Group. The
harness never pre-seeds TCC state.

## Requirements

- Apple Silicon and Homebrew.
- Xcode 26, XcodeGen, and the Go version declared by `go.mod`.
- A Developer ID Application identity for team `SXKCTF23Q2`. The File Provider
  extension will not register ad-hoc-signed.
- Enough free space for a tart image and VM, normally about 90 GB.

## Quickstart

```sh
scripts/vm/vmctl create
scripts/vm/vmctl provision
scripts/vm/vmctl push
scripts/vm/vmctl run verify-signed-topology
scripts/vm/vmctl run verify-worker-deadline
scripts/vm/vmctl run verify-convergence-amplification
scripts/vm/vmctl run verify-atomic-replacement
VMCTL_RUN_TIMEOUT_MIN=30 scripts/vm/vmctl run verify-tcc-upgrade-reboot
# Destructive positive control: this intentionally kernel-panics only the VM.
scripts/vm/vmctl run verify-panic-watcher
scripts/vm/vmctl destroy
```

`push` builds the pure-Go account daemon and the single Developer ID-signed
`CCPoolStatus.app`, installs both, registers the File Provider extension, and
verifies its App Group entitlement. It never installs or authorizes a second
application identity.

## File Provider provisioning

`push` handles both independent File Provider gates: it registers/elects the
appex with `pluginkit`, then enables the provider-consent bit in the disposable
VM's File Provider state. The latter is a test-harness-only substitute for the
user's System Settings toggle; it is not a TCC grant, and the harness never
pre-seeds protected-filesystem TCC authorization.

If headless `pluginkit` election does not stick, boot once with
`VMCTL_GRAPHICS=1` and enable cc-pool under System Settings → General → Login
Items & Extensions → File Provider, then rerun `push`. On a real machine the
user always owns that toggle; production code never edits File Provider state.

## Acceptance scenarios

Every advertised gate below maps to one executable file under
`scripts/vm/scenarios/`. `vmctl push` is the common prerequisite: `vmctl run`
refuses a guest without its exact `BUILD_REV`. Go acceptance binaries are
cross-compiled on the host with `GOWORK=off` from cc-pool's pinned modules and
executed only inside the arm64 guest. Their verbose output and resolved module
versions are retained in the run results.

| Scenario | Exact prerequisite | Result proved |
| --- | --- | --- |
| `verify-signed-topology` | Developer ID host/File Provider profiles and successful `push` | Exact host, File Provider, and widget identities; one host/FP App Group; one host Mach-O; broker/runtime sockets; unchanged protected-filesystem TCC rows; zero daemon TCC rows or App Group capability. |
| `verify-worker-deadline` | Go toolchain on the host and a pushed arm64 guest | A real TERM-handling/continuing worker and TERM-ignoring descendant exceed a deadline; daemonkit waits the fixed grace, escalates to process-group KILL, proves both PIDs gone, removes the durable process record, and admits a second task through the same bounded worker lane. Evidence is `worker-deadline.json`. |
| `verify-convergence-amplification` | Exact pinned FuseKit module available to `GOWORK=off go test -c` | Production convergence/catalog code applies one source change to the reported 14-domain/9-active fleet, targets exactly nine domains with at most two pending acknowledgements, sends no post-ack relaunch, and returns bounded replayable catalog deltas with stable identity. Evidence is `convergence-tests.log` and `catalog-delta-tests.log`. |
| `verify-atomic-replacement` | Exact pinned FuseKit module available to `GOWORK=off go test -c` | Production catalog replacement preserves the source object ID and old handle, has one concurrent winner, publishes final metadata/content in one revision, and remains old-or-new atomic at every failpoint. The same window must contain no `itemCollision`, `ESTALE`, or `itemDocTrackedButNotOnDisk` unified-log event. |
| `verify-tcc-upgrade-reboot` | `verify-signed-topology` prerequisites and a 30-minute run budget | A differently versioned payload replaces the app at the fixed path, retains byte-identical designated requirements and App Data, network/removable-volume, Full Disk Access, and File Provider TCC rows with zero daemon rows, intentionally reboots under panic monitoring, and passes the runtime checks again. |
| `verify-panic-watcher` | Disposable VM with root-authorized `/usr/sbin/dtrace`; never run against a host | DTrace executes a real kernel `panic()` in the verified VM. The scenario supplies no intentional-reboot marker and succeeds only when `vmctl` observes/classifies the panic from boottime or a new `.panic` report. An unavailable/denied trigger is infrastructure failure; exit `3` means the full run window elapsed without panic evidence. |

The convergence and replacement scenarios are deterministic production-package
acceptance gates. The signed topology and TCC scenarios cover the live macOS
host/broker/File Provider boundary; none of the Go acceptance binaries claims
an application identity or opens the App Group.

Live mount, File Provider, process-kill, and TCC exercises belong only in this
VM harness, never on the host.

## Sharing the image cache

To reuse a sibling harness's cached base image:

```sh
VMCTL_TART_HOME=/tmp/fusekit-vm/tart scripts/vm/vmctl create
```

Keep the variable set for every command and use a distinct `VMCTL_NAME`.

## Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `VM_ROOT` | `/tmp/ccpool-vm` | All mutable harness state. |
| `VMCTL_NAME` | `ccpool-test` | tart VM name. |
| `VMCTL_IMAGE` | Tahoe base image | tart source image. |
| `VMCTL_GRAPHICS` | `0` | Set to `1` for GUI consent debugging. |
| `VMCTL_SIGN_IDENTITY` | auto | Developer ID identity override. |
| `VMCTL_SIGN_TEAM` | auto | Signing Team ID override. |
| `VMCTL_TART_HOME` | below `VM_ROOT` | Optional shared tart cache. |
| `VMCTL_RUN_TIMEOUT_MIN` | `10` | Scenario timeout. |

`run` accepts any executable scenario path or a file below
`scripts/vm/scenarios/`. Scenario exit `0` means its expectation held; `1`
means infrastructure or assertion failure; `2` means a panic while
`EXPECT=clean`; `3` means no panic while `EXPECT=panic`.
