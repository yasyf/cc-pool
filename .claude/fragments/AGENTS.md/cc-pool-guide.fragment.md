# cc-pool Development Guide

Full style guide: [STYLEGUIDE.md](STYLEGUIDE.md)

## Project Basics

cc-pool (`ccp`) pools several Claude Max/Pro subscriptions and launches each Claude Code session on the emptiest account. Go, macOS-only, single binary.

- **Build**: `CGO_ENABLED=0 go build ./cmd/cc-pool` (pure-Go default; `-tags fuse` needs cgo + fuse-t)
- **Test**: `scripts/test.sh ./...` — a `ulimit -u` wrapper around `go test` so a runaway spawn can't fork-bomb the host. Must pass with no network, no Keychain, no daemon. **Never run bare `go test` (especially `-tags fuse`) on a real machine** — the fuse/holder spawn path can re-exec a test binary into a fork bomb that exhausts the process table and freezes the machine (see the cc-notes incident doc: `ccn doc show ef281ea`, or `ccn doc search "fork storm"`). Use the harness, or an isolated VM/container.
- **Vet**: `go vet ./...` before every commit

## Releasing

Releases are **tag-triggered** — there is no version file to edit. `Version`/`Commit` in
fusekit's shared `version` package default to `dev` locally and are injected at build time
via `-ldflags`.

Pushing a `vX.Y.Z` tag runs `.github/workflows/release.yml`, which (1) builds the universal
(arm64+amd64) pure-Go binary on macOS and Developer ID-signs + notarizes it (a bare Mach-O,
identifier `com.yasyf.cc-pool`, app-group entitlement), (2) builds, Developer ID-signs,
notarizes, and staples the `CCPoolStatus.app` widget (the `cc-pool-status` cask payload),
(3) creates a GitHub Release with auto-generated notes + the binary tarball, widget zip, and
SHA256SUMS, and (4) renders the
formula and cask from in-repo templates and publishes them to the shared external tap
[`yasyf/homebrew-tap`](https://github.com/yasyf/homebrew-tap) (`brew install yasyf/tap/cc-pool`).
There is no in-repo `Formula/` — **never hand-edit the tap**; the release job owns it. A tag
containing `-` (e.g. `v1.2.3-rc.1`) publishes assets but never touches the tap (prerelease).

Versioning is semver: `feat` → minor, `fix`/`chore`/`refactor` → patch. Latest released version:
`git tag --sort=-creatordate | head`.

This repo is **colocated jj + git**. To cut a release once the change is committed (jj manages
bookmarks; tags go through plain `git`):

```sh
jj bookmark set main -r <change>     # move main to the release commit
jj git push -b main                  # push main (fast-forward)
git tag vX.Y.Z <commit-sha>          # tag the release commit
git push origin vX.Y.Z               # push tag → triggers release.yml
```

## Repository Structure

```
cc-pool/
├── cmd/cc-pool/        # main: CLI entrypoint (installs as cc-pool, ccp symlink)
├── internal/
│   ├── cli/            # ccp subcommands (init, add, select, run, status, doctor, …)
│   ├── creds/          # Claude Code credentials: blob format + Keychain and plaintext-file stores
│   ├── daemon/         # background poller: usage polling, idle token refresh, socket protocol; drives the external fusekit-holder app and the FP bridge
│   ├── forecast/       # per-account burn rates + depletion estimates; pool-wide rollup shipped to the widget
│   ├── oauth/          # Claude OAuth refresh + /api/oauth/usage client
│   ├── overlay/        # shared ~/.claude overlay probing (symlink, fuse-t mirror, File Provider)
│   ├── pool/           # account dirs, paths, pool manager, overlay selection
│   ├── procscan/       # detect live claude sessions per config dir
│   ├── score/          # account scoring (5h/7d headroom, reset credit, burn rate)
│   └── store/          # SQLite state (no secrets — Keychain only)
├── widget/             # CCPoolStatus.app Notification Center widget (SwiftUI, cc-pool-status cask)
├── docs/               # public design doc (ARCHITECTURE.md) + README assets
├── AGENTS.md           # This file — shared conventions
└── STYLEGUIDE.md       # Full style guide
```

The FUSE-T mount machinery — the detached mount-holder protocol (`fusekit/mountd`) and the mount/serve/teardown primitives (the root `fusekit` package) — now lives in [`github.com/yasyf/fusekit`](https://github.com/yasyf/fusekit). cc-pool keeps only its mirror-specific code (the `overlay` provider and the holder seam in `pool/`) and, from the extraction, gains cgofuse-load panic-recovery (a missing libfuse-t surfaces as `ErrFuseUnavailable` instead of crashing the holder) and pre-mount carcass clearing (`ClearCarcass`) — otherwise runtime byte-identical.

Two filesystem trees, never confused:

- `~/.claude` — canonical Claude Code config dir. **NEVER moved, modified structurally, or registered as a pool account.** It is plain `claude`'s home and the shared overlay base; plain `claude` must keep working untouched.
- `~/.cc-pool/` — cc-pool's own state (sqlite db, daemon socket, logs) plus `accounts/acct-NN` pool config dirs (ids start at 1).

Safety rules baked into the architecture — do not regress them:

1. **The pool NEVER reads, writes, deletes, or even names the canonical unsuffixed Keychain item (`Claude Code-credentials`), and never mutates plain claude's OAuth state.** There is no exception: `keychain.ServiceName` always emits a hash-suffixed name, and no code path can name the canonical item. Every pool account — including the user's main subscription — gets its own config dir, its own refresh-token chain (from its own `claude /login`), and its own suffixed Keychain item. This is why there is no credential "adoption": forking a pool account off plain claude's login would require spending plain claude's single-use refresh token, which rotation invalidates — signing plain claude out. A fresh login per account is the only safe path.
2. **No secrets in SQLite** — the macOS Keychain is the sole secret store.
3. **Account dir strings are hashed for Keychain service names** — the path string `ccp` emits and the string hashed must stay byte-identical. No realpath/normalization divergence.
4. **Fuse mounts are hosted by a detached cc-pool mount-holder process** (socket `~/.cc-pool/mounts.sock`); daemon restarts/upgrades never disturb mounts. The holder is only replaced when no live sessions exist (or `ccp service uninstall --force`).

## Style Rules (summary — see STYLEGUIDE.md)

- **Fail fast, fail loud.** No silent fallbacks, sentinels, or defensive coding. No back-compat shims — delete dead code.
- **Errors**: wrap once per layer with `%w`; sentinels + `errors.Is/As`; never log-and-return; fallible call adjacent to its `if err != nil`.
- **Comments**: terse and sparing — the code documents itself through names, types, and organization. The one exception is godoc on exported symbols (each starting with the identifier's name); inside bodies, only TODOs, non-obvious workarounds, or disabled code.
- **Flat over nested**: early returns; nesting >3 is a smell.
- **Concurrency**: `ctx` first param; every goroutine has a defined exit; locks never held across I/O.
- **Tests must catch bugs**: strong assertions, table-driven with named cases, mock externals only, negative tests required, never degrade a test to make it pass.
- **Leave it better**: fix guide violations in code you're touching; stay in scope otherwise.

## Mechanical Linting

`gofmt` and `go vet` own formatting and mechanical issues. Don't hand-flag them in review; only fix issues requiring judgment — logic, architecture, edge cases.
