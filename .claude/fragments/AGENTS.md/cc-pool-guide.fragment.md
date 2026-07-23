# cc-pool Development Guide

Full style guide: [STYLEGUIDE.md](STYLEGUIDE.md)

## Project Basics

cc-pool (`ccp`) pools several Claude Max/Pro subscriptions and launches each Claude Code session on the emptiest account. Go, macOS-only, single binary.

- **Build**: `CGO_ENABLED=0 go build ./cmd/cc-pool`; the signed File Provider runtime archive is built with cgo but has no native filesystem dependency.
- **Test**: `scripts/test.sh ./...` — a `ulimit -u` wrapper around `go test` so a runaway spawn cannot fork-bomb the host. Must pass with no network, no Keychain, no daemon. Never run bare `go test` on a real machine; use the harness, or an isolated VM/container for live signed-runtime and File Provider work.
- **Vet**: `go vet ./...` before every commit

## Releasing

Releases are **tag-triggered** — there is no version file to edit. `Version`/`Commit` in
`internal/version` default to `dev` locally and are injected at build time via `-ldflags`.

Pushing a `vX.Y.Z` tag runs `.github/workflows/release.yml`, which (1) builds the universal
(arm64+amd64) pure-Go binary on macOS and Developer ID-signs + notarizes it without App
Group access, (2) builds, Developer ID-signs, notarizes, and staples the fixed
`CCPoolStatus.app` runtime, broker, File Provider, and widget bundle (the
`cc-pool-status` cask payload),
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
│   ├── daemon/         # account scheduling, polling, daemonkit runtime, and tenant lifecycle
│   ├── forecast/       # per-account burn rates + depletion estimates; pool-wide rollup shipped to the widget
│   ├── oauth/          # Claude OAuth refresh + /api/oauth/usage client
│   ├── holderbridge/   # native bridge embedded in the fixed signed app
│   ├── overlay/        # Claude-specific merge, split, and classification policy
│   ├── pool/           # account paths, policy, reservations, and tenant specs
│   ├── procscan/       # detect live claude sessions per config dir
│   ├── score/          # account scoring (5h/7d headroom, reset credit, burn rate)
│   ├── store/          # SQLite account state (no secrets — Keychain only)
│   └── tenantfs/       # Claude authority policy, materializer, and FuseKit adapter
├── widget/             # fixed signed runtime/broker/File Provider/widget application
├── docs/               # public design doc (ARCHITECTURE.md) + README assets
├── AGENTS.md           # This file — shared conventions
└── STYLEGUIDE.md       # Full style guide
```

Filesystem identity, catalog revisions, tenant convergence, File
Provider enumeration, and domain retirement live in
[`github.com/yasyf/fusekit`](https://github.com/yasyf/fusekit). Process lifecycle,
transport, exact peer trust, and reaping live in
[`github.com/yasyf/daemonkit`](https://github.com/yasyf/daemonkit). cc-pool publishes
Claude source changes and account policy through those APIs; it does not reimplement
runtime, bridge, notification, or reconciliation machinery.

Two filesystem trees, never confused:

- `~/.claude` — canonical Claude Code config dir. **NEVER moved, modified structurally, or registered as a pool account.** It is plain `claude`'s home and the shared source base; plain `claude` must keep working untouched.
- `~/.cc-pool/` — cc-pool's private state and FuseKit source backing. Public account config dirs are File Provider roots selected by macOS.

Safety rules baked into the architecture — do not regress them:

1. **The pool NEVER reads, writes, deletes, or even names the canonical unsuffixed Keychain item (`Claude Code-credentials`), and never mutates plain claude's OAuth state.** There is no exception: `keychain.ServiceName` always emits a hash-suffixed name, and no code path can name the canonical item. Every pool account — including the user's main subscription — gets its own config dir, its own refresh-token chain (from its own `claude /login`), and its own suffixed Keychain item. This is why there is no credential "adoption": forking a pool account off plain claude's login would require spending plain claude's single-use refresh token, which rotation invalidates — signing plain claude out. A fresh login per account is the only safe path.
2. **No secrets in SQLite** — the macOS Keychain is the sole secret store.
3. **Account dir strings are hashed for Keychain service names** — the path string `ccp` emits and the string hashed must stay byte-identical. No realpath/normalization divergence.
4. **Protected filesystem access belongs only to the fixed signed application.** The
   unsigned Go daemon never resolves, names, or traverses the App Group container. The
   app embeds the FuseKit runtime; daemonkit proves its exact signed identity, and FuseKit
   owns File Provider presentation across daemon restarts.

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
