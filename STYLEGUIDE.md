# cc-pool Style Guide

Target Go 1.22+. `gofmt` is law; everything below is what `gofmt` can't decide for you.

## Core Principles

1. **Fail fast, fail loud** — No silent fallbacks, no sentinel values, no defensive coding against impossible states. If a precondition can't hold, return an error; if it can't even be expressed, `panic` (programmer error only). Don't guard against states the type system already rules out.
2. **Make invalid states unrepresentable** — Typed constants over stringly state (`type Status int` + `iota`, not `"running"` literals scattered around). Required fields over pointers-meaning-optional; every `*T` field forces every reader to handle nil. Small, consumer-defined interfaces over big provider-defined ones.
3. **Reuse before creating** — Check the same package, then sibling `internal/*` packages, before writing a helper. No premature `util` package; a helper used by one package lives in that package.
4. **Match surrounding code** — Priority: (1) this guide, (2) same file, (3) same package. If surrounding code violates this guide, fix it while you're there.
5. **No backwards-compat shims** — This is an unreleased single-binary tool. Dead code, compatibility layers, and deprecated aliases get deleted, not kept.
6. **Minimal changes** — Stay within scope. Make the test pass, then stop. Only improve code you're directly touching.

## Errors

- Wrap with context exactly once per layer: `fmt.Errorf("load account %d: %w", id, err)`. Don't re-wrap the same information at every level, and never both log *and* return an error — pick one (return; the caller logs).
- Sentinel errors (`var ErrNotFound = errors.New(...)`) + `errors.Is`/`errors.As` for control flow. Never match on error strings.
- Handle errors at the level that has context to act; pass them up otherwise. `_ =` discards only for genuinely best-effort cleanup (and say why if non-obvious).
- Keep the fallible call adjacent to its `if err != nil` — no unrelated statements between them (the Go analog of "minimal try block").
- `panic` only for programmer error (e.g. `mustHome` in paths that cannot proceed without a home dir). Library-ish packages (`internal/*` consumed by the daemon) return errors; they don't `os.Exit` or `log.Fatal`.

## Comments & Documentation

- **Godoc on every exported symbol** — full sentences, starting with the symbol name. This is the one mandated deviation from "no comments": Go tooling depends on it. Keep it terse: the first sentence carries the doc, and a godoc running past ~6 lines is a smell.
- **Inside function bodies: no noise comments.** Code is self-documenting via names, types, and small functions. Comments only for:
  - TODOs (`// TODO: ...`)
  - Non-obvious workarounds and invariants (e.g. the CLAUDE_CONFIG_DIR string hashed for the Keychain service name MUST stay byte-identical to what `ccp` emits — that comment earns its place)
  - Disabled code that may be re-enabled
- No section-marker comments (`// --- helpers ---`); split into files instead.
- Don't restate the code (`// increment i`). Don't narrate history (`// previously this used X`) — git remembers. Extended rationale, design trade-offs, or investigation write-ups go to cc-notes (`ccn doc add`) with at most a one-line pointer in the source.

## Organization & Naming

- File order: package doc → imports → constants → types → constructors → methods → related helpers. Constants at the top, `UPPER_SNAKE` is not Go — use `MixedCaps` (`maxRetries`, `DefaultTimeout`).
- Imports in two groups: stdlib, then everything else. (`gofmt`/`goimports` handles the rest.)
- Naming: short receivers (`m *Manager`, not `manager` or `self`/`this`); no `Get` prefix (`Name()`, not `GetName()`); acronyms keep case (`ID`, `URL`, `DBPath`); package name is part of the API — `pool.AccountDir`, not `pool.PoolAccountDir`.
- Flat control flow: early returns, happy path at the lowest indentation. Nesting >3 deep is a smell — extract a function.
- One concept per file; file names describe contents (`paths.go`, `scheduler.go`), no `misc.go` dumping grounds growing without bound.

## Concurrency

- `ctx context.Context` is the first parameter of anything that blocks, sleeps, retries, or makes a request. Honor cancellation — `select` on `ctx.Done()` in loops.
- Every goroutine has a defined exit path before it's spawned. If you can't say what makes it return, don't start it.
- Channels are owned (created and closed) by the sender. Never close from the receiver side.
- Mutex scope is minimal: lock, touch the shared state, unlock. Never hold a lock across I/O, a network call, or a subprocess. Prefer `defer mu.Unlock()` unless the critical section is a small early slice of the function.
- Shared state needs exactly one synchronization story (one mutex, one owner goroutine, or immutability) — document which at the declaration.

## Testing

Tests exist to catch bugs, not to satisfy coverage. Before writing one, answer: *what bug would make this fail?* If nothing would, delete it.

- **Strong assertions** — compare against specific expected values. `if got != want` with both in the message. `result != nil` as the sole assertion is worthless.
- **Table-driven tests** with descriptive case names covering happy, edge, and error paths. Golden vectors pin externally-derived values (e.g. Keychain service-name hashes) against an independent oracle.
- **Mock externals, never code under test** — Keychain (`security`), network, launchctl, the filesystem where practical (`t.TempDir()`). If you mock the function you're testing, you're testing the mock.
- **Litmus test** — revert the implementation; the test must fail. If it still passes, it exercises nothing.
- **Never degrade tests to pass**: no deleting failing cases, no weakening `==` to `!= nil`, no dropping error-path tests because setup is annoying. When a test fails: read the error → fix the test if its expectation is wrong → otherwise fix the implementation. Stuck after two attempts? Ask, don't simplify silently.
- **Negative tests required** — invalid input, dependency failure (Keychain item missing, daemon socket dead), boundary conditions.
- Async/daemon tests use real synchronization (channels, `sync.WaitGroup`), never `time.Sleep` as a synchronization primitive.

## Tooling

- `gofmt` and `go vet` are assumed clean — never hand-flag mechanical issues (formatting, import order) in review; tooling owns them.
- Default build is pure Go (`CGO_ENABLED=0`); only `-tags fuse` needs cgo + fuse-t. Code shared by both variants must compile under both — keep build-tag surface minimal (`fuse.go` / `fuse_stub.go` pattern).
- `go test ./...` must pass with no network, no Keychain access, and no daemon running.
