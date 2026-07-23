## Claude-Specific Rules

- **Verify every change** with `go vet ./...` and `scripts/test.sh ./...` before claiming it done. For binary-affecting changes, also run `CGO_ENABLED=0 go build ./cmd/cc-pool` and build the signed runtime archive on macOS.
- **Never run the daemon, `launchctl`, or `security` mutations against the user's real state** during development unless explicitly asked — tests must not touch `~/.claude`, `~/.cc-pool`, or the Keychain.
