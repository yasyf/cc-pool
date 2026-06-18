# End-to-end verification

A manual checklist that maps 1:1 to the acceptance criteria. Steps marked
**(human)** need an interactive Claude login or a real second subscription and
cannot be automated. Run from a clean machine state where possible.

## 0. Build / install

```sh
# From source:
CGO_ENABLED=0 go build -o /usr/local/bin/cc-pool ./cmd/cc-pool
ln -sf /usr/local/bin/cc-pool /usr/local/bin/ccp

# Or via the shared tap once released:
brew install yasyf/tap/cc-pool
brew install --cask yasyf/tap/cc-pool-status   # the Notification Center widget
```

## 1. Release installs and reports version
```sh
ccp --version          # prints the version
cc-pool --version  # same binary, both names work
```

## 2. add auto-inits; plain claude untouched; status; daemon refresh
```sh
ccp add                # (human) interactive: auto-inits the pool, auto-starts the daemon,
                       #         seeds .claude.json, logs in an account, then loops
                       #         ("Add another?") so you can onboard several subscriptions
ccp status             # all accounts shown with live 5h/7d remaining + score
ccp service status     # shows "Homebrew (brew services)" or self-managed, + Daemon: running
claude                 # (human) plain claude STILL launches on ~/.claude, untouched
```
Verify during `ccp add`:
- the `claude /login` session does NOT show the first-run theme wizard
  (onboarding state was seeded from ~/.claude.json), and no
  "configuration file not found / backup file exists" messages appear;
- after login, `~/.cc-pool/accounts/acct-NN/.claude.json` contains the NEW
  account's `oauthAccount` (not your main account's);
- in `Keychain Access`, each account has its own suffixed
  `Claude Code-credentials-<hash>` item; the canonical un-suffixed
  `Claude Code-credentials` item is never modified by the pool;
- daemon refreshes do not raise a prompt, and plain `claude` never gets
  logged out.

## 3. Drive one account up → select returns the other (stdout = path only)
```sh
# Burn 5h usage on acct-01 (human: run a heavy session), then:
ccp select                       # prints ONLY a config dir on stdout
ccp select 2>/dev/null           # stderr suppressed → still just the path
test -n "$(ccp select 2>/dev/null)"   # non-empty path
# Expect the LESS-used account's dir.
```

## 4. Launch on the selected account → subscription, shared workspace, distinct keychain
```sh
CLAUDE_CONFIG_DIR=$(ccp select) claude   # (human)
#   In-session /status shows the expected account on its SUBSCRIPTION (not "Claude API").
ls "$(ccp select 2>/dev/null)/projects"  # shared projects identical to plain claude's
diff <(ls ~/.claude/skills) <(ls "$(ccp env --account 1 2>/dev/null | sed -n 's/.*CLAUDE_CONFIG_DIR=//p' | tr -d "'")/skills")
```

## 5. Overlay parity (writes shared both ways)
```sh
# symlink provider (default):
ls -la ~/.cc-pool/accounts/acct-01            # top-level entries are symlinks into ~/.claude
#                                          except daemon/, ide/, backups/ (private dirs)
#                                          and .claude.json (private per-account file)
echo hi > ~/.cc-pool/accounts/acct-01/projects/_ccp_probe && \
  cat ~/.claude/projects/_ccp_probe       # write through the overlay lands in ~/.claude
rm ~/.claude/projects/_ccp_probe
ls ~/.cc-pool/accounts/acct-01/backups        # private: never shows ~/.claude/backups content

# fuse provider (only with a -tags fuse build + fuse-t + Network Volumes grant):
mount | grep cc-pool                  # account dir is a fuse-t mirror mount
```

## 6. End session → token adopted, checkout released; uninstall leaves ~/.claude intact
```sh
# After a `ccp run` session exits, the daemon re-reads any rotated token and
# closes the checkout:
ccp run -- -p "hello"                     # (human) owns the PID; adopts rotated token on exit
ccp status                                # active sessions for that account back to baseline

ccp service uninstall                     # stops daemon + mount holder, unmounts fuse accounts
                                          # (refuses under live sessions; --force overrides)
ccp service uninstall --purge             # also removes pool accounts/dirs/state
                                          # (aborts if any account dir is still a mountpoint)
brew uninstall cc-pool                # (if installed via brew)
test -d ~/.claude && claude --version     # (human) ~/.claude intact; plain claude still works
```

## Automated coverage (CI, no human needed)
- `go test ./...` — keychain argv contract (fake `security`), OAuth refresh/usage
  request+response shapes and unit normalization, sqlite store, scoring +
  tie-breaks, symlink overlay setup/sync/health/teardown, process-env parsing.
- `go build -tags fuse` — cgofuse + fuse-t compilation canary (the live mount
  test is `t.Skip`-ped without the interactive Network Volumes grant).
