#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL="$ROOT/scripts/install.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/stub" "$WORK/formula/bin"

cat > "$WORK/stub/uname" <<'EOF'
#!/bin/sh
echo "${FAKE_OS:-Darwin}"
EOF

cat > "$WORK/stub/brew" <<'EOF'
#!/bin/sh
printf 'brew %s\n' "$*" >> "$COMMAND_LOG"
[ "${1:-}" != "--prefix" ] || {
  [ "${2:-}" = "yasyf/tap/cc-pool" ]
  printf '%s\n' "$FORMULA_PREFIX"
}
[ "${FAIL_BREW:-0}" != 1 ]
EOF

cat > "$WORK/stub/ccp" <<'EOF'
#!/bin/sh
printf 'shadow-ccp %s\n' "$*" >> "$COMMAND_LOG"
exit 97
EOF

cat > "$WORK/formula/bin/ccp" <<'EOF'
#!/bin/sh
printf 'ccp %s\n' "$*" >> "$COMMAND_LOG"
if [ "${1:-}" = "--version" ]; then
  echo 'v0.64.0 (fixture)'
fi
[ "${FAIL_PACKAGE:-0}" != 1 ]
EOF

chmod +x "$WORK/stub/uname" "$WORK/stub/brew" "$WORK/stub/ccp" "$WORK/formula/bin/ccp"
export COMMAND_LOG="$WORK/commands"
export FORMULA_PREFIX="$WORK/formula"

PATH="$WORK/stub:/usr/bin:/bin" "$INSTALL" >/dev/null
cat > "$WORK/expected" <<'EOF'
brew install yasyf/tap/cc-pool
brew --prefix yasyf/tap/cc-pool
ccp package install
ccp --version
EOF
if ! diff -u "$WORK/expected" "$COMMAND_LOG"; then
  echo "FAIL: macOS bootstrap did not use the exact Homebrew/package sequence" >&2
  exit 1
fi
echo "ok: macOS bootstrap uses Homebrew then explicit package install"

if grep -Fq 'shadow-ccp' "$COMMAND_LOG"; then
  echo "FAIL: macOS bootstrap executed a PATH-shadowing ccp" >&2
  exit 1
fi
echo "ok: macOS bootstrap executes the installed formula's exact ccp"

: > "$COMMAND_LOG"
if PATH="$WORK/stub:/usr/bin:/bin" "$INSTALL" v0.64.0 >/dev/null 2>&1; then
  echo "FAIL: pinned macOS release succeeded outside the formula contract" >&2
  exit 1
fi
if [ -s "$COMMAND_LOG" ]; then
  echo "FAIL: pinned macOS release mutated package state" >&2
  exit 1
fi
echo "ok: pinned macOS release fails before package mutation"

: > "$COMMAND_LOG"
if FAKE_OS=Linux PATH="$WORK/stub:/usr/bin:/bin" "$INSTALL" >/dev/null 2>&1; then
  echo "FAIL: non-macOS bootstrap succeeded" >&2
  exit 1
fi
if [ -s "$COMMAND_LOG" ]; then
  echo "FAIL: non-macOS bootstrap mutated package state" >&2
  exit 1
fi
echo "ok: non-macOS bootstrap fails closed"

: > "$COMMAND_LOG"
if PATH="/usr/bin:/bin" "$INSTALL" >/dev/null 2>&1; then
  echo "FAIL: bootstrap without Homebrew succeeded" >&2
  exit 1
fi
echo "ok: missing Homebrew fails closed"

: > "$COMMAND_LOG"
chmod -x "$WORK/formula/bin/ccp"
if PATH="$WORK/stub:/usr/bin:/bin" "$INSTALL" >/dev/null 2>&1; then
  echo "FAIL: bootstrap without the formula's ccp succeeded" >&2
  exit 1
fi
chmod +x "$WORK/formula/bin/ccp"
if grep -Fq 'ccp package install' "$COMMAND_LOG"; then
  echo "FAIL: missing formula ccp reached package install" >&2
  exit 1
fi
echo "ok: missing formula ccp fails before package mutation"

: > "$COMMAND_LOG"
if FAIL_BREW=1 PATH="$WORK/stub:/usr/bin:/bin" "$INSTALL" >/dev/null 2>&1; then
  echo "FAIL: failed Homebrew delivery reached package install" >&2
  exit 1
fi
if grep -Fq 'ccp package install' "$COMMAND_LOG"; then
  echo "FAIL: failed Homebrew delivery invoked package install" >&2
  exit 1
fi
echo "ok: failed Homebrew delivery stops before package install"

: > "$COMMAND_LOG"
if FAIL_PACKAGE=1 PATH="$WORK/stub:/usr/bin:/bin" "$INSTALL" >/dev/null 2>&1; then
  echo "FAIL: failed daemonkit package apply was ignored" >&2
  exit 1
fi
if ! grep -Fxq 'ccp package install' "$COMMAND_LOG"; then
  echo "FAIL: package apply failure did not pass through the explicit command" >&2
  exit 1
fi
echo "ok: daemonkit package apply failure propagates"

for forbidden in releases/download ditto codesign SHA256SUMS CC_POOL_BIN_DIR '.local/libexec'; do
  if grep -Fq "$forbidden" "$INSTALL"; then
    echo "FAIL: bootstrap retains duplicate delivery primitive $forbidden" >&2
    exit 1
  fi
done
if grep -Eq '(^|[[:space:]])service[[:space:]]+(install|uninstall)' "$INSTALL"; then
  echo "FAIL: package bootstrap mutates daemon service state" >&2
  exit 1
fi
echo "ok: bootstrap contains no duplicate delivery or implicit service protocol"
