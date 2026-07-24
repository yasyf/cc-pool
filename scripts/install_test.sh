#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOURCE_INSTALL="$ROOT/scripts/install.sh"
WORK="$(mktemp -d)"
WORK="$(cd "$WORK" && pwd -P)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/stub" "$WORK/archive"
INSTALL="$WORK/install.sh"
sed \
  -e "s#/usr/bin/codesign#$WORK/stub/codesign#g" \
  -e "s#/usr/libexec/PlistBuddy#$WORK/stub/PlistBuddy#g" \
  "$SOURCE_INSTALL" > "$INSTALL"
chmod +x "$INSTALL"

cat > "$WORK/fixture-binary" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >> "$FIXTURE_COMMAND_LOG"
[ "${1:-}" = "--version" ] && echo "${FIXTURE_VERSION:-v0.9.9 (deadbee)}"
[ "${1:-}" = "package" ] && [ "${FAIL_PACKAGE:-0}" = 1 ] && exit 1
exit 0
EOF
chmod +x "$WORK/fixture-binary"
cp "$WORK/fixture-binary" "$WORK/archive/cc-pool"
tar -czf "$WORK/fixture-cli.tar.gz" -C "$WORK/archive" cc-pool
printf 'signed application archive fixture\n' > "$WORK/fixture-app.zip"

cat > "$WORK/stub/uname" <<'EOF'
#!/bin/sh
echo "${FAKE_OS:-Darwin}"
EOF
chmod +x "$WORK/stub/uname"

cat > "$WORK/stub/ditto" <<'EOF'
#!/bin/sh
for destination; do :; done
mkdir -p "$destination/CCPoolStatus.app/Contents"
printf 'signed fixture\n' > "$destination/CCPoolStatus.app/marker"
printf 'plist fixture\n' > "$destination/CCPoolStatus.app/Contents/Info.plist"
EOF
chmod +x "$WORK/stub/ditto"

cat > "$WORK/stub/codesign" <<'EOF'
#!/bin/sh
case "$*" in
  *-dvvv*)
    cat >&2 <<'METADATA'
Identifier=com.yasyf.cc-pool
TeamIdentifier=SXKCTF23Q2
CodeDirectory v=20500 size=1 flags=0x10000(runtime) hashes=1+1 location=embedded
METADATA
    ;;
esac
exit 0
EOF
chmod +x "$WORK/stub/codesign"

cat > "$WORK/stub/PlistBuddy" <<'EOF'
#!/bin/sh
echo "${FIXTURE_APP_VERSION:-0.9.9}"
EOF
chmod +x "$WORK/stub/PlistBuddy"

cat > "$WORK/stub/curl" <<'EOF'
#!/bin/sh
url=""
out=""
prev=""
for argument in "$@"; do
  [ "$prev" = "-o" ] && out="$argument"
  url="$argument"
  prev="$argument"
done
case "$*" in
  *url_effective*) printf '%s' 'https://github.com/yasyf/cc-pool/releases/tag/v0.9.9' ;;
  *SHA256SUMS.txt*)
    printf '%s  cc-pool-v0.9.9-darwin-universal.tar.gz\n' "$(shasum -a 256 "$FIXTURE_CLI" | awk '{print $1}')"
    ;;
  *cc-pool-status-v0.9.9-darwin.zip.sha256*)
    printf '%s  cc-pool-status-v0.9.9-darwin.zip\n' "$(shasum -a 256 "$FIXTURE_APP" | awk '{print $1}')" > "$out"
    ;;
  *cc-pool-status-v0.9.9-darwin.zip*) cp "$FIXTURE_APP" "$out" ;;
  *cc-pool-v0.9.9-darwin-universal.tar.gz*) cp "$FIXTURE_CLI" "$out" ;;
  *) echo "unexpected curl request: $url" >&2; exit 1 ;;
esac
printf '%s\n' "$url" >> "$REQUESTED_LOG"
EOF
chmod +x "$WORK/stub/curl"

export FIXTURE_CLI="$WORK/fixture-cli.tar.gz"
export FIXTURE_APP="$WORK/fixture-app.zip"
export FIXTURE_COMMAND_LOG="$WORK/commands"
export REQUESTED_LOG="$WORK/requests"
export CC_POOL_BIN_DIR="$WORK/prefix/bin"
PACKAGED_APP_DIR="$WORK/prefix/libexec/CCPoolStatus.app"

PATH="$WORK/stub:$PATH" "$INSTALL" >/dev/null
if [ ! -x "$CC_POOL_BIN_DIR/cc-pool" ] || [ ! -d "$PACKAGED_APP_DIR" ]; then
  echo "FAIL: installer did not publish the complete package" >&2
  exit 1
fi
if ! grep -qx 'package install' "$FIXTURE_COMMAND_LOG"; then
  echo "FAIL: installer did not invoke the explicit package installer" >&2
  exit 1
fi
if grep -Eq '(^| )service (install|uninstall)( |$)' "$FIXTURE_COMMAND_LOG"; then
  echo "FAIL: installer changed daemon service state directly" >&2
  exit 1
fi
echo "ok: complete package installed and activated explicitly"

: > "$REQUESTED_LOG"
PATH="$WORK/stub:$PATH" "$INSTALL" >/dev/null
if ! grep -q '/cc-pool-v0.9.9-darwin-universal.tar.gz$' "$REQUESTED_LOG" ||
  ! grep -q '/cc-pool-status-v0.9.9-darwin.zip$' "$REQUESTED_LOG"; then
  echo "FAIL: rerun trusted existing local bytes instead of reacquiring verified release assets" >&2
  exit 1
fi
echo "ok: rerun reacquires and verifies the complete release package"

prior_marker="$(cat "$PACKAGED_APP_DIR/marker")"
if PATH="$WORK/stub:$PATH" FIXTURE_VERSION='v0.9.8 (wrong)' "$INSTALL" >/dev/null 2>&1; then
  echo "FAIL: staged CLI version mismatch succeeded" >&2
  exit 1
fi
if [ "$(cat "$PACKAGED_APP_DIR/marker")" != "$prior_marker" ]; then
  echo "FAIL: staged CLI version mismatch changed the installed package" >&2
  exit 1
fi
echo "ok: staged CLI version mismatch fails before publication"

if PATH="$WORK/stub:$PATH" FIXTURE_APP_VERSION='0.9.8' "$INSTALL" >/dev/null 2>&1; then
  echo "FAIL: staged application version mismatch succeeded" >&2
  exit 1
fi
if [ "$(cat "$PACKAGED_APP_DIR/marker")" != "$prior_marker" ]; then
  echo "FAIL: staged application version mismatch changed the installed package" >&2
  exit 1
fi
echo "ok: staged application version mismatch fails before publication"

cat > "$CC_POOL_BIN_DIR/cc-pool" <<'EOF'
#!/bin/sh
[ "${1:-}" = "--version" ] && echo "v0.9.8 (old)"
exit 0
EOF
chmod +x "$CC_POOL_BIN_DIR/cc-pool"
printf 'old packaged application\n' > "$PACKAGED_APP_DIR/marker"
if PATH="$WORK/stub:$PATH" FAIL_PACKAGE=1 "$INSTALL" >/dev/null 2>&1; then
  echo "FAIL: activation failure did not fail the direct installer" >&2
  exit 1
fi
if [ "$("$CC_POOL_BIN_DIR"/cc-pool --version)" != "v0.9.9 (deadbee)" ] ||
  [ "$(cat "$PACKAGED_APP_DIR/marker")" != "signed fixture" ]; then
  echo "FAIL: activation ambiguity reverted the exact delivered source generation" >&2
  exit 1
fi
echo "ok: activation ambiguity preserves the exact delivered source generation for recovery"

if PATH="$WORK/stub:$PATH" FAKE_OS=Linux "$INSTALL" >/dev/null 2>&1; then
  echo "FAIL: non-macOS install succeeded" >&2
  exit 1
fi
echo "ok: non-macOS install fails closed"

if PATH="$WORK/stub:$PATH" "$INSTALL" 'v0.9/../bad' >/dev/null 2>&1; then
  echo "FAIL: noncanonical release tag succeeded" >&2
  exit 1
fi
echo "ok: noncanonical release tag fails closed"

rm -f "$CC_POOL_BIN_DIR/ccp"
printf 'owned elsewhere\n' > "$CC_POOL_BIN_DIR/ccp"
if PATH="$WORK/stub:$PATH" "$INSTALL" >/dev/null 2>&1; then
  echo "FAIL: installer replaced an unmanaged ccp object" >&2
  exit 1
fi
if [ "$(cat "$CC_POOL_BIN_DIR/ccp")" != "owned elsewhere" ]; then
  echo "FAIL: unmanaged ccp object changed" >&2
  exit 1
fi
echo "ok: unmanaged ccp object is preserved"

rm -f "$CC_POOL_BIN_DIR/ccp"
ln -s cc-pool "$CC_POOL_BIN_DIR/ccp"
mv "$CC_POOL_BIN_DIR/cc-pool" "$WORK/managed-cc-pool"
ln -s "$WORK/managed-cc-pool" "$CC_POOL_BIN_DIR/cc-pool"
if PATH="$WORK/stub:$PATH" "$INSTALL" >/dev/null 2>&1; then
  echo "FAIL: installer replaced a symlinked CLI destination" >&2
  exit 1
fi
if [ "$(readlink "$CC_POOL_BIN_DIR/cc-pool")" != "$WORK/managed-cc-pool" ]; then
  echo "FAIL: symlinked CLI destination changed" >&2
  exit 1
fi
echo "ok: symlinked CLI destination is preserved"
rm -f "$CC_POOL_BIN_DIR/cc-pool"
mv "$WORK/managed-cc-pool" "$CC_POOL_BIN_DIR/cc-pool"

if CC_POOL_BIN_DIR=relative/bin PATH="$WORK/stub:$PATH" "$INSTALL" >/dev/null 2>&1; then
  echo "FAIL: relative package root succeeded" >&2
  exit 1
fi
echo "ok: relative package root fails closed"

mkdir "$WORK/prefix/.cc-pool-install.lock"
PATH="$WORK/stub:$PATH" "$INSTALL" >/dev/null
echo "ok: ownerless stale install lock is recovered"

mkdir "$WORK/prefix/.cc-pool-install.lock"
{
  printf '%s\n' "$$"
  ps -o lstart= -p "$$" | sed 's/^[[:space:]]*//'
} > "$WORK/prefix/.cc-pool-install.lock/owner"
if PATH="$WORK/stub:$PATH" "$INSTALL" >/dev/null 2>&1; then
  echo "FAIL: concurrent installer lock was ignored" >&2
  exit 1
fi
echo "ok: live concurrent installer is rejected"
