#!/bin/sh
# Install the exact cc-pool CLI and signed application resource from one release.
#
# Usage:
#   install.sh [VERSION]        # VERSION defaults to latest
#   curl -fsSL https://raw.githubusercontent.com/yasyf/cc-pool/main/scripts/install.sh | sh
set -eu

REPO="yasyf/cc-pool"
VERSION="${1:-latest}"
BIN_DIR="${CC_POOL_BIN_DIR:-$HOME/.local/bin}"
PREFIX="$(dirname "$BIN_DIR")"
LIBEXEC_DIR="$PREFIX/libexec"
DEST="$BIN_DIR/cc-pool"
PACKAGED_APP="$LIBEXEC_DIR/CCPoolStatus.app"
cli_requirement='identifier "com.yasyf.cc-pool" and anchor apple generic and certificate leaf[subject.OU] = "SXKCTF23Q2"'
app_requirement='identifier "com.yasyf.cc-pool.status" and anchor apple generic and certificate leaf[subject.OU] = "SXKCTF23Q2"'

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

if [ "$(uname -s)" != "Darwin" ]; then
  echo "cc-pool: signed application packaging requires macOS" >&2
  exit 1
fi

case "$BIN_DIR" in
  /*) ;;
  *)
    echo "cc-pool: binary directory must be an exact absolute path" >&2
    exit 1
    ;;
esac

if [ "$VERSION" = "latest" ]; then
  effective="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/$REPO/releases/latest")"
  VERSION="${effective##*/tag/}"
fi
if ! printf '%s\n' "$VERSION" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$'; then
  echo "cc-pool: invalid release tag '$VERSION'" >&2
  exit 1
fi

link_alias() {
  ln -sfn cc-pool "$BIN_DIR/ccp"
}

CLI_ASSET="cc-pool-${VERSION}-darwin-universal.tar.gz"
APP_ASSET="cc-pool-status-${VERSION}-darwin.zip"
BASE_URL="https://github.com/$REPO/releases/download/$VERSION"
mkdir -p "$BIN_DIR" "$LIBEXEC_DIR"
if [ "$(cd "$BIN_DIR" && pwd -P)" != "$BIN_DIR" ] ||
  [ "$(cd "$LIBEXEC_DIR" && pwd -P)" != "$LIBEXEC_DIR" ]; then
  echo "cc-pool: package directories must be canonical real paths" >&2
  exit 1
fi

lock="$PREFIX/.cc-pool-install.lock"
lock_acquired=0
process_start() {
  ps -o lstart= -p "$1" 2>/dev/null | sed 's/^[[:space:]]*//'
}
acquire_lock() {
  if mkdir "$lock" 2>/dev/null; then
    lock_acquired=1
    umask 077
    {
      printf '%s\n' "$$"
      process_start "$$"
    } > "$lock/owner"
    return 0
  fi
  read_lock_owner() {
    owner_pid=""
    owner_start=""
    if [ -f "$lock/owner" ]; then
      {
        IFS= read -r owner_pid || true
        IFS= read -r owner_start || true
      } < "$lock/owner"
    fi
  }
  read_lock_owner
  retry_owner=0
  case "$owner_pid" in
    '' | *[!0-9]*) retry_owner=1 ;;
  esac
  [ -n "$owner_start" ] || retry_owner=1
  if [ "$retry_owner" = 1 ]; then
    sleep 1
    read_lock_owner
  fi
  case "$owner_pid" in
    '' | *[!0-9]*) ;;
    *)
      if [ -n "$owner_start" ] && [ "$(process_start "$owner_pid")" = "$owner_start" ]; then
        echo "cc-pool: another installation owns $lock" >&2
        return 1
      fi
      ;;
  esac
  stale_lock="$lock.stale.$$"
  if ! mv "$lock" "$stale_lock" 2>/dev/null; then
    echo "cc-pool: another installation changed $lock during recovery" >&2
    return 1
  fi
  rm -rf "$stale_lock"
  if ! mkdir "$lock" 2>/dev/null; then
    echo "cc-pool: another installation owns $lock" >&2
    return 1
  fi
  lock_acquired=1
  umask 077
  {
    printf '%s\n' "$$"
    process_start "$$"
  } > "$lock/owner"
}
acquire_lock || exit 1
stage=""
cli_stage=""
app_stage=""
cli_backup=""
app_backup=""
backups_disposable=1
alias_present=0
cleanup() {
  [ -z "$stage" ] || rm -rf "$stage"
  [ -z "$cli_stage" ] || rm -rf "$cli_stage"
  [ -z "$app_stage" ] || rm -rf "$app_stage"
  if [ "$backups_disposable" = 1 ]; then
    [ -z "$cli_backup" ] || rm -rf "$cli_backup"
    [ -z "$app_backup" ] || rm -rf "$app_backup"
  else
    echo "cc-pool: retained recovery data at $cli_backup and $app_backup" >&2
  fi
  if [ "$lock_acquired" = 1 ]; then
    rm -f "$lock/owner"
    rmdir "$lock" 2>/dev/null || true
  fi
}
trap cleanup EXIT

if [ -L "$BIN_DIR/ccp" ]; then
  if [ "$(readlink "$BIN_DIR/ccp")" != "cc-pool" ]; then
    echo "cc-pool: refusing to replace unmanaged $BIN_DIR/ccp" >&2
    exit 1
  fi
  alias_present=1
elif [ -e "$BIN_DIR/ccp" ]; then
  echo "cc-pool: refusing to replace unmanaged $BIN_DIR/ccp" >&2
  exit 1
fi
if [ -L "$DEST" ] || { [ -e "$DEST" ] && { [ ! -f "$DEST" ] || [ ! -x "$DEST" ]; }; }; then
  echo "cc-pool: refusing to replace unmanaged $DEST" >&2
  exit 1
fi
if [ -e "$DEST" ] &&
  ! /usr/bin/codesign --verify --strict --all-architectures -R="$cli_requirement" "$DEST"; then
  echo "cc-pool: refusing to replace unsigned or differently signed $DEST" >&2
  exit 1
fi
if [ -L "$PACKAGED_APP" ] || { [ -e "$PACKAGED_APP" ] && [ ! -d "$PACKAGED_APP" ]; }; then
  echo "cc-pool: refusing to replace unmanaged $PACKAGED_APP" >&2
  exit 1
fi
if [ -e "$PACKAGED_APP" ] &&
  ! /usr/bin/codesign --verify --deep --strict -R="$app_requirement" "$PACKAGED_APP"; then
  echo "cc-pool: refusing to replace unsigned or differently signed $PACKAGED_APP" >&2
  exit 1
fi

stage="$(mktemp -d "$PREFIX/.cc-pool-download.XXXXXX")"
cli_archive="$stage/$CLI_ASSET"
app_archive="$stage/$APP_ASSET"
app_sidecar="$stage/$APP_ASSET.sha256"
curl -fsSL --retry 2 -o "$cli_archive" "$BASE_URL/$CLI_ASSET"
curl -fsSL --retry 2 -o "$app_archive" "$BASE_URL/$APP_ASSET"
curl -fsSL --retry 2 -o "$app_sidecar" "$BASE_URL/$APP_ASSET.sha256"
if ! sums="$(curl -fsSL --retry 2 "$BASE_URL/SHA256SUMS.txt")"; then
  echo "cc-pool: could not fetch SHA256SUMS.txt for $VERSION" >&2
  exit 1
fi

cli_expected="$(printf '%s\n' "$sums" | awk -v a="$CLI_ASSET" '$2 == a {print $1}')"
if ! printf '%s\n' "$cli_expected" | grep -Eq '^[0-9a-f]{64}$'; then
  echo "cc-pool: no checksum for $CLI_ASSET" >&2
  exit 1
fi
cli_actual="$(sha256_of "$cli_archive")"
if [ "$cli_actual" != "$cli_expected" ]; then
  echo "cc-pool: checksum mismatch for $CLI_ASSET" >&2
  exit 1
fi

app_extra=""
read -r app_expected app_sidecar_path app_extra < "$app_sidecar"
if ! printf '%s\n' "$app_expected" | grep -Eq '^[0-9a-f]{64}$' ||
  [ "$(basename "$app_sidecar_path")" != "$APP_ASSET" ] || [ -n "$app_extra" ] ||
  [ "$(wc -l < "$app_sidecar" | tr -d ' ')" != 1 ]; then
  echo "cc-pool: invalid checksum sidecar for $APP_ASSET" >&2
  exit 1
fi
app_actual="$(sha256_of "$app_archive")"
if [ "$app_actual" != "$app_expected" ]; then
  echo "cc-pool: checksum mismatch for $APP_ASSET" >&2
  exit 1
fi

cli_stage="$(mktemp -d "$BIN_DIR/.cc-pool-cli-stage.XXXXXX")"
app_stage="$(mktemp -d "$LIBEXEC_DIR/.cc-pool-app-stage.XXXXXX")"
if [ "$(tar -tzf "$cli_archive")" != "cc-pool" ]; then
  echo "cc-pool: CLI archive layout is not exact" >&2
  exit 1
fi
tar -xzf "$cli_archive" -C "$cli_stage"
new_cli="$cli_stage/cc-pool"
if [ ! -f "$new_cli" ] || [ -L "$new_cli" ]; then
  echo "cc-pool: CLI archive has no top-level cc-pool binary" >&2
  exit 1
fi
chmod +x "$new_cli"
if ! /usr/bin/codesign --verify --strict --all-architectures -R="$cli_requirement" "$new_cli"; then
  echo "cc-pool: CLI signature does not match the exact Developer ID identity" >&2
  exit 1
fi
cli_metadata="$(/usr/bin/codesign -dvvv "$new_cli" 2>&1)"
if ! printf '%s\n' "$cli_metadata" | grep -q '^Identifier=com.yasyf.cc-pool$' ||
  ! printf '%s\n' "$cli_metadata" | grep -q '^TeamIdentifier=SXKCTF23Q2$' ||
  ! printf '%s\n' "$cli_metadata" | grep -Eq '^CodeDirectory .* flags=.*\(runtime\)'; then
  echo "cc-pool: CLI signature metadata is not exact" >&2
  exit 1
fi
staged_cli_version="$($new_cli --version 2>/dev/null || true)"
case "$staged_cli_version" in
  "$VERSION" | "$VERSION "*) ;;
  *)
    echo "cc-pool: staged CLI version '$staged_cli_version' does not match $VERSION" >&2
    exit 1
    ;;
esac

ditto -x -k "$app_archive" "$app_stage"
new_app="$app_stage/CCPoolStatus.app"
if [ ! -d "$new_app" ] || [ -L "$new_app" ]; then
  echo "cc-pool: application archive has no top-level CCPoolStatus.app" >&2
  exit 1
fi
if ! /usr/bin/codesign --verify --deep --strict -R="$app_requirement" "$new_app"; then
  echo "cc-pool: application signature does not match the exact Developer ID identity" >&2
  exit 1
fi
expected_app_version="${VERSION#v}"
expected_app_version="${expected_app_version%%-*}"
staged_app_version="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "$new_app/Contents/Info.plist" 2>/dev/null || true)"
if [ "$staged_app_version" != "$expected_app_version" ]; then
  echo "cc-pool: staged application version '$staged_app_version' does not match $expected_app_version" >&2
  exit 1
fi

cli_backup="$(mktemp -d "$BIN_DIR/.cc-pool-cli-backup.XXXXXX")"
app_backup="$(mktemp -d "$LIBEXEC_DIR/.cc-pool-app-backup.XXXXXX")"
old_cli="$cli_backup/cc-pool"
old_app="$app_backup/CCPoolStatus.app"
cli_moved=0
app_moved=0
cli_published=0
app_published=0
backups_disposable=0
restore_delivery_before_dispatch() {
  restored=1
  [ "$cli_published" = 0 ] || rm -f "$DEST" || restored=0
  [ "$app_published" = 0 ] || rm -rf "$PACKAGED_APP" || restored=0
  [ "$cli_moved" = 0 ] || mv "$old_cli" "$DEST" || restored=0
  [ "$app_moved" = 0 ] || mv "$old_app" "$PACKAGED_APP" || restored=0
  if [ "$alias_present" = 1 ]; then
    link_alias || restored=0
  else
    rm -f "$BIN_DIR/ccp" || restored=0
  fi
  if [ "$restored" = 1 ]; then
    backups_disposable=1
    return 0
  fi
  return 1
}
if [ -e "$DEST" ]; then
  if ! mv "$DEST" "$old_cli"; then
    backups_disposable=1
    exit 1
  fi
  cli_moved=1
fi
if [ -e "$PACKAGED_APP" ]; then
  if ! mv "$PACKAGED_APP" "$old_app"; then
    restore_delivery_before_dispatch || true
    exit 1
  fi
  app_moved=1
fi
if ! mv "$new_app" "$PACKAGED_APP"; then
  restore_delivery_before_dispatch || true
  exit 1
fi
app_published=1
if ! mv "$new_cli" "$DEST"; then
  restore_delivery_before_dispatch || true
  exit 1
fi
cli_published=1
if ! link_alias; then
  restore_delivery_before_dispatch || true
  exit 1
fi
if ! "$DEST" package install; then
  # Dispatch may have committed before its response was lost. Keep the exact
  # verified source generation in place so a retry recovers the same sealed apply.
  backups_disposable=1
  exit 1
fi
backups_disposable=1
echo "cc-pool: installed $DEST ($($DEST --version))" >&2
