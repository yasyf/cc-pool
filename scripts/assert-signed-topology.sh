#!/usr/bin/env bash
# Assert the one fixed signed CCPoolStatus/File Provider topology.
set -euo pipefail

: "${APP_PATH:?APP_PATH must name the built CCPoolStatus.app}"
: "${TEAM_ID:?TEAM_ID must name the expected signing team}"
: "${APP_GROUP:?APP_GROUP must name the one expected App Group}"

APP="$APP_PATH"
FP="$APP/Contents/PlugIns/CCPoolFileProvider.appex"
WIDGET="$APP/Contents/PlugIns/CCPoolStatusWidget.appex"
HOST="$APP/Contents/MacOS/CCPoolStatus"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

fail() {
  echo "signed-topology assertion failed: $*" >&2
  exit 1
}

assert_identity() {
  local bundle="$1" identifier="$2" metadata requirement
  codesign --verify --strict "$bundle" || fail "$bundle failed strict signature verification"
  metadata="$(codesign -dvvv "$bundle" 2>&1)"
  requirement="$(codesign -d -r- "$bundle" 2>&1)"
  grep -q "^Identifier=$identifier$" <<<"$metadata" \
    || fail "$bundle signing identifier is not $identifier"
  grep -q "^TeamIdentifier=$TEAM_ID$" <<<"$metadata" \
    || fail "$bundle TeamIdentifier is not $TEAM_ID"
  grep -Eq '^CodeDirectory .* flags=.*\(runtime\)' <<<"$metadata" \
    || fail "$bundle is not hardened-runtime signed"
  grep -Fq "identifier \"$identifier\"" <<<"$requirement" \
    || fail "$bundle designated requirement does not pin $identifier"
  grep -Fq "subject.OU] = $TEAM_ID" <<<"$requirement" \
    || fail "$bundle designated requirement does not pin team $TEAM_ID"
}

extract_entitlements() {
  local bundle="$1" output="$2"
  codesign -d --entitlements :- "$bundle" >"$output" 2>/dev/null \
    || fail "could not extract entitlements from $bundle"
  plutil -lint "$output" >/dev/null || fail "$bundle entitlements are not a valid plist"
}

assert_one_group() {
  local entitlements="$1" value
  value="$(/usr/libexec/PlistBuddy -c 'Print :com.apple.security.application-groups:0' "$entitlements" 2>/dev/null)" \
    || fail "$entitlements has no App Group"
  [[ "$value" == "$APP_GROUP" ]] \
    || fail "$entitlements App Group is $value, not $APP_GROUP"
  if /usr/libexec/PlistBuddy -c 'Print :com.apple.security.application-groups:1' "$entitlements" >/dev/null 2>&1; then
    fail "$entitlements claims more than the one intended App Group"
  fi
}

assert_no_injection_entitlements() {
  local entitlements="$1" key
  for key in \
    com.apple.security.cs.disable-library-validation \
    com.apple.security.cs.allow-dyld-environment-variables \
    com.apple.security.cs.allow-unsigned-executable-memory \
    com.apple.security.cs.allow-jit \
    com.apple.security.get-task-allow; do
    if /usr/libexec/PlistBuddy -c "Print :$key" "$entitlements" >/dev/null 2>&1; then
      fail "$entitlements carries forbidden injection entitlement $key"
    fi
  done
}

[[ -d "$APP" ]] || fail "$APP is not installed"
[[ -d "$FP" ]] || fail "$FP is not embedded"
[[ -d "$WIDGET" ]] || fail "$WIDGET is not embedded"
[[ -x "$HOST" ]] || fail "$HOST is not executable"
codesign --verify --deep --strict "$APP" || fail "$APP failed deep signature verification"
assert_identity "$APP" com.yasyf.cc-pool.status
assert_identity "$FP" com.yasyf.cc-pool.status.fileprovider
assert_identity "$WIDGET" com.yasyf.cc-pool.status.widget

extract_entitlements "$APP" "$WORK/host.plist"
extract_entitlements "$FP" "$WORK/fp.plist"
extract_entitlements "$WIDGET" "$WORK/widget.plist"
assert_one_group "$WORK/host.plist"
assert_one_group "$WORK/fp.plist"
assert_no_injection_entitlements "$WORK/host.plist"
assert_no_injection_entitlements "$WORK/fp.plist"
assert_no_injection_entitlements "$WORK/widget.plist"
[[ "$(/usr/libexec/PlistBuddy -c 'Print :com.apple.security.app-sandbox' "$WORK/fp.plist")" == true ]] \
  || fail "$FP is not sandboxed"
FP_ENTITLEMENTS="$(plutil -p "$WORK/fp.plist")"
if grep -q 'temporary-exception.files.home-relative-path' <<<"$FP_ENTITLEMENTS"; then
  fail "$FP has a direct home-directory exception"
fi
if grep -q 'fileprovider.testing-mode' <<<"$FP_ENTITLEMENTS"; then
  fail "$FP carries fileprovider.testing-mode"
fi
[[ "$(/usr/libexec/PlistBuddy -c 'Print :com.apple.security.app-sandbox' "$WORK/widget.plist")" == true ]] \
  || fail "$WIDGET is not sandboxed"
if /usr/libexec/PlistBuddy -c 'Print :com.apple.security.application-groups' "$WORK/widget.plist" >/dev/null 2>&1; then
  fail "$WIDGET claims an App Group instead of its one read-only status path"
fi
widget_status_path="$(/usr/libexec/PlistBuddy -c 'Print :com.apple.security.temporary-exception.files.home-relative-path.read-only:0' "$WORK/widget.plist" 2>/dev/null)" \
  || fail "$WIDGET has no read-only status path"
[[ "$widget_status_path" == '/.cc-pool/' ]] \
  || fail "$WIDGET read-only status path is $widget_status_path"
if /usr/libexec/PlistBuddy -c 'Print :com.apple.security.temporary-exception.files.home-relative-path.read-only:1' "$WORK/widget.plist" >/dev/null 2>&1; then
  fail "$WIDGET has more than one read-only home path"
fi
if /usr/libexec/PlistBuddy -c 'Print :com.apple.security.temporary-exception.files.home-relative-path.read-write' "$WORK/widget.plist" >/dev/null 2>&1; then
  fail "$WIDGET carries a read-write home exception"
fi

document_group="$(plutil -extract NSExtension.NSExtensionFileProviderDocumentGroup raw -o - "$FP/Contents/Info.plist")"
[[ "$document_group" == "$APP_GROUP" ]] \
  || fail "$FP document group is $document_group, not $APP_GROUP"
[[ "$(plutil -extract NSExtension.NSExtensionPointIdentifier raw -o - "$FP/Contents/Info.plist")" == com.apple.fileprovider-nonui ]] \
  || fail "$FP has the wrong extension point"

executable_count="$(find "$APP/Contents/MacOS" -type f -perm -111 | wc -l | tr -d '[:space:]')"
[[ "$executable_count" == 1 ]] \
  || fail "$APP contains $executable_count host/helper executables, expected exactly one"
lipo "$HOST" -verify_arch x86_64 arm64 \
  || fail "$HOST is not universal x86_64+arm64"
NM_OUTPUT="$(nm -gU "$HOST")"
for symbol in \
  _CCPoolFuseKitDispatchChild \
  _CCPoolFuseKitStart \
  _CCPoolFuseKitReady \
  _CCPoolFuseKitWait \
  _CCPoolFuseKitStop; do
  grep -q "${symbol}$" <<<"$NM_OUTPUT" \
    || fail "$HOST does not embed $symbol"
done

# A malformed recognized child contract must exit through Go before SwiftUI.
set +e
child_output="$("$HOST" fusekit-native-v1 2>&1)"
child_rc=$?
set -e
[[ "$child_rc" == 1 ]] \
  || fail "malformed native-child invocation exited $child_rc, expected 1"
grep -q 'native child failed' <<<"$child_output" \
  || fail "malformed native-child invocation did not reach the embedded Go dispatcher"

set +e
child_output="$("$HOST" --fusekit-source-task-child 2>&1)"
child_rc=$?
set -e
[[ "$child_rc" == 1 ]] \
  || fail "malformed source-task child invocation exited $child_rc, expected 1"
grep -q 'source task child' <<<"$child_output" \
  || fail "malformed source-task invocation did not reach the embedded Go dispatcher"

# Broker mode is owned by Swift before the embedded Go dispatcher.
set +e
child_output="$("$HOST" --fusekit-broker-child 2>&1)"
child_rc=$?
set -e
[[ "$child_rc" == 1 ]] \
  || fail "malformed broker-child invocation exited $child_rc, expected 1"
grep -q 'broker child failed' <<<"$child_output" \
  || fail "malformed broker-child invocation did not reach the Swift broker dispatcher"

# When supplied by the VM gate, the ordinary account daemon must carry no
# protected App Group capability or even a compiled Group Container path.
if [[ -n "${CLI_PATH:-}" ]]; then
  [[ -x "$CLI_PATH" ]] || fail "$CLI_PATH is not executable"
  daemon_entitlements="$(codesign -d --entitlements :- "$CLI_PATH" 2>/dev/null || true)"
  if grep -q 'application-groups' <<<"$daemon_entitlements"; then
    fail "$CLI_PATH carries an App Group entitlement"
  fi
  if grep -Fq "$APP_GROUP" < <(strings -a "$CLI_PATH"); then
    fail "$CLI_PATH embeds the App Group identifier"
  fi
  if grep -Fq 'Library/Group Containers' < <(strings -a "$CLI_PATH"); then
    fail "$CLI_PATH embeds a Group Container path"
  fi
fi

echo "signed topology verified: fixed host, File Provider, native child${CLI_PATH:+, and pure-Go boundary}"
