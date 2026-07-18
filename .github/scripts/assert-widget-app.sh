#!/usr/bin/env bash
# Bespoke release gate for CCPoolStatus, run by yasyf/homebrew-tap release-app.yml
# against the built (not yet notarized) .app. Enforces the cc-pool-only invariants.
set -euo pipefail

# Callee contract, from the cc-pool checkout root: APP_PATH is the built .app,
# TEAM_ID the signing Team ID exported by import-developer-id.
: "${APP_PATH:?APP_PATH (the built .app) must be set}"
: "${TEAM_ID:?TEAM_ID must be exported by import-developer-id}"
APP="$APP_PATH"
test -d "$APP" || { echo "::error::APP_PATH '$APP' is not a bundle"; exit 1; }

# The App Group must equal the Go constant compiled into the daemon; its prefix
# must be the signing Team ID. Drift from paths.go fails the release.
APP_GROUP="$(sed -n 's/^const AppGroupID = "\(.*\)"$/\1/p' internal/pool/paths.go)"
test -n "$APP_GROUP"
test "$APP_GROUP" = "${TEAM_ID}.ccp"

FP_APPEX="$APP/Contents/PlugIns/CCPoolFileProvider.appex"
WIDGET_APPEX="$APP/Contents/PlugIns/CCPoolStatusWidget.appex"
test -d "$FP_APPEX"
test -d "$WIDGET_APPEX"

# Host app and FP appex must claim exactly the Go constant's group.
for bundle in "$APP" "$FP_APPEX"; do
  codesign -d --entitlements - "$bundle" 2>&1 | grep -q "$APP_GROUP"
done

# Widget appex: sandbox + ~/.cc-pool read-only exception, or it silently can't
# read status.json.
codesign -d --entitlements - "$WIDGET_APPEX" 2>&1 |
  grep -q "temporary-exception.files.home-relative-path.read-only"

# The version stamp must reach both bundles — a silent 1/1.0 re-ships the stale
# gallery descriptor. The appex is what chronod inspects.
test "$(plutil -extract CFBundleVersion raw "$APP/Contents/Info.plist")" != 1
test "$(plutil -extract CFBundleVersion raw "$WIDGET_APPEX/Contents/Info.plist")" != 1

# FP appex: group-bound + home-relative read-write exception, and no
# fileprovider.testing-mode (restricted; our profiles do not authorize it).
FP_ENTS="$(codesign -d --entitlements - "$FP_APPEX" 2>&1)"
echo "$FP_ENTS" | grep -q "com.apple.security.application-groups"
echo "$FP_ENTS" | grep -q "temporary-exception.files.home-relative-path.read-write"
if echo "$FP_ENTS" | grep -q "fileprovider.testing-mode"; then
  echo "::error::CCPoolFileProvider.appex carries fileprovider.testing-mode"
  exit 1
fi

# $(TeamIdentifierPrefix) expands only under a real DEVELOPMENT_TEAM; assert the
# expanded NSExtension values in the built plist.
FP_PLIST="$FP_APPEX/Contents/Info.plist"
test "$(plutil -extract NSExtension.NSExtensionPointIdentifier raw "$FP_PLIST")" = "com.apple.fileprovider-nonui"
test "$(plutil -extract NSExtension.NSExtensionFileProviderDocumentGroup raw "$FP_PLIST")" = "$APP_GROUP"
