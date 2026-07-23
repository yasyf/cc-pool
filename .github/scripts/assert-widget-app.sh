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

# The fixed signed host and its extension own the App Group boundary. The Go
# account daemon must never resolve or traverse it.
APP_GROUP="$(sed -n 's/^[[:space:]]*static let appGroupIdentifier = "\(.*\)"$/\1/p' widget/Sources/FileProviderRuntime/Configuration.swift)"
test -n "$APP_GROUP"
test "$APP_GROUP" = "${TEAM_ID}.ccp"

FP_APPEX="$APP/Contents/PlugIns/CCPoolFileProvider.appex"
WIDGET_APPEX="$APP/Contents/PlugIns/CCPoolStatusWidget.appex"
test -d "$FP_APPEX"
test -d "$WIDGET_APPEX"

# Product identity, exact App Group, hardened runtime, and single Mach-O share
# one gate with the VM scenario.
APP_PATH="$APP" TEAM_ID="$TEAM_ID" APP_GROUP="$APP_GROUP" \
  bash scripts/assert-signed-topology.sh

# Widget appex: sandbox + ~/.cc-pool read-only exception, or it silently can't
# read status.json.
codesign -d --entitlements - "$WIDGET_APPEX" 2>&1 |
  grep -q "temporary-exception.files.home-relative-path.read-only"

# The version stamp must reach both bundles — a silent 1/1.0 re-ships the stale
# gallery descriptor. The appex is what chronod inspects.
test "$(plutil -extract CFBundleVersion raw "$APP/Contents/Info.plist")" != 1
test "$(plutil -extract CFBundleVersion raw "$WIDGET_APPEX/Contents/Info.plist")" != 1
