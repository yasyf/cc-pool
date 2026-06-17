#!/bin/sh
# Regenerates docs/assets/widget-medium.png from PoolStatus.sample — never from
# a live widget, so it is reproducible and shows no real account data. Needs
# only the Xcode toolchain (xcrun swiftc); no xcodegen / xcodebuild.
set -eu
cd "$(dirname "$0")/.."

build=$(mktemp -d)
trap 'rm -rf "$build"' EXIT

# Excludes CCPoolWidget.swift (its @main collides with the renderer's) and
# Previews.swift (references CCPoolStatusWidget). Provider.swift is in because
# MoodWashBackground and the canvas use its StatusEntry.State.
xcrun swiftc -O -parse-as-library \
  -target "$(uname -m)-apple-macos14.0" \
  Sources/Shared/Status.swift \
  Sources/Widget/Theme.swift \
  Sources/Widget/Critter.swift \
  Sources/Widget/Provider.swift \
  Sources/Widget/Views.swift \
  scripts/render-screenshot.swift \
  -o "$build/render-screenshot"

TZ=UTC "$build/render-screenshot" "$PWD/../docs/assets/widget-medium.png"
echo "wrote docs/assets/widget-medium.png"
