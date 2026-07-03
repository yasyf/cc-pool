# cc-pool Notification Center widget

A macOS WidgetKit widget that shows `ccp status` at a glance: per-account 5h/7d
usage bars, live-session counts, and flags (stale / rate-limited / exhausted /
overage), in the same order as the CLI's table.

<img src="../docs/assets/widget-medium.png" width="377"
     alt="The medium cc-pool widget, rendered from sample data">

## How it gets data

The daemon writes an atomic snapshot to `~/.cc-pool/status.json` after every
completed poll (~3 min) — same schema as `ccp status --json`. The sandboxed
widget extension reads that file via a read-only sandbox exception for
`~/.cc-pool/`; it never touches the socket, the database, or the Keychain.

The host app (`CCPoolStatus.app`) is a Dock-less agent whose only jobs are to
register the widget with the system, keep itself launching at login, and —
while running — watch `~/.cc-pool` and reload the widget when the snapshot
changes. Without the app running, the widget still works on WidgetKit's lazier
schedule. On first launch the app adds itself to Login Items; it registers
itself only once, so disabling it under System Settings → General → Login Items
sticks.

## Install

```sh
ccp widget
```

That installs the prebuilt app from the `cc-pool-status` Homebrew cask (the
release build is Developer ID signed, notarized, and stapled, so a normal
install passes Gatekeeper), launches it once so macOS discovers the widget, and
prints the enable steps:

Open Notification Center (click the menu-bar clock), scroll down →
**Edit Widgets** → search "cc-pool" → add the small, medium, or large widget. Desktop
widgets work too: right-click the desktop → Edit Widgets.

If the widget doesn't appear in the gallery: `killall NotificationCenter
chronod`, relaunch the app, and re-open the gallery.

To remove it: `brew uninstall --cask cc-pool-status`.

## Build from source (development)

Requires full Xcode (CommandLineTools alone cannot build app targets) and
[XcodeGen](https://github.com/yonaskolb/XcodeGen) (`brew install xcodegen`).

```sh
cd widget
xcodegen generate
# macOS caches widget metadata by bundle version — bump it every build or the
# gallery keeps a stale descriptor. Epoch seconds is unique per build (commit
# count, used in CI, wouldn't change across uncommitted rebuilds).
xcodebuild -project CCPoolStatus.xcodeproj -scheme CCPoolStatus \
  -configuration Release -derivedDataPath build build \
  CURRENT_PROJECT_VERSION=$(date +%s)
ditto build/Build/Products/Release/CCPoolStatus.app ~/Applications/CCPoolStatus.app
open ~/Applications/CCPoolStatus.app
```

`project.yml` is the source of truth; the `.xcodeproj` and everything under
`Generated/` are emitted by xcodegen and gitignored. To work in the Xcode UI:
`xcodegen generate && open CCPoolStatus.xcodeproj`.

## Regenerating the docs screenshot

`docs/assets/widget-medium.png` (used above and in the main README) is rendered
from the `PoolStatus.sample` fixture — never captured from a live widget — so
it's reproducible and shows no real account data:

```sh
./scripts/render-screenshot.sh
```

Re-run it whenever the widget UI changes. It needs only the Xcode toolchain
(`xcrun swiftc`), not xcodegen or xcodebuild.

## Signing

Release builds (CI) are **Developer ID signed, notarized, and stapled**, so the
cask installs and launches under Gatekeeper with no quarantine workaround — see
the widget-app step in `.github/workflows/release.yml`.

Local builds are ad-hoc signed (`CODE_SIGN_IDENTITY=-`), which is fine for a
hand-built copy (local builds never get the quarantine bit). If chronod refuses
to load the ad-hoc-signed widget, build with a free personal team instead:

```sh
xcodebuild -project CCPoolStatus.xcodeproj -scheme CCPoolStatus \
  -configuration Release -derivedDataPath build build \
  CODE_SIGN_STYLE=Automatic DEVELOPMENT_TEAM=<YOUR_TEAM_ID> -allowProvisioningUpdates
```

## Troubleshooting

- `codesign -d --entitlements - ~/Applications/CCPoolStatus.app/Contents/PlugIns/CCPoolStatusWidget.appex`
  must show `com.apple.security.app-sandbox` plus the
  `temporary-exception.files.home-relative-path.read-only` entry for `/.cc-pool/`.
- `log stream --predicate 'process == "chronod" OR process CONTAINS "CCPoolStatusWidget"'`
  shows widget discovery/launch errors.
- "daemon not running" in the widget → `ccp service install` (or
  `brew services start cc-pool`); "status unreadable" → version skew between
  the snapshot and the widget — rebuild the widget or update cc-pool, and check
  `ccp status --json`.
