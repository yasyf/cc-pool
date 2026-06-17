# Homebrew cask for the cc-pool Notification Center widget (CCPoolStatus.app).
#
# Install with `ccp widget` — it disables quarantine, which this app needs:
# it is ad-hoc signed (no Developer ID), so a quarantined copy is blocked by
# Gatekeeper. Installing the cask by hand works too (Homebrew 5 dropped the
# --no-quarantine install flag; only the env-var spelling remains):
#
#   HOMEBREW_CASK_OPTS=--no-quarantine brew install --cask yasyf/cc-pool/cc-pool-status
#
# release.yml's bump-formula job rewrites the version line and the `# app`
# sha256 on every tagged release — keep the marker, never hand-edit them.
cask "cc-pool-status" do
  version "0.28.1"
  sha256 "d8361d0717b240b32851697af0ed0744decb58fb4a1fb8caca594d22ca8b30ad" # app

  url "https://github.com/yasyf/cc-pool/releases/download/v#{version}/cc-pool-status-v#{version}-darwin.zip"
  name "cc-pool Status"
  desc "Notification Center widget showing cc-pool account status"
  homepage "https://github.com/yasyf/cc-pool"

  depends_on macos: :sonoma # parsed with a ">=" comparator: sonoma or newer
  depends_on formula: "cc-pool"

  app "CCPoolStatus.app"

  uninstall quit: "com.yasyf.cc-pool.status"

  zap trash: [
    "~/Library/Containers/com.yasyf.cc-pool.status.widget",
  ]

  caveats <<~EOS
    Launch the app once so macOS discovers the widget:
      open -ga CCPoolStatus
    Then: Notification Center (click the menu-bar clock) → Edit Widgets → "cc-pool".

    Installed without `--no-quarantine`? Gatekeeper will block the ad-hoc-signed
    app — reinstall via `ccp widget`, which handles it.
  EOS
end
