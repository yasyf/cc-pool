#!/usr/bin/env bash
# scripts/vm/push.sh — the cc-pool-specific seam of the VM harness.
#
# Host-builds the cc-pool binary (pure Go) and the Developer ID-signed
# CCPoolStatus.app (the File-Provider-enabled CCPoolStatusFP flavor), installs
# both into the guest, registers + enables the File Provider extension, and
# proves the install with an in-guest selftest. The app lands at the production
# cask path (/Applications/CCPoolStatus.app) so pool.WidgetAppPath() and the FP
# control/bridge sockets resolve unmodified in the guest.
#
# The FP appex MUST be Developer ID-signed: an ad-hoc signature will not register
# with pluginkit, so cc-pool's File Provider gate would never see '+'. push
# discovers a "Developer ID Application" identity on the host (team SXKCTF23Q2)
# and STOPS with guidance if none is usable — it never ships an ad-hoc appex.
# The cc-pool binary itself stays pure-Go/ad-hoc (its App-Group access is gated
# by TCC, not by a Developer ID signature).
#
# BUILD_REV (env-overridable; defaults to the repo short HEAD, "-dirty" when the
# tree is unclean) is recorded in the guest and host state so `vmctl run` can
# prove in meta.json which build a verdict applies to.
#
# Adapted from fusekit/scripts/vm/push.sh (see scripts/vm/README.md § Provenance).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

main() {
  require_cmd go
  require_cmd git
  require_cmd codesign
  require_cmd xcodebuild "install Xcode 26 — the widget SDK-skew constraint"
  require_cmd xcodegen "brew install xcodegen"
  vm_require_tart
  vm_ensure_dirs
  vm_ensure_running 600 || die "VM unreachable — run vmctl provision first"
  vm_assert_guest

  local sign_id sign_team
  IFS=$'\t' read -r sign_id sign_team < <(vm_discover_signing)
  log "signing CCPoolStatus.app with Developer ID $sign_id (team $sign_team)"

  # The App Group is Team-ID-prefixed; assert the appex this build will emit
  # matches the group the Go daemon compiled in (internal/pool/paths.go), or the
  # daemon and the extension would never share a container.
  local want_group="${sign_team}.ccp" paths_group
  paths_group="$(sed -n 's/^const AppGroupID = "\(.*\)"$/\1/p' "$REPO_ROOT/internal/pool/paths.go")"
  [[ "$paths_group" == "$want_group" ]] ||
    die "App Group mismatch: signing team yields $want_group but internal/pool/paths.go pins $paths_group"

  local rev
  if [[ -n "${BUILD_REV:-}" ]]; then
    rev="$BUILD_REV"
  else
    rev="$(git -C "$REPO_ROOT" rev-parse --short HEAD)"
    [[ -n "$(git -C "$REPO_ROOT" status --porcelain)" ]] && rev="$rev-dirty"
  fi

  local stage="$VM_ROOT/stage"
  rm -rf "$stage"
  mkdir -p "$stage/bin"

  # --- cc-pool binary (pure Go: symlink + File Provider overlay, no cgo) -------
  local ldflags="-X github.com/yasyf/fusekit/version.Version=vm-$rev -X github.com/yasyf/fusekit/version.Commit=$rev"
  log "building cc-pool (pure Go, arm64, BUILD_REV=$rev)"
  (cd "$REPO_ROOT" && CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags "$ldflags" -o "$stage/bin/cc-pool" ./cmd/cc-pool)
  # ccp is cc-pool's user-facing symlink; the incident command is `ccp migrate`.
  ln -sf cc-pool "$stage/bin/ccp"
  printf '%s\n' "$rev" >"$stage/BUILD_REV"

  # --- CCPoolDaemon.app: the daemon's durable TCC identity (release.yml parity) ---
  local daemon_mode="unprofiled"
  [[ -n "${VMCTL_PROFILE_DAEMON:-}" ]] && daemon_mode="profiled"
  # Dotted-numeric CFBundleShortVersionString; the commit count is monotonic.
  local daemon_ver
  daemon_ver="0.0.$(git -C "$REPO_ROOT" rev-list --count HEAD)"
  log "assembling $VMCTL_DAEMON_BUNDLE_ID bundle (mode=$daemon_mode, version=$daemon_ver)"
  vm_build_daemon_bundle "$stage/bin/cc-pool" "$stage/CCPoolDaemon.app" "$sign_id" "$daemon_ver" "$daemon_mode"

  # --- CCPoolStatus.app (CCPoolStatusFP flavor, Developer ID signed) ----------
  log "building CCPoolStatus.app (CCPoolStatusFP, Developer ID) with $(xcodebuild -version | head -n1)"
  local dd="$stage/widget-dd"
  (
    cd "$REPO_ROOT/widget"
    xcodegen generate
    # Mirrors release.yml's widget build. CODE_SIGN_INJECT_BASE_ENTITLEMENTS=NO
    # keeps a plain `build` from injecting get-task-allow; ENABLE_HARDENED_RUNTIME
    # + --timestamp match the shipped cask payload. The version stamp is cosmetic
    # here (the VM never inspects widget tiles), so it is fixed and numeric.
    xcodebuild -project CCPoolStatus.xcodeproj -scheme CCPoolStatusFP \
      -configuration Release -derivedDataPath "$dd" build \
      MARKETING_VERSION="0.0.0" \
      CURRENT_PROJECT_VERSION="$(git -C "$REPO_ROOT" rev-list --count HEAD)" \
      CODE_SIGN_STYLE=Manual \
      CODE_SIGN_IDENTITY="$sign_id" \
      DEVELOPMENT_TEAM="$sign_team" \
      ENABLE_HARDENED_RUNTIME=YES \
      CODE_SIGN_INJECT_BASE_ENTITLEMENTS=NO \
      OTHER_CODE_SIGN_FLAGS="--timestamp"
  )
  local app="$dd/Build/Products/Release/CCPoolStatus.app"
  [[ -d "$app" ]] || die "widget build did not produce $app (wrong scheme? need CCPoolStatusFP)"

  # Assert the FP appex is present, validly signed, and App-Group-bound — the
  # same drift checks release.yml runs, so a broken build fails here not in-guest.
  local fp_appex="$app/Contents/PlugIns/CCPoolFileProvider.appex"
  [[ -d "$fp_appex" ]] || die "built app is missing CCPoolFileProvider.appex — wrong scheme? (need CCPoolStatusFP)"
  codesign --verify --deep --strict --verbose=2 "$app" || die "codesign verify failed for $app"
  local fp_ents
  fp_ents="$(codesign -d --entitlements - "$fp_appex" 2>&1)"
  printf '%s' "$fp_ents" | grep -q "com.apple.security.application-groups" ||
    die "FP appex lacks the App Group entitlement — the daemon bridge would never reach it"
  if printf '%s' "$fp_ents" | grep -q "fileprovider.testing-mode"; then
    die "FP appex carries fileprovider.testing-mode (restricted) — it would force a provisioning profile"
  fi

  # --- Install into the guest --------------------------------------------------
  # A stale app or daemon would keep serving the OLD build; stop them first.
  log "stopping any running guest daemon / companion app"
  # shellcheck disable=SC2029
  vm_ssh "pkill -x cc-pool 2>/dev/null; osascript -e 'quit app \"CCPoolStatus\"' 2>/dev/null; pkill -f CCPoolStatus 2>/dev/null; true"

  log "installing cc-pool into the guest: $VMCTL_GUEST_DIR"
  # shellcheck disable=SC2029
  vm_ssh "rm -rf '$VMCTL_GUEST_DIR' && mkdir -p '$VMCTL_GUEST_BIN'"
  # tar over ssh preserves the ccp symlink and perms in one trip.
  # shellcheck disable=SC2029
  tar -C "$stage" -cf - bin BUILD_REV | vm_ssh "tar -xf - -C '$VMCTL_GUEST_DIR'"

  log "installing $VMCTL_GUEST_DAEMON_APP (mode=$daemon_mode)"
  vm_install_daemon_bundle "$stage/CCPoolDaemon.app" "$VMCTL_GUEST_DAEMON_APP"

  log "installing $VMCTL_GUEST_APP"
  # shellcheck disable=SC2029
  vm_ssh "rm -rf '$VMCTL_GUEST_APP'"
  # /Applications is admin-group writable; tar preserves the .app structure,
  # perms, and the embedded code signature (signatures live in the bundle, not
  # in xattrs, so plain tar keeps them intact).
  tar -C "$(dirname "$app")" -cf - "$(basename "$app")" | vm_ssh "tar -xf - -C /Applications"
  printf '%s\n' "$rev" >"$VM_STATE_DIR/build-rev"

  # Register + enable the FP extension now that the app is in place.
  fp_register_and_enable ||
    warn "FP extension not enabled headlessly — the selftest below confirms; if it fails, use the VMCTL_GRAPHICS=1 click-Allow path (README: FP provisioning)"

  # Grant fileproviderd's per-provider consent (a SEPARATE gate from pluginkit): the
  # base image defaults providers to user-disabled, which makes the replay's migrate
  # capability gate refuse. Fatal on failure — a disabled provider silently fails the
  # replay downstream, so fail loud here (fp_grant_consent dies unless the read-back
  # confirms Enabled=true).
  fp_grant_consent

  # --- Selftest ----------------------------------------------------------------
  # Verify the extension the SAME way the enable step and cc-pool's FP gate do:
  # `pluginkit -m -i <bundleid>` must lead with '+'. Do NOT `pluginkit -m | grep
  # ccpool` — the bundle id is com.yasyf.cc-pool.status.fileprovider ("cc-pool"
  # with a HYPHEN), so a hyphen-less pattern never matches even when the plugin
  # is registered and enabled. Poll briefly: pluginkit registration can lag a
  # beat behind the enable.
  log "guest selftest: cc-pool binary + File Provider extension enabled"
  # shellcheck disable=SC2029
  vm_ssh "'$VMCTL_GUEST_CCP' --version" || die "ccp --version failed in the guest"
  # The daemon bundle exe must run and carry the app-group entitlement in-guest.
  # shellcheck disable=SC2029
  vm_ssh "'$VMCTL_GUEST_DAEMON_EXE' --version" || die "$VMCTL_GUEST_DAEMON_EXE --version failed — the CCPoolDaemon.app bundle did not install/run"
  # shellcheck disable=SC2029
  vm_ssh "codesign -d --entitlements - '$VMCTL_GUEST_DAEMON_APP' 2>&1" | grep -q "$(vm_app_group)" ||
    die "installed $VMCTL_GUEST_DAEMON_APP is missing the app-group entitlement (signature stripped in transit?)"
  local fp_line="" t0
  t0="$(date +%s)"
  while (($(date +%s) - t0 < 15)); do
    # shellcheck disable=SC2029
    fp_line="$(vm_ssh "pluginkit -m -i '$VMCTL_FP_BUNDLE_ID' 2>/dev/null" | head -n1)"
    [[ "$fp_line" == +* ]] && break
    sleep 2
  done
  if [[ "$fp_line" == +* ]]; then
    log "pushed BUILD_REV=$rev (FP extension enabled: $fp_line)"
    return 0
  fi
  if [[ -z "$fp_line" ]]; then
    die "pluginkit shows no $VMCTL_FP_BUNDLE_ID extension — the app did not register (ad-hoc signature? missing LaunchServices scan?); see README: FP provisioning"
  fi
  die "FP extension $VMCTL_FP_BUNDLE_ID is registered but NOT enabled ('$fp_line') — the replay's FP gate will refuse.
Enable headlessly: scripts/vm/vmctl shell \"pluginkit -e use -i $VMCTL_FP_BUNDLE_ID\"
Or boot VMCTL_GRAPHICS=1 and flip the Settings File-Provider toggle (README: FP provisioning)"
}

main "$@"
