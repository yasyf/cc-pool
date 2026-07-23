#!/usr/bin/env bash
# scripts/vm/push.sh — the cc-pool-specific seam of the VM harness.
#
# Host-builds the cc-pool binary (pure Go) and the Developer ID-signed fixed
# CCPoolStatus.app, installs
# both into the guest, registers + enables the File Provider extension, and
# proves the install with an in-guest selftest. The app lands at the production
# cask path (/Applications/CCPoolStatus.app) so the FP broker/holder identity
# resolves unmodified in the guest.
#
# The FP appex MUST be Developer ID-signed: an ad-hoc signature will not register
# with pluginkit, so the installed extension gate would never see '+'. push
# discovers a "Developer ID Application" identity on the host (team SXKCTF23Q2)
# and STOPS with guidance if none is usable — it never ships an ad-hoc appex.
# The cc-pool binary itself stays pure Go and never accesses the App Group.
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

  # The App Group is Team-ID-prefixed and owned only by the fixed signed app.
  local want_group="${sign_team}.ccp" paths_group
  paths_group="$(sed -n 's/^[[:space:]]*static let appGroupIdentifier = "\(.*\)"$/\1/p' "$REPO_ROOT/widget/Sources/FileProviderRuntime/Configuration.swift")"
  [[ "$paths_group" == "$want_group" ]] ||
    die "App Group mismatch: signing team yields $want_group but the fixed app pins $paths_group"

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

  # --- cc-pool account daemon and CLI (pure Go, no App Group access) -----------
  log "building cc-pool (pure Go, arm64, BUILD_REV=$rev)"
  (cd "$REPO_ROOT" && GOWORK=off CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -o "$stage/bin/cc-pool" ./cmd/cc-pool)
  # ccp is cc-pool's user-facing symlink.
  ln -sf cc-pool "$stage/bin/ccp"
  printf '%s\n' "$rev" >"$stage/BUILD_REV"

  # --- One fixed CCPoolStatus.app identity (holder + broker + extensions) ------
  local app_version="${VMCTL_APP_VERSION:-0.0.0}" dd="$stage/widget-dd"
  [[ "$app_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
    die "VMCTL_APP_VERSION must be dotted numeric MAJOR.MINOR.PATCH (got $app_version)"
  log "building CCPoolStatus.app v$app_version (Developer ID) with $(xcodebuild -version | head -n1)"
  (
    cd "$REPO_ROOT/widget"
    xcodegen generate
    # Mirrors release.yml's widget build. CODE_SIGN_INJECT_BASE_ENTITLEMENTS=NO
    # keeps a plain `build` from injecting get-task-allow; ENABLE_HARDENED_RUNTIME
    # + --timestamp match the shipped cask payload. VMCTL_APP_VERSION lets the
    # upgrade gate change the signed payload without changing its identity.
    xcodebuild -project CCPoolStatus.xcodeproj -scheme CCPoolStatus \
      -configuration Release -derivedDataPath "$dd" build \
      MARKETING_VERSION="$app_version" \
      CURRENT_PROJECT_VERSION="$(git -C "$REPO_ROOT" rev-list --count HEAD)" \
      CODE_SIGN_STYLE=Manual \
      CODE_SIGN_IDENTITY="$sign_id" \
      DEVELOPMENT_TEAM="$sign_team" \
      ENABLE_HARDENED_RUNTIME=YES \
      CODE_SIGN_INJECT_BASE_ENTITLEMENTS=NO \
      OTHER_CODE_SIGN_FLAGS="--timestamp"
  )
  local app="$dd/Build/Products/Release/CCPoolStatus.app"
  [[ -d "$app" ]] || die "status app build did not produce $app"

  # Assert the FP appex is present, validly signed, and App-Group-bound — the
  # same drift checks release.yml runs, so a broken build fails here not in-guest.
  local fp_appex="$app/Contents/PlugIns/CCPoolFileProvider.appex"
  [[ -d "$fp_appex" ]] || die "built app is missing CCPoolFileProvider.appex"
  codesign --verify --deep --strict --verbose=2 "$app" || die "codesign verify failed for $app"
  local fp_ents
  fp_ents="$(codesign -d --entitlements - "$fp_appex" 2>&1)"
  grep -q "com.apple.security.application-groups" <<<"$fp_ents" ||
    die "FP appex lacks the App Group entitlement — the signed broker would never reach it"
  if grep -q "temporary-exception.files.home-relative-path" <<<"$fp_ents"; then
    die "FP appex directly accesses the home directory instead of the signed broker"
  fi
  if grep -q "fileprovider.testing-mode" <<<"$fp_ents"; then
    die "FP appex carries fileprovider.testing-mode (restricted) — it would force a provisioning profile"
  fi

  # --- Install into the guest --------------------------------------------------
  # A stale process would keep serving the old exact protocol; stop the hard-cut
  # stack before replacing either executable.
  log "stopping any running guest daemon / signed app"
  # shellcheck disable=SC2029
  vm_ssh "pkill -x cc-pool 2>/dev/null; osascript -e 'quit app \"CCPoolStatus\"' 2>/dev/null; \
    for _ in \$(seq 1 60); do pgrep -x CCPoolStatus >/dev/null || exit 0; sleep 0.5; done; \
    pkill -TERM -x CCPoolStatus 2>/dev/null; \
    for _ in \$(seq 1 10); do pgrep -x CCPoolStatus >/dev/null || exit 0; sleep 0.5; done; \
    pkill -KILL -x CCPoolStatus 2>/dev/null; true"

  log "installing cc-pool into the guest: $VMCTL_GUEST_DIR"
  # shellcheck disable=SC2029
  vm_ssh "rm -rf '$VMCTL_GUEST_DIR' && mkdir -p '$VMCTL_GUEST_BIN'"
  # tar over ssh preserves the ccp symlink and perms in one trip.
  # shellcheck disable=SC2029
  tar -C "$stage" -cf - bin BUILD_REV | vm_ssh "tar -xf - -C '$VMCTL_GUEST_DIR'"

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
    warn "FP extension not enabled headlessly — the selftest below confirms; if it fails, use the VMCTL_GRAPHICS=1 GUI path (README: File Provider provisioning)"

  # Grant fileproviderd's per-provider consent (a SEPARATE gate from pluginkit): the
  # base image defaults providers to user-disabled. Fatal on failure: a disabled
  # provider makes every File Provider scenario meaningless, so fail loud here.
  fp_grant_consent

  # --- Selftest ----------------------------------------------------------------
  # Verify the extension the same way the enable step and VM scenarios do:
  # `pluginkit -m -i <bundleid>` must lead with '+'. Do NOT `pluginkit -m | grep
  # ccpool` — the bundle id is com.yasyf.cc-pool.status.fileprovider ("cc-pool"
  # with a HYPHEN), so a hyphen-less pattern never matches even when the plugin
  # is registered and enabled. Poll briefly: pluginkit registration can lag a
  # beat behind the enable.
  log "guest selftest: cc-pool binary + File Provider extension enabled"
  # shellcheck disable=SC2029
  vm_ssh "'$VMCTL_GUEST_CCP' --version" || die "ccp --version failed in the guest"
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
    die "pluginkit shows no $VMCTL_FP_BUNDLE_ID extension — the app did not register (ad-hoc signature? missing LaunchServices scan?); see README: File Provider provisioning"
  fi
  die "FP extension $VMCTL_FP_BUNDLE_ID is registered but NOT enabled ('$fp_line') — File Provider scenarios cannot run.
Enable headlessly: scripts/vm/vmctl shell \"pluginkit -e use -i $VMCTL_FP_BUNDLE_ID\"
Or boot VMCTL_GRAPHICS=1 and flip the Settings File-Provider toggle (README: File Provider provisioning)"
}

main "$@"
