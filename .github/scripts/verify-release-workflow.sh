#!/usr/bin/env bash
set -euo pipefail

workflow=.github/workflows/release.yml
stage_pin=e4c3108e693681df1a3c666bae80e890bc44cf3e
draft_pin=54e3e194bda69896894a82c17fcdb2822beefab5
tap_pin=9ca67392d45d66b6ae01e262383c8f3138d56f5e

if grep -Eq 'yasyf/homebrew-tap/.+@(main|v[0-9]+|swift-v[0-9]+)' "$workflow"; then
  echo "homebrew-tap release actions must use an exact commit" >&2
  exit 1
fi
test "$(grep -Ec "actions/stage-draft-release@${stage_pin}$" "$workflow")" = 1
test "$(grep -Ec "actions/publish-draft-release@${draft_pin}$" "$workflow")" = 1
test "$(grep -Ec "actions/publish@${tap_pin}$" "$workflow")" = 1
test "$(grep -Fxc "          release-id: \${{ steps.draft.outputs['release-id'] }}" "$workflow")" = 1
test "$(grep -Fxc "      release_id: \${{ steps.draft.outputs['release-id'] }}" "$workflow")" = 1
test "$(grep -Fxc '          RELEASE_ID: ${{ needs.release.outputs.release_id }}' "$workflow")" = 1

if grep -Eq '/releases/tags/|gh release (create|view|upload|download|edit)' "$workflow"; then
  echo "release publication must retain one exact numeric release ID" >&2
  exit 1
fi
if grep -Eq -- '--(help|version)[^|]*\|[[:space:]]*grep[[:space:]]+-[EF]*q' "$workflow"; then
  echo "release assertions must capture complete command output before matching" >&2
  exit 1
fi

for required in \
  'name: Stage and verify the complete draft release' \
  'name: Publish the verified release' \
  'publish-tap:' \
  'name: Download the final published release bytes' \
  'name: Audit and install the exact rendered formula' \
  'name: Publish the CLI formula to the tap'; do
  grep -Fq "$required" "$workflow"
done

test "$(awk '/^  publish-tap:$/ { getline; print; exit }' "$workflow")" = \
  '    needs: [release, release-app]'
test "$(grep -Fxc '          [ "$(jq -r '\''.draft'\'' <<< "$release")" = false ]' "$workflow")" = 1
test "$(grep -Fxc "          ruby -c \"\$FORMULA\"" "$workflow")" = 1
test "$(grep -Fxc '          audit_tap=cc-pool/release-audit' "$workflow")" = 1
test "$(grep -Fxc "          brew audit --strict --formula \"\$audit_tap/cc-pool\"" "$workflow")" = 1
test "$(grep -Fxc "          HOMEBREW_NO_AUTO_UPDATE=1 brew install --formula \"\$audit_tap/cc-pool\"" "$workflow")" = 1
test "$(grep -Fxc "          brew test \"\$audit_tap/cc-pool\"" "$workflow")" = 1

line() { grep -Fn "$1" "$workflow" | cut -d: -f1; }
stage="$(line 'name: Stage and verify the complete draft release')"
publish="$(line 'name: Publish the verified release')"
tap_job="$(line 'publish-tap:')"
download="$(line 'name: Download the final published release bytes')"
render="$(line 'name: Render the formula into the atomic tap transaction')"
audit="$(line 'name: Audit and install the exact rendered formula')"
tap="$(line 'name: Publish the CLI formula to the tap')"
test "$stage" -lt "$publish"
test "$publish" -lt "$tap_job"
test "$tap_job" -lt "$download"
test "$download" -lt "$render"
test "$render" -lt "$audit"
test "$audit" -lt "$tap"
