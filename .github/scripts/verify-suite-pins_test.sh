#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
check="$root/.github/scripts/verify-suite-pins.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

write_go_mod() {
  cat > "$work/go.mod" <<EOF
module example.test/suite

go 1.26

require (
	github.com/yasyf/daemonkit $1
	github.com/yasyf/fusekit $2
)
EOF
}

write_project() {
  cat > "$work/project.yml" <<EOF
packages:
  FuseKit:
    url: https://github.com/yasyf/fusekit.git
    $1: $2
  DaemonKit:
    url: https://github.com/yasyf/daemonkit.git
    $3: $4
EOF
}

write_go_mod v0.17.2 v1.13.2
write_project exactVersion 1.13.2 exactVersion 0.17.2
"$check" "$work/go.mod" "$work/project.yml"

write_go_mod v0.17.2 v1.13.3-0.20260724123456-0123456789ab
write_project revision 0123456789ab exactVersion 0.17.2
"$check" "$work/go.mod" "$work/project.yml"

write_project exactVersion 1.13.2 exactVersion 0.15.0
if "$check" "$work/go.mod" "$work/project.yml" >/dev/null 2>&1; then
  echo "suite pins: accepted Go/Swift version skew" >&2
  exit 1
fi

write_project revision 0123456789ab from 0.17.2
if "$check" "$work/go.mod" "$work/project.yml" >/dev/null 2>&1; then
  echo "suite pins: accepted a floating Swift selector" >&2
  exit 1
fi
