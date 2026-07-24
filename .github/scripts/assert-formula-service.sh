#!/usr/bin/env bash
set -euo pipefail

formula="${1:-.github/formula/cc-pool.rb.tmpl}"
expected_version="${2:-}"

if grep -Eq '^[[:space:]]*service do|brew services|homebrew\.mxcl' "$formula"; then
  echo "$formula must not define or advertise a Homebrew-managed daemon service" >&2
  exit 1
fi

grep -Fq 'ccp service install' "$formula" || {
  echo "$formula must direct users to the daemonkit-owned service installer" >&2
  exit 1
}

grep -Fq 'ccp package install' "$formula" || {
  echo "$formula must direct users to the explicit signed-application installer" >&2
  exit 1
}

if grep -Eq '^[[:space:]]*head do|install_from_source' "$formula"; then
  echo "$formula must not offer an unsigned source-only installation" >&2
  exit 1
fi

first_resource="$(
  awk '/^[[:space:]]*resource / { print NR; exit }' "$formula"
)"
last_dependency="$(
  awk '/^[[:space:]]*depends_on / { line = NR } END { if (line) print line }' "$formula"
)"
if [[ -z "$first_resource" || -z "$last_dependency" || "$last_dependency" -ge "$first_resource" ]]; then
  echo "$formula must place every depends_on declaration before every resource" >&2
  exit 1
fi

resource_url="$(
  awk '
    /^[[:space:]]*resource "status_app" do/ { in_resource = 1; next }
    in_resource && $1 == "url" {
      line = $0
      sub(/^[[:space:]]*url "/, "", line)
      sub(/".*$/, "", line)
      print line
      exit
    }
  ' "$formula"
)"
if [[ -n "$expected_version" ]]; then
  expected_resource_url="https://github.com/yasyf/cc-pool/releases/download/v${expected_version}/cc-pool-status-v${expected_version}-darwin.zip"
else
  expected_resource_url="https://github.com/yasyf/cc-pool/releases/download/v__VERSION__/cc-pool-status-v__VERSION__-darwin.zip"
fi
if [[ "$resource_url" != "$expected_resource_url" ]]; then
  echo "$formula status_app URL = $resource_url, want $expected_resource_url" >&2
  exit 1
fi
