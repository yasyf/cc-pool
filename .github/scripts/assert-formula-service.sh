#!/usr/bin/env bash
set -euo pipefail

formula="${1:-.github/formula/cc-pool.rb.tmpl}"

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
