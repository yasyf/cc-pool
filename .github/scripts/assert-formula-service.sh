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
