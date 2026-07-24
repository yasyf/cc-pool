#!/bin/sh
# Install cc-pool's formula-bound CLI and signed helper package on macOS.
# Homebrew alone delivers the exact release resources into one Cellar prefix;
# daemonkit alone publishes the helper at its stable per-user path.
#
# Usage:
#   install.sh
#   curl -fsSL https://raw.githubusercontent.com/yasyf/cc-pool/main/scripts/install.sh | sh
set -eu

VERSION="${1:-latest}"

if [ "$(uname -s)" != "Darwin" ]; then
  echo "cc-pool: macOS installation requires Homebrew" >&2
  exit 1
fi
if [ "$VERSION" != "latest" ]; then
  echo "cc-pool: macOS installs only the latest signed formula release" >&2
  exit 1
fi
if ! command -v brew >/dev/null 2>&1; then
  echo "cc-pool: Homebrew is required on macOS" >&2
  exit 1
fi
if ! brew install yasyf/tap/cc-pool >/dev/null 2>&1 || ! command -v ccp >/dev/null 2>&1; then
  echo "cc-pool: Homebrew could not install yasyf/tap/cc-pool" >&2
  exit 1
fi

ccp package install
echo "cc-pool: installed via Homebrew ($(ccp --version))" >&2
