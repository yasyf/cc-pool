#!/bin/sh
# Regenerate docs/assets/demo.png from a real `ccp --help` run.
# `ccp --help` is the demo on purpose: every richer surface (status, doctor,
# list) prints real account emails, which must never land in a committed image.
# Requires: freeze, pngquant.
set -eu
cd "$(dirname "$0")/../.."
freeze --execute "ccp --help" \
  --theme github-dark --background "#0d1117" --window --padding 24 --font.size 28 \
  --output docs/assets/demo.png
pngquant --force --quality 60-85 --output docs/assets/demo.png docs/assets/demo.png
