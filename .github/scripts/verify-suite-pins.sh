#!/usr/bin/env bash
set -euo pipefail

go_mod="${1:-go.mod}"
project="${2:-widget/project.yml}"

fail() {
  echo "suite pins: $*" >&2
  exit 1
}

go_version() {
  local module="$1"
  awk -v module="$module" '
    $1 == module {
      count++
      version = $2
      indirect = ($3 == "//" && $4 == "indirect")
    }
    END {
      if (count != 1 || indirect) exit 1
      print version
    }
  ' "$go_mod" || fail "$module must have one direct Go requirement"
}

swift_selector() {
  local package="$1"
  awk -v package="  ${package}:" '
    $0 == package {
      found_package = 1
      inside = 1
      next
    }
    inside && $0 ~ /^  [[:alnum:]_-]+:$/ { inside = 0 }
    inside && ($1 == "exactVersion:" || $1 == "revision:") {
      count++
      selector = $1
      value = $2
    }
    END {
      if (!found_package || count != 1) exit 1
      sub(/:$/, "", selector)
      print selector "\t" value
    }
  ' "$project" || fail "$package must have one exactVersion or revision Swift selector"
}

verify_module() {
  local module="$1"
  local package="$2"
  local version selector value expected

  version="$(go_version "$module")"
  IFS=$'\t' read -r selector value <<< "$(swift_selector "$package")"

  if [[ "$version" =~ ^v([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
    expected="${BASH_REMATCH[1]}"
    [ "$selector" = exactVersion ] \
      || fail "$package uses $selector while $module is tagged at $version"
    [ "$value" = "$expected" ] \
      || fail "$package $value does not match $module $version"
    return
  fi

  if [[ "$version" =~ [.-][0-9]{14}-([0-9a-f]{12})$ ]]; then
    expected="${BASH_REMATCH[1]}"
    [ "$selector" = revision ] \
      || fail "$package uses $selector while $module is pinned to pseudo-version $version"
    [ "$value" = "$expected" ] \
      || fail "$package revision $value does not match $module pseudo-version $version"
    return
  fi

  fail "$module uses unsupported non-exact Go version $version"
}

verify_module github.com/yasyf/daemonkit DaemonKit
verify_module github.com/yasyf/fusekit FuseKit
