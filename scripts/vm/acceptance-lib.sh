# shellcheck shell=bash
# Shared cross-build and guest-execution helpers for VM acceptance scenarios.

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "acceptance-lib.sh is a library: source it from a vmctl scenario" >&2
  exit 64
fi

ACCEPTANCE_REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ACCEPTANCE_BUILD_DIR="$VM_ROOT/acceptance-build"
ACCEPTANCE_GUEST_DIR="$VMCTL_GUEST_DIR/acceptance"

acceptance_prepare() {
  require_cmd go
  [[ "$(vm_ssh uname -m)" == "arm64" ]] || die "acceptance scenarios require an arm64 guest"
  mkdir -p "$ACCEPTANCE_BUILD_DIR"
  vm_ssh "mkdir -p '$ACCEPTANCE_GUEST_DIR'" || die "could not create guest acceptance directory"
  (
    cd "$ACCEPTANCE_REPO_ROOT"
    GOWORK=off go list -m github.com/yasyf/daemonkit github.com/yasyf/fusekit
  ) >"$VMCTL_RESULTS_DIR/module-versions.txt" || die "could not resolve exact acceptance module versions"
}

acceptance_stage_go_helper() {
  local package="$1" name="$2"
  local host_path="$ACCEPTANCE_BUILD_DIR/$name" guest_path="$ACCEPTANCE_GUEST_DIR/$name"
  (
    cd "$ACCEPTANCE_REPO_ROOT"
    GOWORK=off GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o "$host_path" "$package"
  ) || die "could not cross-build VM helper $package"
  vm_scp_to "$host_path" "$guest_path" || die "could not stage VM helper $name"
  vm_ssh "chmod 700 '$guest_path'" || die "could not protect VM helper $name"
  printf '%s\n' "$guest_path"
}

acceptance_stage_test_binary() {
  local package="$1" name="$2"
  local host_path="$ACCEPTANCE_BUILD_DIR/$name" guest_path="$ACCEPTANCE_GUEST_DIR/$name"
  (
    cd "$ACCEPTANCE_REPO_ROOT"
    GOWORK=off GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go test -c -trimpath -o "$host_path" "$package"
  ) || die "could not cross-build VM test package $package"
  vm_scp_to "$host_path" "$guest_path" || die "could not stage VM test binary $name"
  vm_ssh "chmod 700 '$guest_path'" || die "could not protect VM test binary $name"
  printf '%s\n' "$guest_path"
}

acceptance_run_tests() {
  local binary="$1" regex="$2" output="$3"
  shift 3
  [[ "$regex" =~ ^[A-Za-z0-9_\^\$\|]+$ ]] || die "unsafe acceptance test regex: $regex"
  if ! vm_ssh "'$binary' -test.v -test.count=1 -test.timeout=180s -test.run '$regex'" >"$output" 2>&1; then
    cat "$output" >&2
    die "guest acceptance tests failed: $regex"
  fi
  local test_name
  for test_name in "$@"; do
    grep -Fq -- "--- PASS: $test_name " "$output" || {
      cat "$output" >&2
      die "guest acceptance binary did not execute $test_name"
    }
  done
}
