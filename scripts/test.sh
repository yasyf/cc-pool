#!/usr/bin/env bash
# Run `go test` under a per-UID process cap so a runaway spawn — e.g. a holder
# that re-execs the test binary and re-enters the spawn — hits EAGAIN within a
# small headroom instead of fork-bombing the machine and freezing it.
#
# ALWAYS run these repos' tests through this script (CI, local, and any agent or
# workflow). Never invoke `go test` directly on a real machine.
# See docs/INCIDENT-holder-spawn-storm-2026-06-24.md.
set -euo pipefail

headroom="${TEST_NPROC_HEADROOM:-400}"
# Current process count for this real UID. macOS `ps -U <uid>` rejects a numeric
# id, so filter `ps -axo` instead. Best-effort; defaults to 0.
cur="$(ps -axo uid=,pid= 2>/dev/null | awk -v u="$(id -ru)" '$1==u {n++} END{print n+0}')" || cur=0
[ -n "${cur:-}" ] || cur=0
cap=$(( 10#${cur} + headroom ))
hard="$(ulimit -Hu 2>/dev/null || echo unlimited)"
if [ "$hard" != "unlimited" ] && [ "$cap" -gt "$hard" ]; then
  cap="$hard"
fi
ulimit -Su "$cap"

# Apply a default timeout unless the caller already set one, so a wedged test
# can never hang the cap in place indefinitely.
case " $* " in
  *" -timeout"*) ;;
  *) set -- -timeout 600s "$@" ;;
esac

echo "scripts/test.sh: RLIMIT_NPROC soft cap=$cap (uid procs ~$cur + headroom $headroom); go test $*" >&2
exec go test "$@"
