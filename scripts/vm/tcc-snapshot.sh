#!/usr/bin/env bash
# Print every relevant cc-pool filesystem TCC row from a coherent copy.
set -euo pipefail

snapshot_class() {
  local snapshot_path="$1" label="$2" class="$3" predicate="$4"
  sqlite3 "$snapshot_path" '.mode insert access' "
    SELECT *
      FROM access
     WHERE service IN (
             'kTCCServiceSystemPolicyAppData',
             'kTCCServiceSystemPolicyNetworkVolumes',
             'kTCCServiceSystemPolicyRemovableVolumes',
             'kTCCServiceSystemPolicyAllFiles',
             'kTCCServiceFileProviderDomain'
           )
       AND ($predicate)
     ORDER BY service, client, client_type;" \
    | sed "s/^/$label|$class|/"
}

snapshot() {
  local database="$1" label="$2" work
  [[ -f "$database" ]] || return 0
  work="$(mktemp -d)"
  cp "$database" "$work/TCC.db"
  cp "$database-wal" "$work/TCC.db-wal" 2>/dev/null || true
  cp "$database-shm" "$work/TCC.db-shm" 2>/dev/null || true
  snapshot_class "$work/TCC.db" "$label" signed "
    client IN (
      'com.yasyf.cc-pool.status',
      'com.yasyf.cc-pool.status.fileprovider',
      'com.yasyf.cc-pool.status.widget'
    )
    OR client LIKE '%CCPoolStatus%'
    OR client LIKE '%cc-pool.status%'"
  snapshot_class "$work/TCC.db" "$label" daemon "
    client IN ('com.yasyf.cc-pool', 'com.yasyf.cc-pool.daemon')
    OR client LIKE '%/cc-pool'
    OR client LIKE '%/ccp'"
  rm -rf "$work"
}

snapshot "$HOME/Library/Application Support/com.apple.TCC/TCC.db" user
snapshot "/Library/Application Support/com.apple.TCC/TCC.db" system
