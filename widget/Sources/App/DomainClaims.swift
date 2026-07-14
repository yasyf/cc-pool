import Foundation

/// Per-domain in-flight claims for the File Provider control server. Mirrors
/// fusekit mountd's per-resource inflight map (`mountd/server.go` `claim`):
/// same-key ops serialize — the second reads busy — while different keys
/// proceed concurrently. This replaces the old single global gate so one slow
/// domain op can no longer bounce every other account's control op busy.
final class DomainClaims {
    private let lock = NSLock()
    private var inflight = Set<String>()

    /// Takes the in-flight claim for `key`; false when another op already holds
    /// it (the caller should reply busy). A won claim must be paired with a
    /// `release(key)`.
    func claim(_ key: String) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return inflight.insert(key).inserted
    }

    /// Releases the in-flight claim for `key`. A no-op when `key` is unheld.
    func release(_ key: String) {
        lock.lock()
        defer { lock.unlock() }
        inflight.remove(key)
    }

    /// Routes a control op to the resource key it must claim, or nil when the
    /// op is unclaimed and must never bounce busy:
    /// register/remove/probe-domain/prepare-domain serialize on the domain
    /// identifier, probe serializes on its throwaway probe id, and path/signal
    /// (and every other health-ish read op) run unclaimed. Pure.
    static func key(op: String, domain: String, probeID: String) -> String? {
        switch op {
        case "register", "remove", "probe-domain", "prepare-domain": return domain
        case "probe": return probeID
        default: return nil
        }
    }
}
