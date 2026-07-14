import Foundation

/// Synth read outcome per computed name, keyed by the content version:
/// repeated item()/enumeration stats cost zero bridge reads until the version
/// changes, and fetchContents serves .bytes without a second read. Bounded to
/// the two computed names per domain. Locked — the extension queue is
/// concurrent.
final class SynthSizeCache {
    enum Outcome: Equatable {
        case bytes(Data)
        /// Over byteCap: only the length is cached; fetchContents re-reads.
        case sized(Int64)
        /// Content-level read failure — never stored (a retry must re-read);
        /// exists so callers can size the item 0 without inventing a sentinel.
        case unreadable

        /// Caching policy for a successful read: bytes up to `cap`, size only above.
        static func of(_ data: Data, cap: Int = SynthSizeCache.byteCap) -> Outcome {
            data.count <= cap ? .bytes(data) : .sized(Int64(data.count))
        }

        var size: Int64 {
            switch self {
            case .bytes(let d): Int64(d.count)
            case .sized(let n): n
            case .unreadable: 0
            }
        }
    }

    static let byteCap = 4 << 20

    private static let maxNames = 2

    private var slots: [String: (version: String, outcome: Outcome)] = [:]
    private let lock = NSLock()

    func lookup(name: String, version: String) -> Outcome? {
        lock.lock()
        defer { lock.unlock() }
        guard let slot = slots[name], slot.version == version else { return nil }
        return slot.outcome
    }

    func store(name: String, version: String, outcome: Outcome) {
        lock.lock()
        defer { lock.unlock() }
        if slots[name] == nil, slots.count >= Self.maxNames,
           let evict = slots.keys.first {
            slots.removeValue(forKey: evict)
        }
        slots[name] = (version, outcome)
    }
}

/// Blocking in-flight coalescer: concurrent callers of the same key share one
/// `work` execution — the leader runs, waiters block on its DispatchGroup and
/// take its result, errors included. Sharing only: no TTL, nothing cached —
/// a call arriving after completion runs `work` afresh. The lock is never
/// held across `work`; blocking is bounded by the callee's own timeouts.
final class InFlight<Key: Hashable, Value> {
    private final class Flight {
        let group = DispatchGroup()
        var result: Result<Value, Error>!
    }

    private let lock = NSLock()
    private var flights: [Key: Flight] = [:]

    func run(_ key: Key, _ work: () throws -> Value) throws -> Value {
        lock.lock()
        if let flight = flights[key] {
            lock.unlock()
            flight.group.wait()
            // result is set before the map removal that precedes leave(), so
            // the wait orders this read.
            return try flight.result.get()
        }
        let flight = Flight()
        flight.group.enter()
        flights[key] = flight
        lock.unlock()
        flight.result = Result { try work() }
        lock.lock()
        flights[key] = nil
        lock.unlock()
        flight.group.leave()
        return try flight.result.get()
    }
}
