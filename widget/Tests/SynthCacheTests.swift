import XCTest

final class SynthCacheTests: XCTestCase {
    func testVersionInvalidation() {
        let cache = SynthSizeCache()
        cache.store(name: ".claude.json", version: "v1", outcome: .bytes(Data("one".utf8)))
        XCTAssertEqual(cache.lookup(name: ".claude.json", version: "v1"), .bytes(Data("one".utf8)))
        XCTAssertNil(cache.lookup(name: ".claude.json", version: "v2"),
                     "a stale version must miss, never serve old bytes")

        cache.store(name: ".claude.json", version: "v2", outcome: .sized(9))
        XCTAssertEqual(cache.lookup(name: ".claude.json", version: "v2"), .sized(9))
        XCTAssertNil(cache.lookup(name: ".claude.json", version: "v1"),
                     "storing a new version must evict the old one")
    }

    func testOutcomePolicyByCap() {
        let cases: [(name: String, data: Data, cap: Int, want: SynthSizeCache.Outcome)] = [
            ("under cap keeps bytes", Data("abc".utf8), 10, .bytes(Data("abc".utf8))),
            ("at cap keeps bytes", Data(count: 10), 10, .bytes(Data(count: 10))),
            ("over cap keeps size only", Data(count: 11), 10, .sized(11)),
            ("empty keeps bytes", Data(), 10, .bytes(Data())),
        ]
        for c in cases {
            XCTAssertEqual(SynthSizeCache.Outcome.of(c.data, cap: c.cap), c.want, c.name)
        }
        XCTAssertEqual(SynthSizeCache.Outcome.of(Data(count: SynthSizeCache.byteCap + 1)),
                       .sized(Int64(SynthSizeCache.byteCap) + 1),
                       "default cap must bound at byteCap")
    }

    func testOutcomeSizes() {
        XCTAssertEqual(SynthSizeCache.Outcome.bytes(Data("abcd".utf8)).size, 4)
        XCTAssertEqual(SynthSizeCache.Outcome.sized(77).size, 77)
        XCTAssertEqual(SynthSizeCache.Outcome.unreadable.size, 0,
                       "unreadable content is advertised at size 0")
    }

    func testBoundedToTwoNames() {
        let cache = SynthSizeCache()
        cache.store(name: "a", version: "v", outcome: .sized(1))
        cache.store(name: "b", version: "v", outcome: .sized(2))
        cache.store(name: "c", version: "v", outcome: .sized(3))
        XCTAssertEqual(cache.lookup(name: "c", version: "v"), .sized(3),
                       "the just-stored name must always be retrievable")
        let survivors = ["a", "b", "c"].filter { cache.lookup(name: $0, version: "v") != nil }
        XCTAssertEqual(survivors.count, 2, "the cache must hold at most 2 names")
    }
}

final class InFlightTests: XCTestCase {
    private enum Boom: Error { case boom }

    /// N concurrent callers of one key share a single execution: the leader's
    /// work is gated open only after every caller has had ample time to join.
    func testConcurrentCallersComputeOnce() {
        let flights = InFlight<String, Int>()
        let gate = DispatchSemaphore(value: 0)
        let executions = ManagedAtomicCounter()
        let callers = 8
        let group = DispatchGroup()
        var results = [Int?](repeating: nil, count: callers)
        let resultsLock = NSLock()

        for i in 0..<callers {
            DispatchQueue.global().async(group: group) {
                let v = try? flights.run("k") { () -> Int in
                    executions.increment()
                    gate.wait()
                    return 42
                }
                resultsLock.lock()
                results[i] = v
                resultsLock.unlock()
            }
        }
        // All callers reach run() and block on the leader before it finishes.
        Thread.sleep(forTimeInterval: 0.5)
        gate.signal()
        XCTAssertEqual(group.wait(timeout: .now() + 5), .success, "callers must all return")
        XCTAssertEqual(executions.value, 1, "coalesced callers must compute exactly once")
        XCTAssertEqual(results.compactMap { $0 }, Array(repeating: 42, count: callers),
                       "every caller must receive the leader's value")
    }

    func testErrorPropagatesToWaitersAndIsNeverCached() {
        let flights = InFlight<String, Int>()
        let gate = DispatchSemaphore(value: 0)
        let executions = ManagedAtomicCounter()
        let failures = ManagedAtomicCounter()
        let callers = 4
        let group = DispatchGroup()

        for _ in 0..<callers {
            DispatchQueue.global().async(group: group) {
                do {
                    _ = try flights.run("k") { () -> Int in
                        executions.increment()
                        gate.wait()
                        throw Boom.boom
                    }
                } catch {
                    failures.increment()
                }
            }
        }
        Thread.sleep(forTimeInterval: 0.5)
        gate.signal()
        XCTAssertEqual(group.wait(timeout: .now() + 5), .success)
        XCTAssertEqual(executions.value, 1)
        XCTAssertEqual(failures.value, callers, "the leader's error must reach every waiter")

        // Not cached: the next run must execute afresh and can succeed.
        let v = try? flights.run("k") { () -> Int in
            executions.increment()
            return 7
        }
        XCTAssertEqual(executions.value, 2, "an error must never be cached")
        XCTAssertEqual(v, 7)
    }

    func testDistinctKeysRunIndependently() throws {
        let flights = InFlight<String, String>()
        XCTAssertEqual(try flights.run("a") { "va" }, "va")
        XCTAssertEqual(try flights.run("b") { "vb" }, "vb")
    }

    func testSequentialCallsAreNotCached() throws {
        let flights = InFlight<String, Int>()
        let executions = ManagedAtomicCounter()
        for _ in 0..<3 {
            _ = try flights.run("k") { () -> Int in
                executions.increment()
                return 1
            }
        }
        XCTAssertEqual(executions.value, 3, "in-flight sharing only — no TTL, no result cache")
    }
}

private final class ManagedAtomicCounter {
    private let lock = NSLock()
    private var n = 0

    func increment() {
        lock.lock()
        defer { lock.unlock() }
        n += 1
    }

    var value: Int {
        lock.lock()
        defer { lock.unlock() }
        return n
    }
}
