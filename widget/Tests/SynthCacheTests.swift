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
    }

    func testOutcomeCachesSuccessPerVersion() throws {
        let cache = SynthSizeCache()
        var reads = 0
        let read = { () -> Data in
            reads += 1
            return Data("abc".utf8)
        }
        XCTAssertEqual(try cache.outcome(name: "settings.json", version: "v1", read: read),
                       .bytes(Data("abc".utf8)))
        XCTAssertEqual(try cache.outcome(name: "settings.json", version: "v1", read: read),
                       .bytes(Data("abc".utf8)))
        XCTAssertEqual(reads, 1, "a cached content version must cost zero reads")
    }

    /// Fail-closed: every bridge-level read failure propagates — a listing
    /// must fail, never advertise a size-0 item — and is never cached, so the
    /// next metadata build retries the read.
    func testOutcomeFailClosedPropagatesUncached() {
        for arm in BridgeFailureArms.all {
            let cache = SynthSizeCache()
            var reads = 0
            XCTAssertThrowsError(
                try cache.outcome(name: ".claude.json", version: "v1") {
                    reads += 1
                    throw arm.error
                }, arm.name
            ) { thrown in
                let got = thrown as NSError
                let want = arm.error as NSError
                XCTAssertEqual(got.domain, want.domain, arm.name)
                XCTAssertEqual(got.code, want.code, arm.name)
            }
            XCTAssertNil(cache.lookup(name: ".claude.json", version: "v1"),
                         "\(arm.name): a failure must never be cached")
            XCTAssertEqual(
                try? cache.outcome(name: ".claude.json", version: "v1") {
                    reads += 1
                    return Data("ok".utf8)
                }, .bytes(Data("ok".utf8)),
                "\(arm.name): the retry must re-read and succeed")
            XCTAssertEqual(reads, 2, arm.name)
        }
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
    /// work stays gated until every caller has checked in, so a late-scheduled
    /// caller can never find the flight finished, become a second leader, and
    /// deadlock on the exhausted gate.
    func testConcurrentCallersComputeOnce() {
        let flights = InFlight<String, Int>()
        let gate = DispatchSemaphore(value: 0)
        let arrived = DispatchSemaphore(value: 0)
        let executions = ManagedAtomicCounter()
        let callers = 8
        let group = DispatchGroup()
        var results = [Int?](repeating: nil, count: callers)
        let resultsLock = NSLock()

        for i in 0..<callers {
            DispatchQueue.global().async(group: group) {
                arrived.signal()
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
        // Arrival barrier: release the leader only once all N are scheduled.
        for i in 0..<callers {
            XCTAssertEqual(arrived.wait(timeout: .now() + 5), .success, "caller \(i) never arrived")
        }
        gate.signal()
        XCTAssertEqual(group.wait(timeout: .now() + 5), .success, "callers must all return")
        XCTAssertEqual(executions.value, 1, "coalesced callers must compute exactly once")
        XCTAssertEqual(results.compactMap { $0 }, Array(repeating: 42, count: callers),
                       "every caller must receive the leader's value")
    }

    func testErrorPropagatesToWaitersAndIsNeverCached() {
        let flights = InFlight<String, Int>()
        let gate = DispatchSemaphore(value: 0)
        let arrived = DispatchSemaphore(value: 0)
        let executions = ManagedAtomicCounter()
        let failures = ManagedAtomicCounter()
        let callers = 4
        let group = DispatchGroup()

        for _ in 0..<callers {
            DispatchQueue.global().async(group: group) {
                arrived.signal()
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
        // Same arrival barrier as testConcurrentCallersComputeOnce.
        for i in 0..<callers {
            XCTAssertEqual(arrived.wait(timeout: .now() + 5), .success, "caller \(i) never arrived")
        }
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
