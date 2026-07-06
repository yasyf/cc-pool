import XCTest

final class DomainClaimsTests: XCTestCase {
    func testDistinctKeysBothClaim() {
        let claims = DomainClaims()
        XCTAssertTrue(claims.claim("acct-01"))
        XCTAssertTrue(claims.claim("acct-02"), "a distinct key must never be blocked by another")
    }

    func testSameKeyExcludesUntilRelease() {
        let claims = DomainClaims()
        XCTAssertTrue(claims.claim("acct-01"))
        XCTAssertFalse(claims.claim("acct-01"), "a held key must reject the second claim")
        claims.release("acct-01")
        XCTAssertTrue(claims.claim("acct-01"), "release must free the key for re-claim")
    }

    func testReleaseOfUnheldKeyIsNoop() {
        let claims = DomainClaims()
        claims.release("never-claimed") // must not crash or corrupt state
        XCTAssertTrue(claims.claim("never-claimed"))
    }

    /// Hammers a small key set from every core: if `claim` ever admitted two
    /// holders of the same key, a per-key concurrent-holder count would exceed 1.
    func testConcurrentPerformExactlyOneHolderPerKey() {
        let claims = DomainClaims()
        let keys = ["acct-01", "acct-02", "acct-03", "acct-04"]
        var counters: [String: HolderCounter] = [:]
        for k in keys { counters[k] = HolderCounter() }
        let wins = Counter()

        DispatchQueue.concurrentPerform(iterations: 20_000) { i in
            let key = keys[i % keys.count]
            guard claims.claim(key) else { return }
            wins.increment()
            let concurrent = counters[key]!.enter()
            XCTAssertEqual(concurrent, 1, "two holders observed for \(key)")
            counters[key]!.leave()
            claims.release(key)
        }
        XCTAssertGreaterThan(wins.value, 0, "the hammer must have won some claims")
    }

    func testKeyRouterTable() {
        let probeID = "ccp-probe-4242"
        let cases: [(name: String, op: String, domain: String, want: String?)] = [
            ("register routes to domain", "register", "acct-01", "acct-01"),
            ("remove routes to domain", "remove", "acct-02", "acct-02"),
            ("probe-domain routes to domain", "probe-domain", "acct-06", "acct-06"),
            ("probe routes to probeID", "probe", "", probeID),
            ("path is unclaimed", "path", "acct-03", nil),
            ("signal is unclaimed", "signal", "acct-04", nil),
            ("health is unclaimed", "health", "", nil),
            ("unknown op is unclaimed", "bogus", "acct-05", nil),
        ]
        for c in cases {
            XCTAssertEqual(
                DomainClaims.key(op: c.op, domain: c.domain, probeID: probeID),
                c.want, c.name)
        }
    }

    /// probe-domain shares register/remove's per-domain key: same domain →
    /// mutual exclusion (busy on contention), different domains → none.
    func testProbeDomainSharesRegisterKey() {
        let probeID = "ccp-probe-4242"
        let probeKey = DomainClaims.key(op: "probe-domain", domain: "acct-01", probeID: probeID)
        let registerKey = DomainClaims.key(op: "register", domain: "acct-01", probeID: probeID)
        XCTAssertEqual(probeKey, "acct-01")
        XCTAssertEqual(probeKey, registerKey, "probe-domain must claim the same key as register")
        XCTAssertNotEqual(probeKey, probeID, "probe-domain claims the domain, not the throwaway probe id")

        let claims = DomainClaims()
        XCTAssertTrue(claims.claim(registerKey!))
        XCTAssertFalse(claims.claim(probeKey!),
                       "probe-domain on a domain busy with register must bounce busy")

        let otherKey = DomainClaims.key(op: "probe-domain", domain: "acct-02", probeID: probeID)
        XCTAssertEqual(otherKey, "acct-02")
        XCTAssertTrue(claims.claim(otherKey!),
                      "probe-domain on a different domain must not contend")
    }
}

/// Counts threads concurrently inside a key's claimed region. `enter` returns
/// the live count so the test can assert it never exceeds 1.
private final class HolderCounter {
    private let lock = NSLock()
    private var count = 0

    func enter() -> Int {
        lock.lock()
        defer { lock.unlock() }
        count += 1
        return count
    }

    func leave() {
        lock.lock()
        defer { lock.unlock() }
        count -= 1
    }
}

private final class Counter {
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
