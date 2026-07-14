import XCTest

final class CountdownTests: XCTestCase {
    func testBoundClampsFixedWaitToRemaining() {
        let c = Countdown(0.2)
        XCTAssertLessThanOrEqual(c.bound(5), 0.2, "a fixed wait longer than the budget must clamp")
        XCTAssertGreaterThan(c.bound(5), 0)
        XCTAssertLessThanOrEqual(c.bound(0.01), 0.01, "a fixed wait inside the budget stays fixed")
    }

    func testExpiryIsTerminal() {
        let c = Countdown(0.05)
        XCTAssertFalse(c.expired, "a fresh budget must not start expired")
        Thread.sleep(forTimeInterval: 0.1)
        XCTAssertTrue(c.expired)
        XCTAssertEqual(c.remaining, 0, "remaining floors at zero, never negative")
        XCTAssertEqual(c.bound(3), 0, "an expired budget clamps every wait to zero")
    }

    func testZeroBudgetStartsExpired() {
        XCTAssertTrue(Countdown(0).expired)
    }
}

final class PeerClosedTests: XCTestCase {
    private func pair() throws -> (local: Int32, remote: Int32) {
        var fds: [Int32] = [0, 0]
        guard socketpair(AF_UNIX, SOCK_STREAM, 0, &fds) == 0 else {
            throw XCTSkip("socketpair failed: \(errno)")
        }
        return (fds[0], fds[1])
    }

    func testOpenPeerIsNotClosed() throws {
        let (local, remote) = try pair()
        defer {
            close(local)
            close(remote)
        }
        XCTAssertFalse(peerClosed(local), "a connected idle peer must read as alive")
    }

    func testPendingBytesAreNotClosedAndNotConsumed() throws {
        let (local, remote) = try pair()
        defer {
            close(local)
            close(remote)
        }
        var byte: UInt8 = 0x2a
        XCTAssertEqual(write(remote, &byte, 1), 1)
        XCTAssertFalse(peerClosed(local), "unread bytes mean a live peer")
        var got: UInt8 = 0
        XCTAssertEqual(recv(local, &got, 1, 0), 1, "the peek must not consume the byte")
        XCTAssertEqual(got, 0x2a)
    }

    func testHungUpPeerIsClosed() throws {
        let (local, remote) = try pair()
        defer { close(local) }
        close(remote)
        XCTAssertTrue(peerClosed(local), "EOF must read as an abandoned request")
    }

    func testBadDescriptorIsClosed() {
        XCTAssertTrue(peerClosed(-1), "an unusable connection can never receive the reply")
    }
}
