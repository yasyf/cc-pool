import Foundation

/// Monotonic countdown for a deadline-budgeted control op. DispatchTime is
/// the mach uptime clock — never wall time — so a clock step can't stretch
/// or collapse the caller's budget.
struct Countdown {
    private let deadline: DispatchTime

    init(_ budget: TimeInterval) {
        deadline = .now() + budget
    }

    /// Seconds left, floored at 0.
    var remaining: TimeInterval {
        let now = DispatchTime.now()
        guard deadline > now else { return 0 }
        return Double(deadline.uptimeNanoseconds - now.uptimeNanoseconds) / 1_000_000_000
    }

    var expired: Bool { remaining <= 0 }

    /// Clamps a fixed per-phase wait to the budget's remainder.
    func bound(_ fixed: TimeInterval) -> TimeInterval { min(fixed, remaining) }
}

/// True once the requesting peer has hung up: EOF on a nonblocking MSG_PEEK.
/// Pending unread bytes and a would-block read both mean a live, waiting
/// peer; any other recv failure reads as gone (the reply is unwritable).
func peerClosed(_ fd: Int32) -> Bool {
    var byte: UInt8 = 0
    let n = recv(fd, &byte, 1, MSG_PEEK | MSG_DONTWAIT)
    if n == 0 { return true }
    if n < 0 { return errno != EAGAIN && errno != EINTR }
    return false
}
