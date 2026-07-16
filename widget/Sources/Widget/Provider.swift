import Foundation
import WidgetKit

struct StatusEntry: TimelineEntry {
    let date: Date
    let state: State

    enum State {
        case ok(PoolStatus, stale: Bool) // stale: entry's display date is > staleAfter past generated_at
        case noFile // daemon not running / never ran
        case denied // read refused — sandbox/entitlement problem, not the daemon
        case unreadable // decode failure or proto skew — surface it, never guess
    }
}

struct StatusProvider: TimelineProvider {
    /// The daemon polls every 180s + up to 30s jitter and stamps generated_at
    /// per completed poll; two missed cycles (~7 min) means it's down or wedged.
    static let staleAfter: TimeInterval = 7 * 60
    static let supportedProto = 2

    func placeholder(in _: Context) -> StatusEntry {
        StatusEntry(date: .now, state: .ok(.sample, stale: false))
    }

    func getSnapshot(in context: Context, completion: @escaping (StatusEntry) -> Void) {
        completion(context.isPreview ? placeholder(in: context) : load(at: .now))
    }

    func getTimeline(in _: Context, completion: @escaping (Timeline<StatusEntry>) -> Void) {
        let now = Date()
        // One load, paginated as future-dated entries: the footer's "updated
        // Nm ago" is static text, so the same state is re-emitted with fresher
        // entry.dates against the same generated_at to keep the age honest —
        // minute-spaced across the requested refresh window, then 5-minute
        // steps out to an hour because the .after(5m) ask sits below the
        // reload throttle; without the tail the age would freeze at "4m ago"
        // for however long WidgetKit defers the reload. Staleness is likewise
        // judged per entry against its own display date: dimming claims
        // "these numbers are ≥7 min old", which entry.date − generated_at
        // makes true by construction no matter how throttled reloads get. On
        // a fresh snapshot the first dimmable entry is the +10 offset, which
        // a healthy watcher-driven ~3 min reload cadence never displays —
        // only a pipeline that has genuinely stopped reloading ages into the
        // dimmed tail.
        let state = load(at: now).state
        let minuteOffsets = Array(0 ..< 5) + Array(stride(from: 5, through: 60, by: 5))
        let entries = minuteOffsets.map { minute -> StatusEntry in
            let date = now.addingTimeInterval(Double(minute) * 60)
            guard case .ok(let status, _) = state else { return StatusEntry(date: date, state: state) }
            let stale = date.timeIntervalSince(status.generatedAt) > Self.staleAfter
            return StatusEntry(date: date, state: .ok(status, stale: stale))
        }
        completion(Timeline(entries: entries, policy: .after(now.addingTimeInterval(5 * 60))))
    }

    private func load(at now: Date) -> StatusEntry {
        let data: Data
        do {
            data = try Data(contentsOf: StatusFile.url)
        } catch let err as CocoaError where err.code == .fileReadNoSuchFile {
            return StatusEntry(date: now, state: .noFile)
        } catch {
            // A sandbox denial (EPERM after an entitlement-stripping re-sign)
            // must not masquerade as "daemon not running" — that misdiagnosis
            // sends the user off to reinstall a perfectly healthy daemon.
            return StatusEntry(date: now, state: .denied)
        }
        // No last-good cache on failure: the daemon's atomic rename makes
        // partial reads impossible, so a persistent decode failure means
        // schema/proto skew the user should see, not paper over.
        guard let status = try? JSONDecoder.poolStatus.decode(PoolStatus.self, from: data),
              status.proto == Self.supportedProto
        else {
            return StatusEntry(date: now, state: .unreadable)
        }
        let stale = now.timeIntervalSince(status.generatedAt) > Self.staleAfter
        return StatusEntry(date: now, state: .ok(status, stale: stale))
    }
}
