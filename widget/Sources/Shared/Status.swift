import Foundation

// Models for ~/.cc-pool/status.json, the daemon's atomic status mirror.
// The wire contract (exact keys, omitempty fields, zero-time sentinel) is
// pinned by TestStatusSnapshotJSONKeys in internal/daemon/snapshot_test.go.

struct PoolStatus: Decodable {
    let proto: Int
    let version: String
    let generatedAt: Date
    let accounts: [AccountStatus]
    let pool: PoolOutlook? // absent before the daemon ever samples, or pre-forecast daemons

    enum CodingKeys: String, CodingKey {
        case proto, version, accounts, pool
        case generatedAt = "generated_at"
    }
}

extension PoolStatus {
    // The custom decode lives in an extension so the memberwise initializer
    // survives for the preview fixtures below.
    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        proto = try c.decode(Int.self, forKey: .proto)
        version = try c.decode(String.self, forKey: .version)
        generatedAt = try c.decode(Date.self, forKey: .generatedAt)
        accounts = try c.decode([AccountStatus].self, forKey: .accounts)
        // A malformed pool block is decorative damage only — degrade to the
        // derived outlook rather than bricking the whole account list.
        pool = try? c.decodeIfPresent(PoolOutlook.self, forKey: .pool)
    }

    /// The daemon-computed outlook, or a local approximation for daemons that
    /// predate the pool block (widget cask and daemon formula upgrade
    /// independently, so skew windows are real — and a neutral mascot smiling
    /// at a dry pool would be actively misleading). Burn and dry-out need
    /// daemon-side history, so the fallback simply omits them. nil means no
    /// account has ever been sampled.
    var outlook: PoolOutlook? {
        if let pool {
            // A daemon that ships the pool block but predates pace: re-derive
            // pace5h locally from gross burn and the usable count, mirroring
            // forecast.PoolOf (pace5h = grossBurn / (20 × usable)). PoolOutlook
            // itself lacks the count, so it must be filled here.
            guard pool.pace5hRaw == nil else { return pool }
            let usableCount = accounts.filter { $0.hasUsage && !$0.rateLimited }.count
            let pace5h = usableCount == 0 ? 0 : pool.burn5hPerHour / (20 * Double(usableCount))
            return PoolOutlook(remaining5hPct: pool.remaining5hPct,
                               remaining7dPct: pool.remaining7dPct,
                               burnRaw: pool.burnRaw, netBurnRaw: pool.netBurnRaw,
                               pace5hRaw: pace5h, pace7dRaw: 0,
                               dryAtRaw: pool.dryAtRaw, moodRaw: pool.moodRaw)
        }
        let sampled = accounts.filter(\.hasUsage)
        if sampled.isEmpty { return nil }
        let usable = sampled.filter { !$0.rateLimited }
        let mean5 = usable.map(\.remaining5h).meanClamped
        let mean7 = usable.map(\.remaining7d).meanClamped
        let mood: Mood = usable.isEmpty
            ? .panic : Mood(remaining5h: mean5, dryProjected: false)
        return PoolOutlook(remaining5hPct: mean5, remaining7dPct: mean7,
                           burnRaw: nil, netBurnRaw: nil, pace5hRaw: nil, pace7dRaw: nil,
                           dryAtRaw: nil, moodRaw: mood.rawValue)
    }
}

/// The pool-wide rollup behind the widget headline and mascot, mirroring the
/// Go wire PoolOutlook in internal/daemon/protocol.go.
struct PoolOutlook: Decodable {
    let remaining5hPct: Double
    let remaining7dPct: Double
    // fileprivate, not private: the memberwise initializer inherits the most
    // restrictive property's access, and the fixtures below must call it.
    fileprivate let burnRaw: Double? // omitempty
    fileprivate let netBurnRaw: Double? // omitempty
    fileprivate let pace5hRaw: Double? // pace_5h; nil only from pre-pace daemons
    fileprivate let pace7dRaw: Double? // pace_7d; nil only from pre-pace daemons
    fileprivate let dryAtRaw: Date? // omitzero
    fileprivate let moodRaw: String?

    var burn5hPerHour: Double { burnRaw ?? 0 }
    /// Mean drain net of upcoming 5h refills, pp/h. nil from daemons that
    /// predate the field — callers fall back to the gross burn.
    var netBurn5hPerHour: Double? { netBurnRaw }
    /// Gross 5h burn ÷ pool regeneration rate; 1.0 = sustainable forever.
    /// nil only from pre-pace daemons, where PoolStatus.outlook re-derives it.
    var pace5h: Double { pace5hRaw ?? 0 }
    /// Gross 7d burn ÷ weekly regeneration rate; 1.0 = sustainable forever.
    var pace7d: Double { pace7dRaw ?? 0 }
    var dryAt: Date? { dryAtRaw?.nonZero }
    /// An unknown future mood name degrades by re-deriving from the numbers
    /// the daemon shipped, not by failing decode or defaulting to calm.
    var mood: Mood {
        moodRaw.flatMap(Mood.init(rawValue:))
            ?? Mood(remaining5h: remaining5hPct, dryProjected: dryAt != nil)
    }

    enum CodingKeys: String, CodingKey {
        case remaining5hPct = "remaining_5h_pct"
        case remaining7dPct = "remaining_7d_pct"
        case burnRaw = "burn_5h_per_hour"
        case netBurnRaw = "net_burn_5h_per_hour"
        case pace5hRaw = "pace_5h"
        case pace7dRaw = "pace_7d"
        case dryAtRaw = "dry_at"
        case moodRaw = "mood"
    }
}

/// Pool-health alarm level, calmest first. Raw values are the wire strings
/// the daemon emits (forecast.Mood in internal/forecast/pool.go).
enum Mood: String, CaseIterable {
    case chill, easy, uneasy, worried, alarmed, panic

    /// Mirrors forecast.moodOf's thresholds so a locally-derived mood matches
    /// what the daemon would have computed.
    init(remaining5h: Double, dryProjected: Bool) {
        let base: Mood = switch remaining5h {
        case 60...: .chill
        case 40...: .easy
        case 25...: .uneasy
        case 10...: .worried
        default: .alarmed
        }
        self = dryProjected ? base.worse : base
    }

    /// The next more-alarmed level; panic is terminal.
    var worse: Mood {
        switch self {
        case .chill: .easy
        case .easy: .uneasy
        case .uneasy: .worried
        case .worried: .alarmed
        case .alarmed, .panic: .panic
        }
    }
}

struct AccountStatus: Decodable, Identifiable {
    let id: Int
    let configDir: String
    let label: String
    let score: Double
    let remaining5h: Double
    let remaining7d: Double
    let activeSessions: Int
    let rateLimited: Bool
    let exhausted: Bool? // omitempty in Go
    fileprivate let needsLoginRaw: Bool? // omitempty; refresh token gone/revoked
    let hasUsage: Bool
    let stale: Bool
    let resets5h: Date?
    let resets7d: Date?
    let burn5hPerHour: Double? // omitempty, %/hr drain
    let projected5hAtReset: Double? // omitempty, projected REMAINING % at resets5h
    fileprivate let depleted5hAtRaw: Date? // omitzero when idle or a reset refills first
    let extraEnabled: Bool? // omitempty
    let extraUsed: Double? // omitempty, currency cents
    let extraLimit: Double? // omitempty, currency cents

    // Explicit keys, not .convertFromSnakeCase: digit-leading components like
    // remaining_5h convert ambiguously. Keys the widget ignores (sample_age,
    // overlay_kind, the PascalCase components breakdown) are simply undeclared.
    enum CodingKeys: String, CodingKey {
        case id, label, score, stale, exhausted
        case configDir = "config_dir"
        case remaining5h = "remaining_5h"
        case remaining7d = "remaining_7d"
        case activeSessions = "active_sessions"
        case rateLimited = "rate_limited"
        case needsLoginRaw = "needs_login"
        case hasUsage = "has_usage"
        case resets5h = "resets_5h"
        case resets7d = "resets_7d"
        case burn5hPerHour = "burn_5h_per_hour"
        case projected5hAtReset = "projected_5h_at_reset"
        case depleted5hAtRaw = "depleted_5h_at"
        case extraEnabled = "extra_enabled"
        case extraUsed = "extra_used"
        case extraLimit = "extra_limit"
    }

    var isExhausted: Bool { exhausted ?? false }
    var needsLogin: Bool { needsLoginRaw ?? false }
    var hasOverage: Bool { (extraEnabled ?? false) && (extraUsed ?? 0) > 0 }
    var depleted5hAt: Date? { depleted5hAtRaw?.nonZero }

    /// Display label; empty labels fall back to the acct-NN dir basename.
    var displayName: String {
        label.isEmpty ? (configDir as NSString).lastPathComponent : label
    }

    /// Mirrors snapshotTier in internal/cli/status.go: status must never rank
    /// an unusable account above a usable one, however high its score.
    var tier: Int {
        if needsLogin { return 3 }
        if !rateLimited && !isExhausted { return 0 }
        if !rateLimited { return 1 }
        return 2
    }
}

extension [AccountStatus] {
    /// Display order: usability tier asc, then score desc — the same order
    /// `ccp status` uses, so "first" here matches the CLI's ▸ next pick.
    var ranked: [AccountStatus] {
        sorted { ($0.tier, -$0.score) < ($1.tier, -$1.score) }
    }
}

private extension [Double] {
    /// Mean of values clamped to 0...100; 0 when empty — mirroring the Go
    /// rollup's aggregation.
    var meanClamped: Double {
        isEmpty ? 0 : map { Swift.min(Swift.max($0, 0), 100) }.reduce(0, +) / Double(count)
    }
}

extension JSONDecoder {
    /// Decoder for status.json. Go's time.Time marshals RFC3339Nano, which
    /// omits fractional seconds when ns == 0 — while ISO8601DateFormatter
    /// requires fractions with .withFractionalSeconds and rejects them
    /// without. So try both. (generated_at is whole-second by construction;
    /// resets_* round-trip sqlite at whole-second precision; the dual parse
    /// keeps the widget safe if that ever changes.)
    static let poolStatus: JSONDecoder = {
        let frac = ISO8601DateFormatter()
        frac.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let plain = ISO8601DateFormatter()
        plain.formatOptions = [.withInternetDateTime]
        let d = JSONDecoder()
        d.dateDecodingStrategy = .custom { decoder in
            let s = try decoder.singleValueContainer().decode(String.self)
            guard let t = plain.date(from: s) ?? frac.date(from: s) else {
                throw DecodingError.dataCorrupted(.init(
                    codingPath: decoder.codingPath,
                    debugDescription: "unparseable RFC3339 timestamp: \(s)"))
            }
            return t
        }
        return d
    }()
}

extension Date {
    /// Go's zero time (0001-01-01) means "no active window"; anything before
    /// 2000 reads as nil.
    var nonZero: Date? { timeIntervalSince1970 > 946_684_800 ? self : nil }
}

enum StatusFile {
    /// The real user home. Inside the sandboxed appex, NSHomeDirectory()
    /// points at the container (~/Library/Containers/…/Data); the sandbox
    /// exception is for the real ~/.cc-pool, so resolve home via passwd.
    static var realHome: String {
        if let pw = getpwuid(getuid()), let dir = pw.pointee.pw_dir {
            return String(cString: dir)
        }
        return NSHomeDirectory()
    }

    static var url: URL {
        URL(fileURLWithPath: realHome).appendingPathComponent(".cc-pool/status.json")
    }
}

// MARK: - Preview / gallery fixtures

extension PoolStatus {
    /// Hardcoded fixture for the widget-gallery snapshot and Xcode previews.
    /// Six accounts so the medium preview overflows into "+N more" and the
    /// large preview shows a full board: hard burn with a depletion ETA, a
    /// reset projection, overage, stale, no-data, and unusable rows at once.
    /// The pool block is mid-alarm (worried, dry-out projected) so the
    /// gallery shows the mascot earning its keep.
    static let sample = PoolStatus(
        proto: 1,
        version: "dev",
        generatedAt: Date().addingTimeInterval(-90),
        accounts: [
            AccountStatus(
                id: 1, configDir: "/Users/you/.cc-pool/accounts/acct-01",
                label: "work@example.com", score: 88.4,
                remaining5h: 58, remaining7d: 91, activeSessions: 4,
                rateLimited: false, exhausted: nil, needsLoginRaw: nil, hasUsage: true, stale: false,
                resets5h: Date().addingTimeInterval(3 * 3600),
                resets7d: Date().addingTimeInterval(4 * 86400),
                burn5hPerHour: 22, projected5hAtReset: nil,
                depleted5hAtRaw: Date().addingTimeInterval(2.6 * 3600),
                extraEnabled: nil, extraUsed: nil, extraLimit: nil),
            AccountStatus(
                id: 2, configDir: "/Users/you/.cc-pool/accounts/acct-02",
                label: "rebecca.fallon.engineering@example-corp.com", score: 64.0,
                remaining5h: 60, remaining7d: 41, activeSessions: 2,
                rateLimited: false, exhausted: nil, needsLoginRaw: nil, hasUsage: true, stale: false,
                resets5h: Date().addingTimeInterval(2 * 3600),
                resets7d: Date().addingTimeInterval(3 * 86400),
                burn5hPerHour: 6, projected5hAtReset: 48, depleted5hAtRaw: nil,
                extraEnabled: nil, extraUsed: nil, extraLimit: nil),
            AccountStatus(
                id: 3, configDir: "/Users/you/.cc-pool/accounts/acct-03",
                label: "personal@example.com", score: 41.0,
                remaining5h: 22, remaining7d: 58, activeSessions: 0,
                rateLimited: false, exhausted: nil, needsLoginRaw: nil, hasUsage: true, stale: false,
                resets5h: Date().addingTimeInterval(90 * 60),
                resets7d: Date().addingTimeInterval(2 * 86400),
                burn5hPerHour: 2, projected5hAtReset: 19, depleted5hAtRaw: nil,
                extraEnabled: true, extraUsed: 5073, extraLimit: 10000),
            AccountStatus(
                id: 4, configDir: "/Users/you/.cc-pool/accounts/acct-04",
                label: "side@example.com", score: 18.0,
                remaining5h: 12, remaining7d: 35, activeSessions: 0,
                rateLimited: false, exhausted: nil, needsLoginRaw: nil, hasUsage: true, stale: true,
                resets5h: Date().addingTimeInterval(3600),
                resets7d: Date().addingTimeInterval(5 * 86400),
                burn5hPerHour: nil, projected5hAtReset: nil, depleted5hAtRaw: nil,
                extraEnabled: nil, extraUsed: nil, extraLimit: nil),
            AccountStatus(
                id: 5, configDir: "/Users/you/.cc-pool/accounts/acct-05",
                label: "fresh@example.com", score: 0.0,
                remaining5h: 0, remaining7d: 0, activeSessions: 0,
                rateLimited: false, exhausted: nil, needsLoginRaw: nil, hasUsage: false, stale: false,
                resets5h: nil, resets7d: nil,
                burn5hPerHour: nil, projected5hAtReset: nil, depleted5hAtRaw: nil,
                extraEnabled: nil, extraUsed: nil, extraLimit: nil),
            AccountStatus(
                id: 6, configDir: "/Users/you/.cc-pool/accounts/acct-06",
                label: "", score: -40.2,
                remaining5h: 1, remaining7d: 12, activeSessions: 0,
                rateLimited: true, exhausted: true, needsLoginRaw: nil, hasUsage: true, stale: false,
                resets5h: Date().addingTimeInterval(40 * 60),
                resets7d: Date().addingTimeInterval(86400),
                burn5hPerHour: nil, projected5hAtReset: nil, depleted5hAtRaw: nil,
                extraEnabled: true, extraUsed: 177, extraLimit: 5000),
        ],
        pool: PoolOutlook(
            remaining5hPct: 38, remaining7dPct: 56,
            burnRaw: 13, netBurnRaw: 4, pace5hRaw: 0.62, pace7dRaw: 1.3,
            dryAtRaw: Date().addingTimeInterval(2.6 * 3600),
            moodRaw: Mood.worried.rawValue))

    /// Variant of `sample` pinning a specific mascot mood, for the
    /// mood-sweep preview timeline. (PoolOutlook's raw fields are
    /// fileprivate, so fixture construction has to live in this file.)
    static func sample(mood: Mood) -> PoolStatus {
        PoolStatus(
            proto: 1, version: "dev",
            generatedAt: Date().addingTimeInterval(-90),
            accounts: sample.accounts,
            pool: PoolOutlook(
                remaining5hPct: 38, remaining7dPct: 56, burnRaw: 13, netBurnRaw: 4,
                pace5hRaw: 0.62, pace7dRaw: 1.3,
                dryAtRaw: mood == .chill ? nil : Date().addingTimeInterval(2.6 * 3600),
                moodRaw: mood.rawValue))
    }

    /// A daemon that ships the pool block but predates the pace fields: gross
    /// burn is present, pace_5h/pace_7d absent. Exercises the version-skew
    /// derivation in `outlook` (pace5h = grossBurn / (20 × usable)).
    static let samplePrePace = PoolStatus(
        proto: 1,
        version: "dev",
        generatedAt: Date().addingTimeInterval(-90),
        accounts: sample.accounts,
        pool: PoolOutlook(
            remaining5hPct: 38, remaining7dPct: 56, burnRaw: 13, netBurnRaw: 4,
            pace5hRaw: nil, pace7dRaw: nil,
            dryAtRaw: nil, moodRaw: Mood.worried.rawValue))

    /// A pre-forecast daemon's snapshot: no pool block, no per-account
    /// predictions. Exercises the derived-outlook fallback path in previews.
    static let sampleLegacy = PoolStatus(
        proto: 1,
        version: "dev",
        generatedAt: Date().addingTimeInterval(-90),
        accounts: [
            AccountStatus(
                id: 1, configDir: "/Users/you/.cc-pool/accounts/acct-01",
                label: "work@example.com", score: 80.1,
                remaining5h: 72, remaining7d: 88, activeSessions: 1,
                rateLimited: false, exhausted: nil, needsLoginRaw: nil, hasUsage: true, stale: false,
                resets5h: Date().addingTimeInterval(3 * 3600),
                resets7d: Date().addingTimeInterval(4 * 86400),
                burn5hPerHour: nil, projected5hAtReset: nil, depleted5hAtRaw: nil,
                extraEnabled: nil, extraUsed: nil, extraLimit: nil),
            AccountStatus(
                id: 2, configDir: "/Users/you/.cc-pool/accounts/acct-02",
                label: "personal@example.com", score: 55.0,
                remaining5h: 48, remaining7d: 61, activeSessions: 0,
                rateLimited: false, exhausted: nil, needsLoginRaw: nil, hasUsage: true, stale: false,
                resets5h: Date().addingTimeInterval(2 * 3600),
                resets7d: Date().addingTimeInterval(2 * 86400),
                burn5hPerHour: nil, projected5hAtReset: nil, depleted5hAtRaw: nil,
                extraEnabled: nil, extraUsed: nil, extraLimit: nil),
            AccountStatus(
                id: 3, configDir: "/Users/you/.cc-pool/accounts/acct-03",
                label: "", score: 12.0,
                remaining5h: 15, remaining7d: 30, activeSessions: 0,
                rateLimited: false, exhausted: nil, needsLoginRaw: nil, hasUsage: true, stale: false,
                resets5h: Date().addingTimeInterval(3600),
                resets7d: Date().addingTimeInterval(86400),
                burn5hPerHour: nil, projected5hAtReset: nil, depleted5hAtRaw: nil,
                extraEnabled: nil, extraUsed: nil, extraLimit: nil),
        ],
        pool: nil)
}
