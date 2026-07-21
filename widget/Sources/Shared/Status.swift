import Foundation

// Models for ~/.cc-pool/status.json, the daemon's atomic status mirror.
// The wire contract (exact keys, omitempty fields, zero-time sentinel) is
// pinned by TestStatusSnapshotJSONKeys in internal/daemon/snapshot_test.go.

struct PoolStatus: Decodable {
    static let protocolVersion = 1

    let proto: Int
    let version: String
    let generatedAt: Date
    let accounts: [AccountStatus]
    let pool: PoolOutlook? // absent only before any account has a known-good sample

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
        guard proto == Self.protocolVersion else {
            throw DecodingError.dataCorruptedError(
                forKey: .proto,
                in: c,
                debugDescription: "unsupported cc-pool status protocol \(proto)")
        }
        version = try c.decode(String.self, forKey: .version)
        generatedAt = try c.decode(Date.self, forKey: .generatedAt)
        accounts = try c.decode([AccountStatus].self, forKey: .accounts)
        pool = try c.decodeIfPresent(PoolOutlook.self, forKey: .pool)
        if pool == nil, accounts.contains(where: { $0.hasUsage }) {
            throw DecodingError.dataCorrupted(.init(
                codingPath: c.codingPath,
                debugDescription: "sampled accounts require the daemon-computed pool outlook"))
        }
    }

    /// The exact daemon-computed outlook; nil means no account has sampled.
    var outlook: PoolOutlook? { pool }
}

/// The pool-wide rollup behind the widget headline and mascot, mirroring the
/// Go wire PoolOutlook in internal/daemon/protocol.go.
struct PoolOutlook: Decodable {
    let remaining5hPct: Double
    let remaining7dPct: Double
    // fileprivate, not private: the memberwise initializer inherits the most
    // restrictive property's access, and the fixtures below must call it.
    fileprivate let burnRaw: Double? // omitempty
    let netBurn5hPerHour: Double
    let pace5h: Double
    let pace7d: Double
    fileprivate let dryAtRaw: Date? // omitzero
    let mood: Mood

    var burn5hPerHour: Double { burnRaw ?? 0 }
    var dryAt: Date? { dryAtRaw?.nonZero }

    enum CodingKeys: String, CodingKey {
        case remaining5hPct = "remaining_5h_pct"
        case remaining7dPct = "remaining_7d_pct"
        case burnRaw = "burn_5h_per_hour"
        case netBurn5hPerHour = "net_burn_5h_per_hour"
        case pace5h = "pace_5h"
        case pace7d = "pace_7d"
        case dryAtRaw = "dry_at"
        case mood
    }
}

extension PoolOutlook {
    var used5hPct: Int { Int((100 - remaining5hPct).rounded()) }
    var used7dPct: Int { Int((100 - remaining7dPct).rounded()) }
    var weekBinds: Bool { used7dPct > used5hPct }
    var bindingLabel: String { weekBinds ? "7d" : "5h" }
    var bindingUsedPct: Int { weekBinds ? used7dPct : used5hPct }
    var otherLabel: String { weekBinds ? "5h" : "7d" }
    var otherUsedPct: Int { weekBinds ? used5hPct : used7dPct }
}

/// Pool-health alarm level, calmest first. Raw values are the wire strings
/// the daemon emits (forecast.Mood in internal/forecast/pool.go).
enum Mood: String, CaseIterable, Decodable { case chill, easy, uneasy, worried, alarmed, panic }

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
    fileprivate let credentialQuarantinedRaw: Bool? // omitempty; exact credential recovery required
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
    let scoped7dUtil: Double? // omitempty, utilization 0–100 of the model-scoped weekly cap
    let scoped7dResets: Date? // omitzero, reset time of the scoped weekly window
    let scoped7dModel: String? // omitempty, wire display name of the scoped model (presence signal)
    let weeklyExhausted: Bool? // omitempty, the model-scoped weekly window is fully spent

    // Explicit keys, not .convertFromSnakeCase: digit-leading components like
    // remaining_5h convert ambiguously. Keys the widget ignores (sample_age and
    // the PascalCase components breakdown) are simply undeclared.
    enum CodingKeys: String, CodingKey {
        case id, label, score, stale, exhausted
        case configDir = "config_dir"
        case remaining5h = "remaining_5h"
        case remaining7d = "remaining_7d"
        case activeSessions = "active_sessions"
        case rateLimited = "rate_limited"
        case needsLoginRaw = "needs_login"
        case credentialQuarantinedRaw = "credential_quarantined"
        case hasUsage = "has_usage"
        case resets5h = "resets_5h"
        case resets7d = "resets_7d"
        case burn5hPerHour = "burn_5h_per_hour"
        case projected5hAtReset = "projected_5h_at_reset"
        case depleted5hAtRaw = "depleted_5h_at"
        case extraEnabled = "extra_enabled"
        case extraUsed = "extra_used"
        case extraLimit = "extra_limit"
        case scoped7dUtil = "scoped_7d_util"
        case scoped7dResets = "scoped_7d_resets"
        case scoped7dModel = "scoped_7d_model"
        case weeklyExhausted = "weekly_exhausted"
    }

    var isExhausted: Bool { exhausted ?? false }
    var isWeeklyExhausted: Bool { weeklyExhausted ?? false }
    var needsLogin: Bool { needsLoginRaw ?? false }
    var credentialQuarantined: Bool { credentialQuarantinedRaw ?? false }
    var unusable: Bool { rateLimited || isExhausted || needsLogin || credentialQuarantined }
    var hasOverage: Bool { (extraEnabled ?? false) && (extraUsed ?? 0) > 0 }
    var depleted5hAt: Date? { depleted5hAtRaw?.nonZero }

    /// The model-scoped weekly bucket (e.g. Fable's separate weekly cap), or
    /// nil when the daemon didn't report one. The model name is the presence
    /// signal — an absent or empty name means nothing to render. Mirrors
    /// `hasOverage`'s gate-on-presence shape.
    var scoped7d: (model: String, used: Double, resets: Date?)? {
        guard let model = scoped7dModel, !model.isEmpty else { return nil }
        return (model, scoped7dUtil ?? 0, scoped7dResets?.nonZero)
    }

    var weeklyExhaustionReset: (label: String, reset: Date)? {
        guard isWeeklyExhausted else { return nil }
        if let scoped = scoped7d, scoped.used >= 100 - remaining7d {
            guard let reset = scoped.resets else { return nil }
            return (scoped.model, reset)
        }
        guard let reset = resets7d?.nonZero else { return nil }
        return ("7d", reset)
    }

    /// Display label; empty labels fall back to the acct-NN dir basename.
    var displayName: String {
        label.isEmpty ? (configDir as NSString).lastPathComponent : label
    }

    /// Mirrors snapshotTier in internal/cli/status.go: status must never rank
    /// an unusable account above a usable one, however high its score.
    var tier: Int {
        if needsLogin || credentialQuarantined { return 3 }
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

func footerText(overflow: Int, exhausted: Int) -> (overflow: String, exhausted: String?)? {
    guard overflow > 0 || exhausted > 0 else { return nil }
    let separator = overflow > 0 ? " · " : ""
    return (
        overflow > 0 ? "+\(overflow) more" : "",
        exhausted > 0 ? "\(separator)\(exhausted) exhausted" : nil
    )
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
        proto: protocolVersion,
        version: "dev",
        generatedAt: Date().addingTimeInterval(-90),
        accounts: [
            AccountStatus(
                id: 1, configDir: "/Users/you/.cc-pool/accounts/acct-01",
                label: "work@example.com", score: 88.4,
                remaining5h: 58, remaining7d: 91, activeSessions: 4,
                rateLimited: false, exhausted: nil, needsLoginRaw: nil,
                credentialQuarantinedRaw: nil, hasUsage: true, stale: false,
                resets5h: Date().addingTimeInterval(3 * 3600),
                resets7d: Date().addingTimeInterval(4 * 86400),
                burn5hPerHour: 22, projected5hAtReset: nil,
                depleted5hAtRaw: Date().addingTimeInterval(2.6 * 3600),
                extraEnabled: nil, extraUsed: nil, extraLimit: nil,
                scoped7dUtil: 92, scoped7dResets: Date().addingTimeInterval(4 * 86400),
                scoped7dModel: "Fable", weeklyExhausted: nil),
            AccountStatus(
                id: 2, configDir: "/Users/you/.cc-pool/accounts/acct-02",
                label: "rebecca.fallon.engineering@example-corp.com", score: 64.0,
                remaining5h: 60, remaining7d: 41, activeSessions: 2,
                rateLimited: false, exhausted: nil, needsLoginRaw: nil,
                credentialQuarantinedRaw: nil, hasUsage: true, stale: false,
                resets5h: Date().addingTimeInterval(2 * 3600),
                resets7d: Date().addingTimeInterval(3 * 86400),
                burn5hPerHour: 6, projected5hAtReset: 48, depleted5hAtRaw: nil,
                extraEnabled: nil, extraUsed: nil, extraLimit: nil,
                scoped7dUtil: nil, scoped7dResets: nil, scoped7dModel: nil,
                weeklyExhausted: nil),
            AccountStatus(
                id: 3, configDir: "/Users/you/.cc-pool/accounts/acct-03",
                label: "personal@example.com", score: 41.0,
                remaining5h: 22, remaining7d: 58, activeSessions: 0,
                rateLimited: false, exhausted: nil, needsLoginRaw: nil,
                credentialQuarantinedRaw: nil, hasUsage: true, stale: false,
                resets5h: Date().addingTimeInterval(90 * 60),
                resets7d: Date().addingTimeInterval(2 * 86400),
                burn5hPerHour: 2, projected5hAtReset: 19, depleted5hAtRaw: nil,
                extraEnabled: true, extraUsed: 5073, extraLimit: 10000,
                scoped7dUtil: nil, scoped7dResets: nil, scoped7dModel: nil,
                weeklyExhausted: nil),
            AccountStatus(
                id: 4, configDir: "/Users/you/.cc-pool/accounts/acct-04",
                label: "side@example.com", score: 18.0,
                remaining5h: 12, remaining7d: 35, activeSessions: 0,
                rateLimited: false, exhausted: nil, needsLoginRaw: nil,
                credentialQuarantinedRaw: nil, hasUsage: true, stale: true,
                resets5h: Date().addingTimeInterval(3600),
                resets7d: Date().addingTimeInterval(5 * 86400),
                burn5hPerHour: nil, projected5hAtReset: nil, depleted5hAtRaw: nil,
                extraEnabled: nil, extraUsed: nil, extraLimit: nil,
                scoped7dUtil: nil, scoped7dResets: nil, scoped7dModel: nil,
                weeklyExhausted: nil),
            AccountStatus(
                id: 5, configDir: "/Users/you/.cc-pool/accounts/acct-05",
                label: "fresh@example.com", score: 0.0,
                remaining5h: 0, remaining7d: 0, activeSessions: 0,
                rateLimited: false, exhausted: nil, needsLoginRaw: nil,
                credentialQuarantinedRaw: true, hasUsage: false, stale: false,
                resets5h: nil, resets7d: nil,
                burn5hPerHour: nil, projected5hAtReset: nil, depleted5hAtRaw: nil,
                extraEnabled: nil, extraUsed: nil, extraLimit: nil,
                scoped7dUtil: nil, scoped7dResets: nil, scoped7dModel: nil,
                weeklyExhausted: nil),
            AccountStatus(
                id: 6, configDir: "/Users/you/.cc-pool/accounts/acct-06",
                label: "", score: -40.2,
                remaining5h: 1, remaining7d: 12, activeSessions: 0,
                rateLimited: false, exhausted: true, needsLoginRaw: nil,
                credentialQuarantinedRaw: nil, hasUsage: true, stale: false,
                resets5h: Date().addingTimeInterval(40 * 60),
                resets7d: Date().addingTimeInterval(86400),
                burn5hPerHour: nil, projected5hAtReset: nil, depleted5hAtRaw: nil,
                extraEnabled: true, extraUsed: 177, extraLimit: 5000,
                scoped7dUtil: 100, scoped7dResets: Date().addingTimeInterval(2 * 86400),
                scoped7dModel: "Fable", weeklyExhausted: true),
        ],
        pool: PoolOutlook(
            remaining5hPct: 38, remaining7dPct: 56,
            burnRaw: 13, netBurn5hPerHour: 4, pace5h: 0.62, pace7d: 1.3,
            dryAtRaw: Date().addingTimeInterval(2.6 * 3600),
            mood: .worried))

    /// Variant of `sample` pinning a specific mascot mood, for the
    /// mood-sweep preview timeline. (PoolOutlook's raw fields are
    /// fileprivate, so fixture construction has to live in this file.)
    static func sample(mood: Mood) -> PoolStatus {
        PoolStatus(
            proto: protocolVersion, version: "dev",
            generatedAt: Date().addingTimeInterval(-90),
            accounts: sample.accounts,
            pool: PoolOutlook(
                remaining5hPct: 38, remaining7dPct: 56,
                burnRaw: 13, netBurn5hPerHour: 4, pace5h: 0.62, pace7d: 1.3,
                dryAtRaw: mood == .chill ? nil : Date().addingTimeInterval(2.6 * 3600),
                mood: mood))
    }

}
