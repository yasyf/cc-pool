import SwiftUI
import WidgetKit

struct StatusWidgetView: View {
    @Environment(\.widgetFamily) private var family
    let entry: StatusEntry

    var body: some View {
        switch entry.state {
        case .noFile:
            MessageView(symbol: "moon.zzz", title: "daemon not running",
                        detail: "ccp service install")
        case .denied:
            MessageView(symbol: "lock.slash", title: "can't read ~/.cc-pool",
                        detail: "check widget entitlements")
        case .unreadable:
            MessageView(symbol: "exclamationmark.triangle", title: "status unreadable",
                        detail: "version skew? run ccp doctor")
        case .ok(let status, let stale):
            if status.accounts.isEmpty {
                MessageView(symbol: "person.crop.circle.badge.plus", title: "no accounts",
                            detail: "run ccp add")
            } else {
                switch family {
                case .systemSmall:
                    SmallView(status: status, stale: stale, seed: entry.date)
                case .systemLarge:
                    PoolBoardView(status: status, stale: stale, seed: entry.date,
                                  maxRows: 5, style: .detailed)
                default:
                    PoolBoardView(status: status, stale: stale, seed: entry.date,
                                  maxRows: 3, style: .compact)
                }
            }
        }
    }
}

// MARK: - Medium & large: pool header + ranked account list

/// Row density per family: medium rows are name + bars; large rows add the
/// resets/overage detail line.
enum RowStyle { case compact, detailed }

/// The medium/large body: mascot header over the ranked list. The stale dim
/// wraps both so a dead daemon mutes the whole board, mascot included.
struct PoolBoardView: View {
    let status: PoolStatus
    let stale: Bool
    let seed: Date
    let maxRows: Int
    let style: RowStyle

    var body: some View {
        VStack(alignment: .leading, spacing: style == .detailed ? 6 : 4) {
            PoolHeaderView(outlook: status.outlook,
                           critterSize: style == .detailed ? 44 : 26,
                           detailed: style == .detailed, seed: seed)
            AccountListView(status: status, stale: stale, now: seed,
                            maxRows: maxRows, style: style)
        }
        .opacity(stale ? 0.55 : 1)
    }
}

/// Mascot + pool headline. Compact: one line with the caption right-aligned;
/// detailed: bigger critter and a 7d/burn second line.
struct PoolHeaderView: View {
    let outlook: PoolOutlook?
    let critterSize: CGFloat
    let detailed: Bool
    let seed: Date

    var body: some View {
        HStack(spacing: 8) {
            CritterView(mood: outlook?.mood ?? .chill, size: critterSize, seed: seed)
            if let outlook {
                VStack(alignment: .leading, spacing: 1) {
                    Text(headline(outlook))
                        .font(.system(size: detailed ? 13 : 12, weight: .semibold))
                        .monospacedDigit()
                        .lineLimit(1)
                    if detailed {
                        Text(secondLine(outlook))
                            .font(.system(size: 10))
                            .monospacedDigit()
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                    }
                }
                Spacer(minLength: 4)
                if !detailed {
                    Text(outlook.caption())
                        .font(.caption2)
                        .monospacedDigit()
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                        .layoutPriority(1)
                }
            } else {
                Text("no usage data yet")
                    .font(.system(size: 12, weight: .semibold))
                    .foregroundStyle(.secondary)
                Spacer(minLength: 0)
            }
        }
    }

    private func headline(_ o: PoolOutlook) -> String {
        let pct = "Pool \(Int((100 - o.remaining5hPct).rounded()))% used"
        guard detailed else { return pct }
        // The compact caption moves into the headline on the large widget;
        // burn lives on the second line, so only the dry clock joins here.
        if let dry = o.dryAt, dry > .now {
            return pct + " · dry ~" + dry.formatted(date: .omitted, time: .shortened)
        }
        return o.dryAt == nil ? pct : pct + " · drying now"
    }

    private func secondLine(_ o: PoolOutlook) -> String {
        "7d \(Int((100 - o.remaining7dPct).rounded()))% · " + o.pacePhrase
    }
}

/// Ranked account list filling the widget. Every row gets an equal flexible
/// slot (maxHeight .infinity) so surplus height spreads across rows instead
/// of pooling above the footer; with fewer accounts each row simply breathes.
struct AccountListView: View {
    let status: PoolStatus
    let stale: Bool
    let now: Date
    let maxRows: Int
    let style: RowStyle

    var body: some View {
        let ranked = status.accounts.ranked
        VStack(alignment: .leading, spacing: 0) {
            ForEach(Array(ranked.prefix(maxRows).enumerated()), id: \.element.id) { rank, account in
                AccountRow(account: account, style: style, rank: rank)
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .leading)
            }
            FooterView(generatedAt: status.generatedAt, now: now, stale: stale,
                       overflow: max(0, ranked.count - maxRows))
                .padding(.top, 2)
        }
    }
}

struct AccountRow: View {
    let account: AccountStatus
    let style: RowStyle
    let rank: Int

    /// Lines below the name align past the live dot: dot (7) + spacing (5).
    private static let indent: CGFloat = 12

    var body: some View {
        HStack(alignment: .top, spacing: 6) {
            RoundedRectangle(cornerRadius: 1.5)
                .fill(rankAccent(rank))
                .frame(width: 3)
                .frame(maxHeight: .infinity)
                .padding(.vertical, 1)
            VStack(alignment: .leading, spacing: 2) {
                HStack(spacing: 5) {
                    LiveDot(count: account.activeSessions)
                    Text(account.displayName)
                        .font(.system(size: 12, weight: .medium))
                        .lineLimit(1)
                        .truncationMode(.middle)
                    Spacer(minLength: 4)
                    BadgeRow(account: account, style: style)
                        .layoutPriority(1) // badges hug; the name truncates first
                }
                Group {
                    if account.hasUsage {
                        HStack(spacing: 10) {
                            UsageCell(window: "5h", remaining: account.remaining5h,
                                      alert: account.unusable, kind: .fiveHour)
                            UsageCell(window: "7d", remaining: account.remaining7d,
                                      alert: account.unusable, kind: .sevenDay)
                            // Large only: the per-model weekly cap as a third
                            // labeled bar, named by the wire (never hardcoded).
                            if style == .detailed, let scoped = account.scoped7d {
                                UsageCell(window: scoped.model, remaining: 100 - scoped.used,
                                          alert: account.unusable, kind: .scopedWeekly,
                                          labelWidth: 40)
                            }
                        }
                    } else {
                        Text("no data")
                            .font(.system(size: 10))
                            .foregroundStyle(.tertiary)
                    }
                }
                .padding(.leading, Self.indent)
                if style == .detailed, let detail {
                    Text(detail)
                        .font(.system(size: 10))
                        .monospacedDigit()
                        .foregroundStyle(.tertiary)
                        .lineLimit(1)
                        .padding(.leading, Self.indent)
                }
            }
        }
    }

    /// The week is "heavy" — and so worth showing its reset clock — once this
    /// much of the 7d window is spent. Below it, the 7d reset is just noise.
    private static let weekHeavyUsedPct = 40.0

    /// Detail line: the forward-looking projection (now carrying its own reset
    /// clock), the relief time when depleting before reset, the 7d reset only
    /// when the week is heavily spent, then overage spend.
    private var detail: String? {
        var parts: [String] = []
        if account.unusable {
            // Rate-limited / exhausted: the recovery time is the only fact.
            if let reset = account.resets5h?.nonZero {
                parts.append("resets " + reset.formatted(date: .omitted, time: .shortened))
            }
        } else if let prediction = account.predictionText() {
            parts.append(prediction)
            // The depleting form names a deadline but not the relief; append
            // the 5h reset time so the row shows both.
            if prediction.hasSuffix(" left"), let reset = account.resets5h?.nonZero {
                parts.append("resets " + reset.formatted(date: .omitted, time: .shortened))
            }
        }
        if 100 - account.remaining7d >= Self.weekHeavyUsedPct, let reset = account.resets7d?.nonZero {
            parts.append("7d resets " + weekResetText(reset))
        }
        if account.hasOverage {
            let used = (account.extraUsed ?? 0) / 100
            if let limit = account.extraLimit, limit > 0 {
                parts.append(String(format: "extra $%.2f of $%.2f", used, limit / 100))
            } else {
                parts.append(String(format: "extra $%.2f", used))
            }
        }
        return parts.isEmpty ? nil : parts.joined(separator: " · ")
    }

    /// The 7d reset as an abbreviated weekday ("Wed"), but the hour ("8 AM")
    /// when it lands within a day — so a same-day weekday never misreads as
    /// next week.
    private func weekResetText(_ reset: Date, now: Date = .now) -> String {
        if reset.timeIntervalSince(now) < 24 * 3600 {
            return reset.formatted(.dateTime.hour())
        }
        return reset.formatted(.dateTime.weekday(.abbreviated))
    }
}

// MARK: - Small: the pool at a glance (mascot + headline + next pick)

struct SmallView: View {
    let status: PoolStatus
    let stale: Bool
    let seed: Date

    var body: some View {
        let ranked = status.accounts.ranked
        let best = ranked[0]
        let liveTotal = status.accounts.reduce(0) { $0 + $1.activeSessions }
        VStack(alignment: .leading, spacing: 6) {
            HStack(spacing: 8) {
                CritterView(mood: status.outlook?.mood ?? .chill, size: 54, seed: seed)
                VStack(alignment: .leading, spacing: 0) {
                    if let outlook = status.outlook {
                        Text("\(Int((100 - outlook.remaining5hPct).rounded()))%")
                            .font(.system(size: 26, weight: .bold, design: .rounded))
                            .monospacedDigit()
                            + Text(" used")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Text(outlook.caption())
                            .font(.caption2)
                            .monospacedDigit()
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                            .minimumScaleFactor(0.8)
                    } else {
                        Text("—")
                            .font(.system(size: 26, weight: .bold, design: .rounded))
                            .foregroundStyle(.secondary)
                        Text("no data yet")
                            .font(.caption2)
                            .foregroundStyle(.tertiary)
                    }
                }
            }
            VStack(alignment: .leading, spacing: 2) {
                HStack(spacing: 5) {
                    LiveDot(count: liveTotal)
                    Text(best.displayName)
                        .font(.system(size: 11, weight: .medium))
                        .lineLimit(1)
                        .truncationMode(.middle)
                    Spacer(minLength: 4)
                    if let prediction = best.predictionText() {
                        Text(prediction)
                            .font(.system(size: 9))
                            .monospacedDigit()
                            .foregroundStyle(.secondary)
                            .layoutPriority(1)
                    }
                }
                if best.hasUsage {
                    HeadroomBar(remaining: min(max(best.remaining5h, 0), 100),
                                alert: best.unusable, kind: .fiveHour)
                } else {
                    Text("no data yet")
                        .font(.caption2)
                        .foregroundStyle(.tertiary)
                }
            }
            Spacer(minLength: 0)
            FooterView(generatedAt: status.generatedAt, now: seed, stale: stale, overflow: 0)
        }
        .opacity(stale ? 0.55 : 1)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
}

// MARK: - Pieces

extension AccountStatus {
    var unusable: Bool { rateLimited || isExhausted || needsLogin }
}

/// Capsule gauge. The bar fills with % USED — matching the `ccp status`
/// columns and the row's number. The gradient runs the window's identity
/// hues, heating toward amber/red as headroom drains.
struct HeadroomBar: View {
    let remaining: Double // clamped to 0...100 by the caller
    let alert: Bool
    let kind: WindowKind

    private static let height: CGFloat = 6

    var body: some View {
        let usedFraction = (100 - remaining) / 100
        GeometryReader { geo in
            ZStack(alignment: .leading) {
                Capsule().fill(.quaternary)
                // Floor at the capsule's own diameter so a barely-used
                // account renders a full dot, not a degenerate sliver —
                // while an untouched window shows an honest empty track.
                if usedFraction > 0 {
                    Capsule()
                        .fill(usageGradient(kind: kind, remaining: remaining, alert: alert))
                        .frame(width: max(Self.height, geo.size.width * usedFraction))
                }
            }
        }
        .frame(height: Self.height)
    }
}

/// "5h ▮▮▮░ 58%" cell: fixed label and percent columns around a flexible
/// bar, so bars align across rows and split leftover width with siblings.
/// Bar and number both show % used (see HeadroomBar).
struct UsageCell: View {
    let window: String
    let remaining: Double
    let alert: Bool
    let kind: WindowKind
    /// Widened for the scoped bucket's model-name label; 5h/7d keep 16.
    var labelWidth: CGFloat = 16

    var body: some View {
        let clamped = min(max(remaining, 0), 100)
        HStack(spacing: 4) {
            Text(window)
                .font(.system(size: 10))
                .foregroundStyle(.tertiary)
                .lineLimit(1)
                .truncationMode(.tail)
                .frame(width: labelWidth, alignment: .leading)
            HeadroomBar(remaining: clamped, alert: alert, kind: kind)
            Text("\(Int((100 - clamped).rounded()))%")
                .font(.system(size: 10, weight: .medium, design: .monospaced))
                .foregroundStyle(.secondary)
                .frame(width: 30, alignment: .trailing)
        }
        .frame(maxWidth: .infinity)
    }
}

struct LiveDot: View {
    let count: Int

    var body: some View {
        HStack(spacing: 2) {
            Circle()
                .fill(count > 0 ? Color.green : Color.secondary.opacity(0.35))
                .frame(width: 7, height: 7)
            if count > 0 {
                Text("\(count)")
                    .font(.system(size: 10, design: .monospaced))
                    .foregroundStyle(.secondary)
            }
        }
    }
}

/// The CLI's stale / rate-limited / exhausted / overage flags as compact
/// glyphs, plus the row's burn projection when one exists.
struct BadgeRow: View {
    let account: AccountStatus
    let style: RowStyle

    var body: some View {
        HStack(spacing: 3) {
            if account.stale {
                Image(systemName: "clock.badge.exclamationmark")
                    .font(.system(size: 10))
                    .foregroundStyle(.orange)
            }
            if account.rateLimited {
                Image(systemName: "exclamationmark.octagon.fill")
                    .font(.system(size: 10))
                    .foregroundStyle(.red)
            }
            if account.isExhausted {
                Image(systemName: "hourglass.bottomhalf.filled")
                    .font(.system(size: 10))
                    .foregroundStyle(.red)
            }
            // The bottom tier: no live token at all until `ccp login N` runs, so
            // it gets the red error treatment plus a word, not just a glyph.
            if account.needsLogin {
                Image(systemName: "person.crop.circle.badge.exclamationmark")
                    .font(.system(size: 10))
                    .foregroundStyle(.red)
                Text("needs login")
                    .font(.system(size: 10))
                    .foregroundStyle(.red)
            }
            // Detailed rows carry overage, reset times, and the projection on
            // their own detail line; repeating them here would double-print.
            if style == .compact {
                // The per-model weekly cap: one heat-colored "<model> N%"
                // token whenever the bucket exists — the ask is to see the
                // limit, so it is not threshold-gated. Named by the wire.
                if let scoped = account.scoped7d {
                    Text("\(scoped.model) \(Int(scoped.used.rounded()))%")
                        .font(.system(size: 10, weight: .medium))
                        .foregroundStyle(scopedBadgeColor(used: scoped.used))
                        .lineLimit(1)
                }
                if account.hasOverage {
                    Text(String(format: "$%.2f", (account.extraUsed ?? 0) / 100))
                        .font(.system(size: 10, design: .monospaced))
                        .foregroundStyle(.orange)
                }
                // When the account can't serve, the time it recovers is the one
                // fact that matters — the CLI's RESETS column, shown on demand.
                // predictionText is nil for unusable rows, so the slot is shared,
                // never crowded.
                if account.unusable, let reset = account.resets5h?.nonZero {
                    Text(reset, format: .dateTime.hour().minute())
                        .font(.system(size: 10))
                        .foregroundStyle(.secondary)
                } else if let prediction = account.predictionText() {
                    Text(prediction)
                        .font(.system(size: 10))
                        .monospacedDigit()
                        .foregroundStyle(.secondary)
                }
            }
        }
    }
}

struct FooterView: View {
    let generatedAt: Date
    /// The timeline-entry date. The age text is deliberately static — the
    /// auto-ticking Text(_, style: .relative) counts seconds, far more
    /// precision than a 3-minute poll cadence deserves — so the provider
    /// paginates minute-spaced entries to advance it.
    let now: Date
    let stale: Bool
    let overflow: Int

    var body: some View {
        HStack(spacing: 4) {
            if overflow > 0 {
                Text("+\(overflow) more")
                    .font(.caption2)
                    .foregroundStyle(.tertiary)
            }
            Spacer(minLength: 0)
            if stale {
                Text("stale · \(age) ago")
                    .font(.caption2)
                    .foregroundStyle(.orange)
            } else {
                Text("updated \(age) ago")
                    .font(.caption2)
                    .foregroundStyle(.tertiary)
            }
        }
    }

    /// Coarse single-unit age: "<1m", "3m", "2h" (floored).
    private var age: String {
        let s = max(0, now.timeIntervalSince(generatedAt))
        if s < 60 { return "<1m" }
        if s < 3600 { return "\(Int(s / 60))m" }
        return "\(Int(s / 3600))h"
    }
}

struct MessageView: View {
    let symbol: String
    let title: String
    let detail: String

    var body: some View {
        VStack(spacing: 4) {
            Image(systemName: symbol)
                .font(.title3)
                .foregroundStyle(.secondary)
            Text(title)
                .font(.caption.weight(.semibold))
            Text(detail)
                .font(.caption2.monospaced())
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

// MARK: - Widget background

/// The widget background: the system tertiary fill with a faint radial wash
/// of the mascot's mood color. Lives inside containerBackground — macOS 14
/// widgets must not paint their own full-bleed background — and stays subtle
/// (≤ 0.18 opacity) so secondary/tertiary text keeps contrast in both
/// appearances. Dimmed alongside the content when the snapshot is stale.
struct MoodWashBackground: View {
    let state: StatusEntry.State

    var body: some View {
        ZStack {
            Rectangle().fill(.fill.tertiary)
            if case .ok(let status, let stale) = state, let outlook = status.outlook {
                RadialGradient(colors: [outlook.mood.tint.opacity(0.18), .clear],
                               center: .topLeading, startRadius: 0, endRadius: 240)
                    .opacity(stale ? 0.55 : 1)
            }
        }
    }
}
