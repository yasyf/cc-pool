import SwiftUI

// The widget's color system: window-identity gradients that heat up as
// headroom drains, mood tints for the mascot and background wash, and rank
// accents for the pick order. Numeric sRGB throughout — Color.mix needs
// macOS 15 and the deployment target is 14.

struct RGB {
    let r, g, b: Double

    func mixed(with other: RGB, _ t: Double) -> RGB {
        let t = min(max(t, 0), 1)
        return RGB(r: r + (other.r - r) * t,
                   g: g + (other.g - g) * t,
                   b: b + (other.b - b) * t)
    }

    var color: Color { Color(.sRGB, red: r, green: g, blue: b) }
}

enum Palette {
    static let teal = RGB(r: 0.20, g: 0.78, b: 0.75)
    static let blue = RGB(r: 0.25, g: 0.48, b: 0.98)
    static let purple = RGB(r: 0.62, g: 0.36, b: 0.96)
    static let pink = RGB(r: 0.97, g: 0.40, b: 0.71)
    static let amber = RGB(r: 0.98, g: 0.70, b: 0.16)
    static let red = RGB(r: 0.93, g: 0.25, b: 0.21)
    static let darkRed = RGB(r: 0.72, g: 0.13, b: 0.15)
    static let gold = RGB(r: 0.95, g: 0.74, b: 0.22)
    static let silver = RGB(r: 0.74, g: 0.77, b: 0.82)
    static let bronze = RGB(r: 0.80, g: 0.54, b: 0.33)
}

/// Which usage window a bar renders; each has its own identity hues.
enum WindowKind {
    case fiveHour // teal → blue
    case sevenDay // purple → pink
    case scopedWeekly // gold → amber, heating toward red — the per-model weekly cap
}

/// Gradient bar fill: pure identity hues at comfortable headroom, blending
/// toward amber→red as remaining drops below 40 (the old yellow threshold,
/// so the semantic tinting carries over). Unusable accounts are flat alarm.
func usageGradient(kind: WindowKind, remaining: Double, alert: Bool) -> LinearGradient {
    if alert {
        return LinearGradient(colors: [Palette.red.color, Palette.darkRed.color],
                              startPoint: .leading, endPoint: .trailing)
    }
    let heat = min(max((40 - remaining) / 40, 0), 1)
    let (a, b): (RGB, RGB) = switch kind {
    case .fiveHour: (Palette.teal, Palette.blue)
    case .sevenDay: (Palette.purple, Palette.pink)
    case .scopedWeekly: (Palette.gold, Palette.amber)
    }
    return LinearGradient(colors: [a.mixed(with: Palette.amber, heat).color,
                                   b.mixed(with: Palette.red, heat).color],
                          startPoint: .leading, endPoint: .trailing)
}

/// Solid heat color for the medium widget's scoped-bucket badge: the gold
/// scoped identity while there's slack, blending through to red as the
/// per-model weekly cap fills. Takes utilization (0–100) directly, since the
/// badge shows a used %; starts heating past 60% used to match the bar's
/// 40-remaining threshold.
func scopedBadgeColor(used: Double) -> Color {
    let heat = min(max((used - 60) / 40, 0), 1)
    return Palette.gold.mixed(with: Palette.red, heat).color
}

extension Mood {
    /// The mascot's body color, also the background wash. Color reinforces
    /// the mood but never carries it alone: desktop widgets render
    /// desaturated when the desktop is unfocused, so geometry does the
    /// talking.
    var rgb: RGB {
        switch self {
        case .chill: RGB(r: 0.30, g: 0.84, b: 0.49)
        case .easy: RGB(r: 0.25, g: 0.80, b: 0.70)
        case .uneasy: RGB(r: 0.95, g: 0.82, b: 0.25)
        case .worried: RGB(r: 0.98, g: 0.64, b: 0.15)
        case .alarmed: RGB(r: 0.96, g: 0.42, b: 0.16)
        case .panic: RGB(r: 0.92, g: 0.22, b: 0.20)
        }
    }

    var tint: Color { rgb.color }
}

/// Leading stripe color by pick order: medal colors for the top three, a
/// quiet neutral for the rest.
func rankAccent(_ rank: Int) -> Color {
    switch rank {
    case 0: Palette.gold.color
    case 1: Palette.silver.color
    case 2: Palette.bronze.color
    default: Color.secondary.opacity(0.25)
    }
}

/// Compact duration to a future date: "35m", "1.4h", "12h".
func compactETA(to date: Date, from now: Date = .now) -> String {
    let s = max(0, date.timeIntervalSince(now))
    if s < 3600 { return "\(Int((s / 60).rounded()))m" }
    if s < 10 * 3600 { return String(format: "%.1fh", s / 3600) }
    return "\(Int((s / 3600).rounded()))h"
}

extension AccountStatus {
    /// Burn-based projection for the row: the depletion ETA when the window
    /// runs dry before its reset (the user's real deadline), else the
    /// projected used % at reset while burning meaningfully. nil when
    /// unusable (the reset badge owns that slot) or idle — absence reads as
    /// idle, so dense rows never print filler.
    func predictionText(now: Date = .now) -> String? {
        if unusable { return nil }
        if let depleted = depleted5hAt {
            guard depleted > now else { return "dry" }
            return "~\(compactETA(to: depleted, from: now)) left"
        }
        if let projected = projected5hAtReset, (burn5hPerHour ?? 0) > 0.5 {
            let used = Int((100 - projected).rounded())
            if let reset = resets5h?.nonZero {
                return "→\(used)% by " + reset.formatted(date: .omitted, time: .shortened)
            }
            return "→\(used)% by reset"
        }
        return nil
    }
}

extension PoolOutlook {
    /// The headline caption: the dry-out clock when one is projected, else
    /// the shared pace phrase.
    func caption(now: Date = .now) -> String {
        if let dry = dryAt {
            guard dry > now else { return "drying now" }
            return "dry ~" + dry.formatted(date: .omitted, time: .shortened)
        }
        return pacePhrase
    }

    /// Pace phrase shared by the small/medium caption and the large header's
    /// second line. A negative net burn — refills outpacing drain — reads as
    /// refilling (kept from the net-burn era); otherwise the binding pace (the
    /// max of the dimensionless 5h/7d ratios, 1.0 = sustainable forever) is
    /// shown, labeled when the week binds. Under 0.02 reads as cruising.
    var pacePhrase: String {
        if netBurn5hPerHour <= -1 {
            return "refilling \(Int((-netBurn5hPerHour).rounded()))%/h"
        }
        let binding = max(pace5h, pace7d)
        if binding < 0.02 { return "cruising" }
        let pct = "\(Int((binding * 100).rounded()))%"
        return pace7d > pace5h ? "7d pace \(pct)" : "pace \(pct)"
    }
}
