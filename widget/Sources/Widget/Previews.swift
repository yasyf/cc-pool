import SwiftUI
import WidgetKit

// Widget-gallery preview timelines for all families and moods.

#Preview("medium", as: .systemMedium) {
    CCPoolStatusWidget()
} timeline: {
    StatusEntry(date: .now, state: .ok(.sample, stale: false))
    StatusEntry(date: .now, state: .ok(.samplePrePace, stale: false))
    StatusEntry(date: .now, state: .ok(.sampleLegacy, stale: false))
    StatusEntry(date: .now, state: .ok(.sample, stale: true))
    StatusEntry(date: .now, state: .noFile)
}

#Preview("small", as: .systemSmall) {
    CCPoolStatusWidget()
} timeline: {
    StatusEntry(date: .now, state: .ok(.sample, stale: false))
    StatusEntry(date: .now, state: .ok(.samplePrePace, stale: false))
    StatusEntry(date: .now, state: .ok(.sampleLegacy, stale: false))
    StatusEntry(date: .now, state: .unreadable)
}

#Preview("large", as: .systemLarge) {
    CCPoolStatusWidget()
} timeline: {
    StatusEntry(date: .now, state: .ok(.sample, stale: false))
    StatusEntry(date: .now, state: .ok(.sample, stale: true))
}

#Preview("moods", as: .systemMedium) {
    CCPoolStatusWidget()
} timeline: {
    StatusEntry(date: .now, state: .ok(.sample(mood: .chill), stale: false))
    StatusEntry(date: .now, state: .ok(.sample(mood: .easy), stale: false))
    StatusEntry(date: .now, state: .ok(.sample(mood: .uneasy), stale: false))
    StatusEntry(date: .now, state: .ok(.sample(mood: .worried), stale: false))
    StatusEntry(date: .now, state: .ok(.sample(mood: .alarmed), stale: false))
    StatusEntry(date: .now, state: .ok(.sample(mood: .panic), stale: false))
}
