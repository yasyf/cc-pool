import SwiftUI
import WidgetKit

@main
struct CCPoolWidgetBundle: WidgetBundle {
    var body: some Widget { CCPoolStatusWidget() }
}

struct CCPoolStatusWidget: Widget {
    var body: some WidgetConfiguration {
        StaticConfiguration(kind: "CCPoolStatus", provider: StatusProvider()) { entry in
            StatusWidgetView(entry: entry)
                .containerBackground(for: .widget) {
                    MoodWashBackground(state: entry.state)
                }
        }
        .configurationDisplayName("cc-pool")
        .description("Per-account usage of your pooled Claude subscriptions.")
        .supportedFamilies([.systemSmall, .systemMedium, .systemLarge])
    }
}
