import AppKit
import ImageIO
import SwiftUI
import UniformTypeIdentifiers

/// Prints to stderr and exits non-zero. Fail loud: a bad render must never
/// silently ship a blank or stale screenshot.
func fail(_ message: String) -> Never {
    FileHandle.standardError.write(Data((message + "\n").utf8))
    exit(1)
}

/// The chrome WidgetKit draws around widget content, replicated for the docs
/// screenshot: the real MoodWashBackground, the widget's content margins, and
/// the Notification Center corner radius — on a transparent margin with a soft
/// shadow so the card reads well on both GitHub light and dark themes. The
/// 329×155pt frame is systemMedium at MacBook scale.
struct WidgetScreenshotCanvas: View {
    let status: PoolStatus
    let seed: Date

    var body: some View {
        ZStack {
            // Opaque dark card base: WidgetKit composites the widget over a
            // material backdrop, so MoodWashBackground's translucent .fill
            // needs something solid beneath it to read as a real card.
            Color(red: 0.13, green: 0.13, blue: 0.14)
            MoodWashBackground(state: .ok(status, stale: false))
            PoolBoardView(status: status, stale: false, seed: seed,
                          maxRows: 3, style: .compact) // the systemMedium layout
                .padding(16)
        }
        .frame(width: 329, height: 155)
        .clipShape(RoundedRectangle(cornerRadius: 22, style: .continuous))
        .shadow(color: .black.opacity(0.28), radius: 9, y: 4)
        .padding(24)
        .environment(\.colorScheme, .dark)
    }
}

/// Renders the medium widget from `PoolStatus.sample` into the PNG at argv[1].
/// Build and run via scripts/render-screenshot.sh.
@main @MainActor
struct RenderScreenshot {
    static func main() {
        guard CommandLine.arguments.count == 2 else {
            fail("usage: render-screenshot <output.png>")
        }
        // Pin locale-derived strings (12-hour clock, weekday names) so the
        // render doesn't depend on the generating machine's preferences.
        UserDefaults.standard.setVolatileDomain(
            ["AppleLocale": "en_US", "AppleLanguages": ["en"]],
            forName: UserDefaults.argumentDomain)
        // Semantic styles (.secondary, .fill.tertiary) resolve through the
        // current AppKit drawing appearance, which ImageRenderer latches when
        // it rasterizes — so build the renderer and read cgImage inside an
        // explicit dark-aqua appearance.
        guard let darkAqua = NSAppearance(named: .darkAqua) else {
            fail("dark-aqua appearance unavailable")
        }
        var image: CGImage?
        darkAqua.performAsCurrentDrawingAppearance {
            let renderer = ImageRenderer(
                content: WidgetScreenshotCanvas(status: .sample, seed: .now))
            renderer.scale = 2 // @2x; README displays at 377pt via <img width>
            image = renderer.cgImage
        }
        guard let image else {
            fail("ImageRenderer produced no image")
        }

        let url = URL(fileURLWithPath: CommandLine.arguments[1])
        guard let dest = CGImageDestinationCreateWithURL(
            url as CFURL, UTType.png.identifier as CFString, 1, nil)
        else {
            fail("cannot create \(url.path)")
        }
        CGImageDestinationAddImage(dest, image, nil)
        guard CGImageDestinationFinalize(dest) else {
            fail("PNG write failed: \(url.path)")
        }
    }
}
