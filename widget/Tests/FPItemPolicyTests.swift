import FileProvider
import UniformTypeIdentifiers
import XCTest

final class FPItemPolicyTests: XCTestCase {
    private func item(_ id: ItemID) -> FPItem {
        FPItem(id: id, filename: "f", contentType: .json,
               capabilities: [.allowsReading], versionHex: "00")
    }

    func testComputedItemsAreEagerlyKeptDownloaded() {
        // Lazy policy is the fleet-staleness bug: a materialized replica of a
        // computed document froze for days because nothing re-downloaded it.
        for name in [".claude.json", "settings.json"] {
            XCTAssertEqual(item(.computed(name)).contentPolicy,
                           .downloadEagerlyAndKeepDownloaded, name)
        }
    }

    func testOrdinaryItemsInheritPolicy() {
        let cases: [(name: String, id: ItemID)] = [
            ("shared", .shared("statsig")),
            ("private", .priv("projects/x.jsonl")),
            ("root", .root),
        ]
        for c in cases {
            XCTAssertEqual(item(c.id).contentPolicy, .inherited, c.name)
        }
    }

    func testItemVersionCarriesVersionHex() {
        let it = item(.computed(".claude.json"))
        XCTAssertEqual(it.itemVersion.contentVersion, Data("00".utf8))
    }
}
