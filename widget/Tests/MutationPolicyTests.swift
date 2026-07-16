import FileProvider
import UniformTypeIdentifiers
import XCTest

final class MutationPolicyTests: XCTestCase {
    private func item(_ id: ItemID) -> FPItem {
        FPItem(id: id, filename: "f", contentType: .json,
               capabilities: [.allowsReading], versionHex: "00")
    }

    private func operations(
        write: @escaping (String, Data) throws -> Void = { _, _ in },
        persistPrivate: @escaping (String, Data) throws -> Void = { _, _ in },
        removeStaging: @escaping (String) throws -> Void = { _ in },
        lookup: @escaping (NSFileProviderItemIdentifier) throws -> FPItem,
        announce: @escaping () throws -> Void = {}
    ) -> SynthMutationCoordinator.Operations {
        .init(write: write, persistPrivate: persistPrivate,
              removeStaging: removeStaging, lookup: lookup, announce: announce)
    }

    func testSettingsTemporarySiblingsArePrivateAndHidden() {
        for name in ["settings.json.tmp.1", "settings.json.tmp.",
                     "settings.json.tmp.abcdef"] {
            XCTAssertTrue(RootItemPolicy.isSettingsStaging(name), name)
            XCTAssertEqual(RootItemPolicy.classification(name: name, bridgeKind: "symlink"),
                           "private", name)
            XCTAssertFalse(RootItemPolicy.isEnumerated(name), name)
        }
    }

    func testOnlyExactSettingsTemporaryPrefixIsStaging() {
        for name in ["settings.json", "settings.json.tmp", "xsettings.json.tmp.1"] {
            XCTAssertFalse(RootItemPolicy.isSettingsStaging(name), name)
            XCTAssertEqual(RootItemPolicy.classification(name: name, bridgeKind: "symlink"),
                           "symlink", name)
            XCTAssertTrue(RootItemPolicy.isEnumerated(name), name)
        }
    }

    func testSynthCreateWithContentsAlwaysReplaces() {
        XCTAssertEqual(SynthCreateAction.decide(mayAlreadyExist: false, hasContents: true),
                       .replace)
        XCTAssertEqual(SynthCreateAction.decide(mayAlreadyExist: true, hasContents: true),
                       .replace)
    }

    func testSynthReimportWithoutContentsAdoptsExistingItem() {
        XCTAssertEqual(SynthCreateAction.decide(mayAlreadyExist: true, hasContents: false),
                       .adopt)
    }

    func testSynthCreateWithoutContentsCannotEmptyClobber() {
        XCTAssertEqual(SynthCreateAction.decide(mayAlreadyExist: false, hasContents: false),
                       .rejectMissingContents)
    }

    func testNilReimportAdoptsWithoutBridgeWrite() throws {
        var writes = 0
        var lookedUp: NSFileProviderItemIdentifier?
        let coordinator = SynthMutationCoordinator(operations: operations(
            write: { _, _ in writes += 1 },
            lookup: {
                lookedUp = $0
                return self.item(.computed("settings.json"))
            }))

        let result = try coordinator.adopt(name: "settings.json")

        XCTAssertEqual(writes, 0)
        XCTAssertEqual(lookedUp, ItemID.computed("settings.json").identifier)
        XCTAssertEqual(result.item.itemIdentifier, ItemID.computed("settings.json").identifier)
        XCTAssertFalse(result.shouldFetchContent)
    }

    func testTempCommitCleansStagingAndReusesComputedIdentifier() throws {
        var events: [String] = []
        let staging = "/private/settings.json.tmp.1"
        let coordinator = SynthMutationCoordinator(operations: operations(
            write: { name, data in
                events.append("write:\(name):\(String(decoding: data, as: UTF8.self))")
            },
            removeStaging: { events.append("remove:\($0)") },
            lookup: {
                events.append("lookup:\($0.rawValue)")
                return self.item(.computed("settings.json"))
            },
            announce: { events.append("announce") }))

        let result = try coordinator.commit(
            name: "settings.json", data: Data("new".utf8), removing: staging)

        XCTAssertEqual(result.item.itemIdentifier, ItemID.computed("settings.json").identifier)
        XCTAssertTrue(result.shouldFetchContent)
        XCTAssertEqual(events, [
            "write:settings.json:new",
            "remove:\(staging)",
            "lookup:computed:settings.json",
            "announce",
        ])
    }

    func testComputedMutationCanonicalizationRequiresFetch() throws {
        let coordinator = SynthMutationCoordinator(operations: operations(
            lookup: { _ in self.item(.computed(".claude.json")) }))

        let result = try coordinator.commit(
            name: ".claude.json", data: Data("{}".utf8), removing: nil)

        XCTAssertTrue(result.shouldFetchContent)
        XCTAssertFalse(MutationResult.unchanged(item(.priv("x"))).shouldFetchContent)
    }

    func testClaudeJSONCommitPersistsPrivatelyBeforeLocalAnnouncement() throws {
        var events: [String] = []
        let coordinator = SynthMutationCoordinator(operations: operations(
            write: { _, _ in events.append("write") },
            persistPrivate: { _, _ in events.append("persist-private") },
            lookup: { _ in
                events.append("lookup")
                return self.item(.computed(".claude.json"))
            },
            announce: { events.append("announce-local") }))

        _ = try coordinator.commit(
            name: ".claude.json", data: Data("{}".utf8), removing: nil)

        XCTAssertEqual(events, ["write", "persist-private", "lookup", "announce-local"])
    }

    func testComputedDeletionInvalidatesBothAnchorsAndCompletesBeforeSignals() throws {
        let root = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("ccp-mutation-tests-" + UUID().uuidString, isDirectory: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let anchors = AnchorStore(domainID: "acct-test", root: root)
        let computed = ItemID.computed("settings.json").identifier
        let other = ItemID.priv("x").identifier.rawValue
        for container in ComputedReannouncement.containers {
            anchors.save(container, [computed.rawValue: "old", other: "keep"])
        }
        let reannouncement = ComputedReannouncement(computedName: "settings.json")

        try reannouncement.invalidate(anchors)
        for container in ComputedReannouncement.containers {
            XCTAssertEqual(anchors.load(container), [other: "keep"], container.rawValue)
        }

        var events: [String] = []
        reannouncement.completeDeletion(
            completion: { events.append("completion") },
            signal: { events.append($0.rawValue) })
        XCTAssertEqual(events, ["completion", NSFileProviderItemIdentifier.rootContainer.rawValue,
                                NSFileProviderItemIdentifier.workingSet.rawValue])
    }
}
