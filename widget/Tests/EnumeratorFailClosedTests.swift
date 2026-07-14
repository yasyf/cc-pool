import FileProvider
import UniformTypeIdentifiers
import XCTest

/// A partial listing reads as "deleted remotely" and fileproviderd deletes
/// launch-critical replicas — so a source failure must fail the enumeration,
/// never emit items.
final class EnumeratorFailClosedTests: XCTestCase {
    private var tmp: URL!

    override func setUpWithError() throws {
        tmp = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("ccp-enum-tests-" + UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: tmp, withIntermediateDirectories: true)
    }

    override func tearDownWithError() throws {
        try? FileManager.default.removeItem(at: tmp)
    }

    private func source(root: Result<[FPItem], Error> = .success([]),
                        dir: Result<[FPItem], Error> = .success([])) -> FakeSource {
        FakeSource(anchors: AnchorStore(domainID: "test", root: tmp), root: root, dir: dir)
    }

    private static func item(_ name: String) -> FPItem {
        FPItem(id: .computed(name), filename: name, contentType: .json,
               capabilities: [.allowsReading], versionHex: "0a")
    }

    private enum Bridge: Error { case unreachable }

    func testRootEnumerateErrorNeverPartial() {
        let observer = RecordingEnumerationObserver()
        RootEnumerator(source: source(root: .failure(Bridge.unreachable)), container: .rootContainer)
            .enumerateItems(for: observer, startingAt: NSFileProviderPage("0".data(using: .utf8)!))
        observer.waitFinished(self)
        XCTAssertNotNil(observer.error, "a failed listing must fail the enumeration")
        XCTAssertTrue(observer.enumerated.isEmpty, "no items may leak from a failed listing")
        XCTAssertFalse(observer.finishedCleanly)
    }

    func testRootEnumerateSuccess() {
        let observer = RecordingEnumerationObserver()
        RootEnumerator(source: source(root: .success([Self.item(".claude.json")])),
                       container: .rootContainer)
            .enumerateItems(for: observer, startingAt: NSFileProviderPage("0".data(using: .utf8)!))
        observer.waitFinished(self)
        XCTAssertNil(observer.error)
        XCTAssertTrue(observer.finishedCleanly)
        XCTAssertEqual(observer.enumerated.map(\.filename), [".claude.json"])
    }

    func testDirEnumerateErrorNeverPartial() {
        let observer = RecordingEnumerationObserver()
        DirEnumerator(source: source(dir: .failure(Bridge.unreachable)), rel: "projects")
            .enumerateItems(for: observer, startingAt: NSFileProviderPage("0".data(using: .utf8)!))
        observer.waitFinished(self)
        XCTAssertNotNil(observer.error, "a failed dir listing must fail the enumeration")
        XCTAssertTrue(observer.enumerated.isEmpty)
    }

    func testRootChangesErrorEmitsNoDeletes() {
        let src = source(root: .failure(Bridge.unreachable))
        // Persist a prior listing so a lying "empty" diff WOULD delete it.
        src.anchors.save(.rootContainer, ["computed:.claude.json": "0a"])
        let anchor = NSFileProviderSyncAnchor(
            AnchorStore.anchor(of: ["computed:.claude.json": "0a"]))
        let observer = RecordingChangeObserver()
        RootEnumerator(source: src, container: .rootContainer)
            .enumerateChanges(for: observer, from: anchor)
        observer.waitFinished(self)
        XCTAssertNotNil(observer.error)
        XCTAssertTrue(observer.deleted.isEmpty,
                      "a failed listing must never surface as remote deletes")
        XCTAssertTrue(observer.updated.isEmpty)
    }

    func testDirChangesErrorFailsEnumeration() {
        let src = source(dir: .failure(Bridge.unreachable))
        let container = ItemID.priv("projects").identifier
        src.anchors.save(container, ["private:projects/a": "01"])
        let anchor = NSFileProviderSyncAnchor(AnchorStore.anchor(of: ["private:projects/a": "01"]))
        let observer = RecordingChangeObserver()
        DirEnumerator(source: src, rel: "projects").enumerateChanges(for: observer, from: anchor)
        observer.waitFinished(self)
        XCTAssertNotNil(observer.error)
        XCTAssertTrue(observer.deleted.isEmpty)
    }

    func testCurrentSyncAnchorNilOnFailure() {
        let exp = expectation(description: "anchor")
        var got: NSFileProviderSyncAnchor?
        RootEnumerator(source: source(root: .failure(Bridge.unreachable)), container: .rootContainer)
            .currentSyncAnchor { anchor in
                got = anchor
                exp.fulfill()
            }
        wait(for: [exp], timeout: 5)
        XCTAssertNil(got, "a failed listing must not mint a fresh anchor")
    }
}

private final class FakeSource: EnumerationSource {
    let queue = DispatchQueue(label: "ccp-enum-tests")
    let anchors: AnchorStore
    private let root: Result<[FPItem], Error>
    private let dir: Result<[FPItem], Error>

    init(anchors: AnchorStore, root: Result<[FPItem], Error>, dir: Result<[FPItem], Error>) {
        self.anchors = anchors
        self.root = root
        self.dir = dir
    }

    func rootItems() throws -> [FPItem] { try root.get() }
    func dirItems(rel: String) throws -> [FPItem] { try dir.get() }
}

private final class RecordingEnumerationObserver: NSObject, NSFileProviderEnumerationObserver {
    private let done = XCTestExpectation(description: "enumeration finished")
    private(set) var enumerated: [FPItem] = []
    private(set) var finishedCleanly = false
    private(set) var error: Error?

    func didEnumerate(_ updatedItems: [NSFileProviderItemProtocol]) {
        enumerated.append(contentsOf: updatedItems.compactMap { $0 as? FPItem })
    }

    func finishEnumerating(upTo nextPage: NSFileProviderPage?) {
        finishedCleanly = true
        done.fulfill()
    }

    func finishEnumeratingWithError(_ error: Error) {
        self.error = error
        done.fulfill()
    }

    func waitFinished(_ test: XCTestCase) {
        test.wait(for: [done], timeout: 5)
    }
}

private final class RecordingChangeObserver: NSObject, NSFileProviderChangeObserver {
    private let done = XCTestExpectation(description: "changes finished")
    private(set) var updated: [FPItem] = []
    private(set) var deleted: [NSFileProviderItemIdentifier] = []
    private(set) var error: Error?

    func didUpdate(_ updatedItems: [NSFileProviderItemProtocol]) {
        updated.append(contentsOf: updatedItems.compactMap { $0 as? FPItem })
    }

    func didDeleteItems(withIdentifiers deletedItemIdentifiers: [NSFileProviderItemIdentifier]) {
        deleted.append(contentsOf: deletedItemIdentifiers)
    }

    func finishEnumeratingChanges(upTo anchor: NSFileProviderSyncAnchor, moreComing: Bool) {
        done.fulfill()
    }

    func finishEnumeratingWithError(_ error: Error) {
        self.error = error
        done.fulfill()
    }

    func waitFinished(_ test: XCTestCase) {
        test.wait(for: [done], timeout: 5)
    }
}
