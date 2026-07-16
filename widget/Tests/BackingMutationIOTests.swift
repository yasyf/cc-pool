import XCTest

final class BackingMutationIOTests: XCTestCase {
    private var tmp: URL!

    override func setUpWithError() throws {
        tmp = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("ccp-backing-io-" + UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: tmp, withIntermediateDirectories: true)
    }

    override func tearDownWithError() throws {
        try? FileManager.default.removeItem(at: tmp)
    }

    func testReplaceIsAtomicAndPrivate() throws {
        let path = tmp.appendingPathComponent("settings.json")
        try Data("old".utf8).write(to: path)

        try BackingMutationIO.replace(path: path.path, data: Data("new".utf8))

        XCTAssertEqual(try Data(contentsOf: path), Data("new".utf8))
        let attrs = try FileManager.default.attributesOfItem(atPath: path.path)
        XCTAssertEqual((attrs[.posixPermissions] as? NSNumber)?.intValue, 0o600)
        XCTAssertFalse(try FileManager.default.contentsOfDirectory(atPath: tmp.path)
            .contains(where: { $0.hasPrefix("._ccp-tmp-") }))
    }

    func testRenameReplacesDestination() throws {
        let src = tmp.appendingPathComponent("src")
        let dst = tmp.appendingPathComponent("dst")
        try Data("source".utf8).write(to: src)
        try Data("stale".utf8).write(to: dst)

        try BackingMutationIO.rename(from: src.path, to: dst.path)

        XCTAssertFalse(FileManager.default.fileExists(atPath: src.path))
        XCTAssertEqual(try Data(contentsOf: dst), Data("source".utf8))
    }

    func testRemoveDeletesDirectoriesRecursively() throws {
        let dir = tmp.appendingPathComponent("projects", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        try Data("x".utf8).write(to: dir.appendingPathComponent("entry"))

        try BackingMutationIO.remove(dir.path)

        XCTAssertFalse(FileManager.default.fileExists(atPath: dir.path))
    }

    func testCloneOrCopyCopiesBytes() throws {
        let src = tmp.appendingPathComponent("src")
        let dst = tmp.appendingPathComponent("dst")
        try Data("contents".utf8).write(to: src)

        try BackingMutationIO.cloneOrCopy(from: src.path, to: dst.path)

        XCTAssertEqual(try Data(contentsOf: dst), Data("contents".utf8))
    }

    func testRenameMissingSourceFailsLoudly() {
        XCTAssertThrowsError(try BackingMutationIO.rename(
            from: tmp.appendingPathComponent("missing").path,
            to: tmp.appendingPathComponent("dst").path)) { error in
            let error = error as NSError
            XCTAssertEqual(error.domain, NSPOSIXErrorDomain)
            XCTAssertEqual(error.code, Int(ENOENT))
        }
    }

    func testExtensionNeverRequestsNestedFileCoordination() throws {
        let sourceDir = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/FileProvider", isDirectory: true)
        let files = try FileManager.default.contentsOfDirectory(
            at: sourceDir, includingPropertiesForKeys: nil)
            .filter { $0.pathExtension == "swift" }

        for file in files {
            let source = try String(contentsOf: file, encoding: .utf8)
            XCTAssertNil(
                source.range(of: #"NSFileCoordinator\s*\("#, options: .regularExpression),
                "\(file.lastPathComponent) must not request a nested file coordination claim")
        }
    }
}
