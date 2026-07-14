import XCTest

final class ItemVersionsTests: XCTestCase {
    private var tmp: URL!

    override func setUpWithError() throws {
        tmp = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("ccp-fp-tests-" + UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: tmp, withIntermediateDirectories: true)
    }

    override func tearDownWithError() throws {
        // Restore search permission so removal succeeds after the EACCES case.
        if let sub = try? FileManager.default.contentsOfDirectory(atPath: tmp.path) {
            for name in sub {
                chmod(tmp.appendingPathComponent(name).path, 0o700)
            }
        }
        try? FileManager.default.removeItem(at: tmp)
    }

    func testManifestVersionPreferred() throws {
        // A non-empty manifest version wins outright — the freshness paths are
        // never statted (this one would throw EACCES if they were).
        let denied = try deniedPath()
        XCTAssertEqual(try synthVersionHex(manifestVersion: "abc123", freshness: [denied]), "abc123")
    }

    func testEmptyManifestVersionFallsBackToStatHash() throws {
        let file = tmp.appendingPathComponent("gate.json")
        try Data("x".utf8).write(to: file)
        let want = try ItemVersions.synth(freshness: [file.path])
        XCTAssertEqual(try synthVersionHex(manifestVersion: "", freshness: [file.path]), want)
        XCTAssertFalse(want.isEmpty)
    }

    func testAbsentPathIsValidNotAnError() throws {
        let missing = tmp.appendingPathComponent("never-created").path
        let v1 = try ItemVersions.synth(freshness: [missing])
        let v2 = try ItemVersions.synth(freshness: [missing])
        XCTAssertEqual(v1, v2, "absent must hash deterministically")

        let file = tmp.appendingPathComponent("appears.json")
        try Data("y".utf8).write(to: file)
        XCTAssertNotEqual(try ItemVersions.synth(freshness: [file.path]), v1,
                          "a file appearing must move the version")
    }

    func testNonENOENTErrnoThrows() throws {
        let denied = try deniedPath()
        XCTAssertThrowsError(try ItemVersions.synth(freshness: [denied])) { error in
            let e = error as NSError
            XCTAssertEqual(e.domain, NSPOSIXErrorDomain)
            XCTAssertEqual(e.code, Int(EACCES), "the errno must ride the error, not vanish")
        }
        XCTAssertThrowsError(try BackingStat.lstat(denied),
                             "BackingStat must fail loud on non-ENOENT errno")
    }

    func testLstatENOENTIsAbsent() throws {
        let st = try BackingStat.lstat(tmp.appendingPathComponent("gone").path)
        XCTAssertFalse(st.exists)
        XCTAssertEqual(st.size, -1)
        XCTAssertEqual(st.mtimeNS, -1)
    }

    func testReaddirNamesMissingDirIsEmpty() throws {
        XCTAssertEqual(try readdirNames(tmp.appendingPathComponent("no-dir").path), [])
    }

    func testReaddirNamesDeniedDirThrows() throws {
        let sub = tmp.appendingPathComponent("locked-dir")
        try FileManager.default.createDirectory(at: sub, withIntermediateDirectories: true)
        try Data().write(to: sub.appendingPathComponent("f"))
        guard chmod(sub.path, 0o000) == 0 else { throw XCTSkip("chmod failed") }
        XCTAssertThrowsError(try readdirNames(sub.path),
                             "an unreadable dir must fail the listing, never read as empty")
    }

    /// A path whose lstat fails EACCES: a file under a no-search-permission dir.
    private func deniedPath() throws -> String {
        let sub = tmp.appendingPathComponent("denied-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: sub, withIntermediateDirectories: true)
        let inner = sub.appendingPathComponent("file")
        try Data().write(to: inner)
        guard chmod(sub.path, 0o000) == 0 else { throw XCTSkip("chmod failed") }
        guard getuid() != 0 else { throw XCTSkip("EACCES cases need a non-root runner") }
        return inner.path
    }
}
