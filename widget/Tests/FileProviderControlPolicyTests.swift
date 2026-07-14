import CoreServices
import XCTest

final class FileProviderControlPolicyTests: XCTestCase {
    func testPresenceIsTriState() {
        XCTAssertEqual(FileProviderControlPolicy.presence(result: 0, errno: EIO), .present)
        XCTAssertEqual(FileProviderControlPolicy.presence(result: -1, errno: ENOENT), .missing)
        XCTAssertEqual(FileProviderControlPolicy.presence(result: -1, errno: EIO), .failed(EIO))
    }

    func testMaterializationPreservesMissingAndUnexpectedErrors() {
        XCTAssertEqual(
            FileProviderControlPolicy.materialization(
                result: 0, errno: 0, flags: 0, datalessFlag: 0x10),
            .materialized)
        XCTAssertEqual(
            FileProviderControlPolicy.materialization(
                result: 0, errno: 0, flags: 0x10, datalessFlag: 0x10),
            .dataless)
        XCTAssertEqual(
            FileProviderControlPolicy.materialization(
                result: -1, errno: ENOENT, flags: 0, datalessFlag: 0x10),
            .missing)
        XCTAssertEqual(
            FileProviderControlPolicy.materialization(
                result: -1, errno: EACCES, flags: 0, datalessFlag: 0x10),
            .failed(EACCES))
    }

    func testOnlyExactMissingItemErrorsAreRetryable() {
        let fp = "NSFileProviderErrorDomain"
        XCTAssertEqual(FileProviderControlPolicy.itemFailureDisposition(
            domain: fp, code: -1005, fileProviderDomain: fp,
            fileProviderNoSuchItem: -1005, cocoaNoSuchFile: 4), .domainMissing)
        XCTAssertEqual(FileProviderControlPolicy.itemFailureDisposition(
            domain: NSCocoaErrorDomain, code: 4, fileProviderDomain: fp,
            fileProviderNoSuchItem: -1005, cocoaNoSuchFile: 4), .domainMissing)
        for candidate in [(fp, -1006), (NSPOSIXErrorDomain, Int(EIO)), (NSCocoaErrorDomain, 257)] {
            XCTAssertEqual(FileProviderControlPolicy.itemFailureDisposition(
                domain: candidate.0, code: candidate.1, fileProviderDomain: fp,
                fileProviderNoSuchItem: -1005, cocoaNoSuchFile: 4), .fleetFailure)
        }
    }

    func testRetrySignalsOnceAndBacksOffToCap() {
        var policy = MissingItemRetryPolicy()
        let retries = (0..<7).map { _ in policy.next() }
        XCTAssertEqual(retries.map(\.shouldSignal), [true, false, false, false, false, false, false])
        XCTAssertEqual(retries.map(\.delay),
                       [200_000, 400_000, 800_000, 1_600_000, 2_000_000, 2_000_000, 2_000_000])
    }
}

final class ClaudeBaseEventPolicyTests: XCTestCase {
    private let root = "/Users/test/.claude"
    private let modified = FSEventStreamEventFlags(kFSEventStreamEventFlagItemModified)
    private let metadata = FSEventStreamEventFlags(kFSEventStreamEventFlagItemInodeMetaMod)
    private let created = FSEventStreamEventFlags(kFSEventStreamEventFlagItemCreated)
    private let removed = FSEventStreamEventFlags(kFSEventStreamEventFlagItemRemoved)
    private let renamed = FSEventStreamEventFlags(kFSEventStreamEventFlagItemRenamed)

    func testNestedChurnNeverSignals() {
        for path in ["debug/latest", "plugins/cache/item", "projects/p/transcript.jsonl"] {
            XCTAssertNil(ClaudeBaseEventPolicy.reason(
                claudeRoot: root, path: root + "/" + path, flags: created))
        }
    }

    func testTopLevelEntriesSignalOnlyForStructuralEvents() {
        let path = root + "/commands"
        XCTAssertNil(ClaudeBaseEventPolicy.reason(claudeRoot: root, path: path, flags: modified))
        XCTAssertNil(ClaudeBaseEventPolicy.reason(claudeRoot: root, path: path, flags: metadata))
        for flags in [created, removed, renamed] {
            XCTAssertEqual(ClaudeBaseEventPolicy.reason(
                claudeRoot: root, path: path, flags: flags), "base-root-entry")
        }
    }

    func testSettingsAndAtomicSiblingsUseNarrowRules() {
        XCTAssertEqual(ClaudeBaseEventPolicy.reason(
            claudeRoot: root, path: root + "/settings.json", flags: modified), "base-settings")
        XCTAssertNil(ClaudeBaseEventPolicy.reason(
            claudeRoot: root, path: root + "/settings.json.tmp.1", flags: modified))
        XCTAssertEqual(ClaudeBaseEventPolicy.reason(
            claudeRoot: root, path: root + "/settings.json.tmp.1", flags: renamed),
                       "base-settings-replace")
    }

    func testRootMetadataIsIgnoredButRenameAndRescanSignal() {
        XCTAssertNil(ClaudeBaseEventPolicy.reason(claudeRoot: root, path: root, flags: metadata))
        XCTAssertEqual(ClaudeBaseEventPolicy.reason(
            claudeRoot: root, path: root, flags: renamed), "base-root")
        XCTAssertEqual(ClaudeBaseEventPolicy.reason(
            claudeRoot: root, path: root + "/nested/file",
            flags: FSEventStreamEventFlags(kFSEventStreamEventFlagMustScanSubDirs)),
                       "fsevents-rescan")
    }
}

final class CanonicalConfigPolicyTests: XCTestCase {
    private let first = CanonicalConfigFingerprint(
        device: 1, inode: 2, size: 3,
        modifiedSeconds: 4, modifiedNanoseconds: 5,
        changedSeconds: 6, changedNanoseconds: 7)

    func testFingerprintTransitions() {
        XCTAssertFalse(CanonicalConfigPolicy.changed(from: first, to: first))
        XCTAssertTrue(CanonicalConfigPolicy.changed(from: nil, to: first))
        XCTAssertTrue(CanonicalConfigPolicy.changed(from: first, to: nil))
        XCTAssertTrue(CanonicalConfigPolicy.changed(
            from: first,
            to: CanonicalConfigFingerprint(
                device: 1, inode: 2, size: 3,
                modifiedSeconds: 4, modifiedNanoseconds: 8,
                changedSeconds: 6, changedNanoseconds: 7)))
    }

    func testUpgradeSignalsExactlyWhenBuildChanges() {
        XCTAssertTrue(FileProviderRehydratePolicy.shouldSignal(lastBuild: nil, currentBuild: "1+10"))
        XCTAssertFalse(FileProviderRehydratePolicy.shouldSignal(lastBuild: "1+10", currentBuild: "1+10"))
        XCTAssertTrue(FileProviderRehydratePolicy.shouldSignal(lastBuild: "1+10", currentBuild: "1+11"))
    }
}
