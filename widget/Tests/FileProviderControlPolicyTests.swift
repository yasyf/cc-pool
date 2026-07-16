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
