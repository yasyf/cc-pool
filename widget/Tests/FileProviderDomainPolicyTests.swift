import XCTest

final class FileProviderDomainPolicyTests: XCTestCase {
    func testDomainsDoNotAdvertiseUnsupportedTrash() {
        let domain = FileProviderDomainPolicy.make("acct-17")

        XCTAssertEqual(domain.identifier.rawValue, "acct-17")
        XCTAssertEqual(domain.displayName, "acct-17")
        XCTAssertTrue(domain.isHidden)
        XCTAssertFalse(domain.supportsSyncingTrash)
    }
}
