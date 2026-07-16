import FileProvider
import XCTest

final class FileProviderEnumerationPolicyTests: XCTestCase {
    func testTrashFailsAsUnsupportedInsteadOfDeleted() throws {
        let error = try XCTUnwrap(
            FileProviderEnumerationPolicy.unsupportedContainerError(for: .trashContainer))

        XCTAssertEqual(error.domain, NSCocoaErrorDomain)
        XCTAssertEqual(error.code, NSFeatureUnsupportedError)
        XCTAssertNotEqual(error.domain, NSFileProviderError.errorDomain)
        XCTAssertNotEqual(error.code, NSFileProviderError.Code.noSuchItem.rawValue)
    }

    func testSupportedContainersHaveNoPolicyError() {
        XCTAssertNil(FileProviderEnumerationPolicy.unsupportedContainerError(for: .rootContainer))
        XCTAssertNil(FileProviderEnumerationPolicy.unsupportedContainerError(for: .workingSet))
        XCTAssertNil(FileProviderEnumerationPolicy.unsupportedContainerError(
            for: NSFileProviderItemIdentifier("private:projects")))
    }
}
