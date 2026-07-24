import FileProvider
import FuseKit
import XCTest

final class MutationDispositionPolicyTests: XCTestCase {
  func testExactTopLevelAtomicTemporaryFamiliesArePrivateStaging() {
    XCTAssertEqual(
      CCPoolFileProviderMutationDispositionPolicy.privateStagingPrefixes,
      [
        ".claude.json.tmp.",
        "settings.json.tmp.",
        ".credentials.json.tmp.",
        ".last-update-result.json.tmp.",
        "remote-settings.json.tmp.",
        "mcp-needs-auth-cache.json.tmp.",
        "stats-cache.json.tmp.",
        "policy-limits.json.tmp.",
      ]
    )
    for prefix in CCPoolFileProviderMutationDispositionPolicy.privateStagingPrefixes {
      XCTAssertEqual(
        disposition(parent: .rootContainer, filename: prefix + "A1B2"),
        .privateStaging,
        prefix
      )
      XCTAssertEqual(
        disposition(parent: .rootContainer, filename: prefix.uppercased() + "A1B2"),
        .privateStaging,
        prefix
      )
    }
  }

  func testCanonicalLockAndNestedNamesRemainNamespaceObjects() {
    for filename in [
      ".claude.json",
      "settings.json",
      ".credentials.json",
      ".last-update-result.json",
      "remote-settings.json",
      "mcp-needs-auth-cache.json",
      "stats-cache.json",
      "policy-limits.json",
      ".storage-write.lock",
      ".oauth_refresh.lock",
    ] {
      XCTAssertEqual(
        disposition(parent: .rootContainer, filename: filename),
        .namespace,
        filename
      )
    }
    for prefix in CCPoolFileProviderMutationDispositionPolicy.privateStagingPrefixes {
      XCTAssertEqual(
        disposition(
          parent: NSFileProviderItemIdentifier("nested"),
          filename: prefix + "A1B2"
        ),
        .namespace,
        prefix
      )
    }
  }

  func testPrefixRequiresSuffixAfterTemporaryMarker() {
    for prefix in CCPoolFileProviderMutationDispositionPolicy.privateStagingPrefixes {
      XCTAssertEqual(
        disposition(parent: .rootContainer, filename: String(prefix.dropLast())),
        .namespace,
        prefix
      )
    }
  }

  private func disposition(
    parent: NSFileProviderItemIdentifier,
    filename: String
  ) -> CatalogMutationDisposition {
    CCPoolFileProviderMutationDispositionPolicy.disposition(
      parent: parent,
      filename: filename
    )
  }
}
