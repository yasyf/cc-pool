import XCTest

final class FileProviderRuntimeConfigurationTests: XCTestCase {
  func testSignedTopologyIsExact() {
    XCTAssertEqual(CCPoolFileProviderConfiguration.appGroupEndpoint.identifier, "SXKCTF23Q2.ccp")
    XCTAssertEqual(CCPoolFileProviderConfiguration.appGroupEndpoint.socketLeaf, "fusekit.sock")
    XCTAssertEqual(
      CCPoolFileProviderConfiguration.holderSocketPath,
      URL(fileURLWithPath: CCPoolFileProviderConfiguration.realHome, isDirectory: true)
        .appendingPathComponent(".cc-pool/fusekit/fusekit.sock")
        .path
    )
  }

  func testBrokerConfigurationPinsExactBuildAndTimeout() throws {
    let buildID = "  exact-build-id  "
    let broker = try CCPoolFileProviderConfiguration.makeBrokerConfiguration(
      environment: ["FUSEKIT_BUILD_ID": buildID]
    )
    XCTAssertEqual(broker.appGroupEndpoint, CCPoolFileProviderConfiguration.appGroupEndpoint)
    XCTAssertEqual(broker.daemonSocketPath, CCPoolFileProviderConfiguration.holderSocketPath)
    XCTAssertEqual(broker.expectedRuntimeBuild, buildID)
    XCTAssertEqual(broker.noProgressTimeout, 75)
  }

  func testBrokerConfigurationRejectsMissingOrEmptyBuild() {
    for environment in [[:], ["FUSEKIT_BUILD_ID": ""]] {
      XCTAssertThrowsError(
        try CCPoolFileProviderConfiguration.makeBrokerConfiguration(environment: environment)
      ) { error in
        XCTAssertEqual(
          error as? CCPoolFileProviderConfiguration.ConfigurationError,
          .missingFuseKitBuildID
        )
      }
    }
  }
}
