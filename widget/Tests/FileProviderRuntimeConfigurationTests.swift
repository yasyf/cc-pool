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
    XCTAssertEqual(CCPoolFileProviderConfiguration.brokerNoProgressTimeout, 5)
  }

  func testNormalAppStartupDoesNotConstructBrokerConfiguration() throws {
    XCTAssertNil(
      try CCPoolFileProviderConfiguration.brokerConfigurationIfRequested(
        arguments: ["CCPoolStatus"],
        environment: [:]
      )
    )
  }

  func testBrokerConfigurationUsesExactLaunchBuild() throws {
    let broker = try XCTUnwrap(
      CCPoolFileProviderConfiguration.brokerConfigurationIfRequested(
        arguments: [
          "CCPoolStatus",
          "--fusekit-broker-child",
          "--fusekit-daemon-socket",
          CCPoolFileProviderConfiguration.holderSocketPath,
        ],
        environment: ["FUSEKIT_BUILD_ID": "v0.63.0 (abc1234)"],
      )
    )

    XCTAssertEqual(broker.appGroupEndpoint, CCPoolFileProviderConfiguration.appGroupEndpoint)
    XCTAssertEqual(broker.daemonSocketPath, CCPoolFileProviderConfiguration.holderSocketPath)
    XCTAssertEqual(broker.expectedRuntimeBuild, "v0.63.0 (abc1234)")
    XCTAssertEqual(
      broker.noProgressTimeout,
      CCPoolFileProviderConfiguration.brokerNoProgressTimeout
    )
  }

  func testBrokerConfigurationRejectsMissingLaunchBuild() {
    XCTAssertThrowsError(
      try CCPoolFileProviderConfiguration.brokerConfigurationIfRequested(
        arguments: [
          "CCPoolStatus",
          "--fusekit-broker-child",
          "--fusekit-daemon-socket",
          CCPoolFileProviderConfiguration.holderSocketPath,
        ],
        environment: [:]
      )
    ) { error in
      XCTAssertEqual(error as? CCPoolFileProviderConfigurationError, .missingRuntimeBuild)
    }
  }
}
