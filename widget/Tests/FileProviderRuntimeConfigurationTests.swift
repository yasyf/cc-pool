import XCTest

final class FileProviderRuntimeConfigurationTests: XCTestCase {
  func testSignedTopologyIsExact() {
    XCTAssertEqual(CCPoolFileProviderConfiguration.appGroupEndpoint.identifier, "SXKCTF23Q2.ccp")
    XCTAssertEqual(CCPoolFileProviderConfiguration.appGroupEndpoint.socketLeaf, "fusekit.sock")
    XCTAssertEqual(CCPoolFileProviderConfiguration.brokerTeamIdentifier, "SXKCTF23Q2")
    XCTAssertEqual(
      CCPoolFileProviderConfiguration.brokerSigningIdentifier,
      "com.yasyf.cc-pool.status"
    )
    XCTAssertEqual(CCPoolFileProviderConfiguration.extensionTeamIdentifier, "SXKCTF23Q2")
    XCTAssertEqual(
      CCPoolFileProviderConfiguration.extensionSigningIdentifier,
      "com.yasyf.cc-pool.status.fileprovider"
    )
    XCTAssertEqual(
      CCPoolFileProviderConfiguration.holderSocketPath,
      URL(fileURLWithPath: CCPoolFileProviderConfiguration.realHome, isDirectory: true)
        .appendingPathComponent(".cc-pool/fusekit/fusekit.sock")
        .path
    )

    let broker = CCPoolFileProviderConfiguration.brokerConfiguration
    XCTAssertEqual(broker.appGroupEndpoint, CCPoolFileProviderConfiguration.appGroupEndpoint)
    XCTAssertEqual(broker.daemonSocketPath, CCPoolFileProviderConfiguration.holderSocketPath)
    XCTAssertEqual(
      broker.extensionTeamIdentifier, CCPoolFileProviderConfiguration.extensionTeamIdentifier)
    XCTAssertEqual(
      broker.extensionSigningIdentifier,
      CCPoolFileProviderConfiguration.extensionSigningIdentifier
    )
  }
}
