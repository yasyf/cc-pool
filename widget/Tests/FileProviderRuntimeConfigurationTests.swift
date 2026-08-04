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

  func testBrokerConfigurationBindsTheSignedTopology() {
    let broker = CCPoolFileProviderConfiguration.brokerConfiguration
    XCTAssertEqual(broker.appGroupEndpoint, CCPoolFileProviderConfiguration.appGroupEndpoint)
    XCTAssertEqual(broker.daemonSocketPath, CCPoolFileProviderConfiguration.holderSocketPath)
  }
}
