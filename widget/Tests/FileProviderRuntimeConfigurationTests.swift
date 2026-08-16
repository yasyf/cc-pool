import XCTest

final class FileProviderRuntimeConfigurationTests: XCTestCase {
  func testSignedTopologyIsExact() {
    XCTAssertEqual(CCPoolFileProviderConfiguration.appGroupEndpoint.identifier, "SXKCTF23Q2.ccp")
    XCTAssertEqual(CCPoolFileProviderConfiguration.appGroupEndpoint.socketLeaf, "fusekit.sock")
  }

  func testBrokerConfigurationBindsTheSignedTopology() {
    let broker = CCPoolFileProviderConfiguration.brokerConfiguration
    XCTAssertEqual(broker.appGroupEndpoint, CCPoolFileProviderConfiguration.appGroupEndpoint)
  }
}
