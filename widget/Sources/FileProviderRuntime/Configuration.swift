import FileProvider
import Foundation
import FuseKit

enum CCPoolFileProviderConfiguration {
  static let appGroupIdentifier = "SXKCTF23Q2.ccp"
  static let appGroupEndpoint = try! CatalogAppGroupEndpoint(
    identifier: appGroupIdentifier,
    socketLeaf: "fusekit.sock"
  )
  static let brokerTeamIdentifier = "SXKCTF23Q2"
  static let brokerSigningIdentifier = "com.yasyf.cc-pool.status"
  static let extensionTeamIdentifier = "SXKCTF23Q2"
  static let extensionSigningIdentifier = "com.yasyf.cc-pool.status.fileprovider"

  static var realHome: String {
    if let pw = getpwuid(getuid()), let dir = pw.pointee.pw_dir {
      return String(cString: dir)
    }
    return NSHomeDirectory()
  }

  static var holderSocketPath: String {
    URL(fileURLWithPath: realHome, isDirectory: true)
      .appendingPathComponent(".cc-pool/fusekit/fusekit.sock", isDirectory: false)
      .path
  }

  static var brokerConfiguration: CatalogBroker.Configuration {
    .init(
      appGroupEndpoint: appGroupEndpoint,
      daemonSocketPath: holderSocketPath,
      extensionTeamIdentifier: extensionTeamIdentifier,
      extensionSigningIdentifier: extensionSigningIdentifier
    )
  }

  static func makeRuntime(
    binding: CatalogFileProviderBinding
  ) throws -> CatalogFileProviderRuntime {
    let transport = try SocketCatalogTransport(
      appGroupEndpoint: appGroupEndpoint,
      brokerTeamIdentifier: brokerTeamIdentifier,
      brokerSigningIdentifier: brokerSigningIdentifier,
      brokerRequiredEntitlements: [:]
    )
    return CatalogFileProviderRuntime(
      binding: binding,
      client: CatalogClient(transport: transport)
    )
  }
}
