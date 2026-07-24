import FileProvider
import Foundation
import FuseKit

enum CCPoolFileProviderConfiguration {
  enum ConfigurationError: Error, Equatable, Sendable {
    case missingFuseKitBuildID
  }

  static let appGroupIdentifier = "SXKCTF23Q2.ccp"
  static let appGroupEndpoint = try! CatalogAppGroupEndpoint(
    identifier: appGroupIdentifier,
    socketLeaf: "fusekit.sock"
  )
  static let brokerNoProgressTimeout: TimeInterval = 75

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
    get throws {
      try makeBrokerConfiguration(environment: ProcessInfo.processInfo.environment)
    }
  }

  static func makeBrokerConfiguration(
    environment: [String: String]
  ) throws -> CatalogBroker.Configuration {
    guard let buildID = environment["FUSEKIT_BUILD_ID"], !buildID.isEmpty else {
      throw ConfigurationError.missingFuseKitBuildID
    }
    return .init(
      appGroupEndpoint: appGroupEndpoint,
      daemonSocketPath: holderSocketPath,
      expectedRuntimeBuild: buildID,
      noProgressTimeout: brokerNoProgressTimeout
    )
  }

  static func makeRuntime(
    domain: NSFileProviderDomain,
    binding: CatalogFileProviderBinding
  ) throws -> CatalogFileProviderRuntime {
    let transport = try SocketCatalogTransport(appGroupEndpoint: appGroupEndpoint)
    return try CatalogFileProviderRuntime(
      domain: domain,
      binding: binding,
      client: CatalogClient(transport: transport),
      mutationDispositionPolicy: CCPoolFileProviderMutationDispositionPolicy()
    )
  }
}
