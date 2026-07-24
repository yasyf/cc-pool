import FileProvider
import Foundation
import FuseKit

enum CCPoolFileProviderConfiguration {
  static let appGroupIdentifier = "SXKCTF23Q2.ccp"
  static let appGroupEndpoint = try! CatalogAppGroupEndpoint(
    identifier: appGroupIdentifier,
    socketLeaf: "fusekit.sock"
  )
  static let brokerNoProgressTimeout: TimeInterval = 5

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

  static func brokerConfigurationIfRequested(
    arguments: [String] = ProcessInfo.processInfo.arguments,
    environment: [String: String] = ProcessInfo.processInfo.environment
  ) throws -> CatalogBroker.Configuration? {
    guard try CatalogBrokerChildMode.parse(arguments: arguments) != nil else { return nil }
    guard let runtimeBuild = environment["FUSEKIT_BUILD_ID"], !runtimeBuild.isEmpty else {
      throw CCPoolFileProviderConfigurationError.missingRuntimeBuild
    }
    return .init(
      appGroupEndpoint: appGroupEndpoint,
      daemonSocketPath: holderSocketPath,
      expectedRuntimeBuild: runtimeBuild,
      noProgressTimeout: brokerNoProgressTimeout
    )
  }

  static func makeRuntime(
    binding: CatalogFileProviderBinding
  ) throws -> CatalogFileProviderRuntime {
    let transport = try SocketCatalogTransport(appGroupEndpoint: appGroupEndpoint)
    return CatalogFileProviderRuntime(
      binding: binding,
      client: CatalogClient(transport: transport)
    )
  }
}

enum CCPoolFileProviderConfigurationError: Error, Equatable {
  case brokerChildNotRecognized
  case missingRuntimeBuild
}
