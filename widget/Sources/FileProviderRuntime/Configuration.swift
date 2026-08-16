import FileProvider
import Foundation
import FuseKit

enum CCPoolFileProviderConfiguration {
  static let appGroupIdentifier = "SXKCTF23Q2.ccp"
  static let appGroupEndpoint = try! CatalogAppGroupEndpoint(
    identifier: appGroupIdentifier,
    socketLeaf: "fusekit.sock"
  )

  static var brokerConfiguration: CatalogBroker.Configuration {
    .init(appGroupEndpoint: appGroupEndpoint)
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
