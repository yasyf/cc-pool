import FileProvider
import FuseKit

final class FileProviderExtension: CatalogReplicatedExtension, @unchecked Sendable {
  override class func makeRuntime(
    for domain: NSFileProviderDomain,
    binding: CatalogFileProviderBinding
  ) throws -> CatalogFileProviderRuntime {
    try CCPoolFileProviderConfiguration.makeRuntime(domain: domain, binding: binding)
  }
}
