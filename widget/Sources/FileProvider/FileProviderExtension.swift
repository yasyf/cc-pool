import FileProvider
import FuseKit

final class FileProviderExtension: CatalogReplicatedExtension, @unchecked Sendable {
    override class func makeRuntime(
        for _: NSFileProviderDomain,
        binding: CatalogFileProviderBinding
    ) throws -> CatalogFileProviderRuntime {
        try CCPoolFileProviderConfiguration.makeRuntime(binding: binding)
    }
}
