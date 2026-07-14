import FileProvider
import Foundation
import UniformTypeIdentifiers

/// Immutable NSFileProviderItem snapshot. versionHex feeds both the item
/// version and the enumerators' sync-anchor lines.
final class FPItem: NSObject, NSFileProviderItem {
    let itemIdentifier: NSFileProviderItemIdentifier
    let parentItemIdentifier: NSFileProviderItemIdentifier
    let filename: String
    let contentType: UTType
    let capabilities: NSFileProviderItemCapabilities
    let documentSize: NSNumber?
    let symlinkTargetPath: String?
    let versionHex: String
    /// Computed documents pin .downloadEagerlyAndKeepDownloaded so the OS
    /// refreshes materialized replicas on version change instead of freezing
    /// lazy, untouched copies for days; everything else inherits.
    let contentPolicy: NSFileProviderContentPolicy

    var itemVersion: NSFileProviderItemVersion {
        let v = Data(versionHex.utf8)
        return NSFileProviderItemVersion(contentVersion: v, metadataVersion: v)
    }

    init(id: ItemID, filename: String, contentType: UTType,
         capabilities: NSFileProviderItemCapabilities,
         documentSize: NSNumber? = nil, symlinkTargetPath: String? = nil,
         versionHex: String) {
        itemIdentifier = id.identifier
        parentItemIdentifier = id.parent
        self.filename = filename
        self.contentType = contentType
        self.capabilities = capabilities
        self.documentSize = documentSize
        self.symlinkTargetPath = symlinkTargetPath
        self.versionHex = versionHex
        if case .computed = id {
            contentPolicy = .downloadEagerlyAndKeepDownloaded
        } else {
            contentPolicy = .inherited
        }
    }
}
