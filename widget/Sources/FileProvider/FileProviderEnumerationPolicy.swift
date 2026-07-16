import FileProvider
import Foundation

enum FileProviderEnumerationPolicy {
    static func unsupportedContainerError(
        for identifier: NSFileProviderItemIdentifier
    ) -> NSError? {
        guard identifier == .trashContainer else { return nil }
        return NSError(
            domain: NSCocoaErrorDomain,
            code: NSFeatureUnsupportedError,
            userInfo: [NSLocalizedDescriptionKey: "trash is not supported"])
    }
}
