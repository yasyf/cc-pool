import FileProvider

enum FileProviderDomainPolicy {
    static func make(_ identifier: String) -> NSFileProviderDomain {
        let domain = NSFileProviderDomain(
            identifier: NSFileProviderDomainIdentifier(rawValue: identifier),
            displayName: identifier)
        domain.isHidden = true
        domain.supportsSyncingTrash = false
        return domain
    }
}
