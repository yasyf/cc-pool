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
    }
}

/// Synth read outcome per computed name, keyed by the manifest freshness
/// hash: repeated item()/enumeration stats cost zero bridge reads until the
/// content version changes. Locked — the extension queue is concurrent.
final class SynthSizeCache {
    enum Outcome {
        case size(Int64)
        case unreadable
    }

    private var slots: [String: (version: String, outcome: Outcome)] = [:]
    private let lock = NSLock()

    func lookup(name: String, version: String) -> Outcome? {
        lock.lock()
        defer { lock.unlock() }
        guard let slot = slots[name], slot.version == version else { return nil }
        return slot.outcome
    }

    func store(name: String, version: String, outcome: Outcome) {
        lock.lock()
        defer { lock.unlock() }
        slots[name] = (version, outcome)
    }
}

/// The principal class (NSExtensionPrincipalClass =
/// CCPoolFileProvider.FileProviderExtension). One instance per domain; the
/// domain identifier is the account basename (acct-NN). Shared and private
/// items are served straight off the local backing trees; the two computed
/// documents ride the daemon's FP bridge.
final class FileProviderExtension: NSObject, NSFileProviderReplicatedExtension {
    let domain: NSFileProviderDomain
    let paths: DomainPaths
    let bridge = BridgeClient()
    let synthSizes = SynthSizeCache()
    let anchors: AnchorStore
    /// All work runs here — never on the extension's calling threads.
    let queue = DispatchQueue(label: "com.yasyf.cc-pool.fileprovider", qos: .userInitiated,
                              attributes: .concurrent)

    required init(domain: NSFileProviderDomain) {
        self.domain = domain
        paths = DomainPaths(domainID: domain.identifier.rawValue)
        anchors = AnchorStore(domainID: domain.identifier.rawValue)
        super.init()
    }

    func invalidate() {}

    // MARK: - Item building

    /// Top-level items: readdir(base) ∪ readdir(private store), overlaid by
    /// the bridge manifest — synth entries become computed items, symlink
    /// entries become symlink items the OS follows into ~/.claude (zero
    /// materialization, matching the fuse bulk-I/O carve-outs), private
    /// entries become private-store directory items. The readdir union is the
    /// robustness floor for passthrough leftovers the manifest doesn't cover.
    func rootItems() throws -> [FPItem] {
        let entries = try mapUnreachable { try bridge.manifest(domain: paths.configDir) }
        var items: [FPItem] = []
        var seen = Set<String>()
        for e in entries where !OverlaySkip.skips(e.name) && !seen.contains(e.name) {
            seen.insert(e.name)
            switch e.kind {
            case "synth":
                items.append(computedItem(name: e.name, freshness: e.freshness ?? []))
            case "symlink":
                items.append(symlinkItem(name: e.name, target: e.target ?? paths.base + "/" + e.name))
            case "private":
                items.append(privateItem(rel: e.name))
            default:
                break
            }
        }
        // Base leftovers. Go owns the classifier (internal/overlay/
        // contentsource.go Classify) — parity by RPC, never a Swift port.
        for name in readdirNames(paths.base) where !seen.contains(name) && !OverlaySkip.skips(name) {
            seen.insert(name)
            switch try mapUnreachable({ try bridge.classify(name: name) }) {
            case "symlink":
                items.append(symlinkItem(name: name, target: paths.base + "/" + name))
            case "":
                // Passthrough: dirs surface as symlink items too — shared dirs
                // have no enumerator.
                let st = BackingStat.lstat(paths.base + "/" + name)
                if st.isDir {
                    items.append(symlinkItem(name: name, target: paths.base + "/" + name))
                } else if st.exists {
                    items.append(sharedFileItem(name: name, st: st))
                }
            default:
                break // synth already emitted; private = plain claude's file, never exposed
            }
        }
        // The account's own private files (e.g. .credentials.json).
        // .claude.json is shadowed by its computed item via `seen`.
        for name in readdirNames(paths.privateStore)
        where !seen.contains(name) && !OverlaySkip.skips(name) {
            items.append(privateItem(rel: name))
        }
        return items
    }

    func rootItem() -> FPItem {
        FPItem(id: .root, filename: domain.identifier.rawValue, contentType: .folder,
               capabilities: [.allowsReading, .allowsContentEnumerating, .allowsAddingSubItems],
               versionHex: FNV.hex("root"))
    }

    /// FPFS satisfies a size-0 read without ever calling fetchContents, so a
    /// computed item must advertise the real synth byte length.
    func computedItem(name: String, freshness: [String]) -> FPItem {
        let freshnessHex = ItemVersions.synth(freshness: freshness)
        let size = synthSize(name: name, version: freshnessHex)
        return FPItem(id: .computed(name), filename: name,
                      contentType: UTType(filenameExtension: (name as NSString).pathExtension) ?? .json,
                      // Go owns the split-back: reading + writing only, no
                      // delete/rename/reparent.
                      capabilities: [.allowsReading, .allowsWriting],
                      documentSize: NSNumber(value: size),
                      versionHex: ItemVersions.computed(freshnessHex: freshnessHex, size: size))
    }

    /// Real byte length of a computed document — one bridge read per content
    /// version, never one per stat. A failed read never fails enumeration:
    /// the item is served at size 0 and fetchContents surfaces the error.
    /// Content-level failures (e.g. a domain with no private backing) are
    /// cached because the freshness stats move when the backing appears;
    /// transport failures are not, so a daemon restart retries.
    private func synthSize(name: String, version: String) -> Int64 {
        switch synthSizes.lookup(name: name, version: version) {
        case .size(let n)?: return n
        case .unreadable?: return 0
        case nil: break
        }
        do {
            let size = Int64(try bridge.readSynth(domain: paths.configDir, name: name).count)
            synthSizes.store(name: name, version: version, outcome: .size(size))
            return size
        } catch is BridgeClient.Failure {
            return 0
        } catch {
            synthSizes.store(name: name, version: version, outcome: .unreadable)
            return 0
        }
    }

    func symlinkItem(name: String, target: String) -> FPItem {
        // Version hashes the target only: the link's content IS the target
        // string, and base edits propagate through it without a re-sync.
        FPItem(id: .shared(name), filename: name, contentType: .symbolicLink,
               capabilities: [.allowsReading],
               symlinkTargetPath: target, versionHex: FNV.hex(target))
    }

    func sharedFileItem(name: String, st: BackingStat) -> FPItem {
        let path = paths.base + "/" + name
        if st.isSymlink {
            return FPItem(id: .shared(name), filename: name, contentType: .symbolicLink,
                          capabilities: [.allowsReading],
                          symlinkTargetPath: readlinkTarget(path),
                          versionHex: ItemVersions.backing(st))
        }
        return FPItem(id: .shared(name), filename: name,
                      contentType: UTType(filenameExtension: (name as NSString).pathExtension) ?? .data,
                      capabilities: [.allowsReading, .allowsWriting, .allowsDeleting],
                      documentSize: NSNumber(value: st.size),
                      versionHex: ItemVersions.backing(st))
    }

    func privateItem(rel: String) -> FPItem {
        let path = paths.privateStore + "/" + rel
        let st = BackingStat.lstat(path)
        let name = (rel as NSString).lastPathComponent
        let topLevel = !rel.contains("/")
        if st.isSymlink {
            return FPItem(id: .priv(rel), filename: name, contentType: .symbolicLink,
                          capabilities: [.allowsReading, .allowsDeleting],
                          symlinkTargetPath: readlinkTarget(path),
                          versionHex: ItemVersions.backing(st))
        }
        if st.isDir || !st.exists {
            // A manifest private dir may predate its backing dir; serve the
            // dir either way so the overlay shape is stable. Top-level private
            // dirs are structural (ExcludedEntries) — not deletable/renamable.
            var caps: NSFileProviderItemCapabilities =
                [.allowsReading, .allowsContentEnumerating, .allowsAddingSubItems]
            if !topLevel { caps.formUnion([.allowsDeleting, .allowsRenaming, .allowsReparenting]) }
            return FPItem(id: .priv(rel), filename: name, contentType: .folder,
                          capabilities: caps, versionHex: ItemVersions.backing(st))
        }
        return FPItem(id: .priv(rel), filename: name,
                      contentType: UTType(filenameExtension: (name as NSString).pathExtension) ?? .data,
                      capabilities: [.allowsReading, .allowsWriting, .allowsDeleting, .allowsRenaming],
                      documentSize: NSNumber(value: st.size),
                      versionHex: ItemVersions.backing(st))
    }

    /// Metadata lookup consistent with what enumeration would serve for the id.
    func lookupItem(_ identifier: NSFileProviderItemIdentifier) throws -> FPItem {
        guard let id = ItemID(identifier) else { throw NSFileProviderError(.noSuchItem) }
        switch id {
        case .root:
            return rootItem()
        case .computed(let name):
            let entries = try mapUnreachable { try bridge.manifest(domain: paths.configDir) }
            guard let e = entries.first(where: { $0.name == name && $0.kind == "synth" }) else {
                throw NSFileProviderError(.noSuchItem)
            }
            return computedItem(name: name, freshness: e.freshness ?? [])
        case .shared(let rel):
            let path = paths.base + "/" + rel
            let st = BackingStat.lstat(path)
            if !rel.contains("/") {
                switch try mapUnreachable({ try bridge.classify(name: rel) }) {
                case "symlink":
                    return symlinkItem(name: rel, target: path)
                case "":
                    guard st.exists else { throw NSFileProviderError(.noSuchItem) }
                    return st.isDir ? symlinkItem(name: rel, target: path)
                        : sharedFileItem(name: rel, st: st)
                default:
                    throw NSFileProviderError(.noSuchItem)
                }
            }
            guard st.exists, !st.isDir else { throw NSFileProviderError(.noSuchItem) }
            return sharedFileItem(name: rel, st: st)
        case .priv(let rel):
            let st = BackingStat.lstat(paths.privateStore + "/" + rel)
            if !st.exists {
                // Only manifest-listed structural dirs exist without backing.
                guard !rel.contains("/") else { throw NSFileProviderError(.noSuchItem) }
                let entries = try mapUnreachable { try bridge.manifest(domain: paths.configDir) }
                guard entries.contains(where: { $0.name == rel && $0.kind == "private" }) else {
                    throw NSFileProviderError(.noSuchItem)
                }
            }
            return privateItem(rel: rel)
        }
    }

    // MARK: - NSFileProviderReplicatedExtension

    func item(for identifier: NSFileProviderItemIdentifier, request: NSFileProviderRequest,
              completionHandler: @escaping (NSFileProviderItem?, Error?) -> Void) -> Progress {
        let progress = Progress(totalUnitCount: 1)
        queue.async {
            defer { progress.completedUnitCount = 1 }
            do { completionHandler(try self.lookupItem(identifier), nil) }
            catch { completionHandler(nil, error) }
        }
        return progress
    }

    func fetchContents(for itemIdentifier: NSFileProviderItemIdentifier,
                       version requestedVersion: NSFileProviderItemVersion?,
                       request: NSFileProviderRequest,
                       completionHandler: @escaping (URL?, NSFileProviderItem?, Error?) -> Void) -> Progress {
        let progress = Progress(totalUnitCount: 1)
        queue.async {
            defer { progress.completedUnitCount = 1 }
            do {
                guard let id = ItemID(itemIdentifier) else { throw NSFileProviderError(.noSuchItem) }
                guard let mgr = NSFileProviderManager(for: self.domain) else {
                    throw CocoaError(.fileReadUnknown)
                }
                let dir = try mgr.temporaryDirectoryURL()
                    .appendingPathComponent(UUID().uuidString, isDirectory: true)
                try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
                switch id {
                case .computed(let name):
                    let data = try self.mapUnreachable {
                        try self.bridge.readSynth(domain: self.paths.configDir, name: name)
                    }
                    let dest = dir.appendingPathComponent(name)
                    try data.write(to: dest)
                    completionHandler(dest, try self.lookupItem(itemIdentifier), nil)
                case .shared, .priv:
                    guard let src = self.paths.backing(id) else {
                        throw NSFileProviderError(.noSuchItem)
                    }
                    let st = BackingStat.lstat(src)
                    guard st.exists, !st.isDir else { throw NSFileProviderError(.noSuchItem) }
                    let dest = dir.appendingPathComponent((src as NSString).lastPathComponent)
                    try self.cloneOrCopy(from: src, to: dest.path)
                    completionHandler(dest, try self.lookupItem(itemIdentifier), nil)
                case .root:
                    throw NSFileProviderError(.noSuchItem)
                }
            } catch {
                completionHandler(nil, nil, error)
            }
        }
        return progress
    }

    func createItem(basedOn itemTemplate: NSFileProviderItem, fields: NSFileProviderItemFields,
                    contents url: URL?, options: NSFileProviderCreateItemOptions,
                    request: NSFileProviderRequest,
                    completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void) -> Progress {
        let progress = Progress(totalUnitCount: 1)
        queue.async {
            defer { progress.completedUnitCount = 1 }
            do {
                let item = try self.performCreate(itemTemplate, contents: url, options: options)
                completionHandler(item, [], false, nil)
            } catch {
                completionHandler(nil, fields, false, error)
            }
        }
        return progress
    }

    private func performCreate(_ template: NSFileProviderItem, contents url: URL?,
                               options: NSFileProviderCreateItemOptions) throws -> FPItem {
        let name = template.filename
        switch template.parentItemIdentifier {
        case .rootContainer:
            let kind = try mapUnreachable { try bridge.classify(name: name) }
            switch kind {
            case "synth":
                let data = try url.map { try Data(contentsOf: $0) } ?? Data()
                try mapUnreachable {
                    try bridge.writeSynth(domain: paths.configDir, name: name, data: data)
                }
                return try lookupItem(ItemID.computed(name).identifier)
            case "private":
                return try createBacking(at: paths.privateStore + "/" + name, id: .priv(name),
                                         template: template, contents: url, options: options)
            default: // "symlink" or "" passthrough — route to the shared base
                let item = try createBacking(at: paths.base + "/" + name, id: .shared(name),
                                             template: template, contents: url, options: options)
                // Match enumeration: carved-out names and dirs are symlink items.
                if kind == "symlink" || template.contentType == .folder {
                    return symlinkItem(name: name, target: paths.base + "/" + name)
                }
                return item
            }
        default:
            guard let pid = ItemID(template.parentItemIdentifier), case .priv(let prel) = pid else {
                throw NSFileProviderError(.noSuchItem)
            }
            let rel = prel + "/" + name
            return try createBacking(at: paths.privateStore + "/" + rel, id: .priv(rel),
                                     template: template, contents: url, options: options)
        }
    }

    private func createBacking(at path: String, id: ItemID, template: NSFileProviderItem,
                               contents url: URL?,
                               options: NSFileProviderCreateItemOptions) throws -> FPItem {
        let st = BackingStat.lstat(path)
        if options.contains(.mayAlreadyExist), st.exists {
            // Reimport dance: adopt the existing backing, never clobber it.
            return try lookupItem(id.identifier)
        }
        try FileManager.default.createDirectory(
            atPath: (path as NSString).deletingLastPathComponent, withIntermediateDirectories: true)
        if template.contentType == .folder {
            if !st.isDir {
                try FileManager.default.createDirectory(atPath: path, withIntermediateDirectories: false)
            }
        } else if template.contentType == .symbolicLink {
            guard let target = template.symlinkTargetPath ?? nil else { throw CocoaError(.fileWriteUnknown) }
            _ = unlink(path)
            guard symlink(target, path) == 0 else { throw CocoaError(.fileWriteUnknown) }
        } else {
            let data = try url.map { try Data(contentsOf: $0) } ?? Data()
            try coordinatedReplace(path: path, data: data)
        }
        return try lookupItem(id.identifier)
    }

    func modifyItem(_ item: NSFileProviderItem, baseVersion version: NSFileProviderItemVersion,
                    changedFields: NSFileProviderItemFields, contents newContents: URL?,
                    options: NSFileProviderModifyItemOptions, request: NSFileProviderRequest,
                    completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void) -> Progress {
        let progress = Progress(totalUnitCount: 1)
        queue.async {
            defer { progress.completedUnitCount = 1 }
            do {
                let updated = try self.performModify(item, changedFields: changedFields,
                                                     contents: newContents)
                completionHandler(updated, [], false, nil)
            } catch {
                completionHandler(nil, changedFields, false, error)
            }
        }
        return progress
    }

    // Last-writer-wins mirror semantics: baseVersion is not conflict-checked —
    // the backing trees are the source of truth and enumeration reconciles.
    private func performModify(_ item: NSFileProviderItem,
                               changedFields: NSFileProviderItemFields,
                               contents newContents: URL?) throws -> FPItem {
        guard var id = ItemID(item.itemIdentifier) else { throw NSFileProviderError(.noSuchItem) }

        if changedFields.contains(.filename) || changedFields.contains(.parentItemIdentifier) {
            switch id {
            case .computed:
                throw Self.rejectedMutation("computed documents cannot be renamed or moved")
            case .root:
                throw NSFileProviderError(.noSuchItem)
            case .shared, .priv:
                id = try performRename(id, item: item, contents: newContents)
                // Rename onto a synth name already committed the contents.
                if case .computed = id { return try lookupItem(id.identifier) }
            }
        }

        if changedFields.contains(.contents), let src = newContents {
            switch id {
            case .computed(let name):
                let data = try Data(contentsOf: src)
                try mapUnreachable {
                    try bridge.writeSynth(domain: paths.configDir, name: name, data: data)
                }
                // Division of labor mirrors the fuse holder: Go splits the
                // shareable keys back to base (WriteThrough); the mount-side
                // owner persists the committed document as the account's
                // private backing. Without this, private-key changes vanish
                // on the next merge. settings.json has no private side.
                if name == ".claude.json" {
                    try coordinatedReplace(path: paths.privateStore + "/" + name, data: data)
                }
            case .shared, .priv:
                guard let dest = paths.backing(id) else { throw NSFileProviderError(.noSuchItem) }
                try coordinatedReplace(path: dest, data: try Data(contentsOf: src))
            case .root:
                throw NSFileProviderError(.noSuchItem)
            }
        }
        return try lookupItem(id.identifier)
    }

    /// Moves the backing to wherever the new name classifies. A rename onto a
    /// synth name is claude's atomic tmp→commit dance: read the temp bytes,
    /// write through the bridge, drop the temp.
    private func performRename(_ id: ItemID, item: NSFileProviderItem,
                               contents newContents: URL?) throws -> ItemID {
        guard let src = paths.backing(id) else { throw NSFileProviderError(.noSuchItem) }
        let newName = item.filename
        let destID: ItemID
        let destPath: String
        switch item.parentItemIdentifier {
        case .rootContainer:
            let kind = try mapUnreachable { try bridge.classify(name: newName) }
            switch kind {
            case "synth":
                let data = try newContents.map { try Data(contentsOf: $0) }
                    ?? (try Data(contentsOf: URL(fileURLWithPath: src)))
                try mapUnreachable {
                    try bridge.writeSynth(domain: paths.configDir, name: newName, data: data)
                }
                _ = unlink(src)
                return .computed(newName)
            case "private":
                destID = .priv(newName)
                destPath = paths.privateStore + "/" + newName
            default:
                destID = .shared(newName)
                destPath = paths.base + "/" + newName
            }
        default:
            guard let pid = ItemID(item.parentItemIdentifier), case .priv(let prel) = pid else {
                throw Self.rejectedMutation("items can only move within the account's private store")
            }
            destID = .priv(prel + "/" + newName)
            destPath = paths.privateStore + "/" + prel + "/" + newName
        }
        try FileManager.default.createDirectory(
            atPath: (destPath as NSString).deletingLastPathComponent,
            withIntermediateDirectories: true)
        try coordinatedRename(from: src, to: destPath)
        return destID
    }

    func deleteItem(identifier: NSFileProviderItemIdentifier,
                    baseVersion version: NSFileProviderItemVersion,
                    options: NSFileProviderDeleteItemOptions, request: NSFileProviderRequest,
                    completionHandler: @escaping (Error?) -> Void) -> Progress {
        let progress = Progress(totalUnitCount: 1)
        queue.async {
            defer { progress.completedUnitCount = 1 }
            do {
                guard let id = ItemID(identifier) else { throw NSFileProviderError(.noSuchItem) }
                switch id {
                case .root, .computed:
                    throw NSFileProviderError(.deletionRejected)
                case .shared(let rel):
                    // Carved-out names are read-only symlink items; only
                    // passthrough leftovers are deletable.
                    if !rel.contains("/"),
                       try self.mapUnreachable({ try self.bridge.classify(name: rel) }) == "symlink" {
                        throw NSFileProviderError(.deletionRejected)
                    }
                    try self.coordinatedRemove(self.paths.base + "/" + rel)
                case .priv(let rel):
                    try self.coordinatedRemove(self.paths.privateStore + "/" + rel)
                }
                completionHandler(nil)
            } catch {
                completionHandler(error)
            }
        }
        return progress
    }

    func enumerator(for containerItemIdentifier: NSFileProviderItemIdentifier,
                    request: NSFileProviderRequest) throws -> NSFileProviderEnumerator {
        switch containerItemIdentifier {
        case .rootContainer, .workingSet:
            return RootEnumerator(ext: self, container: containerItemIdentifier)
        case .trashContainer:
            throw NSFileProviderError(.noSuchItem)
        default:
            // Shared dirs are symlink items — no enumerator; only private
            // dirs enumerate.
            guard let id = ItemID(containerItemIdentifier), case .priv(let rel) = id else {
                throw NSFileProviderError(.noSuchItem)
            }
            return DirEnumerator(ext: self, rel: rel)
        }
    }

    // MARK: - Backing I/O

    /// clonefile(2) semantics via copyfile COPYFILE_CLONE (APFS COW, silent
    /// data-copy fallback cross-volume); a coordinated read+copy backstop for
    /// paths under presenter coordination.
    private func cloneOrCopy(from src: String, to dst: String) throws {
        if copyfile(src, dst, nil, copyfile_flags_t(COPYFILE_CLONE)) == 0 { return }
        var coordErr: NSError?
        var innerErr: Error?
        NSFileCoordinator(filePresenter: nil).coordinate(
            readingItemAt: URL(fileURLWithPath: src), options: .withoutChanges,
            error: &coordErr) { url in
            do { try FileManager.default.copyItem(at: url, to: URL(fileURLWithPath: dst)) }
            catch { innerErr = error }
        }
        if let e = coordErr { throw e }
        if let e = innerErr { throw e }
    }

    /// Atomic temp+rename under NSFileCoordinator — uncoordinated writes to
    /// replica-backed files get silently reverted. The "._" temp prefix keeps
    /// the in-flight sibling inside both skip sets (Swift and Go).
    func coordinatedReplace(path: String, data: Data) throws {
        var coordErr: NSError?
        var innerErr: Error?
        NSFileCoordinator(filePresenter: nil).coordinate(
            writingItemAt: URL(fileURLWithPath: path), options: .forReplacing,
            error: &coordErr) { url in
            let tmp = (url.path as NSString).deletingLastPathComponent
                + "/._ccp-tmp-" + UUID().uuidString
            do {
                try data.write(to: URL(fileURLWithPath: tmp))
                try FileManager.default.setAttributes(
                    [.posixPermissions: 0o600], ofItemAtPath: tmp)
                guard rename(tmp, url.path) == 0 else {
                    try? FileManager.default.removeItem(atPath: tmp)
                    throw CocoaError(.fileWriteUnknown)
                }
            } catch {
                innerErr = error
            }
        }
        if let e = coordErr { throw e }
        if let e = innerErr { throw e }
    }

    private func coordinatedRename(from src: String, to dst: String) throws {
        var coordErr: NSError?
        var innerErr: Error?
        NSFileCoordinator(filePresenter: nil).coordinate(
            writingItemAt: URL(fileURLWithPath: src), options: .forMoving,
            writingItemAt: URL(fileURLWithPath: dst), options: .forReplacing,
            error: &coordErr) { _, _ in
            guard rename(src, dst) == 0 else {
                innerErr = CocoaError(.fileWriteUnknown)
                return
            }
        }
        if let e = coordErr { throw e }
        if let e = innerErr { throw e }
    }

    private func coordinatedRemove(_ path: String) throws {
        var coordErr: NSError?
        var innerErr: Error?
        NSFileCoordinator(filePresenter: nil).coordinate(
            writingItemAt: URL(fileURLWithPath: path), options: .forDeleting,
            error: &coordErr) { url in
            do { try FileManager.default.removeItem(at: url) }
            catch { innerErr = error }
        }
        if let e = coordErr { throw e }
        if let e = innerErr { throw e }
    }

    // MARK: - Errors

    /// A bridge transport failure is the FP "server unreachable" signal —
    /// the OS keeps serving the replica and retries.
    func mapUnreachable<T>(_ body: () throws -> T) throws -> T {
        do {
            return try body()
        } catch let BridgeClient.Failure.unreachable(msg) {
            throw NSError(domain: NSFileProviderError.errorDomain,
                          code: NSFileProviderError.Code.serverUnreachable.rawValue,
                          userInfo: [NSLocalizedDescriptionKey: msg])
        }
    }

    static func rejectedMutation(_ msg: String) -> NSError {
        NSError(domain: NSCocoaErrorDomain, code: NSFeatureUnsupportedError,
                userInfo: [NSLocalizedDescriptionKey: msg])
    }
}
