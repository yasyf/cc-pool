import CryptoKit
import FileProvider
import Foundation

/// Per-container item→version maps persisted in the appex's own in-sandbox
/// Application Support. The sync anchor is a digest of the persisted map, so
/// an anchor the map can't reproduce is expired by construction.
struct AnchorStore {
    private let dir: URL
    private let lock = NSLock()

    init(domainID: String,
         root: URL = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]) {
        dir = root.appendingPathComponent("CCPoolFileProvider/anchors/" + domainID, isDirectory: true)
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    }

    private func file(_ container: NSFileProviderItemIdentifier) -> URL {
        let key = SHA256.hash(data: Data(container.rawValue.utf8))
            .map { String(format: "%02x", $0) }.joined()
        return dir.appendingPathComponent(key + ".json")
    }

    func load(_ container: NSFileProviderItemIdentifier) -> [String: String] {
        lock.lock()
        defer { lock.unlock() }
        return loadUnlocked(container)
    }

    private func loadUnlocked(_ container: NSFileProviderItemIdentifier) -> [String: String] {
        guard let data = try? Data(contentsOf: file(container)),
              let map = try? JSONDecoder().decode([String: String].self, from: data)
        else { return [:] }
        return map
    }

    func save(_ container: NSFileProviderItemIdentifier, _ map: [String: String]) {
        lock.lock()
        defer { lock.unlock() }
        try? saveUnlocked(container, map)
    }

    func remove(_ identifier: NSFileProviderItemIdentifier,
                from containers: [NSFileProviderItemIdentifier]) throws {
        lock.lock()
        defer { lock.unlock() }
        for container in containers {
            var map = loadUnlocked(container)
            map.removeValue(forKey: identifier.rawValue)
            try saveUnlocked(container, map)
        }
    }

    private func saveUnlocked(_ container: NSFileProviderItemIdentifier,
                              _ map: [String: String]) throws {
        let data = try JSONEncoder().encode(map)
        try data.write(to: file(container), options: .atomic)
    }

    /// SHA256 over sorted "id\t<contentVersionHex>" lines.
    static func anchor(of map: [String: String]) -> Data {
        var s = ""
        for (id, ver) in map.sorted(by: { $0.key < $1.key }) {
            s += id + "\t" + ver + "\n"
        }
        return Data(SHA256.hash(data: Data(s.utf8)))
    }
}

/// What the enumerators need from the extension. A seam so the fail-closed
/// behavior is testable without a live bridge or FP host.
protocol EnumerationSource: AnyObject {
    var queue: DispatchQueue { get }
    var anchors: AnchorStore { get }
    func rootItems() throws -> [FPItem]
    func dirItems(rel: String) throws -> [FPItem]
}

/// Diff the container's current listing against its persisted map, emit
/// updates/deletes, persist, and finish at the new anchor.
private func finishChanges(source: EnumerationSource, container: NSFileProviderItemIdentifier,
                           current: [FPItem], observer: NSFileProviderChangeObserver,
                           from anchor: NSFileProviderSyncAnchor) {
    let persisted = source.anchors.load(container)
    guard AnchorStore.anchor(of: persisted) == anchor.rawValue else {
        observer.finishEnumeratingWithError(NSFileProviderError(.syncAnchorExpired))
        return
    }
    var currentMap: [String: String] = [:]
    for item in current { currentMap[item.itemIdentifier.rawValue] = item.versionHex }
    let changed = current.filter { persisted[$0.itemIdentifier.rawValue] != $0.versionHex }
    let deleted = persisted.keys.filter { currentMap[$0] == nil }
        .map { NSFileProviderItemIdentifier($0) }
    if !changed.isEmpty { observer.didUpdate(changed) }
    if !deleted.isEmpty { observer.didDeleteItems(withIdentifiers: deleted) }
    source.anchors.save(container, currentMap)
    observer.finishEnumeratingChanges(
        upTo: NSFileProviderSyncAnchor(AnchorStore.anchor(of: currentMap)), moreComing: false)
}

private func finishCurrentAnchor(source: EnumerationSource,
                                 container: NSFileProviderItemIdentifier, current: [FPItem]?,
                                 completionHandler: @escaping (NSFileProviderSyncAnchor?) -> Void) {
    guard let current else {
        completionHandler(nil)
        return
    }
    var map: [String: String] = [:]
    for item in current { map[item.itemIdentifier.rawValue] = item.versionHex }
    source.anchors.save(container, map)
    completionHandler(NSFileProviderSyncAnchor(AnchorStore.anchor(of: map)))
}

/// Serves .rootContainer and .workingSet: all top-level items plus both
/// computed documents (the working set is the signalEnumerator target, so it
/// must cover everything a base edit can change). Small listing — no paging.
/// Fail-closed: a listing that can't be fully built fails the enumeration —
/// a partial listing reads as "deleted remotely" and fileproviderd deletes
/// launch-critical replicas.
final class RootEnumerator: NSObject, NSFileProviderEnumerator {
    private let source: EnumerationSource
    private let container: NSFileProviderItemIdentifier

    init(source: EnumerationSource, container: NSFileProviderItemIdentifier) {
        self.source = source
        self.container = container
    }

    func invalidate() {}

    func enumerateItems(for observer: NSFileProviderEnumerationObserver,
                        startingAt page: NSFileProviderPage) {
        source.queue.async {
            do {
                observer.didEnumerate(try self.source.rootItems())
                observer.finishEnumerating(upTo: nil)
            } catch {
                NSLog("ccp-fp root enumerate failed: %@", error as NSError)
                observer.finishEnumeratingWithError(error)
            }
        }
    }

    func enumerateChanges(for observer: NSFileProviderChangeObserver,
                          from anchor: NSFileProviderSyncAnchor) {
        source.queue.async {
            do {
                finishChanges(source: self.source, container: self.container,
                              current: try self.source.rootItems(), observer: observer, from: anchor)
            } catch {
                NSLog("ccp-fp root changes failed: %@", error as NSError)
                observer.finishEnumeratingWithError(error)
            }
        }
    }

    func currentSyncAnchor(completionHandler: @escaping (NSFileProviderSyncAnchor?) -> Void) {
        source.queue.async {
            finishCurrentAnchor(source: self.source, container: self.container,
                                current: try? self.source.rootItems(),
                                completionHandler: completionHandler)
        }
    }
}

/// Enumerates one private-store directory (shared dirs are symlink items and
/// never enumerate). Pages of 256, cursor = the next index as decimal Data.
/// Fail-closed like RootEnumerator.
final class DirEnumerator: NSObject, NSFileProviderEnumerator {
    private static let pageSize = 256

    private let source: EnumerationSource
    private let rel: String
    private var container: NSFileProviderItemIdentifier { ItemID.priv(rel).identifier }

    init(source: EnumerationSource, rel: String) {
        self.source = source
        self.rel = rel
    }

    func invalidate() {}

    func enumerateItems(for observer: NSFileProviderEnumerationObserver,
                        startingAt page: NSFileProviderPage) {
        source.queue.async {
            do {
                let items = try self.source.dirItems(rel: self.rel)
                let start = min(Self.index(of: page), items.count)
                let end = min(start + Self.pageSize, items.count)
                observer.didEnumerate(Array(items[start..<end]))
                observer.finishEnumerating(
                    upTo: end < items.count ? NSFileProviderPage(Data("\(end)".utf8)) : nil)
            } catch {
                NSLog("ccp-fp dir enumerate %@ failed: %@", self.rel, error as NSError)
                observer.finishEnumeratingWithError(error)
            }
        }
    }

    /// The initial-page sentinels (and anything non-numeric) read as index 0.
    private static func index(of page: NSFileProviderPage) -> Int {
        Int(String(data: page.rawValue, encoding: .utf8) ?? "") ?? 0
    }

    func enumerateChanges(for observer: NSFileProviderChangeObserver,
                          from anchor: NSFileProviderSyncAnchor) {
        source.queue.async {
            do {
                finishChanges(source: self.source, container: self.container,
                              current: try self.source.dirItems(rel: self.rel),
                              observer: observer, from: anchor)
            } catch {
                NSLog("ccp-fp dir changes %@ failed: %@", self.rel, error as NSError)
                observer.finishEnumeratingWithError(error)
            }
        }
    }

    func currentSyncAnchor(completionHandler: @escaping (NSFileProviderSyncAnchor?) -> Void) {
        source.queue.async {
            finishCurrentAnchor(source: self.source, container: self.container,
                                current: try? self.source.dirItems(rel: self.rel),
                                completionHandler: completionHandler)
        }
    }
}
