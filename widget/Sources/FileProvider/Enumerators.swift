import CryptoKit
import FileProvider
import Foundation

/// Per-container item-version snapshots persisted in the appex's own in-sandbox
/// Application Support. Recent snapshots remain addressable by their anchors so
/// overlapping File Provider requests cannot invalidate one another.
struct AnchorStore {
    struct Delta {
        let changed: [FPItem]
        let deleted: [NSFileProviderItemIdentifier]
        let anchor: NSFileProviderSyncAnchor
    }

    private struct State: Codable {
        var snapshots: [String: [String: String]] = [:]
        var order: [String] = []
        var forced: Set<String> = []
    }

    private static let retainedSnapshots = 32
    private let dir: URL
    private let lock = NSLock()

    init(domainID: String,
           root: URL = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]) {
        dir = root.appendingPathComponent("CCPoolFileProvider/anchors/" + domainID, isDirectory: true)
    }

    private func file(_ container: NSFileProviderItemIdentifier) -> URL {
        let key = SHA256.hash(data: Data(container.rawValue.utf8))
            .map { String(format: "%02x", $0) }.joined()
        return dir.appendingPathComponent(key + ".v2.json")
    }

    private func loadUnlocked(_ container: NSFileProviderItemIdentifier) throws -> State {
        let url = file(container)
        guard FileManager.default.fileExists(atPath: url.path) else { return State() }
        return try JSONDecoder().decode(State.self, from: Data(contentsOf: url))
    }

    private func saveUnlocked(_ container: NSFileProviderItemIdentifier, _ state: State) throws {
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        try JSONEncoder().encode(state).write(to: file(container), options: .atomic)
    }

    private static func key(_ anchor: Data) -> String {
        anchor.map { String(format: "%02x", $0) }.joined()
    }

    private func insert(_ map: [String: String], into state: inout State) -> NSFileProviderSyncAnchor {
        let raw = Self.anchor(of: map)
        let key = Self.key(raw)
        state.snapshots[key] = map
        state.order.removeAll { $0 == key }
        state.order.append(key)
        while state.order.count > Self.retainedSnapshots {
            state.snapshots.removeValue(forKey: state.order.removeFirst())
        }
        return NSFileProviderSyncAnchor(raw)
    }

    func record(_ container: NSFileProviderItemIdentifier,
                items: [FPItem]) throws -> NSFileProviderSyncAnchor {
        lock.lock()
        defer { lock.unlock() }
        var state = try loadUnlocked(container)
        let map = Self.versionMap(items)
        let raw = Self.anchor(of: map)
        let key = Self.key(raw)
        if state.snapshots[key] == map && state.order.last == key {
            return NSFileProviderSyncAnchor(raw)
        }
        let anchor = insert(map, into: &state)
        try saveUnlocked(container, state)
        return anchor
    }

    func changes(_ container: NSFileProviderItemIdentifier,
                 from anchor: NSFileProviderSyncAnchor, current: [FPItem]) throws -> Delta? {
        lock.lock()
        defer { lock.unlock() }
        var state = try loadUnlocked(container)
        guard let persisted = state.snapshots[Self.key(anchor.rawValue)] else { return nil }
        let currentMap = Self.versionMap(current)
        let changed = current.filter {
            persisted[$0.itemIdentifier.rawValue] != $0.versionHex
                || state.forced.contains($0.itemIdentifier.rawValue)
        }
        let deleted = persisted.keys.filter { currentMap[$0] == nil }
            .map { NSFileProviderItemIdentifier($0) }
        if changed.isEmpty && deleted.isEmpty && state.forced.isEmpty {
            return Delta(changed: [], deleted: [], anchor: anchor)
        }
        state.forced.removeAll()
        let next = insert(currentMap, into: &state)
        try saveUnlocked(container, state)
        return Delta(changed: changed, deleted: deleted, anchor: next)
    }

    func forceUpdate(_ identifier: NSFileProviderItemIdentifier,
                     in containers: [NSFileProviderItemIdentifier]) throws {
        lock.lock()
        defer { lock.unlock() }
        for container in containers {
            var state = try loadUnlocked(container)
            if state.forced.insert(identifier.rawValue).inserted {
                try saveUnlocked(container, state)
            }
        }
    }

    /// SHA256 over sorted "id\t<contentVersionHex>" lines.
    static func anchor(of map: [String: String]) -> Data {
        var s = ""
        for (id, ver) in map.sorted(by: { $0.key < $1.key }) {
            s += id + "\t" + ver + "\n"
        }
        return Data(SHA256.hash(data: Data(s.utf8)))
    }

    private static func versionMap(_ items: [FPItem]) -> [String: String] {
        Dictionary(uniqueKeysWithValues: items.map { ($0.itemIdentifier.rawValue, $0.versionHex) })
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
                           from anchor: NSFileProviderSyncAnchor) throws {
    guard let delta = try source.anchors.changes(container, from: anchor, current: current) else {
        observer.finishEnumeratingWithError(NSFileProviderError(.syncAnchorExpired))
        return
    }
    if !delta.changed.isEmpty { observer.didUpdate(delta.changed) }
    if !delta.deleted.isEmpty { observer.didDeleteItems(withIdentifiers: delta.deleted) }
    observer.finishEnumeratingChanges(upTo: delta.anchor, moreComing: false)
}

private func finishCurrentAnchor(source: EnumerationSource,
                                 container: NSFileProviderItemIdentifier, current: [FPItem],
                                 completionHandler: @escaping (NSFileProviderSyncAnchor?) -> Void) throws {
    completionHandler(try source.anchors.record(container, items: current))
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
                try finishChanges(source: self.source, container: self.container,
                                  current: self.source.rootItems(), observer: observer, from: anchor)
            } catch {
                NSLog("ccp-fp root changes failed: %@", error as NSError)
                observer.finishEnumeratingWithError(error)
            }
        }
    }

    func currentSyncAnchor(completionHandler: @escaping (NSFileProviderSyncAnchor?) -> Void) {
        source.queue.async {
            do {
                try finishCurrentAnchor(source: self.source, container: self.container,
                                        current: self.source.rootItems(),
                                        completionHandler: completionHandler)
            } catch {
                NSLog("ccp-fp root current anchor failed: %@", error as NSError)
                completionHandler(nil)
            }
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
                try finishChanges(source: self.source, container: self.container,
                                  current: self.source.dirItems(rel: self.rel),
                                  observer: observer, from: anchor)
            } catch {
                NSLog("ccp-fp dir changes %@ failed: %@", self.rel, error as NSError)
                observer.finishEnumeratingWithError(error)
            }
        }
    }

    func currentSyncAnchor(completionHandler: @escaping (NSFileProviderSyncAnchor?) -> Void) {
        source.queue.async {
            do {
                try finishCurrentAnchor(source: self.source, container: self.container,
                                        current: self.source.dirItems(rel: self.rel),
                                        completionHandler: completionHandler)
            } catch {
                NSLog("ccp-fp dir current anchor %@ failed: %@", self.rel, error as NSError)
                completionHandler(nil)
            }
        }
    }
}
