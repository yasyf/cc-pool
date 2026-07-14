import FileProvider
import Foundation

// Item-identifier codec. One extension instance serves one domain, so ids
// never carry the domain: "shared:<rel>" resolves under ~/.claude,
// "private:<rel>" under the account's private store, "computed:<name>" is one
// of the two bridge-backed synthetic documents.
enum ItemID: Equatable {
    case root
    case shared(String)
    case priv(String)
    case computed(String)

    init?(_ identifier: NSFileProviderItemIdentifier) {
        if identifier == .rootContainer {
            self = .root
            return
        }
        let raw = identifier.rawValue
        if raw.hasPrefix("shared:") {
            self = .shared(String(raw.dropFirst("shared:".count)))
        } else if raw.hasPrefix("private:") {
            self = .priv(String(raw.dropFirst("private:".count)))
        } else if raw.hasPrefix("computed:") {
            self = .computed(String(raw.dropFirst("computed:".count)))
        } else {
            return nil
        }
    }

    var identifier: NSFileProviderItemIdentifier {
        switch self {
        case .root: .rootContainer
        case .shared(let rel): NSFileProviderItemIdentifier("shared:" + rel)
        case .priv(let rel): NSFileProviderItemIdentifier("private:" + rel)
        case .computed(let name): NSFileProviderItemIdentifier("computed:" + name)
        }
    }

    /// Shared entries never nest as items (shared dirs are symlink items the
    /// OS follows out of the domain), so every non-private id parents at root.
    var parent: NSFileProviderItemIdentifier {
        switch self {
        case .root, .computed, .shared:
            return .rootContainer
        case .priv(let rel):
            let dir = (rel as NSString).deletingLastPathComponent
            return dir.isEmpty ? .rootContainer : ItemID.priv(dir).identifier
        }
    }
}

/// Per-domain backing paths. The domain identifier is the account basename
/// (acct-NN); the bridge wire "domain" is the LITERAL account config dir path
/// (PoolContentSource derives the private store by appending ".private").
struct DomainPaths {
    /// Shared overlay base: ~/.claude.
    let base: String
    /// The literal account config dir — the bridge "domain" wire string.
    let configDir: String
    /// fkoverlay.FusePrivateRoot parity: configDir + ".private".
    let privateStore: String

    init(domainID: String) {
        let home = StatusFile.realHome
        base = home + "/.claude"
        configDir = home + "/.cc-pool/accounts/" + domainID
        privateStore = configDir + ".private"
    }

    func backing(_ id: ItemID) -> String? {
        switch id {
        case .root, .computed: nil
        case .shared(let rel): base + "/" + rel
        case .priv(let rel): privateStore + "/" + rel
        }
    }
}

// Parity with internal/overlay/classify.go: SkipEntries (:43-45) and
// SkipPrefixes (:47-53, the "._" AppleDouble litter plus the ".fuse_hidden"/
// ".nfs." silly-rename litter), plus the synthetic probe file
// (internal/overlay/probe.go ProbeFileName). Names the Go classifier never
// serves must never surface as items.
enum OverlaySkip {
    static let names: Set<String> = [".DS_Store", ".ccp-probe"]
    static let prefixes = ["._", ".fuse_hidden", ".nfs."]

    static func skips(_ name: String) -> Bool {
        names.contains(name) || prefixes.contains { name.hasPrefix($0) }
    }
}

/// lstat snapshot of a backing path; absent files read as (-1, -1) — the
/// holder's freshness convention for a missing gate file. ENOENT is the one
/// valid "absent" state; any other errno throws so the failure is visible and
/// the OS retries — never a silently frozen version.
struct BackingStat {
    let exists: Bool
    let isDir: Bool
    let isSymlink: Bool
    let size: Int64
    let mtimeNS: Int64

    static let absent = BackingStat(exists: false, isDir: false, isSymlink: false, size: -1, mtimeNS: -1)

    static func lstat(_ path: String) throws -> BackingStat {
        var st = stat()
        guard Darwin.lstat(path, &st) == 0 else {
            let err = errno
            if err == ENOENT { return .absent }
            throw NSError(domain: NSPOSIXErrorDomain, code: Int(err), userInfo: [
                NSLocalizedDescriptionKey: "lstat \(path): \(String(cString: strerror(err)))",
            ])
        }
        let mode = st.st_mode & S_IFMT
        return BackingStat(
            exists: true, isDir: mode == S_IFDIR, isSymlink: mode == S_IFLNK,
            size: Int64(st.st_size),
            mtimeNS: Int64(st.st_mtimespec.tv_sec) * 1_000_000_000 + Int64(st.st_mtimespec.tv_nsec))
    }
}

/// fnv-1a-64, the holder's freshness hash.
enum FNV {
    static func hex(_ s: String) -> String {
        var h: UInt64 = 0xcbf2_9ce4_8422_2325
        for b in s.utf8 { h = (h ^ UInt64(b)) &* 0x100_0000_01b3 }
        return String(format: "%016llx", h)
    }
}

enum ItemVersions {
    /// Shared/private items: hash of the backing file's "<mtime_ns>:<size>".
    static func backing(_ st: BackingStat) -> String {
        FNV.hex("\(st.mtimeNS):\(st.size)")
    }

    /// Synth items: hash over every manifest freshness path's
    /// (path, mtime_ns, size), statted locally — the exact staleness gate the
    /// fuse holder applies, computed where the stat is cheap. Feeds the synth
    /// size cache key; the item version is computed(freshnessHex:size:).
    static func synth(freshness: [String]) throws -> String {
        var s = ""
        for p in freshness {
            let st = try BackingStat.lstat(p)
            s += "\(p)\0\(st.mtimeNS)\0\(st.size)\n"
        }
        return FNV.hex(s)
    }

    /// Computed items only: the freshness hash salted ("sz1:") with the
    /// advertised size, so domains that materialized content back when synth
    /// items carried no size see a version change and re-fetch.
    static func computed(freshnessHex: String, size: Int64) -> String {
        FNV.hex("sz1:\(size):\(freshnessHex)")
    }
}

/// Content version of a synth entry: the daemon-computed manifest version
/// (FreshnessVersion, statted unsandboxed) when present; the local stat hash
/// only as the old-daemon fallback.
func synthVersionHex(manifestVersion: String, freshness: [String]) throws -> String {
    manifestVersion.isEmpty ? try ItemVersions.synth(freshness: freshness) : manifestVersion
}

/// Directory listing; a missing directory is a valid empty state (private
/// dirs may predate their backing), any other failure throws.
func readdirNames(_ path: String) throws -> [String] {
    do {
        return try FileManager.default.contentsOfDirectory(atPath: path)
    } catch CocoaError.fileReadNoSuchFile {
        return []
    } catch let e as NSError where e.domain == NSPOSIXErrorDomain && e.code == Int(ENOENT) {
        return []
    }
}

func readlinkTarget(_ path: String) -> String? {
    var buf = [CChar](repeating: 0, count: Int(PATH_MAX))
    let n = readlink(path, &buf, buf.count - 1)
    guard n > 0 else { return nil }
    return String(bytes: buf[0..<n].map { UInt8(bitPattern: $0) }, encoding: .utf8)
}
