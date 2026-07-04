import Foundation

/// Wire mirror of fusekit/content.Entry (content/source.go). Go []byte and
/// Swift Data both ride JSON as base64, so Codable defaults match the wire.
struct BridgeEntry: Decodable {
    let name: String
    let kind: String
    let version: String
    let size: Int64?
    let target: String?
    let isPrivate: Bool?
    let freshness: [String]?
    let mtime: Int64?
    let birth: Int64?
    let ino: UInt64?

    enum CodingKeys: String, CodingKey {
        case name, kind, version, size, target, freshness, mtime, birth, ino
        case isPrivate = "private"
    }
}

/// Client of the daemon's FP bridge socket (App Group container b.sock): one
/// newline-delimited JSON request, one reply, per connection — the frozen
/// proto=1 wire of fusekit/content/bridge.go, ops manifest/read/write/classify
/// only. Raw POSIX AF_UNIX because Network.framework has no by-path UDS.
final class BridgeClient {
    /// Transport-level failure (socket missing, daemon down) — mapped to
    /// NSFileProviderError.serverUnreachable by callers, never a content verdict.
    enum Failure: Error {
        case unreachable(String)
    }

    private struct Request: Encodable {
        var proto = 1
        let op: String
        var domain: String?
        var name: String?
        var data: Data?
    }

    private struct Response: Decodable {
        let ok: Bool
        let error: String?
        let errClass: String?
        let entries: [BridgeEntry]?
        let kind: String?
        let data: Data?

        enum CodingKeys: String, CodingKey {
            case ok, error, entries, kind, data
            case errClass = "err_class"
        }
    }

    /// Socket path in the App Group container — the one location the sandbox
    /// allows AF_UNIX connect (file temp-exceptions cover file ops only;
    /// unix-socket connect is a network-outbound operation the container
    /// implicitly grants). The group id is read from this appex's own
    /// Info.plist (NSExtensionFileProviderDocumentGroup), never hardcoded.
    static func socketPath() -> String {
        (FileManager.default.containerURL(
            forSecurityApplicationGroupIdentifier: appGroupID())?.path
            ?? StatusFile.realHome) + "/b.sock"
    }

    private static func appGroupID() -> String {
        let ext = Bundle.main.infoDictionary?["NSExtension"] as? [String: Any]
        return ext?["NSExtensionFileProviderDocumentGroup"] as? String ?? ""
    }

    func manifest(domain: String) throws -> [BridgeEntry] {
        try roundTrip(Request(op: "manifest", domain: domain), failCode: NSFileReadUnknownError)
            .entries ?? []
    }

    func readSynth(domain: String, name: String) throws -> Data {
        try roundTrip(Request(op: "read", domain: domain, name: name), failCode: NSFileReadUnknownError)
            .data ?? Data()
    }

    func writeSynth(domain: String, name: String, data: Data) throws {
        _ = try roundTrip(
            Request(op: "write", domain: domain, name: name, data: data),
            failCode: NSFileWriteUnknownError)
    }

    func classify(name: String) throws -> String {
        try roundTrip(Request(op: "classify", name: name), failCode: NSFileReadUnknownError)
            .kind ?? ""
    }

    private func roundTrip(_ req: Request, failCode: Int) throws -> Response {
        let path = Self.socketPath()
        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else { throw Failure.unreachable("socket: \(errnoString())") }
        defer { close(fd) }
        // Server-side per-conn deadline is 10s; stay comfortably inside it.
        var tv = timeval(tv_sec: 5, tv_usec: 0)
        _ = setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, socklen_t(MemoryLayout<timeval>.size))
        _ = setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, socklen_t(MemoryLayout<timeval>.size))

        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        let fits = path.withCString { src in
            withUnsafeMutableBytes(of: &addr.sun_path) { dst -> Bool in
                let len = strlen(src)
                guard len < dst.count else { return false }
                memcpy(dst.baseAddress!, src, len + 1)
                return true
            }
        }
        guard fits else { throw Failure.unreachable("socket path too long: \(path)") }
        let rc = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                connect(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
        guard rc == 0 else { throw Failure.unreachable("connect \(path): \(errnoString())") }

        var payload = try JSONEncoder().encode(req)
        payload.append(0x0a)
        try writeAll(fd, payload)
        let resp = try JSONDecoder().decode(Response.self, from: try readReply(fd))
        guard resp.ok else {
            throw NSError(domain: NSCocoaErrorDomain, code: failCode, userInfo: [
                NSLocalizedDescriptionKey: "bridge \(req.op): \(resp.error ?? "unknown error")",
            ])
        }
        return resp
    }

    private func writeAll(_ fd: Int32, _ data: Data) throws {
        try data.withUnsafeBytes { (buf: UnsafeRawBufferPointer) in
            var off = 0
            while off < buf.count {
                let n = write(fd, buf.baseAddress!.advanced(by: off), buf.count - off)
                guard n > 0 else { throw Failure.unreachable("write: \(errnoString())") }
                off += n
            }
        }
    }

    private func readReply(_ fd: Int32) throws -> Data {
        var out = Data()
        var buf = [UInt8](repeating: 0, count: 64 * 1024)
        while true {
            let n = read(fd, &buf, buf.count)
            if n > 0 {
                out.append(contentsOf: buf[0..<n])
                if buf[0..<n].contains(0x0a) { return out }
                guard out.count <= 64 << 20 else {
                    throw Failure.unreachable("bridge reply exceeds 64MiB")
                }
            } else if n == 0 {
                guard !out.isEmpty else { throw Failure.unreachable("bridge closed without reply") }
                return out
            } else {
                throw Failure.unreachable("read: \(errnoString())")
            }
        }
    }
}

private func errnoString() -> String { String(cString: strerror(errno)) }
