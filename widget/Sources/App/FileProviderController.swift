import CoreServices
import FileProvider
import Foundation

// Control plane for the CCPoolFileProvider extension, hosted in this
// (non-sandboxed) app: the cc-pool daemon is the CLIENT of a control socket
// the app SERVES. The wire is FROZEN at proto=1 — one JSON line request, one
// JSON line response per connection; op names, field names, and error-class
// strings must match fusekit fileproviderd/control.go exactly.

private let controlProto = 1

private struct ControlRequest: Decodable {
    let proto: Int
    let op: String
    let domain: String?
}

private struct ControlResponse: Encodable {
    let proto = controlProto
    var ok: Bool
    var error: String?
    var errClass: String?
    var version: String?
    var fpOK: Bool?
    var path: String?
    /// probe-domain only. Absent = domain serves but `.claude.json` is absent;
    /// 0 = present and empty; >0 = bytes actually read. nil is omitted by the
    /// synthesized encoder, which is exactly the "absent" wire shape.
    var jsonBytes: Int64?

    enum CodingKeys: String, CodingKey {
        case proto, ok, error, version, path
        case errClass = "err_class"
        case fpOK = "fp_ok"
        case jsonBytes = "json_bytes"
    }

    static func failure(_ message: String, _ cls: ErrClass?) -> ControlResponse {
        ControlResponse(ok: false, error: message, errClass: cls?.rawValue)
    }
}

/// Error classes the app mints. "app-unreachable" also exists on the wire but
/// only the Go client mints it (dial/connection failures).
private enum ErrClass: String {
    /// Permanent capability "no" — the ONLY class that retreats an account to
    /// the symlink floor. Emitted solely for provably-capability NSErrors.
    case noEntitlement = "no-entitlement"
    /// Transient OS rejection, including every timeout and I/O failure.
    case registerFailed = "register-failed"
    case noDomain = "no-domain"
    case busy = "busy"
    /// probe-domain: a registered domain whose URL, enumeration, or read fails
    /// or times out — it is not actually serving.
    case domainNotServing = "domain-not-serving"
}

private enum OpFailure: Error {
    case timeout
    case error(NSError)

    var message: String {
        switch self {
        case .timeout: return "operation timed out in the companion app"
        case .error(let e): return "\(e.domain) \(e.code): \(e.localizedDescription)"
        }
    }
}

final class FileProviderController {
    private static var socketPath: String { StatusFile.realHome + "/.cc-pool/domains.sock" }
    private static let appVersion =
        (Bundle.main.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String) ?? "dev"

    /// Reply bounds stay ~20% under the Go client's per-op deadlines (fusekit
    /// fileproviderd/appclient.go: health 2s, probe 25s, path/signal 3s,
    /// register/remove 20s) so a slow NSFileProviderManager call surfaces as a
    /// classified reply, never a client-side hangup.
    private enum Bound {
        static let add: TimeInterval = 12
        static let url: TimeInterval = 4 // register total 16s
        static let removeLookup: TimeInterval = 3
        static let remove: TimeInterval = 12 // remove total 15s
        static let lookup: TimeInterval = 1
        static let quick: TimeInterval = 1.4 // path/signal total 2.4s
        static let probeAdd: TimeInterval = 7
        static let probeURL: TimeInterval = 3
        static let probeEnum: TimeInterval = 5
        static let probeRemove: TimeInterval = 4 // probe total ≤19s
        static let probeRead: TimeInterval = 4 // probe-domain total ≤13s (lookup+URL+enum+read)
    }

    private let acceptQueue = DispatchQueue(label: "cc-pool.fp.accept")
    private let connQueue = DispatchQueue(label: "cc-pool.fp.conn", attributes: .concurrent)
    /// Concurrent: different domains' NSFileProviderManager calls run in
    /// parallel (XPC bounds each domain at one op). Mutual exclusion is
    /// per-domain via `claims`, not queue serialization.
    private let domainQueue = DispatchQueue(
        label: "cc-pool.fp.domain", qos: .utility, attributes: .concurrent)
    /// Per-domain in-flight claims: same-domain ops serialize, different
    /// domains proceed concurrently. Health-ish reads (path/signal) run
    /// unclaimed so they can never bounce busy.
    private let claims = DomainClaims()
    /// Throwaway domain id the probe op registers; per-process stable so a
    /// probe claims a key disjoint from every real account domain.
    private let probeDomainID = "ccp-probe-\(getpid())"
    private var listenFD: Int32 = -1
    private lazy var baseWatcher = ClaudeBaseWatcher { [weak self] in self?.signalAllDomains() }
    private var loggedSignalError = false

    /// Safe on machines without the extension: every arm fails soft (logged
    /// no-op), so the widget-only build keeps launching cleanly.
    func start() {
        serveControlSocket()
        baseWatcher.start()
        rehydrate()
    }

    // MARK: - Control socket server

    private func serveControlSocket() {
        let path = Self.socketPath
        guard var addr = Self.unixAddr(path) else {
            NSLog("CCPoolStatus: control socket path too long: %@", path)
            return
        }
        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else {
            NSLog("CCPoolStatus: control socket create failed: %d", errno)
            return
        }
        var bound = Self.bindSocket(fd, &addr)
        if !bound, errno == EADDRINUSE {
            // Mirrors proc.SingleEntrant: a peer that answers health owns the
            // socket; a dead one left a stale file we can reclaim.
            if peerAnswersHealth() {
                NSLog("CCPoolStatus: live peer already serves %@; not serving", path)
                close(fd)
                return
            }
            unlink(path)
            bound = Self.bindSocket(fd, &addr)
        }
        guard bound else {
            let err = errno
            close(fd)
            if err == ENOENT {
                // ~/.cc-pool missing (pre-`ccp init`): retry once a minute,
                // mirroring StatusWatcher.
                DispatchQueue.main.asyncAfter(deadline: .now() + 60) { [weak self] in
                    self?.serveControlSocket()
                }
                return
            }
            NSLog("CCPoolStatus: control socket bind failed: %d", err)
            return
        }
        chmod(path, 0o600)
        guard listen(fd, 16) == 0 else {
            NSLog("CCPoolStatus: control socket listen failed: %d", errno)
            close(fd)
            unlink(path)
            return
        }
        listenFD = fd
        acceptQueue.async { [weak self] in self?.acceptLoop(fd) }
    }

    private func acceptLoop(_ fd: Int32) {
        while true {
            let conn = accept(fd, nil, nil)
            if conn < 0 {
                if errno == EINTR { continue }
                NSLog("CCPoolStatus: control accept failed: %d", errno)
                return
            }
            connQueue.async { [weak self] in self?.handle(conn) }
        }
    }

    private func handle(_ fd: Int32) {
        Self.setTimeouts(fd, seconds: 5)
        guard let line = Self.readLine(fd) else {
            close(fd)
            return
        }
        guard let req = try? JSONDecoder().decode(ControlRequest.self, from: line) else {
            reply(fd, .failure("malformed request", nil))
            return
        }
        guard req.proto == controlProto else {
            reply(fd, .failure("unsupported proto \(req.proto)", nil))
            return
        }
        switch req.op {
        case "health":
            var resp = ControlResponse(ok: true)
            resp.version = Self.appVersion
            reply(fd, resp)
        case "probe", "register", "path", "signal", "remove", "probe-domain":
            let domain = req.domain ?? ""
            guard req.op == "probe" || !domain.isEmpty else {
                reply(fd, .failure("domain required for op \(req.op)", nil))
                return
            }
            guard let key = DomainClaims.key(op: req.op, domain: domain, probeID: probeDomainID) else {
                // Unclaimed (path/signal): a health-ish read that must never
                // bounce busy — dispatch straight onto the concurrent queue.
                domainQueue.async { self.reply(fd, self.perform(req.op, domain: domain)) }
                return
            }
            guard claims.claim(key) else {
                reply(fd, .failure("domain \(key) is busy with another operation", .busy))
                return
            }
            domainQueue.async {
                defer { self.claims.release(key) }
                self.reply(fd, self.perform(req.op, domain: domain))
            }
        default:
            reply(fd, .failure("unknown op \(req.op)", nil))
        }
    }

    private func reply(_ fd: Int32, _ resp: ControlResponse) {
        if var data = try? JSONEncoder().encode(resp) {
            data.append(0x0A)
            _ = Self.writeAll(fd, data)
        }
        close(fd)
    }

    private func peerAnswersHealth() -> Bool {
        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else { return false }
        defer { close(fd) }
        guard var addr = Self.unixAddr(Self.socketPath) else { return false }
        let connected = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                connect(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size)) == 0
            }
        }
        guard connected else { return false }
        Self.setTimeouts(fd, seconds: 1)
        guard Self.writeAll(fd, Data("{\"proto\":1,\"op\":\"health\"}\n".utf8)) else { return false }
        struct Peer: Decodable { let proto: Int }
        guard let line = Self.readLine(fd) else { return false }
        return (try? JSONDecoder().decode(Peer.self, from: line)) != nil
    }

    // MARK: - Domain ops (per-domain serialized via claims, concurrent across domains)

    private func perform(_ op: String, domain: String) -> ControlResponse {
        switch op {
        case "register": return register(domain)
        case "remove": return remove(domain)
        case "path": return visiblePath(domain)
        case "signal": return signal(domain)
        case "probe": return probe()
        case "probe-domain": return probeDomain(domain)
        default: return .failure("unknown op \(op)", nil) // unreachable: handle() routed
        }
    }

    private func register(_ domain: String) -> ControlResponse {
        let d = NSFileProviderDomain(
            identifier: NSFileProviderDomainIdentifier(rawValue: domain), displayName: domain)
        d.isHidden = true // overlay-only: keep it out of Finder's Locations sidebar (files stay on disk)
        if let f = waitVoid(Bound.add, { NSFileProviderManager.add(d, completionHandler: $0) }) {
            return .failure("add domain \(domain): \(f.message)", classify(f))
        }
        guard let mgr = NSFileProviderManager(for: d) else {
            return .failure("no manager for domain \(domain)", .registerFailed)
        }
        switch waitURL(Bound.url, { mgr.getUserVisibleURL(for: .rootContainer, completionHandler: $0) }) {
        case .failure(let f):
            return .failure("user-visible URL for \(domain): \(f.message)", .registerFailed)
        case .success(let url):
            var resp = ControlResponse(ok: true)
            resp.path = url.path
            return resp
        }
    }

    private func remove(_ domain: String) -> ControlResponse {
        switch registeredDomains(Bound.removeLookup) {
        case .failure(let f):
            return .failure("list domains: \(f.message)", .registerFailed)
        case .success(let ds):
            guard let d = ds.first(where: { $0.identifier.rawValue == domain }) else {
                return ControlResponse(ok: true) // idempotent: already gone
            }
            if let f = waitVoid(Bound.remove, { NSFileProviderManager.remove(d, completionHandler: $0) }) {
                return .failure("remove domain \(domain): \(f.message)", classify(f))
            }
            return ControlResponse(ok: true)
        }
    }

    private func visiblePath(_ domain: String) -> ControlResponse {
        switch manager(for: domain, bound: Bound.lookup) {
        case .reply(let resp):
            return resp
        case .manager(let mgr):
            switch waitURL(Bound.quick, { mgr.getUserVisibleURL(for: .rootContainer, completionHandler: $0) }) {
            case .failure(let f):
                return .failure("user-visible URL for \(domain): \(f.message)", .registerFailed)
            case .success(let url):
                var resp = ControlResponse(ok: true)
                resp.path = url.path
                return resp
            }
        }
    }

    private func signal(_ domain: String) -> ControlResponse {
        switch manager(for: domain, bound: Bound.lookup) {
        case .reply(let resp):
            return resp
        case .manager(let mgr):
            if let f = waitVoid(Bound.quick, { mgr.signalEnumerator(for: .workingSet, completionHandler: $0) }) {
                return .failure("signal \(domain): \(f.message)", .registerFailed)
            }
            return ControlResponse(ok: true)
        }
    }

    /// Registers a throwaway domain and drives one real enumeration through
    /// the appex — the daemon's adoption gate. Conservative by design: only
    /// provable capability denials become no-entitlement (the irreversible
    /// retreat); every other failure is transient. Raw NSErrors are logged on
    /// every failure path to pin macOS-26 codes before the mapping is trusted.
    private func probe() -> ControlResponse {
        let id = probeDomainID
        let d = NSFileProviderDomain(
            identifier: NSFileProviderDomainIdentifier(rawValue: id), displayName: id)
        d.isHidden = true // never flash the throwaway probe domain in Finder's Locations
        defer {
            // Always tear the throwaway domain down, failure paths included.
            if let f = waitVoid(Bound.probeRemove, { NSFileProviderManager.remove(d, completionHandler: $0) }) {
                logProbe("remove", f)
            }
        }
        if let f = waitVoid(Bound.probeAdd, { NSFileProviderManager.add(d, completionHandler: $0) }) {
            logProbe("add", f)
            return .failure("probe add: \(f.message)", classify(f))
        }
        guard let mgr = NSFileProviderManager(for: d) else {
            NSLog("CCPoolStatus: probe: no manager for throwaway domain %@", id)
            return .failure("probe: no manager for throwaway domain", .registerFailed)
        }
        let url: URL
        switch waitURL(Bound.probeURL, { mgr.getUserVisibleURL(for: .rootContainer, completionHandler: $0) }) {
        case .failure(let f):
            logProbe("user-visible URL", f)
            return .failure("probe URL: \(f.message)", classify(f))
        case .success(let u):
            url = u
        }
        // A real readdir through the appex, bounded so a hung enumeration
        // stays inside the probe budget (worst case leaks one blocked thread).
        let sem = DispatchSemaphore(value: 0)
        var enumError: NSError?
        DispatchQueue.global(qos: .utility).async {
            do {
                _ = try FileManager.default.contentsOfDirectory(atPath: url.path)
            } catch {
                enumError = error as NSError
            }
            sem.signal()
        }
        guard sem.wait(timeout: .now() + Bound.probeEnum) == .success else {
            logProbe("enumerate", .timeout)
            return .failure("probe enumerate timed out", .registerFailed)
        }
        if let e = enumError {
            logProbe("enumerate", .error(e))
            return .failure("probe enumerate: \(OpFailure.error(e).message)", classify(.error(e)))
        }
        var resp = ControlResponse(ok: true)
        resp.fpOK = true
        return resp
    }

    private func logProbe(_ stage: String, _ f: OpFailure) {
        switch f {
        case .timeout:
            NSLog("CCPoolStatus: probe %@ timed out", stage)
        case .error(let e):
            NSLog("CCPoolStatus: probe %@ failed: domain=%@ code=%ld userInfo=%@",
                  stage, e.domain, e.code, String(describing: e.userInfo))
        }
    }

    /// Probes a registered account domain (the daemon's readiness gate),
    /// reporting `.claude.json`'s byte length via json_bytes: absent = no file,
    /// 0 = empty, >0 = real content read in full — never stat'd, since FPFS
    /// reports size 0 for materialized items. A getDomains/URL/enumerate/read
    /// failure or timeout is domain-not-serving; an unregistered id is no-domain.
    private func probeDomain(_ domain: String) -> ControlResponse {
        let mgr: NSFileProviderManager
        switch manager(for: domain, bound: Bound.lookup, lookupFailClass: .domainNotServing) {
        case .reply(let resp): return resp // unregistered → no-domain; lookup failure → domain-not-serving
        case .manager(let m): mgr = m
        }
        let url: URL
        switch waitURL(Bound.probeURL, { mgr.getUserVisibleURL(for: .rootContainer, completionHandler: $0) }) {
        case .failure(let f):
            return .failure("probe-domain URL for \(domain): \(f.message)", .domainNotServing)
        case .success(let u):
            url = u
        }
        let entries: [String]
        switch waitBlocking(Bound.probeEnum, { try FileManager.default.contentsOfDirectory(atPath: url.path) }) {
        case .failure(let f):
            return .failure("probe-domain enumerate \(domain): \(f.message)", .domainNotServing)
        case .success(let e):
            entries = e
        }
        guard entries.contains(".claude.json") else {
            return ControlResponse(ok: true) // serves, but no .claude.json → json_bytes absent
        }
        let file = url.appendingPathComponent(".claude.json").path
        switch waitBlocking(Bound.probeRead, { try Self.byteCount(ofFileAt: file) }) {
        case .failure(let f):
            return .failure("probe-domain read \(domain): \(f.message)", .domainNotServing)
        case .success(let n):
            var resp = ControlResponse(ok: true)
            resp.jsonBytes = Int64(n)
            return resp
        }
    }

    // MARK: - Error mapping

    /// Conservative: ONLY errors that provably mean "File Provider cannot
    /// serve here" become no-entitlement — the one class the daemon treats as
    /// a permanent retreat to the symlink floor (control.go ClassNoEntitlement).
    /// Everything else, including every timeout, stays register-failed.
    private func classify(_ f: OpFailure) -> ErrClass {
        guard case .error(let e) = f else { return .registerFailed }
        guard isCapabilityDenial(e) else { return .registerFailed }
        NSLog("CCPoolStatus: file provider capability denial: domain=%@ code=%ld userInfo=%@",
              e.domain, e.code, String(describing: e.userInfo))
        return .noEntitlement
    }

    private func isCapabilityDenial(_ e: NSError) -> Bool {
        if e.domain == NSFileProviderErrorDomain {
            if NSFileProviderError.Code(rawValue: e.code) == .providerNotFound { return true }
            if #available(macOS 14.1, *),
               NSFileProviderError.Code(rawValue: e.code) == .applicationExtensionNotFound {
                return true
            }
            return false
        }
        return e.domain == NSCocoaErrorDomain && e.code == CocoaError.featureUnsupported.rawValue
    }

    // MARK: - Manager lookup

    private enum Lookup {
        case manager(NSFileProviderManager)
        case reply(ControlResponse)
    }

    /// Resolves a registered domain to its manager. An unregistered id is always
    /// no-domain (transient — the daemon re-registers); a getDomains error/timeout or
    /// a nil manager surfaces as `lookupFailClass`. probe-domain passes
    /// .domainNotServing (a lookup it can't complete = not serving = a real retryable
    /// wedge on the Go side); other ops keep the default .registerFailed.
    private func manager(for domain: String, bound: TimeInterval, lookupFailClass: ErrClass = .registerFailed) -> Lookup {
        switch registeredDomains(bound) {
        case .failure(let f):
            return .reply(.failure("list domains: \(f.message)", lookupFailClass))
        case .success(let ds):
            guard let d = ds.first(where: { $0.identifier.rawValue == domain }) else {
                return .reply(.failure("domain \(domain) is not registered", .noDomain))
            }
            guard let mgr = NSFileProviderManager(for: d) else {
                return .reply(.failure("no manager for domain \(domain)", lookupFailClass))
            }
            return .manager(mgr)
        }
    }

    // MARK: - Completion-handler bridging

    /// Bridges a completion-handler call to a bounded synchronous wait. A
    /// callback firing after the timeout only writes captured locals nothing
    /// reads again; the semaphore orders the in-time path.
    private func waitVoid(_ bound: TimeInterval, _ start: (@escaping (Error?) -> Void) -> Void) -> OpFailure? {
        let sem = DispatchSemaphore(value: 0)
        var failure: NSError?
        start { err in
            failure = err as NSError?
            sem.signal()
        }
        guard sem.wait(timeout: .now() + bound) == .success else { return .timeout }
        return failure.map { .error($0) }
    }

    private func waitURL(_ bound: TimeInterval, _ start: (@escaping (URL?, Error?) -> Void) -> Void) -> Result<URL, OpFailure> {
        let sem = DispatchSemaphore(value: 0)
        var url: URL?
        var failure: NSError?
        start { u, err in
            url = u
            failure = err as NSError?
            sem.signal()
        }
        guard sem.wait(timeout: .now() + bound) == .success else { return .failure(.timeout) }
        if let failure { return .failure(.error(failure)) }
        guard let url else {
            return .failure(.error(NSError(
                domain: NSCocoaErrorDomain, code: CocoaError.fileReadUnknown.rawValue,
                userInfo: [NSLocalizedDescriptionKey: "no URL returned"])))
        }
        return .success(url)
    }

    private func registeredDomains(_ bound: TimeInterval) -> Result<[NSFileProviderDomain], OpFailure> {
        let sem = DispatchSemaphore(value: 0)
        var domains: [NSFileProviderDomain] = []
        var failure: NSError?
        NSFileProviderManager.getDomainsWithCompletionHandler { ds, err in
            domains = ds
            failure = err as NSError?
            sem.signal()
        }
        guard sem.wait(timeout: .now() + bound) == .success else { return .failure(.timeout) }
        if let failure { return .failure(.error(failure)) }
        return .success(domains)
    }

    /// Bounds a blocking filesystem closure on a utility queue. A closure that
    /// outruns `bound` surfaces as .timeout, leaking one blocked thread until it
    /// returns (acceptable inside the probe budget).
    private func waitBlocking<T>(
        _ bound: TimeInterval, _ work: @escaping () throws -> T
    ) -> Result<T, OpFailure> {
        let sem = DispatchSemaphore(value: 0)
        var result: Result<T, OpFailure>?
        DispatchQueue.global(qos: .utility).async {
            do { result = .success(try work()) } catch { result = .failure(.error(error as NSError)) }
            sem.signal()
        }
        guard sem.wait(timeout: .now() + bound) == .success else { return .failure(.timeout) }
        return result ?? .failure(.timeout)
    }

    // MARK: - Base watcher + rehydrate

    /// ~/.claude changed (edits by plain `claude`): nudge every registered
    /// domain's working set so replicas re-enumerate. Daemon-originated
    /// changes arrive as targeted control signals instead.
    private func signalAllDomains() {
        NSFileProviderManager.getDomainsWithCompletionHandler { [weak self] domains, error in
            if let error {
                // Expected forever on widget-only installs: log once.
                guard let self, !self.loggedSignalError else { return }
                self.loggedSignalError = true
                NSLog("CCPoolStatus: list domains for base signal failed: %@", String(describing: error))
                return
            }
            for d in domains {
                NSFileProviderManager(for: d)?.signalEnumerator(for: .workingSet) { err in
                    if let err {
                        NSLog("CCPoolStatus: signal %@ failed: %@",
                              d.identifier.rawValue, String(describing: err))
                    }
                }
            }
        }
    }

    /// NSFileProviderManager is the source of truth for registered domains —
    /// no local DB. On launch, log what survives and freshen each working set
    /// (the app may have been dead across base edits).
    private func rehydrate() {
        NSFileProviderManager.getDomainsWithCompletionHandler { domains, error in
            if let error {
                NSLog("CCPoolStatus: file provider rehydrate skipped: %@", String(describing: error))
                return
            }
            guard !domains.isEmpty else { return }
            let ids = domains.map { $0.identifier.rawValue }.sorted().joined(separator: ", ")
            NSLog("CCPoolStatus: file provider domains: %@", ids)
            for d in domains {
                NSFileProviderManager(for: d)?.signalEnumerator(for: .workingSet) { _ in }
            }
        }
    }

    // MARK: - POSIX plumbing

    private static func unixAddr(_ path: String) -> sockaddr_un? {
        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        addr.sun_len = UInt8(MemoryLayout<sockaddr_un>.size)
        let bytes = Array(path.utf8)
        guard bytes.count < MemoryLayout.size(ofValue: addr.sun_path) else { return nil }
        withUnsafeMutableBytes(of: &addr.sun_path) { $0.copyBytes(from: bytes) }
        return addr
    }

    private static func bindSocket(_ fd: Int32, _ addr: inout sockaddr_un) -> Bool {
        withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.bind(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size)) == 0
            }
        }
    }

    /// Byte count of the whole file, from read(2) looped to EOF — never
    /// st_size, which FPFS reports as 0 for materialized items.
    private static func byteCount(ofFileAt path: String) throws -> Int {
        let fd = open(path, O_RDONLY)
        guard fd >= 0 else { throw posixError("open", path) }
        defer { close(fd) }
        var total = 0
        var buf = [UInt8](repeating: 0, count: 64 * 1024)
        while true {
            let n = read(fd, &buf, buf.count)
            if n < 0 {
                if errno == EINTR { continue }
                throw posixError("read", path)
            }
            if n == 0 { return total }
            total += n
        }
    }

    private static func posixError(_ op: String, _ path: String) -> NSError {
        let code = errno
        return NSError(domain: NSPOSIXErrorDomain, code: Int(code),
                       userInfo: [NSLocalizedDescriptionKey: "\(op) \(path): \(String(cString: strerror(code)))"])
    }

    private static func setTimeouts(_ fd: Int32, seconds: Int) {
        var tv = timeval(tv_sec: seconds, tv_usec: 0)
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, socklen_t(MemoryLayout<timeval>.size))
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, socklen_t(MemoryLayout<timeval>.size))
    }

    /// Reads one newline-terminated request (newline stripped); nil on EOF,
    /// error, or a line over 64 KiB.
    private static func readLine(_ fd: Int32) -> Data? {
        var data = Data()
        var chunk = [UInt8](repeating: 0, count: 4096)
        while data.count < 64 * 1024 {
            let n = read(fd, &chunk, chunk.count)
            if n <= 0 { return data.isEmpty ? nil : data }
            if let nl = chunk[0..<n].firstIndex(of: 0x0A) {
                data.append(contentsOf: chunk[0..<nl])
                return data
            }
            data.append(contentsOf: chunk[0..<n])
        }
        return nil
    }

    private static func writeAll(_ fd: Int32, _ data: Data) -> Bool {
        data.withUnsafeBytes { (raw: UnsafeRawBufferPointer) -> Bool in
            var off = 0
            while off < raw.count {
                let n = write(fd, raw.baseAddress! + off, raw.count - off)
                if n <= 0 { return false }
                off += n
            }
            return true
        }
    }
}

/// Watches ~/.claude with FSEvents (file-level, nested — base edits happen at
/// arbitrary depth) and fires after a trailing debounce. StatusWatcher's
/// single-dir vnode watch can't see nested changes, hence a separate mechanism
/// with the same lifecycle shape.
private final class ClaudeBaseWatcher {
    private let queue = DispatchQueue(label: "cc-pool.fp.base-watch")
    private var stream: FSEventStreamRef?
    private var pending: DispatchWorkItem?
    private let onChange: () -> Void

    init(onChange: @escaping () -> Void) {
        self.onChange = onChange
    }

    func start() {
        guard stream == nil else { return }
        let path = StatusFile.realHome + "/.claude"
        var context = FSEventStreamContext(
            version: 0, info: Unmanaged.passUnretained(self).toOpaque(),
            retain: nil, release: nil, copyDescription: nil)
        let callback: FSEventStreamCallback = { _, info, _, _, _, _ in
            guard let info else { return }
            Unmanaged<ClaudeBaseWatcher>.fromOpaque(info).takeUnretainedValue().changed()
        }
        guard let s = FSEventStreamCreate(
            nil, callback, &context, [path] as CFArray,
            FSEventStreamEventId(kFSEventStreamEventIdSinceNow), 1.0,
            FSEventStreamCreateFlags(kFSEventStreamCreateFlagFileEvents | kFSEventStreamCreateFlagNoDefer))
        else {
            NSLog("CCPoolStatus: FSEvents stream create failed for %@", path)
            return
        }
        FSEventStreamSetDispatchQueue(s, queue)
        FSEventStreamStart(s)
        stream = s
    }

    /// Trailing debounce: a burst (temp write + rename) collapses into one
    /// change after 2s of quiet — unlike a leading-edge limiter it never
    /// drops the final event of a burst. Runs on `queue` (the stream's
    /// dispatch queue), which serializes `pending`.
    private func changed() {
        pending?.cancel()
        let item = DispatchWorkItem { [onChange] in onChange() }
        pending = item
        queue.asyncAfter(deadline: .now() + 2, execute: item)
    }
}
