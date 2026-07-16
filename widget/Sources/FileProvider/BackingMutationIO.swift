import Darwin
import Foundation

enum BackingMutationIO {
    /// File Provider calls arrive while fileproviderd owns an access claim.
    /// Asking NSFileCoordinator for a nested claim deadlocks until the appex is killed.
    static func replace(path: String, data: Data) throws {
        let parent = (path as NSString).deletingLastPathComponent
        let tmp = parent + "/._ccp-tmp-" + UUID().uuidString
        defer { _ = Darwin.unlink(tmp) }

        try data.write(to: URL(fileURLWithPath: tmp), options: .withoutOverwriting)
        guard Darwin.chmod(tmp, S_IRUSR | S_IWUSR) == 0 else {
            throw posixError("chmod", tmp)
        }
        guard Darwin.rename(tmp, path) == 0 else {
            throw posixError("rename", path)
        }
    }

    static func rename(from src: String, to dst: String) throws {
        guard Darwin.rename(src, dst) == 0 else {
            throw posixError("rename", dst)
        }
    }

    static func remove(_ path: String) throws {
        try FileManager.default.removeItem(atPath: path)
    }

    static func cloneOrCopy(from src: String, to dst: String) throws {
        if copyfile(src, dst, nil, copyfile_flags_t(COPYFILE_CLONE)) == 0 { return }
        _ = Darwin.unlink(dst)
        try FileManager.default.copyItem(
            at: URL(fileURLWithPath: src), to: URL(fileURLWithPath: dst))
    }

    private static func posixError(_ operation: String, _ path: String) -> NSError {
        let code = errno
        return NSError(
            domain: NSPOSIXErrorDomain,
            code: Int(code),
            userInfo: [NSLocalizedDescriptionKey: "\(operation) \(path): \(String(cString: strerror(code)))"])
    }
}
