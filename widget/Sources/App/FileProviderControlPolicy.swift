import CoreServices
import Foundation

enum LstatPresence: Equatable {
    case present
    case missing
    case failed(Int32)
}

enum ReplicaMaterialization: Equatable {
    case materialized
    case dataless
    case missing
    case failed(Int32)
}

enum ItemOperationFailureDisposition: Equatable {
    case domainMissing
    case fleetFailure
}

enum FileProviderControlPolicy {
    static func presence(result: Int32, errno code: Int32) -> LstatPresence {
        if result == 0 { return .present }
        return code == ENOENT ? .missing : .failed(code)
    }

    static func materialization(
        result: Int32, errno code: Int32, flags: UInt32, datalessFlag: UInt32
    ) -> ReplicaMaterialization {
        if result == 0 {
            return flags & datalessFlag == 0 ? .materialized : .dataless
        }
        return code == ENOENT ? .missing : .failed(code)
    }

    static func itemFailureDisposition(
        domain: String, code: Int, fileProviderDomain: String,
        fileProviderNoSuchItem: Int, cocoaNoSuchFile: Int
    ) -> ItemOperationFailureDisposition {
        if domain == fileProviderDomain, code == fileProviderNoSuchItem {
            return .domainMissing
        }
        if domain == NSCocoaErrorDomain, code == cocoaNoSuchFile {
            return .domainMissing
        }
        return .fleetFailure
    }
}

struct MissingItemRetryPolicy {
    private(set) var attempt = 0

    mutating func next() -> (shouldSignal: Bool, delay: useconds_t) {
        let shift = min(attempt, 4)
        let delay = min(useconds_t(200_000) << shift, 2_000_000)
        defer { attempt += 1 }
        return (attempt == 0, delay)
    }
}

enum ClaudeBaseEventPolicy {
    static func reason(
        claudeRoot: String, path: String, flags: FSEventStreamEventFlags
    ) -> String? {
        let rescan = FSEventStreamEventFlags(
            kFSEventStreamEventFlagMustScanSubDirs
                | kFSEventStreamEventFlagUserDropped
                | kFSEventStreamEventFlagKernelDropped
                | kFSEventStreamEventFlagEventIdsWrapped
                | kFSEventStreamEventFlagRootChanged)
        if flags & rescan != 0 { return "fsevents-rescan" }

        let structural = FSEventStreamEventFlags(
            kFSEventStreamEventFlagItemCreated
                | kFSEventStreamEventFlagItemRemoved
                | kFSEventStreamEventFlagItemRenamed)
        if path == claudeRoot {
            return flags & structural == 0 ? nil : "base-root"
        }
        let prefix = claudeRoot + "/"
        guard path.hasPrefix(prefix) else { return nil }
        let relative = path.dropFirst(prefix.count)
        guard !relative.contains("/") else { return nil }

        if relative == "settings.json" { return "base-settings" }
        if relative.hasPrefix("settings.json.") {
            return flags & structural == 0 ? nil : "base-settings-replace"
        }
        return flags & structural == 0 ? nil : "base-root-entry"
    }
}

struct CanonicalConfigFingerprint: Equatable {
    let device: UInt64
    let inode: UInt64
    let size: Int64
    let modifiedSeconds: Int
    let modifiedNanoseconds: Int
    let changedSeconds: Int
    let changedNanoseconds: Int
}

enum CanonicalConfigPolicy {
    static func changed(
        from previous: CanonicalConfigFingerprint?, to current: CanonicalConfigFingerprint?
    ) -> Bool {
        previous != current
    }
}

enum FileProviderRehydratePolicy {
    static func shouldSignal(lastBuild: String?, currentBuild: String) -> Bool {
        lastBuild != currentBuild
    }
}
