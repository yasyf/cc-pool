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
