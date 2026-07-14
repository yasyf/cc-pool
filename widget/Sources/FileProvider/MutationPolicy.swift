import FileProvider
import Foundation

enum SynthCreateAction: Equatable {
    case adopt
    case replace
    case rejectMissingContents

    static func decide(mayAlreadyExist: Bool, hasContents: Bool) -> SynthCreateAction {
        if hasContents { return .replace }
        return mayAlreadyExist ? .adopt : .rejectMissingContents
    }
}

struct MutationResult {
    let item: FPItem
    let shouldFetchContent: Bool

    static func unchanged(_ item: FPItem) -> MutationResult {
        MutationResult(item: item, shouldFetchContent: false)
    }

    static func canonicalized(_ item: FPItem) -> MutationResult {
        MutationResult(item: item, shouldFetchContent: true)
    }
}

struct SynthMutationCoordinator {
    struct Operations {
        let write: (String, Data) throws -> Void
        let persistPrivate: (String, Data) throws -> Void
        let removeStaging: (String) throws -> Void
        let lookup: (NSFileProviderItemIdentifier) throws -> FPItem
        let announce: () throws -> Void
    }

    let operations: Operations

    func adopt(name: String) throws -> MutationResult {
        .unchanged(try operations.lookup(ItemID.computed(name).identifier))
    }

    func commit(name: String, data: Data, removing stagingPath: String?) throws -> MutationResult {
        try operations.write(name, data)
        if name == ".claude.json" {
            try operations.persistPrivate(name, data)
        }
        if let stagingPath {
            try operations.removeStaging(stagingPath)
        }
        let result = MutationResult.canonicalized(
            try operations.lookup(ItemID.computed(name).identifier))
        try operations.announce()
        return result
    }
}

struct ComputedReannouncement {
    static let containers: [NSFileProviderItemIdentifier] = [.rootContainer, .workingSet]

    let identifier: NSFileProviderItemIdentifier

    init(computedName: String) {
        identifier = ItemID.computed(computedName).identifier
    }

    func invalidate(_ anchors: AnchorStore) throws {
        try anchors.remove(identifier, from: Self.containers)
    }

    func signal(_ body: (NSFileProviderItemIdentifier) -> Void) {
        for container in Self.containers { body(container) }
    }

    func completeDeletion(completion: () -> Void,
                          signal body: (NSFileProviderItemIdentifier) -> Void) {
        completion()
        signal(body)
    }
}
