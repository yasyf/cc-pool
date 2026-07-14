import Foundation

/// Bridge-level failure shapes a synth metadata build can hit, exactly as
/// BridgeClient surfaces them: ok:false replies and missing synths are
/// NSCocoaErrorDomain NSFileReadUnknownError; a malformed/truncated reply is
/// a JSONDecoder DecodingError.
enum BridgeFailureArms {
    static let all: [(name: String, error: Error)] = [
        ("ok:false reply", replyError("bridge read: unknown error")),
        ("malformed reply", malformedReplyError()),
        ("missing synth", replyError("bridge read: no synth named .claude.json")),
    ]

    private static func replyError(_ msg: String) -> Error {
        NSError(domain: NSCocoaErrorDomain, code: NSFileReadUnknownError,
                userInfo: [NSLocalizedDescriptionKey: msg])
    }

    private static func malformedReplyError() -> Error {
        do {
            _ = try JSONDecoder().decode([String: Int].self, from: Data("{\"ok\":tru".utf8))
        } catch {
            return error
        }
        fatalError("truncated JSON must fail to decode")
    }
}
