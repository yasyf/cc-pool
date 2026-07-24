import FileProvider
import FuseKit

struct CCPoolFileProviderMutationDispositionPolicy:
  CatalogFileProviderMutationDispositionPolicy
{
  static let privateStagingPrefixes = [
    ".claude.json.tmp.",
    "settings.json.tmp.",
    ".credentials.json.tmp.",
    ".last-update-result.json.tmp.",
    "remote-settings.json.tmp.",
    "mcp-needs-auth-cache.json.tmp.",
    "stats-cache.json.tmp.",
    "policy-limits.json.tmp.",
  ]

  func disposition(
    for template: any NSFileProviderItem,
    fields _: NSFileProviderItemFields
  ) throws -> CatalogMutationDisposition {
    Self.disposition(
      parent: template.parentItemIdentifier,
      filename: template.filename
    )
  }

  static func disposition(
    parent: NSFileProviderItemIdentifier,
    filename: String
  ) -> CatalogMutationDisposition {
    guard parent == .rootContainer,
      privateStagingPrefixes.contains(where: {
        hasASCIICaseInsensitivePrefix(filename, $0)
      })
    else {
      return .namespace
    }
    return .privateStaging
  }

  private static func hasASCIICaseInsensitivePrefix(
    _ value: String,
    _ prefix: String
  ) -> Bool {
    let valueBytes = Array(value.utf8)
    let prefixBytes = Array(prefix.utf8)
    guard valueBytes.count >= prefixBytes.count else { return false }
    for index in prefixBytes.indices
    where asciiLower(valueBytes[index]) != asciiLower(prefixBytes[index]) {
      return false
    }
    return true
  }

  private static func asciiLower(_ byte: UInt8) -> UInt8 {
    byte >= 65 && byte <= 90 ? byte + 32 : byte
  }
}
