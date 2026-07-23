package creds

import "fmt"

// Source identifies the Keychain credential backend.
type Source int

const (
	// SourceKeychain is the macOS Keychain item named ServiceName(configDir).
	SourceKeychain Source = iota
)

// String names the backend for diagnostics.
func (s Source) String() string {
	if s == SourceKeychain {
		return "keychain"
	}
	return fmt.Sprintf("source(%d)", int(s))
}
