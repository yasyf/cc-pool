package main

import "testing"

func TestRunRequiresExactApplicationAndSigningIdentity(t *testing.T) {
	for _, arguments := range [][]string{
		nil,
		{"-app", "/Applications/CCPoolStatus.app"},
		{"-signing-identity", "Developer ID Application: Example"},
		{"-app", "/Applications/CCPoolStatus.app", "-signing-identity", "Developer ID Application: Example", "extra"},
	} {
		if err := run(t.Context(), arguments); err == nil {
			t.Fatalf("run(%q) accepted incomplete invocation", arguments)
		}
	}
}
