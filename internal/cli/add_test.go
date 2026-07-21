package cli

import (
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/store"
)

func TestAccountHeader(t *testing.T) {
	cases := []struct {
		name string
		n    int
		opts addOptions
		want string
	}{
		{"interactive loop numbers each section", 2, addOptions{}, "Account 2"},
		{"counted run shows progress", 2, addOptions{count: 3}, "Account 2 of 3"},
		{"count of one is a lone section", 1, addOptions{count: 1}, ""},
		{"auto-yes adds exactly one account", 1, addOptions{autoYes: true}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := accountHeader(tc.n, tc.opts)
			if tc.want == "" && got != "" {
				t.Fatalf("accountHeader = %q, want empty", got)
			}
			if tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("accountHeader = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAddedSummary(t *testing.T) {
	if got := addedSummary([]store.Account{{Label: "A"}, {Label: "B"}}); got != "Added A and B." {
		t.Fatalf("addedSummary = %q", got)
	}
}
