package daemon

import (
	"strings"
	"testing"
)

func TestValidateDaemonHealthRequiresExactReadyBusinessResponse(t *testing.T) {
	const build = "cc-pool-test"
	healthy := Response{OK: true, Version: build}
	if err := validateDaemonHealth(healthy); err != nil {
		t.Fatalf("healthy business response: %v", err)
	}
	for _, test := range []struct {
		name string
		edit func(*Response)
		want string
	}{
		{name: "not ready", edit: func(h *Response) { h.OK = false }, want: "is invalid"},
		{name: "build", edit: func(h *Response) { h.Version = "" }, want: "is invalid"},
		{name: "error", edit: func(h *Response) { h.Error = "degraded" }, want: "is invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := healthy
			test.edit(&got)
			if err := validateDaemonHealth(got); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateDaemonHealth(%#v) = %v, want %q", got, err, test.want)
			}
		})
	}
}
