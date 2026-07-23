package main

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
)

func TestCanonicalSchemaTracksWireShapeOnly(t *testing.T) {
	write := func(name, source string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	base := `package daemon
const WireBuild = "old"
const SnapshotVersion = 1
const ServiceRoleID = "old-role"
const OpStatus Op = "status"
type Op string
type Request struct { Op Op ` + "`json:\"-\"`" + `; Account int ` + "`json:\"account\"`" + ` }
type Response struct{}
type HealthRequest struct{}
type HealthResponse struct{}
type Snapshot struct { Value string }
`
	identityOnly := `package daemon
const WireBuild = "new"
const SnapshotVersion = 99
const ServiceRoleID = "new-role"
const OpStatus Op = "status"
type Op string
type Request struct { Op Op ` + "`json:\"-\"`" + `; Account int ` + "`json:\"account\"`" + ` }
type Response struct{}
type HealthRequest struct{}
type HealthResponse struct{}
type Snapshot struct { Value int }
`
	changedShape := `package daemon
const WireBuild = "new"
const SnapshotVersion = 99
const ServiceRoleID = "new-role"
const OpStatus Op = "status"
type Op string
type Request struct { Op Op ` + "`json:\"-\"`" + `; Account int ` + "`json:\"account_id\"`" + ` }
type Response struct{}
type HealthRequest struct{}
type HealthResponse struct{}
type Snapshot struct { Value int }
`
	canonical := func(source string) []byte {
		t.Helper()
		got, err := canonicalSchema(write("protocol.go", source))
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	want := canonical(base)
	if got := canonical(identityOnly); !bytes.Equal(got, want) {
		t.Fatalf("identity-only change altered canonical schema:\n%s\nwant:\n%s", got, want)
	}
	if got := canonical(changedShape); bytes.Equal(got, want) {
		t.Fatalf("wire shape change preserved canonical schema:\n%s", got)
	}
}

func TestCanonicalSchemaTracksNestedLocalShapeAndEnumValues(t *testing.T) {
	write := func(source string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "protocol.go")
		if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	base := `package daemon
type Mode string
const ModeReady Mode = "ready"
type Nested struct { Value string ` + "`json:\"value\"`" + ` }
type Request struct { Nested Nested ` + "`json:\"nested\"`" + `; Mode Mode ` + "`json:\"mode\"`" + ` }
type Response struct{}
type HealthRequest struct{}
type HealthResponse struct{}
`
	nestedShapeChanged := `package daemon
type Mode string
const ModeReady Mode = "ready"
type Nested struct { Value int ` + "`json:\"value\"`" + ` }
type Request struct { Nested Nested ` + "`json:\"nested\"`" + `; Mode Mode ` + "`json:\"mode\"`" + ` }
type Response struct{}
type HealthRequest struct{}
type HealthResponse struct{}
`
	enumValueChanged := `package daemon
type Mode string
const ModeReady Mode = "prepared"
type Nested struct { Value string ` + "`json:\"value\"`" + ` }
type Request struct { Nested Nested ` + "`json:\"nested\"`" + `; Mode Mode ` + "`json:\"mode\"`" + ` }
type Response struct{}
type HealthRequest struct{}
type HealthResponse struct{}
`
	canonical := func(source string) []byte {
		t.Helper()
		result, err := canonicalSchema(write(source))
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	want := canonical(base)
	wantDigest := sha256.Sum256(want)
	for name, source := range map[string]string{
		"nested shape":     nestedShapeChanged,
		"typed enum value": enumValueChanged,
	} {
		t.Run(name, func(t *testing.T) {
			got := canonical(source)
			if bytes.Equal(got, want) {
				t.Fatalf("%s change preserved canonical schema:\n%s", name, got)
			}
			if digest := sha256.Sum256(got); digest == wantDigest {
				t.Fatalf("%s change preserved schema digest %x", name, digest)
			}
		})
	}
}
