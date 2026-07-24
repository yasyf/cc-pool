package daemon

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
)

func TestBootstrapProgressIsExactAndDeterministic(t *testing.T) {
	server := &Server{}
	server.beginBootstrap()
	server.setBootstrapTotal(3)
	server.settleBootstrapAccount(3, true, nil)
	server.settleBootstrapAccount(1, false, errors.New("presentation failed"))
	server.settleBootstrapAccount(2, false, nil)
	server.finishBootstrap(errors.New("aggregate failure"))

	progress := server.bootstrapSnapshot()
	if progress.Total != 3 || progress.Settled != 3 ||
		progress.Quarantined != 1 || !progress.Terminal || progress.Revision != 6 {
		t.Fatalf("bootstrap progress = %+v", progress)
	}
	if progress.FailureCount != 1 || len(progress.FailureDigest) != 64 {
		t.Fatalf("bootstrap failure summary = %+v", progress)
	}
}

func TestBootstrapPreFleetFailureIsTerminalProgress(t *testing.T) {
	server := &Server{}
	server.beginBootstrap()
	server.finishBootstrap(errors.New("open account store"))
	progress := server.bootstrapSnapshot()
	if !progress.Terminal || progress.FailureCount != 1 || len(progress.FailureDigest) != 64 ||
		progress.Revision != 2 {
		t.Fatalf("pre-fleet progress = %+v", progress)
	}
}

func TestBootstrapBarrierSnapshotIsOpaqueExactAndMonotonic(t *testing.T) {
	server := &Server{}
	server.beginBootstrap()
	firstRevision, firstPayload, err := server.bootstrapBarrierSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	server.setBootstrapTotal(1)
	server.settleBootstrapAccount(1, false, nil)
	server.finishBootstrap(nil)
	server.finishBootstrap(errors.New("ignored duplicate finish"))
	finalRevision, finalPayload, err := server.bootstrapBarrierSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	repeatRevision, repeatPayload, err := server.bootstrapBarrierSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if firstRevision != 1 || finalRevision != 4 || finalRevision <= firstRevision {
		t.Fatalf("revisions = %d -> %d", firstRevision, finalRevision)
	}
	if repeatRevision != finalRevision || !bytes.Equal(repeatPayload, finalPayload) {
		t.Fatalf("same revision changed token: %d/%s then %d/%s", finalRevision, finalPayload, repeatRevision, repeatPayload)
	}
	var decoded bootstrapProgress
	if err := json.Unmarshal(finalPayload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Schema != 1 || decoded.Revision != finalRevision || decoded.Total != 1 ||
		decoded.Settled != 1 || !decoded.Terminal || decoded.FailureCount != 0 ||
		len(decoded.FailureDigest) != 64 || bytes.Contains(finalPayload, []byte("ignored duplicate finish")) {
		t.Fatalf("opaque progress = %+v payload=%s first=%s", decoded, finalPayload, firstPayload)
	}
}

func TestBootstrapBarrierFailureTokenIsOrderIndependentAndRedacted(t *testing.T) {
	build := func(order []int) []byte {
		t.Helper()
		server := &Server{}
		server.beginBootstrap()
		server.setBootstrapTotal(2)
		for _, accountID := range order {
			server.settleBootstrapAccount(accountID, false, errors.New(map[int]string{
				1: "secret first failure", 2: "secret second failure",
			}[accountID]))
		}
		server.finishBootstrap(errors.New("aggregate contains secrets"))
		_, payload, err := server.bootstrapBarrierSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	forward := build([]int{1, 2})
	reverse := build([]int{2, 1})
	if !bytes.Equal(forward, reverse) {
		t.Fatalf("failure ordering changed token: %s != %s", forward, reverse)
	}
	if bytes.Contains(forward, []byte("secret first")) || bytes.Contains(forward, []byte("secret second")) {
		t.Fatalf("raw failure leaked into token: %s", forward)
	}
}
