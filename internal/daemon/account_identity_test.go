package daemon

import (
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
)

func TestAccountIdentityRequiresStoredPositiveAccountAndReturnsMinimalProjection(t *testing.T) {
	s, _, account := newAccountMutationTestServer(t, true)
	if response := s.dispatch(
		t.Context(), Request{Op: OpAccountIdentity},
	); response.OK || response.Error == "" {
		t.Fatalf("nil account identity request = %+v", response)
	}
	zero := 0
	if response := s.dispatch(
		t.Context(), Request{Op: OpAccountIdentity, Account: &zero},
	); response.OK || response.Error == "" {
		t.Fatalf("zero account identity request = %+v", response)
	}
	missing := account.ID + 1
	if response := s.dispatch(
		t.Context(), Request{Op: OpAccountIdentity, Account: &missing},
	); response.OK || response.Error == "" {
		t.Fatalf("missing account identity request = %+v", response)
	}
	response := s.dispatch(
		t.Context(), Request{Op: OpAccountIdentity, Account: &account.ID},
	)
	if !response.OK || response.Error != "" || response.AccountIdentity == nil {
		t.Fatalf("account identity response = %+v", response)
	}
	if got := *response.AccountIdentity; got.AccountID != account.ID ||
		got.AccountUUID != "old-uuid" || got.EmailAddress != "old@example.com" {
		t.Fatalf("account identity projection = %+v", got)
	}
	ladder, err := operationLadder()
	if err != nil {
		t.Fatal(err)
	}
	server, client, ok := ladder.Deadlines(wire.Op(OpAccountIdentity))
	if !ok || server != 31*time.Second || client != 32*time.Second {
		t.Fatalf("account identity ladder = server %s client %s ok=%t", server, client, ok)
	}
}

func TestAccountHealthRequiresStoredPositiveAccountAndWorkerProof(t *testing.T) {
	s, _, account := newAccountMutationTestServer(t, true)
	if response := s.dispatch(
		t.Context(), Request{Op: OpAccountHealth},
	); response.OK || response.Error == "" {
		t.Fatalf("nil account health request = %+v", response)
	}
	zero := 0
	if response := s.dispatch(
		t.Context(), Request{Op: OpAccountHealth, Account: &zero},
	); response.OK || response.Error == "" {
		t.Fatalf("zero account health request = %+v", response)
	}
	missing := account.ID + 1
	if response := s.dispatch(
		t.Context(), Request{Op: OpAccountHealth, Account: &missing},
	); response.OK || response.Error == "" {
		t.Fatalf("missing account health request = %+v", response)
	}
	response := s.dispatch(
		t.Context(), Request{Op: OpAccountHealth, Account: &account.ID},
	)
	if !response.OK || response.Error != "" || response.AccountHealth == nil ||
		response.AccountHealth.AccountID != account.ID {
		t.Fatalf("account health response = %+v", response)
	}
	ladder, err := operationLadder()
	if err != nil {
		t.Fatal(err)
	}
	server, client, ok := ladder.Deadlines(wire.Op(OpAccountHealth))
	if !ok || server != 61*time.Second || client != 62*time.Second {
		t.Fatalf("account health ladder = server %s client %s ok=%t", server, client, ok)
	}
}
