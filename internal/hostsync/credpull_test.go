package hostsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// pullCred builds an OWNED credential with every OAuth field populated so
// round-trip assertions cover the optional fields too.
func pullCred(suffix string, expiresAt int64) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = "at-" + suffix
	c.ClaudeAiOauth.RefreshToken = "rt-" + suffix
	c.ClaudeAiOauth.ExpiresAt = expiresAt
	c.ClaudeAiOauth.Scopes = []string{"user:inference", "user:profile"}
	c.ClaudeAiOauth.SubscriptionType = "max"
	c.ClaudeAiOauth.RateLimitTier = "raven"
	c.ClaudeAiOauth.ClientID = "client-" + suffix
	return c
}

// fakeTransport is a hand fake syncservice.Transport driving an injected Do.
type fakeTransport struct {
	do     func(ctx context.Context, req *rpc.Request) (*syncservice.Response, error)
	closed bool
}

func (t *fakeTransport) Do(ctx context.Context, req *rpc.Request) (*syncservice.Response, error) {
	return t.do(ctx, req)
}

func (t *fakeTransport) Close() error {
	t.closed = true
	return nil
}

// handlerTransport wraps an rpc.Handler the way the real server does: the
// handler's result is json.Marshal'd into the raw response envelope, so the
// client-side decode exercises the same byte path as the wire.
func handlerTransport(h rpc.Handler) *fakeTransport {
	return &fakeTransport{do: func(ctx context.Context, req *rpc.Request) (*syncservice.Response, error) {
		result, err := h(ctx, req.Params)
		if err != nil {
			return &syncservice.Response{OK: false, Error: err.Error()}, nil
		}
		b, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		return &syncservice.Response{OK: true, Result: b}, nil
	}}
}

// envelopeTransport serves a fixed raw envelope, simulating a downrev or
// lying peer that bypasses the stripping handler.
func envelopeTransport(t *testing.T, cred *creds.Credential, hash string) *fakeTransport {
	t.Helper()
	blob, err := cred.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	env := CredentialEnvelope{Credential: blob, ExpiresAt: cred.ClaudeAiOauth.ExpiresAt, Hash: hash}
	return handlerTransport(func(context.Context, map[string]any) (any, error) { return env, nil })
}

// failingTransport always errors on Do — an unreachable peer.
func failingTransport() *fakeTransport {
	return &fakeTransport{do: func(context.Context, *rpc.Request) (*syncservice.Response, error) {
		return nil, errors.New("connection refused")
	}}
}

// servingSeams returns lookup/read seams serving cred for uuid as account a,
// recording read calls into *reads.
func servingSeams(t *testing.T, uuid string, a store.Account, cred *creds.Credential, reads *int) (AccountLookup, CredentialReader) {
	t.Helper()
	lookup := func(u string) (store.Account, bool, error) {
		if u != uuid {
			return store.Account{}, false, nil
		}
		return a, true, nil
	}
	read := func(_ context.Context, got store.Account) (*creds.Credential, error) {
		if got.ID != a.ID {
			t.Errorf("read seam got account ID %d, want %d", got.ID, a.ID)
		}
		*reads++
		return cred, nil
	}
	return lookup, read
}

// serving wraps an owned credential in the real handler behind a transport.
func serving(t *testing.T, cred *creds.Credential) *fakeTransport {
	t.Helper()
	reads := 0
	lookup, read := servingSeams(t, "u-1", store.Account{ID: 1}, cred, &reads)
	return handlerTransport(NewFetchCredentialHandler(lookup, read))
}

// TestFetchCredentialHandlerStripsSecret pins the secret boundary on the exact
// wire bytes: the envelope is cred.Strip() with no refreshToken key and no
// refresh-token value anywhere, its hash is the AccessHash, and the client
// returns the stripped credential deep-equal, optional fields included.
func TestFetchCredentialHandlerStripsSecret(t *testing.T) {
	cred := pullCred("served", 2_000_000)
	a := store.Account{ID: 7, ConfigDir: "/cfg/acct-07", KeychainService: "svc7", KeychainAccount: "me"}
	reads := 0
	lookup, read := servingSeams(t, "u-7", a, cred, &reads)
	handler := NewFetchCredentialHandler(lookup, read)

	res, err := handler(context.Background(), map[string]any{"uuid": "u-7"})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	env, ok := res.(CredentialEnvelope)
	if !ok {
		t.Fatalf("handler result is %T, want CredentialEnvelope", res)
	}
	wantBlob, err := cred.Strip().Marshal()
	if err != nil {
		t.Fatalf("marshal stripped credential: %v", err)
	}
	if !bytes.Equal(env.Credential, wantBlob) {
		t.Fatalf("envelope credential = %s, want the stripped %s", env.Credential, wantBlob)
	}
	for _, leak := range [][]byte{[]byte("refreshToken"), []byte(cred.ClaudeAiOauth.RefreshToken)} {
		if bytes.Contains(env.Credential, leak) {
			t.Fatalf("envelope bytes leak %q: %s", leak, env.Credential)
		}
	}
	if env.ExpiresAt != cred.ClaudeAiOauth.ExpiresAt {
		t.Fatalf("envelope expiresAt = %d, want %d", env.ExpiresAt, cred.ClaudeAiOauth.ExpiresAt)
	}
	if env.Hash != creds.AccessHash(cred) {
		t.Fatalf("envelope hash = %q, want AccessHash %q", env.Hash, creds.AccessHash(cred))
	}

	// Client side: the pulled credential is the stripped form, deep-equal.
	var gotMethod string
	var gotUUID any
	tx := handlerTransport(handler)
	inner := tx.do
	tx.do = func(ctx context.Context, req *rpc.Request) (*syncservice.Response, error) {
		gotMethod = req.Method
		gotUUID = req.Params["uuid"]
		return inner(ctx, req)
	}
	chain := ChainStamp{Origin: "hostA", ExpiresAt: cred.ClaudeAiOauth.ExpiresAt, Hash: creds.AccessHash(cred)}
	got, err := FetchCredential(context.Background(), func(string) syncservice.Transport { return tx }, "u-7", chain, 1_000_000, []string{"hostA"})
	if err != nil {
		t.Fatalf("FetchCredential: %v", err)
	}
	if !reflect.DeepEqual(got, cred.Strip()) {
		t.Fatalf("pulled credential = %+v, want the stripped %+v", got, cred.Strip())
	}
	if got.HasRefreshToken() {
		t.Fatal("pulled credential carries a refresh token")
	}
	if gotMethod != MethodFetchCredential {
		t.Fatalf("request method = %q, want %q", gotMethod, MethodFetchCredential)
	}
	if gotUUID != "u-7" {
		t.Fatalf("request uuid param = %v, want u-7", gotUUID)
	}
	if !tx.closed {
		t.Fatal("transport was not closed after the fetch")
	}
}

// TestFetchCredentialHandlerErrors pins the handler's failure modes: a missing
// or non-string uuid, an unknown uuid, a lookup failure, and a read failure all
// error loudly, and the read seam runs only after a successful lookup.
func TestFetchCredentialHandlerErrors(t *testing.T) {
	a := store.Account{ID: 3}
	lookupErr := errors.New("db exploded")
	readErr := errors.New("keychain locked")
	cases := map[string]struct {
		params    map[string]any
		lookup    AccountLookup
		readErr   error
		wantErrIs error
		wantReads int
	}{
		"missing uuid": {
			params: map[string]any{},
			lookup: func(string) (store.Account, bool, error) { return a, true, nil },
		},
		"non-string uuid": {
			params: map[string]any{"uuid": 42},
			lookup: func(string) (store.Account, bool, error) { return a, true, nil },
		},
		"unknown uuid": {
			params: map[string]any{"uuid": "u-missing"},
			lookup: func(string) (store.Account, bool, error) { return store.Account{}, false, nil },
		},
		"lookup error": {
			params:    map[string]any{"uuid": "u-3"},
			lookup:    func(string) (store.Account, bool, error) { return store.Account{}, false, lookupErr },
			wantErrIs: lookupErr,
		},
		"read error": {
			params:    map[string]any{"uuid": "u-3"},
			lookup:    func(string) (store.Account, bool, error) { return a, true, nil },
			readErr:   readErr,
			wantErrIs: readErr,
			wantReads: 1,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			reads := 0
			read := func(context.Context, store.Account) (*creds.Credential, error) {
				reads++
				if tc.readErr != nil {
					return nil, tc.readErr
				}
				return pullCred("x", 1), nil
			}
			handler := NewFetchCredentialHandler(tc.lookup, read)
			res, err := handler(context.Background(), tc.params)
			if err == nil {
				t.Fatalf("handler = %v, want error", res)
			}
			if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
				t.Fatalf("handler err = %v, want errors.Is %v", err, tc.wantErrIs)
			}
			if reads != tc.wantReads {
				t.Fatalf("read seam ran %d times, want %d", reads, tc.wantReads)
			}
		})
	}
}

// TestFetchFromPeerRejectsInvalidEnvelopes pins the client-side envelope
// validation: an RT-bearing envelope (downrev peer) and a tokenless one are
// rejected with their sentinels even when hash-consistent, and a credential
// that does not hash to its own envelope is rejected by recomputation.
func TestFetchFromPeerRejectsInvalidEnvelopes(t *testing.T) {
	owned := pullCred("owned", 5_000)
	tokenless := &creds.Credential{}
	tokenless.ClaudeAiOauth.ExpiresAt = 5_000

	cases := map[string]struct {
		tx        *fakeTransport
		wantErrIs error
	}{
		"RT-bearing envelope rejected even when self-consistent": {
			tx:        envelopeTransport(t, owned, creds.AccessHash(owned)),
			wantErrIs: pool.ErrEnvelopeCarriesSecret,
		},
		"tokenless envelope rejected": {
			tx:        envelopeTransport(t, tokenless, creds.AccessHash(tokenless)),
			wantErrIs: pool.ErrEnvelopeNoAccessToken,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			chain := ChainStamp{Origin: "hostA", ExpiresAt: 5_000, Hash: creds.AccessHash(owned)}
			dial := func(string) syncservice.Transport { return tc.tx }
			// The sentinel is visible per attempt (origin AND relay reject alike)...
			for _, isOrigin := range []bool{true, false} {
				_, err := fetchFromPeer(context.Background(), dial, "hostA", "u-1", chain, 0, isOrigin)
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("fetchFromPeer(isOrigin=%v) err = %v, want errors.Is %v", isOrigin, err, tc.wantErrIs)
				}
			}
			// ...and the pull as a whole is the deferred outcome.
			got, err := FetchCredential(context.Background(), dial, "u-1", chain, 0, []string{"hostA"})
			if got != nil {
				t.Fatalf("pulled credential = %+v, want nil", got)
			}
			if !errors.Is(err, ErrNoPeerCredential) {
				t.Fatalf("err = %v, want errors.Is ErrNoPeerCredential", err)
			}
		})
	}

	t.Run("credential not hashing to its envelope is rejected", func(t *testing.T) {
		stripped := pullCred("lying", 5_000).Strip()
		tx := envelopeTransport(t, stripped, "garbage-hash")
		chain := ChainStamp{Origin: "hostA", ExpiresAt: 5_000, Hash: "garbage-hash"}
		got, err := FetchCredential(context.Background(), func(string) syncservice.Transport { return tx }, "u-1", chain, 0, []string{"hostA"})
		if got != nil {
			t.Fatalf("pulled credential = %+v, want nil", got)
		}
		if !errors.Is(err, ErrNoPeerCredential) {
			t.Fatalf("err = %v, want errors.Is ErrNoPeerCredential", err)
		}
	})
}

// TestFetchCredentialOriginAuthoritativeRelayMustMatch pins the trust split: a
// relay whose answer does not match the registry hash is rejected, while the
// origin's self-consistent, strictly-fresher answer is accepted even when it
// is ahead of the registry stamp (mirror lag after a rotation).
func TestFetchCredentialOriginAuthoritativeRelayMustMatch(t *testing.T) {
	advertised := pullCred("t1", 5_000) // what the registry still advertises
	rotated := pullCred("t2", 9_000)    // what the origin already rotated to
	chain := ChainStamp{Origin: "hostA", ExpiresAt: 5_000, Hash: creds.AccessHash(advertised)}

	t.Run("origin ahead of the registry is accepted", func(t *testing.T) {
		var dialed []string
		dial := func(peer string) syncservice.Transport {
			dialed = append(dialed, peer)
			if peer == "hostA" {
				return serving(t, rotated)
			}
			return serving(t, advertised)
		}
		got, err := FetchCredential(context.Background(), dial, "u-1", chain, 0, []string{"hostB"})
		if err != nil {
			t.Fatalf("FetchCredential: %v", err)
		}
		if !reflect.DeepEqual(got, rotated.Strip()) {
			t.Fatalf("pulled %+v, want the origin's rotated chain", got)
		}
		if want := []string{"hostA"}; !reflect.DeepEqual(dialed, want) {
			t.Fatalf("dialed = %v, want %v — the origin answer is authoritative", dialed, want)
		}
	})

	t.Run("origin staler than local is rejected", func(t *testing.T) {
		dial := func(peer string) syncservice.Transport { return serving(t, advertised) }
		got, err := FetchCredential(context.Background(), dial, "u-1", chain, 6_000, []string{"hostB"})
		if got != nil {
			t.Fatalf("pulled %+v, want nil — origin authority never overrides freshness", got)
		}
		if !errors.Is(err, ErrNoPeerCredential) {
			t.Fatalf("err = %v, want errors.Is ErrNoPeerCredential", err)
		}
	})

	t.Run("relay ahead of the registry is rejected, matching relay serves", func(t *testing.T) {
		dial := func(peer string) syncservice.Transport {
			switch peer {
			case "hostA": // origin down
				return failingTransport()
			case "hostB": // stale-registry relay serving a chain the registry doesn't advertise
				return serving(t, rotated)
			default: // hostC matches the registry
				return serving(t, advertised)
			}
		}
		got, err := FetchCredential(context.Background(), dial, "u-1", chain, 0, []string{"hostB", "hostC"})
		if err != nil {
			t.Fatalf("FetchCredential: %v", err)
		}
		if !reflect.DeepEqual(got, advertised.Strip()) {
			t.Fatalf("pulled %+v, want the registry-matching chain from hostC", got)
		}
	})
}

// TestFetchCredentialRequiresStrictlyLaterExpiry pins the freshness gate:
// only a credential expiring strictly later than the local one is accepted;
// equal and earlier expiries are the deferred outcome.
func TestFetchCredentialRequiresStrictlyLaterExpiry(t *testing.T) {
	cases := map[string]struct {
		remote, local int64
		wantOK        bool
	}{
		"strictly later accepted": {remote: 1_001, local: 1_000, wantOK: true},
		"equal rejected":          {remote: 1_000, local: 1_000},
		"earlier rejected":        {remote: 999, local: 1_000},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			served := pullCred("r", tc.remote)
			tx := serving(t, served)
			chain := ChainStamp{ExpiresAt: tc.remote, Hash: creds.AccessHash(served)}

			got, err := FetchCredential(context.Background(), func(string) syncservice.Transport { return tx }, "u-1", chain, tc.local, []string{"peerA"})
			if tc.wantOK {
				if err != nil {
					t.Fatalf("FetchCredential: %v", err)
				}
				if got.ClaudeAiOauth.ExpiresAt != tc.remote {
					t.Fatalf("pulled expiry = %d, want %d", got.ClaudeAiOauth.ExpiresAt, tc.remote)
				}
				return
			}
			if got != nil {
				t.Fatalf("pulled credential = %+v, want nil", got)
			}
			if !errors.Is(err, ErrNoPeerCredential) {
				t.Fatalf("err = %v, want errors.Is ErrNoPeerCredential", err)
			}
		})
	}
}

// TestFetchCredentialOriginFirstThenPeers pins the peer order: the origin
// first, dialed once, falling back to the remaining peers; every transport is
// closed.
func TestFetchCredentialOriginFirstThenPeers(t *testing.T) {
	served := pullCred("good", 9_000)
	chain := ChainStamp{Origin: "hostA", ExpiresAt: 9_000, Hash: creds.AccessHash(served)}

	var dialed []string
	transports := map[string]*fakeTransport{}
	dial := func(peer string) syncservice.Transport {
		dialed = append(dialed, peer)
		var tx *fakeTransport
		if peer == "hostA" {
			tx = failingTransport()
		} else {
			tx = serving(t, served)
		}
		transports[peer] = tx
		return tx
	}

	got, err := FetchCredential(context.Background(), dial, "u-1", chain, 0, []string{"hostB", "hostA"})
	if err != nil {
		t.Fatalf("FetchCredential: %v", err)
	}
	if !reflect.DeepEqual(got, served.Strip()) {
		t.Fatalf("pulled credential = %+v, want %+v", got, served.Strip())
	}
	if want := []string{"hostA", "hostB"}; !reflect.DeepEqual(dialed, want) {
		t.Fatalf("dial order = %v, want %v", dialed, want)
	}
	for peer, tx := range transports {
		if !tx.closed {
			t.Errorf("transport to %s was not closed", peer)
		}
	}
}

// TestFetchCredentialAllPeersUnreachable pins the deferred outcome: every peer
// failing — or there being no peers at all — returns the ErrNoPeerCredential
// sentinel, while a canceled caller ctx surfaces as the ctx error instead.
func TestFetchCredentialAllPeersUnreachable(t *testing.T) {
	chain := ChainStamp{Origin: "hostA", ExpiresAt: 1, Hash: "h"}

	t.Run("all peers fail", func(t *testing.T) {
		var dialed []string
		dial := func(peer string) syncservice.Transport {
			dialed = append(dialed, peer)
			return failingTransport()
		}
		got, err := FetchCredential(context.Background(), dial, "u-1", chain, 0, []string{"hostB"})
		if got != nil {
			t.Fatalf("pulled credential = %+v, want nil", got)
		}
		if !errors.Is(err, ErrNoPeerCredential) {
			t.Fatalf("err = %v, want errors.Is ErrNoPeerCredential", err)
		}
		if want := []string{"hostA", "hostB"}; !reflect.DeepEqual(dialed, want) {
			t.Fatalf("dialed = %v, want %v", dialed, want)
		}
	})

	t.Run("no candidate peers", func(t *testing.T) {
		dial := func(string) syncservice.Transport {
			t.Fatal("dial must not be called with no candidates")
			return nil
		}
		_, err := FetchCredential(context.Background(), dial, "u-1", ChainStamp{}, 0, nil)
		if !errors.Is(err, ErrNoPeerCredential) {
			t.Fatalf("err = %v, want errors.Is ErrNoPeerCredential", err)
		}
	})

	t.Run("canceled ctx is not the deferred sentinel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := FetchCredential(ctx, func(string) syncservice.Transport { return failingTransport() }, "u-1", chain, 0, []string{"hostB"})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want errors.Is context.Canceled", err)
		}
		if errors.Is(err, ErrNoPeerCredential) {
			t.Fatalf("err = %v, must not be the deferred sentinel", err)
		}
	})
}

// TestFetchCredentialPerPeerTimeout pins that FetchTimeout bounds each peer
// attempt independently: a peer that hangs until its ctx expires is abandoned
// and the next peer still serves the credential.
func TestFetchCredentialPerPeerTimeout(t *testing.T) {
	prev := FetchTimeout
	FetchTimeout = 50 * time.Millisecond
	t.Cleanup(func() { FetchTimeout = prev })

	served := pullCred("good", 7_000)
	chain := ChainStamp{Origin: "slow", ExpiresAt: 7_000, Hash: creds.AccessHash(served)}

	hang := &fakeTransport{do: func(ctx context.Context, _ *rpc.Request) (*syncservice.Response, error) {
		<-ctx.Done()
		return nil, fmt.Errorf("peer hung: %w", ctx.Err())
	}}
	dial := func(peer string) syncservice.Transport {
		if peer == "slow" {
			return hang
		}
		return serving(t, served)
	}

	start := time.Now()
	got, err := FetchCredential(context.Background(), dial, "u-1", chain, 0, []string{"fast"})
	if err != nil {
		t.Fatalf("FetchCredential: %v", err)
	}
	if !reflect.DeepEqual(got, served.Strip()) {
		t.Fatalf("pulled credential = %+v, want %+v", got, served.Strip())
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("fetch took %v; the hung peer was not bounded by FetchTimeout", elapsed)
	}
	if !hang.closed {
		t.Fatal("hung transport was not closed")
	}
}
