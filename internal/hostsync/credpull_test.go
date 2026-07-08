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
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// pullCred builds a credential with every OAuth field populated so round-trip
// assertions cover the optional fields too.
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

// TestFetchCredentialEnvelopeRoundTripsByteExact pins that the envelope
// carries the credential byte-exact plus its stamp fields, and the client
// returns it deep-equal, optional fields included.
func TestFetchCredentialEnvelopeRoundTripsByteExact(t *testing.T) {
	cred := pullCred("served", 2_000_000)
	a := store.Account{ID: 7, ConfigDir: "/cfg/acct-07", KeychainService: "svc7", KeychainAccount: "me"}
	reads := 0
	lookup, read := servingSeams(t, "u-7", a, cred, &reads)
	handler := NewFetchCredentialHandler(lookup, read)

	// Server side: the envelope's blob is exactly cred.Marshal().
	res, err := handler(context.Background(), map[string]any{"uuid": "u-7"})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	env, ok := res.(CredentialEnvelope)
	if !ok {
		t.Fatalf("handler result is %T, want CredentialEnvelope", res)
	}
	wantBlob, err := cred.Marshal()
	if err != nil {
		t.Fatalf("marshal credential: %v", err)
	}
	if !bytes.Equal(env.Credential, wantBlob) {
		t.Fatalf("envelope credential = %s, want %s", env.Credential, wantBlob)
	}
	if env.ExpiresAt != cred.ClaudeAiOauth.ExpiresAt {
		t.Fatalf("envelope expiresAt = %d, want %d", env.ExpiresAt, cred.ClaudeAiOauth.ExpiresAt)
	}
	if env.Hash != CredentialHash(cred) {
		t.Fatalf("envelope hash = %q, want %q", env.Hash, CredentialHash(cred))
	}

	// Client side: the pulled credential is deep-equal to what was served.
	var gotMethod string
	var gotUUID any
	tx := handlerTransport(handler)
	inner := tx.do
	tx.do = func(ctx context.Context, req *rpc.Request) (*syncservice.Response, error) {
		gotMethod = req.Method
		gotUUID = req.Params["uuid"]
		return inner(ctx, req)
	}
	chain := ChainStamp{ExpiresAt: cred.ClaudeAiOauth.ExpiresAt, Hash: CredentialHash(cred), Holder: "hostA"}
	got, err := FetchCredential(context.Background(), func(string) syncservice.Transport { return tx }, "u-7", chain, 1_000_000, "", []string{"hostA"})
	if err != nil {
		t.Fatalf("FetchCredential: %v", err)
	}
	if !reflect.DeepEqual(got, cred) {
		t.Fatalf("pulled credential = %+v, want %+v", got, cred)
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

// TestFetchCredentialRejectsHashMismatch pins the stale-peer-registry guard: a
// credential that does not hash to the registry chain is rejected, whether the
// envelope advertises honestly or lies (caught by local recomputation).
func TestFetchCredentialRejectsHashMismatch(t *testing.T) {
	served := pullCred("stale", 5_000_000)
	chain := ChainStamp{ExpiresAt: 5_000_000, Hash: CredentialHash(pullCred("expected", 5_000_000))}

	honest := func() syncservice.Transport {
		reads := 0
		lookup, read := servingSeams(t, "u-1", store.Account{ID: 1}, served, &reads)
		return handlerTransport(NewFetchCredentialHandler(lookup, read))
	}
	lying := func() syncservice.Transport {
		blob, err := served.Marshal()
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		env := CredentialEnvelope{Credential: blob, ExpiresAt: served.ClaudeAiOauth.ExpiresAt, Hash: chain.Hash}
		return handlerTransport(func(context.Context, map[string]any) (any, error) { return env, nil })
	}

	cases := map[string]func() syncservice.Transport{
		"envelope advertises mismatched hash":      honest,
		"envelope lies about a mismatched payload": lying,
	}
	for name, mk := range cases {
		t.Run(name, func(t *testing.T) {
			tx := mk()
			got, err := FetchCredential(context.Background(), func(string) syncservice.Transport { return tx }, "u-1", chain, 0, "", []string{"peerA"})
			if got != nil {
				t.Fatalf("pulled credential = %+v, want nil", got)
			}
			if !errors.Is(err, ErrNoPeerCredential) {
				t.Fatalf("err = %v, want errors.Is ErrNoPeerCredential", err)
			}
		})
	}
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
			reads := 0
			lookup, read := servingSeams(t, "u-1", store.Account{ID: 1}, served, &reads)
			tx := handlerTransport(NewFetchCredentialHandler(lookup, read))
			chain := ChainStamp{ExpiresAt: tc.remote, Hash: CredentialHash(served)}

			got, err := FetchCredential(context.Background(), func(string) syncservice.Transport { return tx }, "u-1", chain, tc.local, "", []string{"peerA"})
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

// TestFetchCredentialAcceptsChildDespiteSkewedExpiry pins the lineage OR-arm:
// a chain descending from the local credential is accepted despite a
// skewed-earlier expiry; an unlinked parent still hits the strictly-later rule.
func TestFetchCredentialAcceptsChildDespiteSkewedExpiry(t *testing.T) {
	parent := pullCred("parent", 2_000) // local credential, expiry skewed AHEAD
	child := pullCred("child", 1_500)   // the live child a peer serves
	localHash := CredentialHash(parent)
	serve := func() syncservice.Transport {
		reads := 0
		lookup, read := servingSeams(t, "u-1", store.Account{ID: 1}, child, &reads)
		return handlerTransport(NewFetchCredentialHandler(lookup, read))
	}

	t.Run("child of local accepted", func(t *testing.T) {
		chain := ChainStamp{ExpiresAt: 1_500, Hash: CredentialHash(child), ParentHash: localHash}
		got, err := FetchCredential(context.Background(), func(string) syncservice.Transport { return serve() }, "u-1", chain, 2_000, localHash, []string{"peerA"})
		if err != nil {
			t.Fatalf("FetchCredential: %v", err)
		}
		if !reflect.DeepEqual(got, child) {
			t.Fatalf("pulled %+v, want the child", got)
		}
	})

	t.Run("unlinked parent still requires strictly-later expiry", func(t *testing.T) {
		chain := ChainStamp{ExpiresAt: 1_500, Hash: CredentialHash(child), ParentHash: "h-stranger"}
		got, err := FetchCredential(context.Background(), func(string) syncservice.Transport { return serve() }, "u-1", chain, 2_000, localHash, []string{"peerA"})
		if got != nil {
			t.Fatalf("pulled %+v, want nil", got)
		}
		if !errors.Is(err, ErrNoPeerCredential) {
			t.Fatalf("err = %v, want errors.Is ErrNoPeerCredential", err)
		}
	})
}

// TestFetchCredentialHolderFirstThenPeers pins the peer order: holder first,
// dialed once, falling back to the remaining peers; every transport is closed.
func TestFetchCredentialHolderFirstThenPeers(t *testing.T) {
	served := pullCred("good", 9_000)
	reads := 0
	lookup, read := servingSeams(t, "u-1", store.Account{ID: 1}, served, &reads)
	chain := ChainStamp{ExpiresAt: 9_000, Hash: CredentialHash(served), Holder: "hostA"}

	var dialed []string
	transports := map[string]*fakeTransport{}
	dial := func(peer string) syncservice.Transport {
		dialed = append(dialed, peer)
		var tx *fakeTransport
		if peer == "hostA" {
			tx = failingTransport()
		} else {
			tx = handlerTransport(NewFetchCredentialHandler(lookup, read))
		}
		transports[peer] = tx
		return tx
	}

	got, err := FetchCredential(context.Background(), dial, "u-1", chain, 0, "", []string{"hostB", "hostA"})
	if err != nil {
		t.Fatalf("FetchCredential: %v", err)
	}
	if !reflect.DeepEqual(got, served) {
		t.Fatalf("pulled credential = %+v, want %+v", got, served)
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

// TestFetchCredentialHolderAheadHaltsFallback pins the mirror-lag guard: a
// holder answering self-consistent and strictly fresher than the registry
// chain defers the pull without falling back; an unhealthy holder still falls back.
func TestFetchCredentialHolderAheadHaltsFallback(t *testing.T) {
	advertised := pullCred("t1", 5_000) // what the registry still advertises
	rotated := pullCred("t2", 9_000)    // what the holder already rotated to
	chain := ChainStamp{ExpiresAt: 5_000, Hash: CredentialHash(advertised), Holder: "hostA"}

	serving := func(cred *creds.Credential) *fakeTransport {
		reads := 0
		lookup, read := servingSeams(t, "u-1", store.Account{ID: 1}, cred, &reads)
		return handlerTransport(NewFetchCredentialHandler(lookup, read))
	}

	t.Run("holder ahead defers without consulting laggards", func(t *testing.T) {
		var dialed []string
		dial := func(peer string) syncservice.Transport {
			dialed = append(dialed, peer)
			if peer == "hostA" {
				return serving(rotated)
			}
			return serving(advertised)
		}
		got, err := FetchCredential(context.Background(), dial, "u-1", chain, 0, "", []string{"hostB"})
		if got != nil {
			t.Fatalf("pulled %+v, want nil — the advertised chain is a spent dead end", got)
		}
		if !errors.Is(err, ErrNoPeerCredential) {
			t.Fatalf("err = %v, want the deferred sentinel", err)
		}
		if want := []string{"hostA"}; !reflect.DeepEqual(dialed, want) {
			t.Fatalf("dialed = %v, want %v — no laggard once the holder proves the registry stale", dialed, want)
		}
	})

	t.Run("holder behind the registry still falls back", func(t *testing.T) {
		older := pullCred("t0", 1_000)
		dial := func(peer string) syncservice.Transport {
			if peer == "hostA" {
				return serving(older)
			}
			return serving(advertised)
		}
		got, err := FetchCredential(context.Background(), dial, "u-1", chain, 0, "", []string{"hostB"})
		if err != nil {
			t.Fatalf("FetchCredential: %v", err)
		}
		if !reflect.DeepEqual(got, advertised) {
			t.Fatalf("pulled %+v, want the advertised chain from the fallback peer", got)
		}
	})

	t.Run("holder self-inconsistent answer still falls back", func(t *testing.T) {
		blob, err := rotated.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		lying := CredentialEnvelope{Credential: blob, ExpiresAt: rotated.ClaudeAiOauth.ExpiresAt, Hash: "garbage"}
		dial := func(peer string) syncservice.Transport {
			if peer == "hostA" {
				return handlerTransport(func(context.Context, map[string]any) (any, error) { return lying, nil })
			}
			return serving(advertised)
		}
		got, err := FetchCredential(context.Background(), dial, "u-1", chain, 0, "", []string{"hostB"})
		if err != nil {
			t.Fatalf("FetchCredential: %v", err)
		}
		if !reflect.DeepEqual(got, advertised) {
			t.Fatalf("pulled %+v, want the advertised chain from the fallback peer", got)
		}
	})
}

// TestFetchCredentialAllPeersUnreachable pins the deferred outcome: every peer
// failing — or there being no peers at all — returns the ErrNoPeerCredential
// sentinel, while a canceled caller ctx surfaces as the ctx error instead.
func TestFetchCredentialAllPeersUnreachable(t *testing.T) {
	chain := ChainStamp{ExpiresAt: 1, Hash: "h", Holder: "hostA"}

	t.Run("all peers fail", func(t *testing.T) {
		var dialed []string
		dial := func(peer string) syncservice.Transport {
			dialed = append(dialed, peer)
			return failingTransport()
		}
		got, err := FetchCredential(context.Background(), dial, "u-1", chain, 0, "", []string{"hostB"})
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
		_, err := FetchCredential(context.Background(), dial, "u-1", ChainStamp{}, 0, "", nil)
		if !errors.Is(err, ErrNoPeerCredential) {
			t.Fatalf("err = %v, want errors.Is ErrNoPeerCredential", err)
		}
	})

	t.Run("canceled ctx is not the deferred sentinel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := FetchCredential(ctx, func(string) syncservice.Transport { return failingTransport() }, "u-1", chain, 0, "", []string{"hostB"})
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
	reads := 0
	lookup, read := servingSeams(t, "u-1", store.Account{ID: 1}, served, &reads)
	chain := ChainStamp{ExpiresAt: 7_000, Hash: CredentialHash(served), Holder: "slow"}

	hang := &fakeTransport{do: func(ctx context.Context, _ *rpc.Request) (*syncservice.Response, error) {
		<-ctx.Done()
		return nil, fmt.Errorf("peer hung: %w", ctx.Err())
	}}
	dial := func(peer string) syncservice.Transport {
		if peer == "slow" {
			return hang
		}
		return handlerTransport(NewFetchCredentialHandler(lookup, read))
	}

	start := time.Now()
	got, err := FetchCredential(context.Background(), dial, "u-1", chain, 0, "", []string{"fast"})
	if err != nil {
		t.Fatalf("FetchCredential: %v", err)
	}
	if !reflect.DeepEqual(got, served) {
		t.Fatalf("pulled credential = %+v, want %+v", got, served)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("fetch took %v; the hung peer was not bounded by FetchTimeout", elapsed)
	}
	if !hang.closed {
		t.Fatal("hung transport was not closed")
	}
}
