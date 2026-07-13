package daemon

import (
	"context"
	"log"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/store"
)

// credMirrorQueueSize bounds the async mirror queue; a var so tests shrink it.
var credMirrorQueueSize = 64

// credEvent is one credential write awaiting its registry mirror.
type credEvent struct {
	acct store.Account
	cred creds.Credential
}

// credMirror decouples Manager.OnCredWrite from the registry flock: the hook
// only enqueues, and Run records chain stamps outside any account lock.
type credMirror struct {
	// note records a chain stamp; wired to hostsync.Service.NoteCredWrite.
	note func(ctx context.Context, uuid string, chain hostsync.ChainStamp) error
	// self is this host's name, published as the rotated chain's origin.
	self string
	// now supplies the RotatedAt clock; tests inject a fake.
	now func() time.Time
	log *log.Logger
	ch  chan credEvent
}

// newCredMirror builds the mirror; the sync setup sets Manager.OnCredWrite =
// mirror.Hook and runs mirror.Run on a wg-tracked goroutine.
func newCredMirror(note func(ctx context.Context, uuid string, chain hostsync.ChainStamp) error, self string, logger *log.Logger) *credMirror {
	return &credMirror{
		note: note,
		self: self,
		now:  time.Now,
		log:  logger,
		ch:   make(chan credEvent, credMirrorQueueSize),
	}
}

// Hook is the Manager.OnCredWrite implementation. It runs under the
// per-account lock and must never block: it enqueues and returns; a full
// queue drops the event loudly — see ccn 10bf17d.
func (c *credMirror) Hook(a store.Account, cred *creds.Credential) error {
	if a.AccountUUID == "" {
		// No uuid means not in the registry yet; the scan-publish fold covers it.
		return nil
	}
	if !cred.HasRefreshToken() {
		// A synced install or a stripped double-spend loser must never publish a
		// stamp claiming this host as origin; only owned rotations do.
		return nil
	}
	select {
	case c.ch <- credEvent{acct: a, cred: *cred}:
	default:
		c.log.Printf("acct-%02d cred-write mirror queue full; event DROPPED (scan-publish heals on the next converge)", a.ID)
	}
	return nil
}

// Run drains the queue until ctx is done, recording each chain stamp outside
// any account lock; the caller tracks it on the daemon WaitGroup.
func (c *credMirror) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-c.ch:
			chain := hostsync.ChainStamp{
				Origin:    c.self,
				ExpiresAt: ev.cred.ClaudeAiOauth.ExpiresAt,
				Hash:      creds.AccessHash(&ev.cred),
				RotatedAt: c.now().UnixMilli(),
			}
			if err := c.note(ctx, ev.acct.AccountUUID, chain); err != nil {
				c.log.Printf("acct-%02d cred-write mirror: %v", ev.acct.ID, err)
			}
		}
	}
}
