package tenantfs

import (
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
)

func TestSessionDoneTracksExactEventStreamLifetime(t *testing.T) {
	events := make(chan wire.Event)
	done := sessionDone(events)
	select {
	case <-done:
		t.Fatal("session done before event stream closed")
	default:
	}
	close(events)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("session done did not close with event stream")
	}
}
