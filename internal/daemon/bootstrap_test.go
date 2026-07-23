package daemon

import (
	"errors"
	"testing"
	"time"
)

func TestBootstrapProgressIsGenerationBoundedAndDeterministic(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	server := &Server{bootstrapNow: func() time.Time { return now }}
	server.beginBootstrap()
	server.setBootstrapTotal(3)
	now = now.Add(time.Second)
	server.settleBootstrapAccount(3, true, nil)
	now = now.Add(time.Second)
	server.settleBootstrapAccount(1, false, errors.New("presentation failed"))
	now = now.Add(time.Second)
	server.settleBootstrapAccount(2, false, nil)
	server.finishBootstrap(errors.New("aggregate failure"))

	progress := server.bootstrapSnapshot("generation-7")
	if progress.Generation != "generation-7" || progress.Total != 3 || progress.Settled != 3 ||
		progress.Quarantined != 1 || !progress.Terminal || !progress.LastProgressAt.Equal(now) {
		t.Fatalf("bootstrap progress = %+v", progress)
	}
	if len(progress.Failures) != 1 || progress.Failures[0].AccountID != 1 ||
		progress.Failures[0].Error != "presentation failed" {
		t.Fatalf("bootstrap failures = %+v", progress.Failures)
	}
}

func TestBootstrapPreFleetFailureIsTerminalProgress(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	server := &Server{bootstrapNow: func() time.Time { return now }}
	server.beginBootstrap()
	server.finishBootstrap(errors.New("open account store"))
	progress := server.bootstrapSnapshot("generation-8")
	if !progress.Terminal || len(progress.Failures) != 1 || progress.Failures[0].AccountID != 0 ||
		progress.Failures[0].Error != "open account store" {
		t.Fatalf("pre-fleet progress = %+v", progress)
	}
}
