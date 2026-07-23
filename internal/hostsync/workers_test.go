package hostsync

import (
	"testing"

	"github.com/yasyf/synckit/syncservice"
)

func withHostSyncTestTransportRunner(t *testing.T, run func(syncservice.TransportRunner)) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", home)
	err := syncservice.WithTransportRunner(t.Context(), func(runner syncservice.TransportRunner) error {
		run(runner)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
