package cli

import (
	"errors"
	"strings"
	"testing"

	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// TestFuseGrantHintFlipsWithBackend proves cc-pool holds no pane literal:
// fuseGrantHint's pane and deep link come entirely from the backend's
// fkoverlay.Enablement, so a different backend flips the output with zero cc-pool change.
func TestFuseGrantHintFlipsWithBackend(t *testing.T) {
	for _, backend := range []fkoverlay.Backend{fkoverlay.BackendNFS, fkoverlay.BackendFSKit} {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			en := backend.Enablement()
			if !en.Needed {
				t.Fatalf("backend %q reports no grant needed; the fuse backends always need one", backend)
			}
			if len(en.URLs) == 0 {
				t.Fatalf("backend %q has no enablement URLs; the hint cannot offer a deep link", backend)
			}
			hint := fuseGrantHint(backend)
			if !strings.Contains(hint, en.Pane) {
				t.Errorf("fuseGrantHint(%q) = %q, want it to name the backend's pane %q", backend, hint, en.Pane)
			}
			if !strings.Contains(hint, en.URLs[0]) {
				t.Errorf("fuseGrantHint(%q) = %q, want it to carry the backend's first deep link %q", backend, hint, en.URLs[0])
			}
		})
	}
}

// TestFuseGrantHintIsNotPinnedToOneBackend proves the helper is not hardcoded to
// one backend's pane: the FSKit hint must not carry the NFS pane, nor vice-versa.
func TestFuseGrantHintIsNotPinnedToOneBackend(t *testing.T) {
	nfsPane := fkoverlay.BackendNFS.Enablement().Pane
	fskitPane := fkoverlay.BackendFSKit.Enablement().Pane
	if nfsPane == fskitPane {
		t.Fatalf("the NFS and FSKit panes are identical (%q); the flip test cannot distinguish them", nfsPane)
	}

	nfsHint := fuseGrantHint(fkoverlay.BackendNFS)
	if strings.Contains(nfsHint, fskitPane) {
		t.Errorf("the NFS hint %q leaked the FSKit pane %q; the helper is pinned to FSKit", nfsHint, fskitPane)
	}

	fskitHint := fuseGrantHint(fkoverlay.BackendFSKit)
	if strings.Contains(fskitHint, nfsPane) {
		t.Errorf("the FSKit hint %q leaked the NFS pane %q; the helper is pinned to NFS", fskitHint, nfsPane)
	}
}

// TestFuseGrantHintNoGrantBackend pins the symlink path: a grant-less backend
// yields a bare permission line, no pane and no deep link.
func TestFuseGrantHintNoGrantBackend(t *testing.T) {
	hint := fuseGrantHint(fkoverlay.BackendSymlink)
	if strings.Contains(hint, "Settings") {
		t.Errorf("fuseGrantHint(symlink) = %q, want no Settings pane for a grant-less backend", hint)
	}
	if hint != "grant the required macOS permission" {
		t.Errorf("fuseGrantHint(symlink) = %q, want the bare permission line", hint)
	}
}

// TestOverlayKindPersistenceFailsLoud documents cc-pool's no-migration stance:
// the legacy "fuse" overlay_kind no longer parses and fuseBackedRow reads it as
// non-fuse — the safe degrade onto the symlink path, never a live mount. Valid
// backends round-trip through Parse.
func TestOverlayKindPersistenceFailsLoud(t *testing.T) {
	t.Run("legacy fuse fails Parse", func(t *testing.T) {
		_, err := fkoverlay.Parse("fuse")
		if err == nil {
			t.Fatal("Parse(\"fuse\") = nil error; the legacy value must no longer parse")
		}
		if !errors.Is(err, fkoverlay.ErrUnknownBackend) {
			t.Errorf("Parse(\"fuse\") error = %v, want ErrUnknownBackend", err)
		}
	})

	t.Run("legacy fuse reads as non-fuse via fuseBackedRow", func(t *testing.T) {
		if fuseBackedRow("fuse") {
			t.Error("fuseBackedRow(\"fuse\") = true; an unparseable legacy value must degrade to non-fuse")
		}
	})

	roundTrip := []struct {
		stored string
		want   fkoverlay.Backend
		isFuse bool
	}{
		{stored: "symlink", want: fkoverlay.BackendSymlink, isFuse: false},
		{stored: "nfs", want: fkoverlay.BackendNFS, isFuse: true},
	}
	for _, tc := range roundTrip {
		t.Run(tc.stored+" round-trips", func(t *testing.T) {
			b, err := fkoverlay.Parse(tc.stored)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.stored, err)
			}
			if b != tc.want {
				t.Errorf("Parse(%q) = %q, want %q", tc.stored, b, tc.want)
			}
			if got := fuseBackedRow(tc.stored); got != tc.isFuse {
				t.Errorf("fuseBackedRow(%q) = %v, want %v", tc.stored, got, tc.isFuse)
			}
		})
	}
}
