package pool

import (
	"testing"

	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// TestOverlayProviderMemoizesFileProvider pins that the File Provider provider is
// memoized on the Manager — the same pointer across resolves through both the
// exported and internal paths. Non-FP backends are not memoized.
func TestOverlayProviderMemoizesFileProvider(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	m := &Manager{} // OverlayFor nil -> real resolution

	a, err := m.OverlayProvider(fkoverlay.BackendFileProvider)
	if err != nil {
		t.Fatalf("resolve FP: %v", err)
	}
	if a == nil {
		t.Fatal("nil File Provider provider")
	}
	b, err := m.OverlayProvider(fkoverlay.BackendFileProvider)
	if err != nil {
		t.Fatalf("resolve FP again: %v", err)
	}
	if a != b {
		t.Fatal("File Provider provider must be memoized (same pointer across resolves)")
	}

	c, err := m.overlayFor(fkoverlay.BackendFileProvider)
	if err != nil {
		t.Fatalf("overlayFor FP: %v", err)
	}
	if c != a {
		t.Fatal("overlayFor must share the one memoized FP instance")
	}

	s1, err := m.OverlayProvider(fkoverlay.BackendSymlink)
	if err != nil {
		t.Fatalf("resolve symlink: %v", err)
	}
	s2, err := m.OverlayProvider(fkoverlay.BackendSymlink)
	if err != nil {
		t.Fatalf("resolve symlink again: %v", err)
	}
	if s1 == s2 {
		t.Fatal("symlink provider must NOT be memoized (a fresh stateless provider each resolve)")
	}
}
