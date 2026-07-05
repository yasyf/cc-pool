package overlay

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/yasyf/fusekit/content"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// claudeJSONName and settingsName are the two top-level entries served as
// synthetic documents; every other entry is a passthrough mirror, shared symlink,
// or private redirect.
const (
	claudeJSONName = ".claude.json"
	settingsName   = "settings.json"
)

// writeThroughMu serializes the base read→split→write cycle across ALL account
// domains. Held across I/O — a lock-scope exception, safe because the holder
// runs WriteThrough off its fuse handler path, so it never stalls an NFS server.
var writeThroughMu sync.Mutex

// PoolContentSource implements content.Source for cc-pool's overlay: serving the
// merged .claude.json and plansDirectory-injected settings.json as synthetic
// entries, classifying shared/private/excluded entries, and writing shareable keys
// back to the shared base files. One instance serves every account domain (a domain
// is the account ConfigDir); safe for concurrent calls, with every field immutable
// or guarded (writeThroughMu, errMu).
type PoolContentSource struct {
	// claudeDir is the shared overlay base (~/.claude); baseClaudeJSON is plain
	// claude's ~/.claude.json (base's sibling, not inside it). Injected at
	// construction to avoid importing pool (import cycle).
	claudeDir      string
	baseClaudeJSON string

	errMu    sync.Mutex
	readErr  map[string]error // "<domain>/<name>" -> last merged/served-read failure
	writeErr map[string]error // "<domain>/<name>" -> last base write-through failure
}

// NewPoolContentSource builds the source from the shared base paths (claudeDir
// ~/.claude, baseClaudeJSON ~/.claude.json).
func NewPoolContentSource(claudeDir, baseClaudeJSON string) *PoolContentSource {
	return &PoolContentSource{
		claudeDir:      claudeDir,
		baseClaudeJSON: baseClaudeJSON,
		readErr:        map[string]error{},
		writeErr:       map[string]error{},
	}
}

func (s *PoolContentSource) privClaudeJSON(domain string) string {
	return filepath.Join(fkoverlay.FusePrivateRoot(domain), claudeJSONName)
}
func (s *PoolContentSource) baseSettings() string { return filepath.Join(s.claudeDir, settingsName) }
func (s *PoolContentSource) plansDir() string     { return filepath.Join(s.claudeDir, "plans") }

// Manifest classifies a domain's specially-treated top-level entries: synths,
// private dirs, and live symlinks into base (bulk I/O off the synth path). The
// base snapshot is deliberately per-Build: promoting a just-born base entry to a
// symlink mid-mount would race the kernel's post-CREATE Getattr and EIO the
// in-flight write.
func (s *PoolContentSource) Manifest(domain string) ([]content.Entry, error) {
	baseEntries, err := os.ReadDir(s.claudeDir)
	if err != nil {
		return nil, fmt.Errorf("manifest: read base %s: %w", s.claudeDir, err)
	}
	entries := make([]content.Entry, 0, 2+len(SharedEntries)+len(ExcludedEntries)+len(baseEntries))
	entries = append(entries,
		content.Entry{Name: claudeJSONName, Kind: content.EntrySynth, Private: true, Freshness: []string{s.privClaudeJSON(domain), s.baseClaudeJSON}},
		content.Entry{Name: settingsName, Kind: content.EntrySynth, Freshness: []string{s.baseSettings()}},
	)
	for name := range SharedEntries {
		entries = append(entries, content.Entry{Name: name, Kind: content.EntrySymlink, Target: filepath.Join(s.claudeDir, name)})
	}
	for name := range ExcludedEntries {
		entries = append(entries, content.Entry{Name: name, Kind: content.EntryPrivate})
	}
	for _, e := range baseEntries {
		name := e.Name()
		if !sharedTopLevel(name) || SharedEntries[name] {
			continue // litter, private, synth, probe, or already emitted (forced-even-if-absent)
		}
		entries = append(entries, content.Entry{Name: name, Kind: content.EntrySymlink, Target: filepath.Join(s.claudeDir, name)})
	}
	return entries, nil
}

// ReadSynth computes a synthetic entry's bytes: .claude.json is base's shareable
// keys merged onto the account's private copy; settings.json is base with
// plansDirectory injected. A parse failure falls back to the raw private/base bytes
// (recording the error for HealthErrors) so a session never sees EIO over a corrupt
// file.
func (s *PoolContentSource) ReadSynth(domain, name string) ([]byte, error) {
	switch name {
	case claudeJSONName:
		priv, err := os.ReadFile(s.privClaudeJSON(domain))
		if err != nil {
			// A seeded fuse account always has a private file (swept in before the
			// mount), so a missing one is genuine: surface it, never fabricate a view
			// from base alone.
			return nil, fmt.Errorf("read private claude.json for %s: %w", domain, err)
		}
		base, err := os.ReadFile(s.baseClaudeJSON)
		if err != nil {
			if !os.IsNotExist(err) {
				s.recordRead(domain, name, fmt.Errorf("read base claude.json: %w", err))
				return priv, nil // base unreadable; serve private, never EIO
			}
			base = nil // no base yet; merge returns the private copy verbatim
		}
		merged, _, err := MergeClaudeJSON(priv, base)
		if err != nil {
			s.recordRead(domain, name, fmt.Errorf("merge claude.json for %s: %w", domain, err))
			return priv, nil
		}
		s.clearRead(domain, name)
		return merged, nil
	case settingsName:
		base, err := os.ReadFile(s.baseSettings())
		if err != nil {
			return nil, fmt.Errorf("read settings.json: %w", err)
		}
		served, err := injectPlansDirectory(base, s.plansDir())
		if err != nil {
			s.recordRead(domain, name, fmt.Errorf("inject plansDirectory: %w", err))
			return base, nil
		}
		s.clearRead(domain, name)
		return served, nil
	default:
		return nil, fmt.Errorf("read synth: unknown entry %q", name)
	}
}

// SynthNonEmpty reports whether the synthetic .claude.json this source serves for
// domain (an account ConfigDir) has content. The File Provider wedge detector uses
// it to tell a genuine data-plane wedge — 0 bytes served for an account that has an
// identity — from an empty-by-design account whose synth is legitimately empty (no
// private .claude.json yet).
func (s *PoolContentSource) SynthNonEmpty(domain string) bool {
	b, err := s.ReadSynth(domain, claudeJSONName)
	return err == nil && len(b) > 0
}

// WriteThrough persists a committed synthetic document's shareable parts back to
// the shared base under writeThroughMu; identical-byte rewrites are skipped (they
// would bump base's mtime and thrash every mount's merge cache), and a missing
// base is a deliberate no-op — cc-pool never mints plain claude's files.
func (s *PoolContentSource) WriteThrough(domain, name string, data []byte) error {
	writeThroughMu.Lock()
	defer writeThroughMu.Unlock()
	var err error
	switch name {
	case claudeJSONName:
		err = s.writeThroughClaudeJSON(data)
	case settingsName:
		err = s.writeThroughSettings(data)
	default:
		return fmt.Errorf("write-through: unknown entry %q", name)
	}
	s.recordWrite(domain, name, err)
	return err
}

func (s *PoolContentSource) writeThroughClaudeJSON(payload []byte) error {
	base, err := os.ReadFile(s.baseClaudeJSON)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("write-through claude.json: read base: %w", err)
	}
	newBase, err := SplitClaudeJSON(payload, base)
	if err != nil {
		return fmt.Errorf("write-through claude.json: %w", err)
	}
	if bytes.Equal(newBase, base) {
		return nil
	}
	if err := WriteAtomic0600(s.baseClaudeJSON, newBase); err != nil {
		return fmt.Errorf("write-through claude.json: %w", err)
	}
	return nil
}

func (s *PoolContentSource) writeThroughSettings(payload []byte) error {
	target := s.baseSettings()
	base, err := os.ReadFile(target) //nolint:gosec // G304: target is the daemon's own base settings path, not external input
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("write-through settings.json: read base: %w", err)
	}
	newBase, err := stripInjectedPlansDirectory(payload, s.plansDir())
	if err != nil {
		return fmt.Errorf("write-through settings.json: %w", err)
	}
	if bytes.Equal(newBase, base) {
		return nil
	}
	if err := WriteAtomic0600(target, newBase); err != nil {
		return fmt.Errorf("write-through settings.json: %w", err)
	}
	return nil
}

// Classify reports a top-level entry's kind for a fully-remote (Tree) consumer.
// cc-pool drives the holder through Manifest and never calls this; it completes the
// content.Source contract, so it must agree with Manifest: the synth documents and
// private/excluded names keep their kinds, and every sharedTopLevel name — SharedEntries
// or any other carve-out — is a live symlink. Only names sharedTopLevel rejects that are
// not otherwise classified (skipped litter, the probe) stay passthrough.
func (s *PoolContentSource) Classify(name string) content.EntryKind {
	switch {
	case name == claudeJSONName || name == settingsName:
		return content.EntrySynth
	case SharedEntries[name]:
		return content.EntrySymlink
	case PrivateEntry(name):
		return content.EntryPrivate
	case carveOutPrivate(name):
		// Gap-class private siblings (bare-prefix or case-variant family names):
		// the holder private-routes these, never symlinks them.
		return content.EntryPrivate
	case sharedTopLevel(name):
		return content.EntrySymlink
	default:
		return "" // passthrough entry (skipped litter, the probe)
	}
}

// HealthErrors joins every domain's last read failure and last write-through
// failure for the daemon's status/doctor surface. A read failure is cleared by the
// next successful read; a write failure by the next successful write-through.
func (s *PoolContentSource) HealthErrors() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	errs := make([]error, 0, len(s.readErr)+len(s.writeErr))
	for _, e := range s.readErr {
		errs = append(errs, e)
	}
	for _, e := range s.writeErr {
		errs = append(errs, e)
	}
	return errors.Join(errs...)
}

func (s *PoolContentSource) recordRead(domain, name string, err error) {
	s.errMu.Lock()
	s.readErr[domain+"/"+name] = err
	s.errMu.Unlock()
}

func (s *PoolContentSource) clearRead(domain, name string) {
	s.errMu.Lock()
	delete(s.readErr, domain+"/"+name)
	s.errMu.Unlock()
}

func (s *PoolContentSource) recordWrite(domain, name string, err error) {
	s.errMu.Lock()
	if err == nil {
		delete(s.writeErr, domain+"/"+name)
	} else {
		s.writeErr[domain+"/"+name] = err
	}
	s.errMu.Unlock()
}
