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
// domains (every domain writes through to the same ~/.claude.json and
// ~/.claude/settings.json), all landing in this one BridgeServer process. Held
// across the cycle's I/O — a lock-scope exception, safe because the holder runs
// every WriteThrough on a background worker off its fuse handler path, so it can
// never stall a mount's NFS server. Against a concurrent vanilla claude,
// last-writer-wins within the window; blacklisted keys stay safe because base is
// re-read each cycle and never copied.
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

// Manifest classifies the top-level entries the holder treats specially for a
// domain: shared entries as live symlinks into base (bulk I/O off the synth path),
// excluded entries as private empty dirs, and the two synthetic documents;
// everything else is a plain passthrough. Each synth's Freshness lists the local
// files whose (mtime,size) gate the holder's cached bytes, so a steady-state
// Getattr costs a local stat, not a bridge RPC.
func (s *PoolContentSource) Manifest(domain string) ([]content.Entry, error) {
	entries := make([]content.Entry, 0, 2+len(SharedEntries)+len(ExcludedEntries))
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

// WriteThrough persists a committed synthetic document back to the shared base:
// .claude.json's shareable keys split into ~/.claude.json (blacklisted keys never
// cross); settings.json's injected plansDirectory is stripped back out. Both run
// under writeThroughMu and skip the rewrite when nothing shareable changed (writing
// identical bytes would bump base's mtime and thrash every mount's merge cache). A
// missing base is a deliberate no-op: cc-pool must never mint plain claude's files.
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
// content.Source contract.
func (s *PoolContentSource) Classify(name string) content.EntryKind {
	switch {
	case name == claudeJSONName || name == settingsName:
		return content.EntrySynth
	case SharedEntries[name]:
		return content.EntrySymlink
	case PrivateEntry(name):
		return content.EntryPrivate
	default:
		return "" // passthrough entry
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
