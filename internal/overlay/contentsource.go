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

// claudeJSONName and settingsName are the two top-level entries cc-pool serves as
// SYNTHetic documents over the bridge: .claude.json as the live merged view (base
// shareable keys over the account's private copy, shareable writes split back to
// base) and settings.json as the base with plansDirectory injected (stripped on
// write-through). Every other top-level entry is a passthrough mirror, a shared
// live symlink, or a private redirect — none of which the consumer computes.
const (
	claudeJSONName = ".claude.json"
	settingsName   = "settings.json"
)

// writeThroughMu serializes every base read→split→write cycle across ALL account
// domains: each domain's mount writes through to the same shared base files
// (~/.claude.json, ~/.claude/settings.json), and the read-modify-write must be
// atomic against other in-process write-throughs. The shared holder issues these
// over the bridge, but they all land in this one BridgeServer process, so this
// lock is the complete story. Held across the cycle's I/O — a documented exception
// to the lock-scope rule — but the holder runs every WriteThrough on a background
// worker OFF its fuse handler path, so a contended lock or a slow base rewrite can
// never park a fuse-t worker and stall a mount's NFS server. Against a
// concurrently-running vanilla claude we accept last-writer-wins within the
// window; blacklisted keys are structurally protected because base is re-read each
// cycle and they are never copied.
var writeThroughMu sync.Mutex

// PoolContentSource implements content.Source for cc-pool's overlay: it serves the
// merged .claude.json and the plansDirectory-injected settings.json as synthetic
// entries over the shared holder's bridge, classifies shared/private/excluded
// entries from the same predicates the symlink overlay uses, and writes shareable
// keys back through to the shared base files. ONE instance serves every account
// domain (a domain is the account ConfigDir): the per-domain private root is
// derived from the domain, and the shared bases are fixed for the process.
// Implementations of content.Source must be safe for concurrent calls; every field
// here is either immutable after construction or guarded (writeThroughMu, errMu).
type PoolContentSource struct {
	// claudeDir is the shared overlay base (~/.claude); baseClaudeJSON is plain
	// claude's state file (~/.claude.json, base's SIBLING, not inside it). Injected
	// at construction so this package never imports pool (which would cycle).
	claudeDir      string
	baseClaudeJSON string

	errMu    sync.Mutex
	readErr  map[string]error // "<domain>/<name>" -> last merged/served-read failure
	writeErr map[string]error // "<domain>/<name>" -> last base write-through failure
}

// NewPoolContentSource builds the source from the shared base paths: claudeDir is
// ~/.claude and baseClaudeJSON is ~/.claude.json.
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

// Manifest classifies the top-level entries the holder must treat specially for an
// account domain: the shared entries as live symlinks into the base (bulk I/O
// stays off the synth path), the excluded entries as private empty dirs, and the
// two synthetic documents. Every other base entry is a plain passthrough the
// holder mirrors without consulting the source. The synth Freshness lists the
// local files whose (mtime,size) gate the holder's cached bytes, so a steady-state
// Getattr costs a local stat, not a bridge RPC.
func (s *PoolContentSource) Manifest(domain string) ([]content.Entry, error) {
	entries := []content.Entry{
		{Name: claudeJSONName, Kind: content.EntrySynth, Private: true, Freshness: []string{s.privClaudeJSON(domain), s.baseClaudeJSON}},
		{Name: settingsName, Kind: content.EntrySynth, Freshness: []string{s.baseSettings()}},
	}
	for name := range SharedEntries {
		entries = append(entries, content.Entry{Name: name, Kind: content.EntrySymlink, Target: filepath.Join(s.claudeDir, name)})
	}
	for name := range ExcludedEntries {
		entries = append(entries, content.Entry{Name: name, Kind: content.EntryPrivate})
	}
	return entries, nil
}

// ReadSynth computes a synthetic entry's bytes. .claude.json is base's shareable
// keys merged onto the account's private copy; settings.json is the base file with
// plansDirectory injected. A parse failure falls back to the raw private/base bytes
// (recording the error for HealthErrors) so a session never sees EIO over a corrupt
// file — the same contract the in-process mirror held.
func (s *PoolContentSource) ReadSynth(domain, name string) ([]byte, error) {
	switch name {
	case claudeJSONName:
		priv, err := os.ReadFile(s.privClaudeJSON(domain))
		if err != nil {
			// A missing private file is genuine: a seeded fuse account always has
			// one (it is swept in before the mount). Surface it, never fabricate a
			// view from base alone.
			return nil, fmt.Errorf("read private claude.json for %s: %w", domain, err)
		}
		base, err := os.ReadFile(s.baseClaudeJSON)
		if err != nil {
			if !os.IsNotExist(err) {
				s.recordRead(domain, name, fmt.Errorf("read base claude.json: %w", err))
				return priv, nil // base unreadable but the private copy serves — never EIO
			}
			base = nil // no base yet: MergeClaudeJSON returns the private copy verbatim
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

// WriteThrough persists a committed synthetic document back to the shared base.
// .claude.json's shareable keys split into ~/.claude.json (blacklisted keys never
// cross; the private file keeps the full payload, which the holder already wrote
// durably); settings.json's injected plansDirectory is stripped back out so the
// base stays pristine. Both run under writeThroughMu for cross-domain base
// atomicity, and both skip the rewrite when nothing shareable changed (writing
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
	base, err := os.ReadFile(target)
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
// cc-pool drives the holder through Manifest, so the holder never calls this; it
// completes the content.Source contract and keeps the classification in one place.
func (s *PoolContentSource) Classify(name string) content.EntryKind {
	switch {
	case name == claudeJSONName || name == settingsName:
		return content.EntrySynth
	case SharedEntries[name]:
		return content.EntrySymlink
	case PrivateEntry(name):
		return content.EntryPrivate
	default:
		return "" // a plain passthrough entry
	}
}

// HealthErrors joins every domain's last merged/served-read failure and last base
// write-through failure, for the daemon's status/doctor surface (the in-process
// mirror's healthErr, now over the bridge). A read failure is cleared by the next
// successful read; a write failure by the next successful write-through.
func (s *PoolContentSource) HealthErrors() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	var errs []error
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
