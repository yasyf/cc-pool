// Package ptyrelay runs a child process on a pseudo-terminal, mirrors its
// output to the real terminal while observing the byte stream, and on teardown
// resets exactly the terminal modes the child left set.
package ptyrelay

import (
	"fmt"
	"strconv"
	"strings"
)

type parseState int

const (
	stateGround parseState = iota
	stateEsc
	stateCSI
	stateOSC
	stateOSCEsc    // ESC seen inside an OSC, awaiting '\' for a String Terminator
	stateString    // DCS/SOS/PM/APC payload, discarded
	stateStringEsc // ESC seen inside a string payload, awaiting '\'
)

const (
	wordCap = 2048 // URL-candidate word cap; a longer word is discarded
	oscCap  = 4096 // OSC payload cap; a longer payload is discarded
	csiCap  = 128  // CSI parameter cap
)

// altScreenModes are the alternate-screen/save-cursor DEC private modes, reset
// first (1049, the combined mode, ahead of the legacy 1047/1048/47 triplet).
var altScreenModes = []int{1049, 1047, 1048, 47}

// otherModes are the remaining tracked DEC private modes, reset in ascending
// numeric order: mouse reporting (1000/1002/1003), focus (1004), SGR mouse
// (1006), bracketed paste (2004), and synchronized output (2026).
var otherModes = []int{1000, 1002, 1003, 1004, 1006, 2004, 2026}

var trackedModes = func() map[int]bool {
	m := make(map[int]bool, len(altScreenModes)+len(otherModes))
	for _, n := range altScreenModes {
		m[n] = true
	}
	for _, n := range otherModes {
		m[n] = true
	}
	return m
}()

// Scanner is a streaming observer of a terminal byte stream. It never blocks,
// never fails, and reproduces nothing — the relay writes the raw bytes through
// separately. It tracks which DEC private modes, cursor visibility, kitty
// keyboard depth, and SGR state the stream leaves set, and scrapes the first
// http(s) URL. A Scanner is used by a single goroutine.
type Scanner struct {
	state parseState

	modes        map[int]bool // tracked DEC private modes currently set (excludes 25)
	cursorHidden bool         // DECTCEM (?25) currently hidden
	kittyMain    int          // kitty keyboard stack depth pushed on the main screen
	kittyAlt     int          // kitty keyboard stack depth pushed on the alternate screen
	sgrDirty     bool         // a non-reset SGR is in effect

	lineFresh bool // last byte written was '\n' (true before any write)

	word         []byte
	wordOverflow bool
	osc          []byte
	oscOverflow  bool
	csi          []byte

	url string
}

// NewScanner returns an idle Scanner in the ground parser state.
func NewScanner() *Scanner {
	return &Scanner{
		state:     stateGround,
		modes:     make(map[int]bool),
		lineFresh: true,
	}
}

// Write feeds bytes to the observer. It always consumes all of p and never
// errors, satisfying io.Writer for convenient tee-ing.
func (s *Scanner) Write(p []byte) (int, error) {
	for _, b := range p {
		s.feed(b)
	}
	if len(p) > 0 {
		s.lineFresh = p[len(p)-1] == '\n'
	}
	return len(p), nil
}

// Flush finalizes a trailing URL-candidate word at stream end (a URL with no
// terminating whitespace is otherwise never recorded).
func (s *Scanner) Flush() { s.endWord() }

// URL returns the first http(s):// URL observed, or "" if none.
func (s *Scanner) URL() string { return s.url }

// LineFresh reports whether the last byte written was '\n' (true before any
// write) — the cursor is at column 0, a safe point to inject text.
func (s *Scanner) LineFresh() bool { return s.lineFresh }

// Ground reports whether the parser is between escape sequences — combined with
// LineFresh, a safe injection point.
func (s *Scanner) Ground() bool { return s.state == stateGround }

// ResetSeq returns the escapes that undo exactly the terminal modes still set,
// or "" if the stream left the terminal clean. Kitty keyboard stacks are
// per-screen, so alternate-screen pushes pop BEFORE the alt-screen exit and
// main-screen pushes pop after; then remaining modes ascending, show cursor,
// SGR reset.
func (s *Scanner) ResetSeq() string {
	var b strings.Builder
	if s.inAltScreen() && s.kittyAlt > 0 {
		fmt.Fprintf(&b, "\x1b[<%du", s.kittyAlt)
	}
	for _, m := range altScreenModes {
		if s.modes[m] {
			fmt.Fprintf(&b, "\x1b[?%dl", m)
		}
	}
	for _, m := range otherModes {
		if s.modes[m] {
			fmt.Fprintf(&b, "\x1b[?%dl", m)
		}
	}
	if s.kittyMain > 0 {
		fmt.Fprintf(&b, "\x1b[<%du", s.kittyMain)
	}
	if s.cursorHidden {
		b.WriteString("\x1b[?25h")
	}
	if s.sgrDirty {
		b.WriteString("\x1b[0m")
	}
	return b.String()
}

// AbortSeq returns the bytes that abort an escape sequence the stream ended
// inside of — ST for control strings, CAN for ESC/CSI — or "" in the ground
// state. The relay emits it before any other teardown output so a child killed
// mid-sequence can't make the terminal swallow what follows as payload.
func (s *Scanner) AbortSeq() string {
	switch s.state {
	case stateOSC, stateOSCEsc, stateString, stateStringEsc:
		return "\x1b\\"
	case stateEsc, stateCSI:
		return "\x18"
	}
	return ""
}

// inAltScreen reports whether an alternate-screen mode is active (1048 is
// cursor save/restore, not a screen switch).
func (s *Scanner) inAltScreen() bool {
	return s.modes[1049] || s.modes[1047] || s.modes[47]
}

func (s *Scanner) feed(b byte) {
	// CAN and SUB abort any in-progress sequence (ECMA-48 §8.3.6, §8.3.148).
	if (b == 0x18 || b == 0x1a) && s.state != stateGround {
		s.csi = s.csi[:0]
		s.osc = s.osc[:0]
		s.oscOverflow = false
		s.state = stateGround
		return
	}
reprocess:
	switch s.state {
	case stateGround:
		s.feedGround(b)
	case stateEsc:
		s.feedEsc(b)
	case stateCSI:
		s.feedCSI(b)
	case stateOSC:
		switch b {
		case 0x1b:
			s.state = stateOSCEsc
		case 0x07:
			s.finishOSC()
			s.state = stateGround
		default:
			if len(s.osc) < oscCap {
				s.osc = append(s.osc, b)
			} else {
				s.oscOverflow = true
			}
		}
	case stateOSCEsc:
		switch b {
		case '\\':
			s.finishOSC()
			s.state = stateGround
		case 0x1b:
			// another ESC; stay
		default:
			// aborted OSC: finalize, then restart as a fresh escape for this byte
			s.finishOSC()
			s.state = stateEsc
			goto reprocess
		}
	case stateString:
		if b == 0x1b {
			s.state = stateStringEsc
		}
	case stateStringEsc:
		switch b {
		case '\\':
			s.state = stateGround
		case 0x1b:
			// stay
		default:
			s.state = stateEsc
			goto reprocess
		}
	}
}

func (s *Scanner) feedGround(b byte) {
	if b == 0x1b {
		s.endWord()
		s.state = stateEsc
		return
	}
	if b > 0x20 && b < 0x7f {
		if s.wordOverflow {
			return
		}
		if len(s.word) >= wordCap {
			s.wordOverflow = true
			s.word = s.word[:0]
			return
		}
		s.word = append(s.word, b)
		return
	}
	// whitespace, control, or high byte: word boundary
	s.endWord()
}

func (s *Scanner) feedEsc(b byte) {
	switch b {
	case '[':
		s.csi = s.csi[:0]
		s.state = stateCSI
	case ']':
		s.osc = s.osc[:0]
		s.oscOverflow = false
		s.state = stateOSC
	case 'P', 'X', '^', '_': // DCS, SOS, PM, APC
		s.state = stateString
	case 0x1b:
		// redundant ESC; stay
	default:
		// two-byte escape (ESC x) or ST (ESC \): nothing to track
		s.state = stateGround
	}
}

func (s *Scanner) feedCSI(b byte) {
	switch {
	case b == 0x1b:
		s.state = stateEsc
	case b >= 0x40 && b <= 0x7e:
		s.dispatchCSI(b)
		s.state = stateGround
	case b >= 0x20 && b <= 0x3f:
		if len(s.csi) < csiCap {
			s.csi = append(s.csi, b)
		}
	default:
		// C0 control inside CSI is executed without aborting; ignore it
	}
}

func (s *Scanner) dispatchCSI(final byte) {
	params := s.csi
	var marker byte
	if len(params) > 0 && params[0] >= '<' && params[0] <= '?' {
		marker = params[0]
		params = params[1:]
	}
	switch final {
	case 'h', 'l':
		if marker == '?' {
			s.applyModes(params, final == 'h')
		}
	case 'm':
		if marker == 0 {
			s.applySGR(params)
		}
	case 'u':
		depth := &s.kittyMain
		if s.inAltScreen() {
			depth = &s.kittyAlt
		}
		switch marker {
		case '>':
			*depth++
		case '<':
			*depth -= parseFirstParam(params, 1)
			if *depth < 0 {
				*depth = 0
			}
		}
	}
}

func (s *Scanner) applyModes(params []byte, set bool) {
	for _, tok := range splitParams(params) {
		n, ok := atoi(tok)
		if !ok {
			continue
		}
		switch {
		case n == 25:
			s.cursorHidden = !set // ?25h shows, ?25l hides
		case trackedModes[n]:
			s.modes[n] = set
		}
	}
}

func (s *Scanner) applySGR(params []byte) {
	for _, tok := range splitParams(params) {
		if tok != "" && tok != "0" {
			s.sgrDirty = true
			return
		}
	}
	s.sgrDirty = false
}

func (s *Scanner) finishOSC() {
	if s.oscOverflow {
		return
	}
	// OSC 8 hyperlink: 8 ; params ; URI — the payload names the URI exactly,
	// so no punctuation trim.
	parts := strings.SplitN(string(s.osc), ";", 3)
	if len(parts) == 3 && parts[0] == "8" {
		s.recordCandidate(parts[2], true)
	}
}

func (s *Scanner) endWord() {
	if !s.wordOverflow && len(s.word) > 0 {
		s.recordCandidate(string(s.word), false)
	}
	s.word = s.word[:0]
	s.wordOverflow = false
}

func (s *Scanner) recordCandidate(w string, exact bool) {
	if s.url != "" {
		return
	}
	if !strings.HasPrefix(w, "http://") && !strings.HasPrefix(w, "https://") {
		return
	}
	if !exact {
		w = strings.TrimRight(w, ".,)>'\"")
	}
	s.url = w
}

func splitParams(params []byte) []string {
	if len(params) == 0 {
		return nil
	}
	return strings.Split(string(params), ";")
}

func parseFirstParam(params []byte, def int) int {
	toks := splitParams(params)
	if len(toks) == 0 {
		return def
	}
	if n, ok := atoi(toks[0]); ok {
		return n
	}
	return def
}

func atoi(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return n, true
}
