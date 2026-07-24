package accountterminal

import (
	"fmt"
	"strings"
	"testing"
)

func scanChunks(t *testing.T, chunks []string, flush bool) *TerminalDisplayState {
	t.Helper()
	s := NewTerminalDisplayState()
	for _, c := range chunks {
		n, err := s.Write([]byte(c))
		if err != nil || n != len(c) {
			t.Fatalf("Write(%q) = %d, %v; want %d, nil", c, n, err, len(c))
		}
	}
	if flush {
		s.Flush()
	}
	return s
}

func TestScannerURL(t *testing.T) {
	tests := []struct {
		name   string
		chunks []string
		flush  bool
		want   string
	}{
		{"plain", []string{"visit https://example.com/auth now"}, false, "https://example.com/auth"},
		{"http scheme", []string{"go to http://example.com/x "}, false, "http://example.com/x"},
		{"split mid-url", []string{"visit https://exa", "mple.com/x done"}, false, "https://example.com/x"},
		{"osc8 bel", []string{"\x1b]8;;https://example.com/hi\x07label\x1b]8;;\x07"}, false, "https://example.com/hi"},
		{"osc8 st", []string{"\x1b]8;;https://example.com/st\x1b\\text "}, false, "https://example.com/st"},
		{"after sgr", []string{"\x1b[32mhttps://example.com/green\x1b[0m "}, false, "https://example.com/green"},
		{"first wins", []string{"https://first.com https://second.com "}, false, "https://first.com"},
		{"no url", []string{"just some text\n"}, false, ""},
		{"eof no whitespace, no flush", []string{"https://example.com/end"}, false, ""},
		{"eof no whitespace, flushed", []string{"https://example.com/end"}, true, "https://example.com/end"},
		{"trailing period trimmed", []string{"see https://example.com/x. "}, false, "https://example.com/x"},
		{"osc8 uri exact, no trim", []string{"\x1b]8;;https://example.test/release.\x07"}, false, "https://example.test/release."},
		{"trailing punct cluster trimmed", []string{"https://example.com/x).,\n"}, false, "https://example.com/x"},
		{"osc8 non-http ignored", []string{"\x1b]8;;mailto:a@b.com\x07"}, false, ""},
		{"osc0 title not url", []string{"\x1b]0;https://title.example\x07"}, false, ""},
		{"non-url word", []string{"nothttps://x.com "}, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := scanChunks(t, tt.chunks, tt.flush)
			if got := s.URL(); got != tt.want {
				t.Errorf("URL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestScannerModeEachReset(t *testing.T) {
	all := append(append([]int{}, altScreenModes...), otherModes...)
	for _, m := range all {
		t.Run(fmt.Sprintf("mode_%d", m), func(t *testing.T) {
			set := scanChunks(t, []string{fmt.Sprintf("\x1b[?%dh", m)}, false)
			want := fmt.Sprintf("\x1b[?%dl", m)
			if got := set.ResetSeq(); !strings.Contains(got, want) {
				t.Errorf("set: ResetSeq() = %q, want contains %q", got, want)
			}
			cleared := scanChunks(t, []string{fmt.Sprintf("\x1b[?%dh\x1b[?%dl", m, m)}, false)
			if got := cleared.ResetSeq(); got != "" {
				t.Errorf("set-then-clear: ResetSeq() = %q, want \"\"", got)
			}
		})
	}
}

func TestScannerModeCases(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantEmpty  bool
		wantPrefix string
		contains   []string
		absent     []string
	}{
		{name: "clean stream", input: "hello world\n", wantEmpty: true},
		{name: "1049 leads", input: "\x1b[?1049h", wantPrefix: "\x1b[?1049l", contains: []string{"\x1b[?1049l"}},
		{name: "1049 set then clear", input: "\x1b[?1049h\x1b[?1049l", wantEmpty: true},
		{name: "legacy alt pair", input: "\x1b[?1047h\x1b[?1048h", contains: []string{"\x1b[?1047l", "\x1b[?1048l"}, absent: []string{"\x1b[?1049l"}},
		{name: "legacy 47", input: "\x1b[?47h", contains: []string{"\x1b[?47l"}},
		{name: "multi-param set", input: "\x1b[?1000;1006h", contains: []string{"\x1b[?1000l", "\x1b[?1006l"}},
		{name: "cursor hidden", input: "\x1b[?25l", contains: []string{"\x1b[?25h"}},
		{name: "cursor hidden then shown", input: "\x1b[?25l\x1b[?25h", wantEmpty: true},
		{name: "kitty push twice", input: "\x1b[>1u\x1b[>1u", contains: []string{"\x1b[<2u"}},
		{name: "kitty push then pop default", input: "\x1b[>1u\x1b[<u", wantEmpty: true},
		{name: "kitty pop floors at zero", input: "\x1b[>1u\x1b[<5u", wantEmpty: true},
		{name: "sgr dirty", input: "\x1b[31m", contains: []string{"\x1b[0m"}},
		{name: "sgr dirty then reset", input: "\x1b[31m\x1b[0m", wantEmpty: true},
		{name: "sgr explicit zero clears", input: "\x1b[0m", wantEmpty: true},
		{name: "sgr compound has color", input: "\x1b[0;31m", contains: []string{"\x1b[0m"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := scanChunks(t, []string{tt.input}, false)
			got := s.ResetSeq()
			if tt.wantEmpty && got != "" {
				t.Fatalf("ResetSeq() = %q, want \"\"", got)
			}
			if tt.wantPrefix != "" && !strings.HasPrefix(got, tt.wantPrefix) {
				t.Errorf("ResetSeq() = %q, want prefix %q", got, tt.wantPrefix)
			}
			for _, sub := range tt.contains {
				if !strings.Contains(got, sub) {
					t.Errorf("ResetSeq() = %q, want contains %q", got, sub)
				}
			}
			for _, sub := range tt.absent {
				if strings.Contains(got, sub) {
					t.Errorf("ResetSeq() = %q, want NOT contains %q", got, sub)
				}
			}
		})
	}
}

func TestScannerResetOrder(t *testing.T) {
	// The push lands inside the alt screen, so its pop leads (kitty stacks are
	// per-screen); then alt-screen exit, modes ascending, cursor, SGR reset.
	s := scanChunks(t, []string{"\x1b[?1049h\x1b[?2004h\x1b[?1000h\x1b[>1u\x1b[?25l\x1b[31m"}, false)
	want := "\x1b[<1u" + "\x1b[?1049l" + "\x1b[?1000l" + "\x1b[?2004l" + "\x1b[?25h" + "\x1b[0m"
	if got := s.ResetSeq(); got != want {
		t.Errorf("ResetSeq() = %q, want %q", got, want)
	}
}

func TestScannerKittyPerScreen(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"push in alt pops before alt exit", "\x1b[?1049h\x1b[>1u", "\x1b[<1u\x1b[?1049l"},
		{"push in main pops after alt exit", "\x1b[>1u\x1b[?1049h", "\x1b[?1049l\x1b[<1u"},
		{"push both pops straddle alt exit", "\x1b[>1u\x1b[?1049h\x1b[>1u", "\x1b[<1u\x1b[?1049l\x1b[<1u"},
		{"pop in alt drains alt only", "\x1b[>1u\x1b[?1049h\x1b[<u", "\x1b[?1049l\x1b[<1u"},
		// An alt-screen push left behind after the child exits the alt screen is
		// unreachable residue: popping it would corrupt the main stack.
		{"alt residue after alt exit untouched", "\x1b[?1049h\x1b[>1u\x1b[?1049l", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := scanChunks(t, []string{tt.input}, false)
			if got := s.ResetSeq(); got != tt.want {
				t.Errorf("ResetSeq() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestScannerCanAborts(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"can aborts csi", "\x1b[?1049\x18h"},
		{"sub aborts csi", "\x1b[?1049\x1ah"},
		{"can aborts osc", "\x1b]0;title\x18"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := scanChunks(t, []string{tt.input}, false)
			if got := s.ResetSeq(); got != "" {
				t.Errorf("ResetSeq() = %q, want \"\" after abort", got)
			}
			if !s.Ground() {
				t.Error("Ground() = false, want true after abort")
			}
		})
	}
}

func TestScannerAbortSeq(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"ground", "plain text", ""},
		{"completed csi", "\x1b[?1049h", ""},
		{"mid csi", "\x1b[?104", "\x18"},
		{"lone esc", "\x1b", "\x18"},
		{"mid osc", "\x1b]0;title", "\x1b\\"},
		{"mid osc esc", "\x1b]0;title\x1b", "\x1b\\"},
		{"mid dcs", "\x1bP1;2q", "\x1b\\"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := scanChunks(t, []string{tt.input}, false)
			if got := s.AbortSeq(); got != tt.want {
				t.Errorf("AbortSeq() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestScannerEscapeSplit(t *testing.T) {
	seq := "\x1b[?1049h"
	for i := 0; i <= len(seq); i++ {
		t.Run(fmt.Sprintf("split_at_%d", i), func(t *testing.T) {
			s := scanChunks(t, []string{seq[:i], seq[i:]}, false)
			if got := s.ResetSeq(); !strings.Contains(got, "\x1b[?1049l") {
				t.Errorf("split at %d: ResetSeq() = %q, want contains 1049l", i, got)
			}
		})
	}
}

func TestScannerNegatives(t *testing.T) {
	t.Run("literal text not tracked", func(t *testing.T) {
		s := scanChunks(t, []string{"?1049h"}, false)
		if got := s.ResetSeq(); got != "" {
			t.Errorf("ResetSeq() = %q, want \"\" for literal text", got)
		}
	})
	t.Run("osc bytes not url words", func(t *testing.T) {
		s := scanChunks(t, []string{"\x1b]0;https://title.example\x07after"}, false)
		if got := s.URL(); got != "" {
			t.Errorf("URL() = %q, want \"\" (OSC swallowed)", got)
		}
	})
	t.Run("dcs string consumed", func(t *testing.T) {
		s := scanChunks(t, []string{"\x1bP1;2;3q\x1b\\https://real.example "}, false)
		if got := s.URL(); got != "https://real.example" {
			t.Errorf("URL() = %q, want https://real.example after DCS", got)
		}
	})
}

func TestScannerLineFresh(t *testing.T) {
	tests := []struct {
		name   string
		chunks []string
		want   bool
	}{
		{"nothing written", nil, true},
		{"trailing newline", []string{"abc\n"}, true},
		{"mid line", []string{"abc"}, false},
		{"bare cr", []string{"abc\r"}, false},
		{"newline then more", []string{"abc\n", "d"}, false},
		{"more then newline", []string{"abc", "d\n"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := scanChunks(t, tt.chunks, false)
			if got := s.LineFresh(); got != tt.want {
				t.Errorf("LineFresh() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScannerGround(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"plain text", "abc", true},
		{"completed csi", "\x1b[0m", true},
		{"mid csi", "\x1b[", false},
		{"lone esc", "\x1b", false},
		{"mid osc", "\x1b]8;;http://x", false},
		{"completed osc", "\x1b]8;;http://x\x07", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := scanChunks(t, []string{tt.input}, false)
			if got := s.Ground(); got != tt.want {
				t.Errorf("Ground() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScannerWordCapDiscard(t *testing.T) {
	// A URL longer than the word cap is discarded, not recorded.
	long := "https://example.com/" + strings.Repeat("a", wordCap+10)
	s := scanChunks(t, []string{long + " "}, false)
	if got := s.URL(); got != "" {
		t.Errorf("URL() = %q, want \"\" for over-cap word", got)
	}
}
