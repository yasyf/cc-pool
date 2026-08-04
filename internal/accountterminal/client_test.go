package accountterminal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/creack/pty"
	"golang.org/x/term"
)

type terminalClientEndpointStub struct {
	mu      sync.Mutex
	outputs []TerminalOutput
	index   int
	block   bool
	seen    chan struct{}
}

func (s *terminalClientEndpointStub) Send(context.Context, TerminalInput) error { return nil }

func (s *terminalClientEndpointStub) Receive(ctx context.Context) (TerminalOutput, error) {
	s.mu.Lock()
	if s.index < len(s.outputs) {
		output := s.outputs[s.index]
		s.index++
		if s.seen != nil && s.index == len(s.outputs) {
			close(s.seen)
		}
		s.mu.Unlock()
		return output, nil
	}
	block := s.block
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return TerminalOutput{}, ctx.Err()
	}
	return TerminalOutput{}, io.EOF
}

type recordingTerminalEndpoint struct {
	mu   sync.Mutex
	sent []TerminalInput
	eof  chan struct{}
}

func (e *recordingTerminalEndpoint) Send(_ context.Context, input TerminalInput) error {
	e.mu.Lock()
	e.sent = append(e.sent, cloneTerminalInput(input))
	e.mu.Unlock()
	if input.Kind == TerminalInputEOF {
		close(e.eof)
	}
	return nil
}

func (e *recordingTerminalEndpoint) Receive(ctx context.Context) (TerminalOutput, error) {
	select {
	case <-e.eof:
		return TerminalOutput{}, io.EOF
	case <-ctx.Done():
		return TerminalOutput{}, ctx.Err()
	}
}

func TestRunTerminalClientEmptyStdinSendsExactlyOneEOFAndSettles(t *testing.T) {
	stdin, writer := terminalClientPipe(t)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	endpoint := &recordingTerminalEndpoint{eof: make(chan struct{})}
	var stdout bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	if err := RunTerminalClient(ctx, TerminalClientConfig{
		Endpoint: endpoint, Stdin: stdin, Stdout: &stdout,
	}); err != nil {
		t.Fatal(err)
	}
	endpoint.mu.Lock()
	sent := append([]TerminalInput(nil), endpoint.sent...)
	endpoint.mu.Unlock()
	if len(sent) != 1 || sent[0].Kind != TerminalInputEOF {
		t.Fatalf("empty stdin sent %+v, want exactly one terminal EOF", sent)
	}
}

func terminalClientPipe(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})
	return reader, writer
}

func TestRunTerminalClientNormalEOFCleansDisplayAndObservesFirstURL(t *testing.T) {
	stdin, _ := terminalClientPipe(t)
	endpoint := &terminalClientEndpointStub{outputs: []TerminalOutput{{
		Sequence: 3,
		Data:     []byte("\x1b[?1049h\x1b[?2004h\x1b[?25l\x1b[31mhttps://example.test/auth"),
	}}}
	var stdout bytes.Buffer
	var observed string
	err := RunTerminalClient(context.Background(), TerminalClientConfig{
		Endpoint: endpoint, Stdin: stdin, Stdout: &stdout,
		OnURL: func(ctx context.Context, url string) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			observed = url
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed != "https://example.test/auth" {
		t.Fatalf("observed URL = %q", observed)
	}
	wantSuffix := "\r\n\x1b[?1049l\x1b[?2004l\x1b[?25h\x1b[0m"
	if !strings.HasSuffix(stdout.String(), wantSuffix) {
		t.Fatalf("terminal cleanup = %q, want suffix %q", stdout.String(), wantSuffix)
	}
}

func TestRunTerminalClientCancellationAbortsDanglingOSC(t *testing.T) {
	stdin, _ := terminalClientPipe(t)
	seen := make(chan struct{})
	endpoint := &terminalClientEndpointStub{
		outputs: []TerminalOutput{{Sequence: 0, Data: []byte("\x1b]0;unfinished")}}, block: true, seen: seen,
	}
	var stdout bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunTerminalClient(ctx, TerminalClientConfig{Endpoint: endpoint, Stdin: stdin, Stdout: &stdout})
	}()
	<-seen
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled terminal client = %v", err)
	}
	if !strings.HasSuffix(stdout.String(), "\x1b\\\r\n") {
		t.Fatalf("dangling OSC cleanup = %q", stdout.String())
	}
}

func TestRunTerminalClientRestoresRawTerminalState(t *testing.T) {
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = master.Close()
		_ = slave.Close()
	})
	before, err := term.GetState(int(slave.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &terminalClientEndpointStub{}
	var stdout bytes.Buffer
	if err := RunTerminalClient(context.Background(), TerminalClientConfig{
		Endpoint: endpoint, Stdin: slave, Stdout: &stdout,
	}); err != nil {
		t.Fatal(err)
	}
	after, err := term.GetState(int(slave.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatal("terminal mode was not restored exactly")
	}
}
