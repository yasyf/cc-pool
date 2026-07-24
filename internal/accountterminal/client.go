package accountterminal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/creack/pty"
	"github.com/muesli/cancelreader"
	"golang.org/x/term"
)

// TerminalClientEndpoint carries terminal events without owning local display state.
type TerminalClientEndpoint interface {
	Send(context.Context, TerminalInput) error
	Receive(context.Context) (TerminalOutput, error)
}

// TerminalClientConfig describes one local terminal attachment.
type TerminalClientConfig struct {
	Endpoint     TerminalClientEndpoint
	Stdin        *os.File
	Stdout       io.Writer
	ResizeSource *os.File
	OnURL        func(context.Context, string) error
}

type terminalClientOutput struct {
	err      error
	callback <-chan error
	display  *TerminalDisplayState
}

// RunTerminalClient joins local input, output, resize, display cleanup, and URL observation.
func RunTerminalClient(ctx context.Context, config TerminalClientConfig) error {
	if config.Endpoint == nil {
		return errors.New("account terminal: terminal client endpoint is required")
	}
	if config.Stdin == nil {
		return errors.New("account terminal: terminal client stdin is required")
	}
	if config.Stdout == nil {
		return errors.New("account terminal: terminal client stdout is required")
	}
	resizeSource := config.ResizeSource
	if resizeSource == nil {
		resizeSource = config.Stdin
	}

	reader, err := cancelreader.NewReader(config.Stdin)
	if err != nil {
		return fmt.Errorf("account terminal: wrap terminal stdin: %w", err)
	}
	defer reader.Close()

	var previous *term.State
	if term.IsTerminal(int(config.Stdin.Fd())) {
		previous, err = term.MakeRaw(int(config.Stdin.Fd()))
		if err != nil {
			return fmt.Errorf("account terminal: set terminal raw mode: %w", err)
		}
		if err := enableTerminalInterrupt(int(config.Stdin.Fd())); err != nil {
			_ = term.Restore(int(config.Stdin.Fd()), previous)
			return fmt.Errorf("account terminal: enable terminal interrupt: %w", err)
		}
	}
	var restoreErr error
	restore := sync.OnceFunc(func() {
		if previous != nil {
			if err := term.Restore(int(config.Stdin.Fd()), previous); err != nil {
				restoreErr = fmt.Errorf("account terminal: restore terminal mode: %w", err)
			}
		}
	})
	defer restore()

	clientCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := sendTerminalClientResize(clientCtx, config.Endpoint, resizeSource); err != nil {
		return err
	}

	inputDone := make(chan error, 1)
	outputDone := make(chan terminalClientOutput, 1)
	resizeDone := make(chan error, 1)
	go pumpTerminalClientInput(clientCtx, reader, config.Endpoint, inputDone)
	go pumpTerminalClientOutput(clientCtx, ctx, config, outputDone)
	stopResize := watchTerminalClientResize(clientCtx, config.Endpoint, resizeSource, resizeDone)

	var inputErr error
	var output terminalClientOutput
	inputSettled := false
	outputSettled := false
	for !outputSettled {
		select {
		case output = <-outputDone:
			outputSettled = true
			cancel()
			_ = reader.Cancel()
		case err := <-inputDone:
			inputSettled = true
			if err != nil {
				inputErr = err
				cancel()
				output = <-outputDone
				outputSettled = true
			}
		case err := <-resizeDone:
			if err != nil {
				inputErr = err
				cancel()
				_ = reader.Cancel()
				output = <-outputDone
				outputSettled = true
			}
		case <-ctx.Done():
			inputErr = fmt.Errorf("account terminal: terminal client: %w", ctx.Err())
			cancel()
			_ = reader.Cancel()
			output = <-outputDone
			outputSettled = true
		}
	}
	cancel()
	stopResize()
	if !inputSettled {
		_ = reader.Cancel()
		inputErr = errors.Join(inputErr, <-inputDone)
	}
	restore()
	displayErr := settleTerminalDisplay(config.Stdout, output)
	if ctx.Err() != nil && inputErr == nil {
		inputErr = fmt.Errorf("account terminal: terminal client: %w", ctx.Err())
	}
	return errors.Join(inputErr, output.err, restoreErr, displayErr)
}

func pumpTerminalClientInput(
	ctx context.Context,
	reader cancelreader.CancelReader,
	endpoint TerminalClientEndpoint,
	done chan<- error,
) {
	buffer := make([]byte, TerminalChunkSize)
	for {
		count, readErr := reader.Read(buffer)
		if count > 0 {
			if err := endpoint.Send(ctx, TerminalInput{Kind: TerminalInputBytes, Data: buffer[:count]}); err != nil {
				done <- fmt.Errorf("account terminal: relay terminal input: %w", err)
				return
			}
		}
		if readErr == nil {
			continue
		}
		if ctx.Err() != nil {
			done <- nil
			return
		}
		if errors.Is(readErr, io.EOF) {
			done <- endpoint.Send(ctx, TerminalInput{Kind: TerminalInputEOF})
			return
		}
		done <- fmt.Errorf("account terminal: read terminal input: %w", readErr)
		return
	}
}

func pumpTerminalClientOutput(
	ctx context.Context,
	callbackCtx context.Context,
	config TerminalClientConfig,
	done chan<- terminalClientOutput,
) {
	display := NewTerminalDisplayState()
	var callback chan error
	for {
		output, err := config.Endpoint.Receive(ctx)
		if len(output.Data) > 0 {
			_, _ = display.Write(output.Data)
			if writeErr := writeTerminalOutput(config.Stdout, output.Data); writeErr != nil {
				display.Flush()
				callback = startTerminalURLCallback(callbackCtx, config.OnURL, display, callback)
				done <- terminalClientOutput{
					err: fmt.Errorf("account terminal: write terminal output: %w", writeErr), callback: callback, display: display,
				}
				return
			}
			callback = startTerminalURLCallback(callbackCtx, config.OnURL, display, callback)
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			display.Flush()
			callback = startTerminalURLCallback(callbackCtx, config.OnURL, display, callback)
			done <- terminalClientOutput{callback: callback, display: display}
			return
		}
		if ctx.Err() != nil {
			display.Flush()
			callback = startTerminalURLCallback(callbackCtx, config.OnURL, display, callback)
			done <- terminalClientOutput{callback: callback, display: display}
			return
		}
		display.Flush()
		callback = startTerminalURLCallback(callbackCtx, config.OnURL, display, callback)
		done <- terminalClientOutput{
			err: fmt.Errorf("account terminal: receive terminal output: %w", err), callback: callback, display: display,
		}
		return
	}
}

func startTerminalURLCallback(
	ctx context.Context,
	callback func(context.Context, string) error,
	display *TerminalDisplayState,
	started chan error,
) chan error {
	if started != nil || callback == nil || display.URL() == "" {
		return started
	}
	started = make(chan error, 1)
	url := display.URL()
	go func() { started <- callback(ctx, url) }()
	return started
}

func sendTerminalClientResize(ctx context.Context, endpoint TerminalClientEndpoint, source *os.File) error {
	if source == nil || !term.IsTerminal(int(source.Fd())) {
		return nil
	}
	size, err := pty.GetsizeFull(source)
	if err != nil {
		return fmt.Errorf("account terminal: read terminal size: %w", err)
	}
	if err := endpoint.Send(ctx, TerminalInput{Kind: TerminalInputResize, Size: TerminalSize{Rows: size.Rows, Cols: size.Cols}}); err != nil {
		return fmt.Errorf("account terminal: relay terminal resize: %w", err)
	}
	return nil
}

func watchTerminalClientResize(
	ctx context.Context,
	endpoint TerminalClientEndpoint,
	source *os.File,
	done chan<- error,
) func() {
	if source == nil || !term.IsTerminal(int(source.Fd())) {
		return func() {}
	}
	signals := make(chan os.Signal, 1)
	stopped := make(chan struct{})
	signal.Notify(signals, syscall.SIGWINCH)
	go func() {
		defer close(stopped)
		defer signal.Stop(signals)
		for {
			select {
			case <-signals:
				if err := sendTerminalClientResize(ctx, endpoint, source); err != nil {
					done <- err
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return func() {
		signal.Stop(signals)
		<-stopped
	}
}

func settleTerminalDisplay(stdout io.Writer, output terminalClientOutput) error {
	resetErr := resetTerminalState(stdout, output.display)
	var callbackErr error
	if output.callback != nil {
		callbackErr = <-output.callback
	}
	return errors.Join(resetErr, callbackErr)
}

func resetTerminalState(stdout io.Writer, display *TerminalDisplayState) error {
	if display == nil {
		return nil
	}
	display.Flush()
	var cleanup string
	if sequence := display.AbortSeq(); sequence != "" {
		cleanup += sequence
	}
	if !display.LineFresh() {
		cleanup += "\r\n"
	}
	cleanup += display.ResetSeq()
	if cleanup == "" {
		return nil
	}
	return writeTerminalOutput(stdout, []byte(cleanup))
}

func writeTerminalOutput(output io.Writer, data []byte) error {
	for len(data) > 0 {
		count, err := output.Write(data)
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
		data = data[count:]
	}
	return nil
}
