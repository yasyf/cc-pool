package daemon

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/accountterminal"
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/wire"
)

const (
	accountTerminalBytes  byte = 1
	accountTerminalResize byte = 2
	accountTerminalEOF    byte = 3
)

type accountMutationTerminalRunner interface {
	Start(context.Context, store.AccountMutation, accountterminal.TerminalSize) (accountMutationTerminal, error)
	LoginReady(context.Context, store.AccountMutation) (bool, error)
}

type accountMutationTerminal interface {
	Attach(context.Context, accountterminal.TerminalAttachmentSpec) (accountMutationTerminalAttachment, error)
	Cancel(context.Context) error
	Wait(context.Context) (accountterminal.TerminalOutcome, error)
	Acknowledge(context.Context, [32]byte) error
	Retired() <-chan struct{}
}

type accountMutationTerminalAttachment interface {
	ClaimControl(
		context.Context,
		accountterminal.TerminalDisconnectPolicy,
		time.Duration,
	) (accountterminal.TerminalControllerLease, error)
	RenewControl(context.Context) (accountterminal.TerminalControllerLease, error)
	Send(context.Context, accountterminal.TerminalInput) error
	Receive(context.Context) (accountterminal.TerminalOutput, error)
	Close() error
}

type managedAccountMutationTerminalRunner struct {
	terminals *accountterminal.Manager
	manager   *pool.Manager
}

type managedAccountMutationTerminal struct {
	terminal *accountterminal.Terminal
}

func (r managedAccountMutationTerminalRunner) Start(
	ctx context.Context,
	mutation store.AccountMutation,
	size accountterminal.TerminalSize,
) (accountMutationTerminal, error) {
	if r.terminals == nil || r.manager == nil {
		return nil, errors.New("account terminal manager is unavailable")
	}
	args := []string{"auth", "login"}
	if mutation.Kind == store.AccountMutationRelogin {
		if identity, err := r.manager.AccountIdentity(
			ctx, mutation.AccountID, mutation.ConfigDir,
		); err == nil && identity.EmailAddress != "" {
			args = append(args, "--email", identity.EmailAddress)
		}
	}
	terminal, err := r.terminals.Start(ctx, accountterminal.TerminalSpec{
		Path: "claude",
		Args: args,
		Dir:  mutation.ConfigDir,
		Env: accountMutationTerminalEnv(
			os.Environ(), mutation.ConfigDir, filepath.Join(pool.ClaudeDir(), "plugins"),
		),
		Size: size, AttachTimeout: accountterminal.DefaultTerminalAttachTimeout,
	})
	if err != nil {
		return nil, err
	}
	return managedAccountMutationTerminal{terminal: terminal}, nil
}

func (r managedAccountMutationTerminalRunner) LoginReady(
	ctx context.Context,
	mutation store.AccountMutation,
) (bool, error) {
	account := accountMutationAccount(mutation)
	state, err := r.manager.CredentialExternalState(ctx, account)
	if err != nil {
		return false, err
	}
	digest, err := state.Digest()
	if err != nil {
		return false, err
	}
	if digest == mutation.ExpectedCredentialDigest {
		return false, nil
	}
	credential, _, err := r.manager.ReadCredential(ctx, account)
	if err != nil {
		if errors.Is(err, creds.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if !credential.HasRefreshToken() || credential.Expired() {
		return false, nil
	}
	if _, err := r.manager.AccountIdentity(ctx, mutation.AccountID, mutation.ConfigDir); err != nil {
		if errors.Is(err, pool.ErrNoIdentity) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (t managedAccountMutationTerminal) Attach(
	ctx context.Context,
	spec accountterminal.TerminalAttachmentSpec,
) (accountMutationTerminalAttachment, error) {
	return t.terminal.Attach(ctx, spec)
}

func (t managedAccountMutationTerminal) Cancel(ctx context.Context) error {
	return t.terminal.Cancel(ctx)
}

func (t managedAccountMutationTerminal) Wait(
	ctx context.Context,
) (accountterminal.TerminalOutcome, error) {
	return t.terminal.Wait(ctx)
}

func (t managedAccountMutationTerminal) Acknowledge(ctx context.Context, digest [32]byte) error {
	return t.terminal.Acknowledge(ctx, digest)
}

func (t managedAccountMutationTerminal) Retired() <-chan struct{} {
	return t.terminal.Retired()
}

func waitAccountMutationTerminal(
	lifetime context.Context,
	runner accountMutationTerminalRunner,
	terminal accountMutationTerminal,
	mutation store.AccountMutation,
) (accountterminal.TerminalOutcome, error) {
	type terminalWait struct {
		outcome accountterminal.TerminalOutcome
		err     error
	}
	waitDone := make(chan terminalWait, 1)
	go func() {
		outcome, err := terminal.Wait(context.WithoutCancel(lifetime))
		waitDone <- terminalWait{outcome: outcome, err: err}
	}()
	probe := time.NewTicker(500 * time.Millisecond)
	defer probe.Stop()
	for {
		select {
		case waited := <-waitDone:
			return waited.outcome, accountMutationTerminalOutcome(waited.outcome, waited.err)
		case <-lifetime.Done():
			cancelCtx, cancel := context.WithTimeout(
				context.WithoutCancel(lifetime), accountMutationProbeWait,
			)
			cancelErr := terminal.Cancel(cancelCtx)
			cancel()
			waited := <-waitDone
			return waited.outcome, errors.Join(lifetime.Err(), cancelErr, waited.err)
		case <-probe.C:
			probeCtx, cancel := context.WithTimeout(lifetime, accountMutationProbeWait)
			ready, err := runner.LoginReady(probeCtx, mutation)
			cancel()
			if err != nil {
				cancelCtx, cancel := context.WithTimeout(
					context.WithoutCancel(lifetime), accountMutationProbeWait,
				)
				cancelErr := terminal.Cancel(cancelCtx)
				cancel()
				waited := <-waitDone
				return waited.outcome, errors.Join(err, cancelErr, waited.err)
			}
			if ready {
				cancelCtx, cancel := context.WithTimeout(
					context.WithoutCancel(lifetime), accountMutationProbeWait,
				)
				cancelErr := terminal.Cancel(cancelCtx)
				cancel()
				waited := <-waitDone
				return waited.outcome, errors.Join(cancelErr, waited.err)
			}
		}
	}
}

func accountMutationTerminalOutcome(outcome accountterminal.TerminalOutcome, err error) error {
	if err != nil {
		return err
	}
	switch outcome.Kind {
	case accountterminal.TerminalExited:
		if outcome.ExitCode == 0 {
			return nil
		}
		return fmt.Errorf("claude auth login exited with status %d", outcome.ExitCode)
	case accountterminal.TerminalSignaled:
		return fmt.Errorf("claude auth login terminated by %s", outcome.Signal)
	case accountterminal.TerminalCanceled:
		return errors.New("claude auth login was cancelled")
	default:
		return errors.New("claude auth login returned an invalid terminal outcome")
	}
}

func accountMutationTerminalEnv(base []string, configDir, pluginDir string) []string {
	env := make([]string, 0, len(base)+2)
	for _, value := range base {
		if hasEnvKey(value, "CLAUDE_CONFIG_DIR") || hasEnvKey(value, "CLAUDE_CODE_PLUGIN_CACHE_DIR") {
			continue
		}
		env = append(env, value)
	}
	return append(env,
		"CLAUDE_CONFIG_DIR="+configDir,
		"CLAUDE_CODE_PLUGIN_CACHE_DIR="+pluginDir,
	)
}

func hasEnvKey(value, key string) bool {
	return len(value) > len(key) && value[:len(key)] == key && value[len(key)] == '='
}

func encodeAccountTerminalInput(event accountterminal.TerminalInput) ([]byte, error) {
	switch event.Kind {
	case accountterminal.TerminalInputBytes:
		if len(event.Data) == 0 || len(event.Data) > accountterminal.TerminalChunkSize-1 {
			return nil, errors.New("terminal byte input is empty or oversized")
		}
		return append([]byte{accountTerminalBytes}, event.Data...), nil
	case accountterminal.TerminalInputResize:
		payload := make([]byte, 5)
		payload[0] = accountTerminalResize
		binary.BigEndian.PutUint16(payload[1:3], event.Size.Rows)
		binary.BigEndian.PutUint16(payload[3:5], event.Size.Cols)
		return payload, nil
	case accountterminal.TerminalInputEOF:
		return []byte{accountTerminalEOF}, nil
	default:
		return nil, errors.New("unknown terminal input kind")
	}
}

func decodeAccountTerminalInput(chunk wire.Chunk) (accountterminal.TerminalInput, error) {
	if chunk.End && len(chunk.Payload) == 0 {
		return accountterminal.TerminalInput{Kind: accountterminal.TerminalInputEOF}, nil
	}
	if len(chunk.Payload) == 0 {
		return accountterminal.TerminalInput{}, nil
	}
	switch chunk.Payload[0] {
	case accountTerminalBytes:
		if len(chunk.Payload) == 1 || len(chunk.Payload) > accountterminal.TerminalChunkSize {
			return accountterminal.TerminalInput{}, errors.New("terminal byte input is empty or oversized")
		}
		return accountterminal.TerminalInput{
			Kind: accountterminal.TerminalInputBytes, Data: append([]byte(nil), chunk.Payload[1:]...),
		}, nil
	case accountTerminalResize:
		if len(chunk.Payload) != 5 {
			return accountterminal.TerminalInput{}, errors.New("terminal resize input is malformed")
		}
		return accountterminal.TerminalInput{
			Kind: accountterminal.TerminalInputResize,
			Size: accountterminal.TerminalSize{
				Rows: binary.BigEndian.Uint16(chunk.Payload[1:3]),
				Cols: binary.BigEndian.Uint16(chunk.Payload[3:5]),
			},
		}, nil
	case accountTerminalEOF:
		if len(chunk.Payload) != 1 {
			return accountterminal.TerminalInput{}, errors.New("terminal EOF input is malformed")
		}
		return accountterminal.TerminalInput{Kind: accountterminal.TerminalInputEOF}, nil
	default:
		return accountterminal.TerminalInput{}, errors.New("terminal input kind is unknown")
	}
}

func encodeAccountTerminalOutput(output accountterminal.TerminalOutput) ([]byte, error) {
	if len(output.Data) == 0 || len(output.Data) > accountterminal.TerminalChunkSize {
		return nil, errors.New("terminal output is empty or oversized")
	}
	payload := make([]byte, 8+len(output.Data))
	binary.BigEndian.PutUint64(payload[:8], output.Sequence)
	copy(payload[8:], output.Data)
	return payload, nil
}
