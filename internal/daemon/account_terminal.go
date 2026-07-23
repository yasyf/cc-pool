package daemon

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"
)

const (
	accountTerminalBytes  byte = 1
	accountTerminalResize byte = 2
	accountTerminalEOF    byte = 3
)

type accountMutationTerminalRunner interface {
	Start(context.Context, store.AccountMutation, supervise.TerminalSize) (accountMutationTerminal, error)
	LoginReady(context.Context, store.AccountMutation) (bool, error)
}

type accountMutationTerminal interface {
	Attach(context.Context, supervise.TerminalAttachmentSpec) (accountMutationTerminalAttachment, error)
	Cancel(context.Context) error
	Wait(context.Context) (supervise.TerminalOutcome, error)
	Acknowledge(context.Context, [32]byte) error
	Retired() <-chan struct{}
}

type accountMutationTerminalAttachment interface {
	ClaimControl(
		context.Context,
		supervise.TerminalDisconnectPolicy,
		time.Duration,
	) (supervise.TerminalControllerLease, error)
	RenewControl(context.Context) (supervise.TerminalControllerLease, error)
	Send(context.Context, supervise.TerminalInput) error
	Receive(context.Context) (supervise.TerminalOutput, error)
	Close() error
}

type daemonkitAccountMutationTerminalRunner struct {
	workers *supervise.Pool
	manager *pool.Manager
}

type daemonkitAccountMutationTerminal struct {
	terminal *supervise.Terminal
}

func (r daemonkitAccountMutationTerminalRunner) Start(
	ctx context.Context,
	mutation store.AccountMutation,
	size supervise.TerminalSize,
) (accountMutationTerminal, error) {
	if r.workers == nil || r.manager == nil {
		return nil, errors.New("daemonkit terminal worker pool is unavailable")
	}
	args := []string{"auth", "login"}
	if mutation.Kind == store.AccountMutationRelogin ||
		mutation.Kind == store.AccountMutationPresentationRebind {
		identityConfigDir := mutation.ConfigDir
		if mutation.Kind == store.AccountMutationPresentationRebind {
			identityConfigDir = mutation.PreviousConfigDir
		}
		if identity, err := r.manager.AccountIdentity(
			ctx, mutation.AccountID, identityConfigDir,
		); err == nil && identity.EmailAddress != "" {
			args = append(args, "--email", identity.EmailAddress)
		}
	}
	terminal, err := r.workers.StartTerminal(ctx, supervise.TerminalSpec{
		RecoveryClass: proc.RecoveryTask,
		Path:          "claude",
		Args:          args,
		Dir:           mutation.ConfigDir,
		Env: accountMutationTerminalEnv(
			os.Environ(), mutation.ConfigDir, filepath.Join(pool.ClaudeDir(), "plugins"),
		),
		Size: size, AttachTimeout: supervise.DefaultTerminalAttachTimeout,
	})
	if err != nil {
		return nil, err
	}
	return daemonkitAccountMutationTerminal{terminal: terminal}, nil
}

func (r daemonkitAccountMutationTerminalRunner) LoginReady(
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

func (t daemonkitAccountMutationTerminal) Attach(
	ctx context.Context,
	spec supervise.TerminalAttachmentSpec,
) (accountMutationTerminalAttachment, error) {
	return t.terminal.Attach(ctx, spec)
}

func (t daemonkitAccountMutationTerminal) Cancel(ctx context.Context) error {
	return t.terminal.Cancel(ctx)
}

func (t daemonkitAccountMutationTerminal) Wait(
	ctx context.Context,
) (supervise.TerminalOutcome, error) {
	return t.terminal.Wait(ctx)
}

func (t daemonkitAccountMutationTerminal) Acknowledge(ctx context.Context, digest [32]byte) error {
	return t.terminal.Acknowledge(ctx, digest)
}

func (t daemonkitAccountMutationTerminal) Retired() <-chan struct{} {
	return t.terminal.Retired()
}

func waitAccountMutationTerminal(
	lifetime context.Context,
	runner accountMutationTerminalRunner,
	terminal accountMutationTerminal,
	mutation store.AccountMutation,
) (supervise.TerminalOutcome, error) {
	type terminalWait struct {
		outcome supervise.TerminalOutcome
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

func accountMutationTerminalOutcome(outcome supervise.TerminalOutcome, err error) error {
	if err != nil {
		return err
	}
	switch outcome.Kind {
	case supervise.TerminalExited:
		if outcome.ExitCode == 0 {
			return nil
		}
		return fmt.Errorf("claude auth login exited with status %d", outcome.ExitCode)
	case supervise.TerminalSignaled:
		return fmt.Errorf("claude auth login terminated by %s", outcome.Signal)
	case supervise.TerminalCanceled:
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

func encodeAccountTerminalInput(event supervise.TerminalInput) ([]byte, error) {
	switch event.Kind {
	case supervise.TerminalInputBytes:
		if len(event.Data) == 0 || len(event.Data) > supervise.TerminalChunkSize-1 {
			return nil, errors.New("terminal byte input is empty or oversized")
		}
		return append([]byte{accountTerminalBytes}, event.Data...), nil
	case supervise.TerminalInputResize:
		payload := make([]byte, 5)
		payload[0] = accountTerminalResize
		binary.BigEndian.PutUint16(payload[1:3], event.Size.Rows)
		binary.BigEndian.PutUint16(payload[3:5], event.Size.Cols)
		return payload, nil
	case supervise.TerminalInputEOF:
		return []byte{accountTerminalEOF}, nil
	default:
		return nil, errors.New("unknown terminal input kind")
	}
}

func decodeAccountTerminalInput(chunk wire.Chunk) (supervise.TerminalInput, error) {
	if chunk.End && len(chunk.Payload) == 0 {
		return supervise.TerminalInput{Kind: supervise.TerminalInputEOF}, nil
	}
	if len(chunk.Payload) == 0 {
		return supervise.TerminalInput{}, nil
	}
	switch chunk.Payload[0] {
	case accountTerminalBytes:
		if len(chunk.Payload) == 1 || len(chunk.Payload) > supervise.TerminalChunkSize {
			return supervise.TerminalInput{}, errors.New("terminal byte input is empty or oversized")
		}
		return supervise.TerminalInput{
			Kind: supervise.TerminalInputBytes, Data: append([]byte(nil), chunk.Payload[1:]...),
		}, nil
	case accountTerminalResize:
		if len(chunk.Payload) != 5 {
			return supervise.TerminalInput{}, errors.New("terminal resize input is malformed")
		}
		return supervise.TerminalInput{
			Kind: supervise.TerminalInputResize,
			Size: supervise.TerminalSize{
				Rows: binary.BigEndian.Uint16(chunk.Payload[1:3]),
				Cols: binary.BigEndian.Uint16(chunk.Payload[3:5]),
			},
		}, nil
	case accountTerminalEOF:
		if len(chunk.Payload) != 1 {
			return supervise.TerminalInput{}, errors.New("terminal EOF input is malformed")
		}
		return supervise.TerminalInput{Kind: supervise.TerminalInputEOF}, nil
	default:
		return supervise.TerminalInput{}, errors.New("terminal input kind is unknown")
	}
}

func encodeAccountTerminalOutput(output supervise.TerminalOutput) ([]byte, error) {
	if len(output.Data) == 0 || len(output.Data) > supervise.TerminalChunkSize {
		return nil, errors.New("terminal output is empty or oversized")
	}
	payload := make([]byte, 8+len(output.Data))
	binary.BigEndian.PutUint64(payload[:8], output.Sequence)
	copy(payload[8:], output.Data)
	return payload, nil
}
