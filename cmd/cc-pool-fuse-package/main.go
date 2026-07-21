package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yasyf/cc-pool/internal/holderbridge"
)

func run(ctx context.Context, arguments []string) (resultErr error) {
	flags := flag.NewFlagSet("cc-pool-fuse-package", flag.ContinueOnError)
	appPath := flags.String("app", "", "exact application bundle path")
	signingIdentity := flags.String("signing-identity", "", "Developer ID signing identity")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || *appPath == "" || *signingIdentity == "" {
		return errors.New("cc-pool-fuse-package: -app and -signing-identity are required")
	}
	runner, err := holderbridge.NewToolRunner(ctx)
	if err != nil {
		return fmt.Errorf("cc-pool-fuse-package: start tool runner: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, runner.Close()) }()
	if err := holderbridge.PackageFUSE(ctx, runner, *signingIdentity, *appPath); err != nil {
		return fmt.Errorf("cc-pool-fuse-package: %w", err)
	}
	return nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
