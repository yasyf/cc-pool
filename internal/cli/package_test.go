package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPackageCommandsAreExplicitMachineOperations(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string
		swap func(*testing.T, func(context.Context) error)
	}{
		{name: "install", want: "installed: CCPoolStatus package", swap: func(t *testing.T, fn func(context.Context) error) {
			swapVar(t, &installPackage, fn)
		}},
		{name: "uninstall", want: "uninstalled: CCPoolStatus package", swap: func(t *testing.T, fn func(context.Context) error) {
			swapVar(t, &uninstallPackage, fn)
		}},
		{name: "reset", want: "reset: CCPoolStatus package", swap: func(t *testing.T, fn func(context.Context) error) {
			swapVar(t, &resetPackage, fn)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := 0
			tc.swap(t, func(context.Context) error { called++; return nil })
			cmd := newPackageCmd()
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetArgs([]string{tc.name})
			if err := cmd.ExecuteContext(t.Context()); err != nil {
				t.Fatal(err)
			}
			if called != 1 || !strings.Contains(output.String(), tc.want) {
				t.Fatalf("calls = %d, output = %q", called, output.String())
			}
		})
	}
}

func TestPackageCommandsRejectArgumentsAndPreserveOperationErrors(t *testing.T) {
	cmd := newPackageCmd()
	cmd.SetArgs([]string{"install", "unexpected"})
	if err := cmd.ExecuteContext(t.Context()); err == nil {
		t.Fatal("package install accepted an argument")
	}
	want := errors.New("install failed")
	swapVar(t, &installPackage, func(context.Context) error { return want })
	cmd = newPackageCmd()
	cmd.SetArgs([]string{"install"})
	if err := cmd.ExecuteContext(t.Context()); !errors.Is(err, want) {
		t.Fatalf("package install error = %v, want %v", err, want)
	}
}
