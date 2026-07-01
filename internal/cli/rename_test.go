package cli

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

func renameTestManager(t *testing.T) *pool.Manager {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return &pool.Manager{Store: st}
}

func addRenameAccount(t *testing.T, m *pool.Manager, id int, label, email string) {
	t.Helper()
	dir := t.TempDir()
	switch email {
	case "":
	case "-":
		writeClaudeJSON(t, dir, `{"accountUuid": "u-x"}`)
	default:
		writeClaudeJSON(t, dir, fmt.Sprintf(`{"accountUuid": "u-%d", "emailAddress": %q}`, id, email))
	}
	if err := m.Store.UpsertAccount(store.Account{
		ID: id, ConfigDir: dir, Label: label, OverlayKind: "symlink",
		KeychainService: "ccp-test", KeychainAccount: "ccp-test",
	}); err != nil {
		t.Fatal(err)
	}
}

func writeClaudeJSON(t *testing.T, dir, oauthJSON string) {
	t.Helper()
	body := `{"oauthAccount": ` + oauthJSON + `}`
	if err := os.WriteFile(filepath.Join(dir, ".claude.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func renameCmd(t *testing.T, m *pool.Manager, args []string, opts renameOptions) (string, string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	err := runRename(cmd, m, args, opts)
	return stripANSI(out.String()), stripANSI(errOut.String()), err
}

func mustLabel(t *testing.T, m *pool.Manager, id int, want string) {
	t.Helper()
	a, err := m.Store.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	if a.Label != want {
		t.Fatalf("acct-%02d label = %q, want %q", id, a.Label, want)
	}
}

func TestParseAccountRef(t *testing.T) {
	cases := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"3", 3, false},
		{"03", 3, false},
		{"acct-03", 3, false},
		{"acct-3", 3, false},
		{"0", 0, true},
		{"-1", 0, true},
		{"abc", 0, true},
		{"acct-", 0, true},
		{"", 0, true},
		{"ACCT-3", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseAccountRef(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseAccountRef(%q) = %d, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("parseAccountRef(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestRunRenameManual(t *testing.T) {
	t.Run("renames and reports old name", func(t *testing.T) {
		m := renameTestManager(t)
		addRenameAccount(t, m, 5, "work@example.com", "work@example.com")
		out, _, err := renameCmd(t, m, []string{"acct-05", "Work"}, renameOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if want := "Renamed acct-05: work@example.com → Work."; !bytes.Contains([]byte(out), []byte(want)) {
			t.Fatalf("output %q missing %q", out, want)
		}
		mustLabel(t, m, 5, "Work")
	})

	t.Run("unnamed old label renders as (unnamed)", func(t *testing.T) {
		m := renameTestManager(t)
		addRenameAccount(t, m, 2, "", "")
		out, _, err := renameCmd(t, m, []string{"2", "Personal"}, renameOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if want := "Renamed acct-02: (unnamed) → Personal."; !bytes.Contains([]byte(out), []byte(want)) {
			t.Fatalf("output %q missing %q", out, want)
		}
		mustLabel(t, m, 2, "Personal")
	})

	t.Run("unknown account errors and changes nothing", func(t *testing.T) {
		m := renameTestManager(t)
		addRenameAccount(t, m, 1, "keep", "")
		if _, _, err := renameCmd(t, m, []string{"9", "X"}, renameOptions{}); err == nil {
			t.Fatal("want error for unknown account")
		}
		mustLabel(t, m, 1, "keep")
	})

	t.Run("empty new label errors", func(t *testing.T) {
		m := renameTestManager(t)
		addRenameAccount(t, m, 1, "keep", "")
		if _, _, err := renameCmd(t, m, []string{"1", ""}, renameOptions{}); err == nil {
			t.Fatal("want error for empty label")
		}
		mustLabel(t, m, 1, "keep")
	})

	t.Run("wrong arg counts error", func(t *testing.T) {
		m := renameTestManager(t)
		for _, args := range [][]string{{}, {"1"}, {"1", "a", "b"}} {
			if _, _, err := renameCmd(t, m, args, renameOptions{}); err == nil {
				t.Fatalf("args %v: want arity error", args)
			}
		}
	})

	t.Run("force without auto errors", func(t *testing.T) {
		m := renameTestManager(t)
		addRenameAccount(t, m, 1, "keep", "")
		if _, _, err := renameCmd(t, m, []string{"1", "X"}, renameOptions{force: true}); err == nil {
			t.Fatal("want error for --force without --auto")
		}
		mustLabel(t, m, 1, "keep")
	})
}

func TestRunRenameAuto(t *testing.T) {
	t.Run("derives for empty and raw-email labels, keeps custom", func(t *testing.T) {
		m := renameTestManager(t)
		addRenameAccount(t, m, 1, "", "me@aneta.company")
		addRenameAccount(t, m, 2, "x@ucsf.edu", "x@ucsf.edu")
		addRenameAccount(t, m, 3, "Keeper", "y@gmail.com")
		out, _, err := renameCmd(t, m, nil, renameOptions{auto: true})
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{
			"acct-01: (unnamed) → Aneta",
			"acct-02: x@ucsf.edu → UCSF",
			`acct-03: kept "Keeper" (use --force to overwrite)`,
		} {
			if !bytes.Contains([]byte(out), []byte(want)) {
				t.Errorf("output %q missing %q", out, want)
			}
		}
		mustLabel(t, m, 1, "Aneta")
		mustLabel(t, m, 2, "UCSF")
		mustLabel(t, m, 3, "Keeper")
	})

	t.Run("force overwrites custom labels", func(t *testing.T) {
		m := renameTestManager(t)
		addRenameAccount(t, m, 3, "Keeper", "y@gmail.com")
		if _, _, err := renameCmd(t, m, nil, renameOptions{auto: true, force: true}); err != nil {
			t.Fatal(err)
		}
		mustLabel(t, m, 3, "y")
	})

	t.Run("missing identity is skipped without failing", func(t *testing.T) {
		m := renameTestManager(t)
		addRenameAccount(t, m, 4, "old", "")
		out, _, err := renameCmd(t, m, nil, renameOptions{auto: true})
		if err != nil {
			t.Fatal(err)
		}
		if want := "acct-04: no readable identity; skipped"; !bytes.Contains([]byte(out), []byte(want)) {
			t.Fatalf("output %q missing %q", out, want)
		}
		mustLabel(t, m, 4, "old")
	})

	t.Run("identity without email is skipped", func(t *testing.T) {
		m := renameTestManager(t)
		addRenameAccount(t, m, 5, "old", "-")
		out, _, err := renameCmd(t, m, nil, renameOptions{auto: true})
		if err != nil {
			t.Fatal(err)
		}
		if want := "acct-05: no email on its login; skipped"; !bytes.Contains([]byte(out), []byte(want)) {
			t.Fatalf("output %q missing %q", out, want)
		}
		mustLabel(t, m, 5, "old")
	})

	t.Run("already-derived label is reported, not rewritten", func(t *testing.T) {
		m := renameTestManager(t)
		addRenameAccount(t, m, 6, "Aneta", "me@aneta.company")
		out, _, err := renameCmd(t, m, nil, renameOptions{auto: true})
		if err != nil {
			t.Fatal(err)
		}
		if want := "acct-06: already Aneta"; !bytes.Contains([]byte(out), []byte(want)) {
			t.Fatalf("output %q missing %q", out, want)
		}
	})

	t.Run("explicit refs touch only the named accounts", func(t *testing.T) {
		m := renameTestManager(t)
		addRenameAccount(t, m, 1, "a@aneta.company", "a@aneta.company")
		addRenameAccount(t, m, 2, "b@ucsf.edu", "b@ucsf.edu")
		if _, _, err := renameCmd(t, m, []string{"acct-01"}, renameOptions{auto: true}); err != nil {
			t.Fatal(err)
		}
		mustLabel(t, m, 1, "Aneta")
		mustLabel(t, m, 2, "b@ucsf.edu")
	})

	t.Run("unknown explicit ref fails before any rename", func(t *testing.T) {
		m := renameTestManager(t)
		addRenameAccount(t, m, 1, "a@aneta.company", "a@aneta.company")
		if _, _, err := renameCmd(t, m, []string{"1", "99"}, renameOptions{auto: true}); err == nil {
			t.Fatal("want error for unknown ref")
		}
		mustLabel(t, m, 1, "a@aneta.company")
	})

	t.Run("no accounts prints a friendly note", func(t *testing.T) {
		m := renameTestManager(t)
		_, errOut, err := renameCmd(t, m, nil, renameOptions{auto: true})
		if err != nil {
			t.Fatal(err)
		}
		if want := "No accounts yet."; !bytes.Contains([]byte(errOut), []byte(want)) {
			t.Fatalf("stderr %q missing %q", errOut, want)
		}
	})
}
