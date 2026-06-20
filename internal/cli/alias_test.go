package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDetectShell(t *testing.T) {
	cases := []struct {
		name     string
		shellEnv string
		want     shellKind
	}{
		{"bash", "/bin/bash", shellBash},
		{"zsh", "/usr/bin/zsh", shellZsh},
		{"fish homebrew path", "/opt/homebrew/bin/fish", shellFish},
		{"empty", "", shellUnknown},
		{"sh is not bash", "/bin/sh", shellUnknown},
		{"versioned basename", "bash-5.2", shellUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := detectShell(c.shellEnv); got != c.want {
				t.Errorf("detectShell(%q) = %d, want %d", c.shellEnv, got, c.want)
			}
		})
	}
}

func TestRCPath(t *testing.T) {
	t.Run("zsh", func(t *testing.T) {
		home := t.TempDir()
		got, ok := rcPath(shellZsh, home)
		if !ok || got != filepath.Join(home, ".zshrc") {
			t.Errorf("rcPath(zsh) = %q, %v", got, ok)
		}
	})
	t.Run("fish", func(t *testing.T) {
		home := t.TempDir()
		got, ok := rcPath(shellFish, home)
		if !ok || got != filepath.Join(home, ".config", "fish", "config.fish") {
			t.Errorf("rcPath(fish) = %q, %v", got, ok)
		}
	})
	t.Run("unknown", func(t *testing.T) {
		got, ok := rcPath(shellUnknown, t.TempDir())
		if ok || got != "" {
			t.Errorf("rcPath(unknown) = %q, %v; want \"\", false", got, ok)
		}
	})

	bashCases := []struct {
		name    string
		present []string // files to create under home before resolving
		want    string
	}{
		{"neither exists defaults to bash_profile", nil, ".bash_profile"},
		{"only bashrc exists", []string{".bashrc"}, ".bashrc"},
		{"only bash_profile exists", []string{".bash_profile"}, ".bash_profile"},
		{"both exist, profile wins", []string{".bash_profile", ".bashrc"}, ".bash_profile"},
	}
	for _, c := range bashCases {
		t.Run("bash "+c.name, func(t *testing.T) {
			home := t.TempDir()
			for _, f := range c.present {
				if err := os.WriteFile(filepath.Join(home, f), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			got, ok := rcPath(shellBash, home)
			if !ok || got != filepath.Join(home, c.want) {
				t.Errorf("rcPath(bash) = %q, %v; want %q", got, ok, c.want)
			}
		})
	}
}

func TestAliasLine(t *testing.T) {
	const posix = `alias claude='ccp run'`
	const fish = `alias claude 'ccp run'`
	cases := []struct {
		name string
		kind shellKind
		want string
	}{
		{"bash", shellBash, posix},
		{"zsh", shellZsh, posix},
		{"fish", shellFish, fish},
		{"unknown", shellUnknown, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := aliasLine(c.kind); got != c.want {
				t.Errorf("aliasLine(%s) = %q, want %q", c.name, got, c.want)
			}
		})
	}
}

func TestAliasInstalled(t *testing.T) {
	cases := []struct {
		name    string
		content string // "" means: do not create the file
		write   bool
		want    bool
	}{
		{"missing file", "", false, false},
		{"our marker", aliasMarker + "\nalias claude='x'\n", true, true},
		{"user posix alias", "alias claude='mine'\n", true, true},
		{"user fish alias", "alias claude 'mine'\n", true, true},
		{"user function keyword", "function claude\n  echo hi\nend\n", true, true},
		{"user posix function", "claude() {\n  echo hi\n}\n", true, true},
		{"commented alias does not count", "# alias claude='x'\n", true, false},
		{"different alias name", "alias claudex='x'\nalias claude-foo='y'\n", true, false},
		{"unrelated content", "export FOO=bar\n", true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), ".zshrc")
			if c.write {
				if err := os.WriteFile(path, []byte(c.content), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := aliasInstalled(path)
			if err != nil {
				t.Fatalf("aliasInstalled: %v", err)
			}
			if got != c.want {
				t.Errorf("aliasInstalled(%q) = %v, want %v", c.content, got, c.want)
			}
		})
	}
}

func TestAppendAlias(t *testing.T) {
	t.Run("fresh zsh writes the block", func(t *testing.T) {
		home := t.TempDir()
		res, err := appendAlias(shellZsh, home)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Wrote || res.AlreadyPresent {
			t.Fatalf("result = %+v, want Wrote", res)
		}
		data, err := os.ReadFile(filepath.Join(home, ".zshrc")) //nolint:gosec // G304: home is the test's own t.TempDir()
		if err != nil {
			t.Fatal(err)
		}
		want := "\n" + aliasMarker + "\n" + aliasLine(shellZsh) + "\n"
		if string(data) != want {
			t.Errorf("file = %q, want %q", data, want)
		}
	})

	t.Run("second append is a no-op", func(t *testing.T) {
		home := t.TempDir()
		if _, err := appendAlias(shellZsh, home); err != nil {
			t.Fatal(err)
		}
		first, err := os.ReadFile(filepath.Join(home, ".zshrc")) //nolint:gosec // G304: home/path is under the test's own t.TempDir()
		if err != nil {
			t.Fatal(err)
		}
		res, err := appendAlias(shellZsh, home)
		if err != nil {
			t.Fatal(err)
		}
		if res.Wrote || !res.AlreadyPresent {
			t.Fatalf("result = %+v, want AlreadyPresent", res)
		}
		second, err := os.ReadFile(filepath.Join(home, ".zshrc")) //nolint:gosec // G304: home/path is under the test's own t.TempDir()
		if err != nil {
			t.Fatal(err)
		}
		if string(first) != string(second) {
			t.Errorf("file changed on re-append:\n%q\n%q", first, second)
		}
	})

	t.Run("preserves prior content", func(t *testing.T) {
		home := t.TempDir()
		prior := "export FOO=bar\n"
		path := filepath.Join(home, ".zshrc")
		if err := os.WriteFile(path, []byte(prior), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := appendAlias(shellZsh, home); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path) //nolint:gosec // G304: home/path is under the test's own t.TempDir()
		if err != nil {
			t.Fatal(err)
		}
		if got := string(data); !strings.HasPrefix(got, prior) {
			t.Errorf("prior content not preserved: %q", got)
		}
		if want := aliasLine(shellZsh); !strings.Contains(string(data), want) {
			t.Errorf("alias line %q not appended; got %q", want, data)
		}
	})

	t.Run("fish creates the config dir", func(t *testing.T) {
		home := t.TempDir()
		res, err := appendAlias(shellFish, home)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Wrote {
			t.Fatalf("result = %+v, want Wrote", res)
		}
		info, err := os.Stat(filepath.Join(home, ".config", "fish"))
		if err != nil || !info.IsDir() {
			t.Fatalf("fish config dir not created: %v", err)
		}
		data, err := os.ReadFile(filepath.Join(home, ".config", "fish", "config.fish")) //nolint:gosec // G304: path is a cc-pool-managed/test-owned file, not external input
		if err != nil {
			t.Fatal(err)
		}
		if want := aliasLine(shellFish); !strings.Contains(string(data), want) {
			t.Errorf("fish alias %q not written; got %q", want, data)
		}
	})

	t.Run("does not clobber a user-defined claude", func(t *testing.T) {
		home := t.TempDir()
		path := filepath.Join(home, ".zshrc")
		prior := "alias claude='mine'\n"
		if err := os.WriteFile(path, []byte(prior), 0o600); err != nil {
			t.Fatal(err)
		}
		res, err := appendAlias(shellZsh, home)
		if err != nil {
			t.Fatal(err)
		}
		if res.Wrote || !res.AlreadyPresent {
			t.Fatalf("result = %+v, want AlreadyPresent", res)
		}
		data, err := os.ReadFile(path) //nolint:gosec // G304: path is a cc-pool-managed/test-owned file, not external input
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != prior {
			t.Errorf("file changed: %q", data)
		}
	})

	t.Run("bash routes to the existing bashrc", func(t *testing.T) {
		home := t.TempDir()
		if err := os.WriteFile(filepath.Join(home, ".bashrc"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		res, err := appendAlias(shellBash, home)
		if err != nil {
			t.Fatal(err)
		}
		if res.Path != filepath.Join(home, ".bashrc") {
			t.Errorf("wrote to %q, want .bashrc", res.Path)
		}
		if fileExists(filepath.Join(home, ".bash_profile")) {
			t.Error("unexpectedly created .bash_profile")
		}
	})
}
