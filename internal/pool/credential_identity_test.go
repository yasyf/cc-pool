package pool

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
)

const credentialBoundaryTestInstance = "11223344556677889900aabbccddeeff"

func TestCredentialBoundaryRejectsInvalidExecutionIdentityBeforeKeychain(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *store.Account, string)
	}{
		{
			name: "public-path-as-config",
			mutate: func(_ *testing.T, account *store.Account, publicPath string) {
				account.ConfigDir = publicPath
				account.KeychainService = creds.ServiceName(publicPath)
			},
		},
		{
			name: "wrong-service",
			mutate: func(_ *testing.T, account *store.Account, _ string) {
				account.KeychainService = "foreign-service"
			},
		},
		{
			name: "wrong-leaf",
			mutate: func(t *testing.T, account *store.Account, _ string) {
				t.Helper()
				if err := os.Remove(account.ConfigDir); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(account.ConfigDir, 0o700); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "relative-link",
			mutate: func(t *testing.T, account *store.Account, _ string) {
				t.Helper()
				if err := os.Remove(account.ConfigDir); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("relative-target", account.ConfigDir); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "replaced-link",
			mutate: func(t *testing.T, account *store.Account, publicPath string) {
				t.Helper()
				foreign := filepath.Join(filepath.Dir(publicPath), "Foreign")
				if err := os.Mkdir(foreign, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(account.ConfigDir); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(foreign, account.ConfigDir); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "nested-public-link",
			mutate: func(t *testing.T, account *store.Account, publicPath string) {
				t.Helper()
				realTarget := filepath.Join(filepath.Dir(publicPath), "Real")
				nested := filepath.Join(filepath.Dir(publicPath), "Nested")
				if err := os.Mkdir(realTarget, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(realTarget, nested); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(account.ConfigDir); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(nested, account.ConfigDir); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "world-writable-target",
			mutate: func(t *testing.T, _ *store.Account, publicPath string) {
				t.Helper()
				if err := os.Chmod(publicPath, 0o777); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			account, publicPath, fake := credentialBoundaryFixture(t)
			test.mutate(t, &account, publicPath)
			manager := &Manager{Creds: fake}
			if _, err := manager.DiscoverCredentialAccount(t.Context(), account, publicPath); err == nil {
				t.Fatal("invalid identity reached Keychain discovery")
			}
			if _, _, err := manager.ReadCredentialAt(t.Context(), account, publicPath); err == nil {
				t.Fatal("invalid identity reached Keychain read")
			}
			if touched := fake.TouchedServices(); len(touched) != 0 {
				t.Fatalf("invalid identity touched Keychain services: %v", touched)
			}
		})
	}
}

func TestCredentialBoundaryRequiresExactExpectedPublicPath(t *testing.T) {
	account, publicPath, fake := credentialBoundaryFixture(t)
	foreign := filepath.Join(filepath.Dir(publicPath), "Foreign")
	if err := os.Mkdir(foreign, 0o700); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{Creds: fake}
	if _, err := manager.DiscoverCredentialAccount(t.Context(), account, foreign); err == nil {
		t.Fatal("foreign expected path reached Keychain discovery")
	}
	if touched := fake.TouchedServices(); len(touched) != 0 {
		t.Fatalf("foreign expected path touched Keychain services: %v", touched)
	}
}

func credentialBoundaryFixture(t *testing.T) (store.Account, string, *credstest.Fake) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	publicPath := filepath.Join(home, "Library", "CloudStorage", "CCPool", "Account")
	if err := os.MkdirAll(publicPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := EnsureAccountConfigDir(credentialBoundaryTestInstance, publicPath); err != nil {
		t.Fatal(err)
	}
	configDir, err := AccountConfigDir(credentialBoundaryTestInstance)
	if err != nil {
		t.Fatal(err)
	}
	service, err := AccountKeychainService(credentialBoundaryTestInstance)
	if err != nil {
		t.Fatal(err)
	}
	return store.Account{
		ID: 1, InstanceID: credentialBoundaryTestInstance, Generation: 1,
		ConfigDir: configDir, KeychainService: service, KeychainAccount: "claude",
	}, publicPath, credstest.NewFake()
}

func TestCredentialBoundaryWrongOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("changing target ownership requires root")
	}
	account, publicPath, fake := credentialBoundaryFixture(t)
	if err := os.Chown(publicPath, 1, -1); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{Creds: fake}
	_, err := manager.DiscoverCredentialAccount(t.Context(), account, publicPath)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("wrong-owner target validation = %v", err)
	}
	if touched := fake.TouchedServices(); len(touched) != 0 {
		t.Fatalf("wrong-owner target touched Keychain services: %v", touched)
	}
}
