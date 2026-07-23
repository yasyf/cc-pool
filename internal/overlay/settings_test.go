package overlay

import (
	"bytes"
	"encoding/json"
	"testing"
)

// plansDir is the value the materializer injects and strips by value-equality.
const plansDir = "/Users/test/.claude/plans"

func TestInjectPlansDirectory(t *testing.T) {
	cases := map[string]struct {
		base string
		// want pins per-key raw JSON values in the served output.
		want map[string]string
		// wantUnchanged asserts served is byte-identical to base (no re-marshal).
		wantUnchanged bool
		wantErr       bool
	}{
		"inject when absent": {
			base: `{"theme":"dark"}`,
			want: map[string]string{
				"theme":           `"dark"`,
				plansDirectoryKey: `"` + plansDir + `"`,
			},
		},
		"inject preserves other keys": {
			base: `{"theme":"dark","model":"opus","verbose":true,"nested":{"a":[1,2,3]}}`,
			want: map[string]string{
				"theme":           `"dark"`,
				"model":           `"opus"`,
				"verbose":         `true`,
				"nested":          `{"a":[1,2,3]}`,
				plansDirectoryKey: `"` + plansDir + `"`,
			},
		},
		"inject into empty object": {
			base: `{}`,
			want: map[string]string{plansDirectoryKey: `"` + plansDir + `"`},
		},
		"respect user override (different value) returns base byte-identical": {
			base:          `{"plansDirectory":"/some/other/dir","theme":"dark"}`,
			wantUnchanged: true,
		},
		"respect override equal to ours returns base byte-identical": {
			base:          `{"plansDirectory":"` + plansDir + `","theme":"dark"}`,
			wantUnchanged: true,
		},
		"user override null value still counts as present, base unchanged": {
			base:          `{"plansDirectory":null,"theme":"dark"}`,
			wantUnchanged: true,
		},
		"unparseable base errors":       {base: `{not json`, wantErr: true},
		"non-object base null errors":   {base: `null`, wantErr: true},
		"non-object base array errors":  {base: `[1,2,3]`, wantErr: true},
		"non-object base number errors": {base: `42`, wantErr: true},
		"non-object base string errors": {base: `"hi"`, wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			served, err := injectPlansDirectory([]byte(tc.base), plansDir)
			if tc.wantErr {
				if err == nil {
					t.Fatal("want error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantUnchanged {
				if !bytes.Equal(served, []byte(tc.base)) {
					t.Fatalf("served = %q, want base byte-identical %q", served, tc.base)
				}
				return
			}
			got := raw(t, served)
			for k, want := range tc.want {
				if string(got[k]) != want {
					t.Errorf("served[%q] = %s, want %s", k, got[k], want)
				}
			}
		})
	}
}

func TestStripInjectedPlansDirectory(t *testing.T) {
	cases := map[string]struct {
		committed string
		// wantAbsent asserts plansDirectory is gone from the output.
		wantAbsent bool
		// want pins surviving keys when wantAbsent.
		want map[string]string
		// wantUnchanged asserts output is byte-identical to committed.
		wantUnchanged bool
		wantErr       bool
	}{
		"strip our injected value, other keys survive": {
			committed:  `{"plansDirectory":"` + plansDir + `","theme":"dark","model":"opus"}`,
			wantAbsent: true,
			want:       map[string]string{"theme": `"dark"`, "model": `"opus"`},
		},
		"strip our value normalized from a pretty committed doc": {
			committed:  "{\n  \"plansDirectory\": \"" + plansDir + "\",\n  \"theme\": \"dark\"\n}",
			wantAbsent: true,
			want:       map[string]string{"theme": `"dark"`},
		},
		"different value left untouched, base byte-identical": {
			committed:     `{"plansDirectory":"/some/other/dir","theme":"dark"}`,
			wantUnchanged: true,
		},
		"absent key is a no-op, base byte-identical": {
			committed:     `{"theme":"dark","model":"opus"}`,
			wantUnchanged: true,
		},
		"null value (not ours) left untouched, base byte-identical": {
			committed:     `{"plansDirectory":null,"theme":"dark"}`,
			wantUnchanged: true,
		},
		"unparseable committed errors":      {committed: `{not json`, wantErr: true},
		"non-object committed null errors":  {committed: `null`, wantErr: true},
		"non-object committed array errors": {committed: `[1,2]`, wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			newBase, err := stripInjectedPlansDirectory([]byte(tc.committed), plansDir)
			if tc.wantErr {
				if err == nil {
					t.Fatal("want error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantUnchanged {
				if !bytes.Equal(newBase, []byte(tc.committed)) {
					t.Fatalf("newBase = %q, want committed byte-identical %q", newBase, tc.committed)
				}
				return
			}
			got := raw(t, newBase)
			if tc.wantAbsent {
				if v, ok := got[plansDirectoryKey]; ok {
					t.Errorf("newBase[%q] = %s, want absent", plansDirectoryKey, v)
				}
			}
			for k, want := range tc.want {
				if string(got[k]) != want {
					t.Errorf("newBase[%q] = %s, want %s", k, got[k], want)
				}
			}
		})
	}
}

// TestInjectStripRoundTrip pins strip(inject(base)) == base byte-for-byte. Bases
// must be json.Marshal canonical form (key-sorted, compact): both helpers emit
// that form, so a non-canonical base would re-flow on inject and never round-trip.
func TestInjectStripRoundTrip(t *testing.T) {
	bases := map[string]string{
		"empty object":     `{}`,
		"single key":       `{"theme":"dark"}`,
		"multiple sorted":  `{"model":"opus","theme":"dark","verbose":true}`,
		"nested composite": `{"hooks":{"PreToolUse":[]},"permissions":{"allow":["Bash"]},"theme":"dark"}`,
	}
	for name, base := range bases {
		t.Run(name, func(t *testing.T) {
			served, err := injectPlansDirectory([]byte(base), plansDir)
			if err != nil {
				t.Fatal(err)
			}
			got := raw(t, served)
			if string(got[plansDirectoryKey]) != `"`+plansDir+`"` {
				t.Fatalf("served[%q] = %s, want our injected value", plansDirectoryKey, got[plansDirectoryKey])
			}
			newBase, err := stripInjectedPlansDirectory(served, plansDir)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(newBase, []byte(base)) {
				t.Fatalf("strip(inject(base)) = %q, want base %q", newBase, base)
			}
		})
	}
}

// TestInjectStripNestedKeysUntouched pins that neither helper re-flows nested
// values: the nested keys are deliberately UNSORTED, so a top-level re-marshal
// that reordered them would be caught.
func TestInjectStripNestedKeysUntouched(t *testing.T) {
	const nested = `{"zKey":1,"aKey":2,"mKey":{"inner2":"b","inner1":"a"}}`
	base := `{"theme":"dark","permissions":` + nested + `}`

	served, err := injectPlansDirectory([]byte(base), plansDir)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw(t, served)["permissions"]) != nested {
		t.Errorf("served permissions = %s, want nested bytes untouched %s", raw(t, served)["permissions"], nested)
	}

	newBase, err := stripInjectedPlansDirectory(served, plansDir)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw(t, newBase)["permissions"]) != nested {
		t.Errorf("stripped permissions = %s, want nested bytes untouched %s", raw(t, newBase)["permissions"], nested)
	}
}

// TestInjectPlansDirectoryDeterministic pins byte-determinism: two injections of
// the same input must be byte-equal (json.Marshal key-sorts maps). Catalog size
// metadata and immutable content snapshots depend on it.
func TestInjectPlansDirectoryDeterministic(t *testing.T) {
	base := []byte(`{"theme":"dark","model":"opus","verbose":true}`)
	a, err := injectPlansDirectory(base, plansDir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := injectPlansDirectory(base, plansDir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("two injections diverged:\n%s\n%s", a, b)
	}
	var got string
	if err := json.Unmarshal(raw(t, a)[plansDirectoryKey], &got); err != nil {
		t.Fatal(err)
	}
	if got != plansDir {
		t.Fatalf("injected value = %q, want %q", got, plansDir)
	}
}
