package pool

import "testing"

func TestLabelForEmail(t *testing.T) {
	cases := []struct {
		name  string
		email string
		want  string
	}{
		{"gmail keeps local", "yasyfm@gmail.com", "yasyfm"},
		{"plus tag stripped", "user+tag@gmail.com", "user"},
		{"plus tag only falls back", "+tag@gmail.com", "+tag@gmail.com"},
		{"local case kept verbatim", "RebeccaFang_2005@yahoo.com", "RebeccaFang_2005"},
		{"consumer domain case-insensitive", "me@GMAIL.COM", "me"},
		{"dots in local kept", "a.b@googlemail.com", "a.b"},
		{"consumer ccTLD variant", "x@yahoo.co.uk", "x"},
		{"consumer ccTLD variant 2", "x@hotmail.co.uk", "x"},
		{"provider subdomain matches via eTLD+1", "x@mail.yahoo.com", "x"},
		{"outlook", "o@outlook.com", "o"},
		{"live", "l@live.com", "l"},
		{"icloud", "i@icloud.com", "i"},
		{"me dot com", "i@me.com", "i"},
		{"aol", "a@aol.com", "a"},
		{"hey", "h@hey.com", "h"},
		{"fastmail", "f@fastmail.com", "f"},
		{"gmx ccTLD", "g@gmx.de", "g"},
		{"mail dot com", "m@mail.com", "m"},
		{"yandex", "y@yandex.ru", "y"},
		{"zoho", "z@zoho.com", "z"},
		{"msn", "m@msn.com", "m"},
		{"proton", "p@proton.me", "p"},
		{"protonmail", "p@protonmail.com", "p"},
		{"pm dot me", "p@pm.me", "p"},
		{"qq", "q@qq.com", "q"},
		{"163", "n@163.com", "n"},

		{"org capitalized", "yasyf@aneta.company", "Aneta"},
		{"short org all caps", "rebecca.fang@ucsf.edu", "UCSF"},
		{"subdomain stripped to eTLD+1", "x@cs.stanford.edu", "Stanford"},
		{"deep subdomain", "x@a.b.deepmind.com", "Deepmind"},
		{"multi-part public suffix short org", "a@bar.co.uk", "BAR"},
		{"multi-part public suffix long org", "a@monzo.co.uk", "Monzo"},
		{"three-letter org all caps", "a@ibm.com", "IBM"},
		{"hyphenated org", "e@e-corp.com", "E-Corp"},
		{"hyphenated under multi-part suffix", "a@big-co.co.uk", "Big-Co"},
		{"digits long", "n@37signals.com", "37signals"},
		{"digits short all caps", "x@a16z.com", "A16Z"},
		{"uppercase org domain", "X@CS.STANFORD.EDU", "Stanford"},
		{"unlisted TLD default rule", "u@internal.corp", "Internal"},

		{"empty", "", ""},
		{"no at sign", "not-an-email", "not-an-email"},
		{"empty local", "@gmail.com", "@gmail.com"},
		{"empty domain", "user@", "user@"},
		{"single-label domain", "user@localhost", "user@localhost"},
		{"ipv4 literal", "user@127.0.0.1", "user@127.0.0.1"},
		{"bracketed ipv6 literal", "user@[2001:db8::1]", "user@[2001:db8::1]"},
		{"trailing dot", "user@example.com.", "user@example.com."},
		{"multiple at splits on last", `"a@b"@example.com`, "Example"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := LabelForEmail(tc.email); got != tc.want {
				t.Errorf("LabelForEmail(%q) = %q, want %q", tc.email, got, tc.want)
			}
		})
	}
}
