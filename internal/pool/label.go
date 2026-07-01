package pool

import (
	"net"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/descope/go-free-email-providers/free"
	"golang.org/x/net/publicsuffix"
)

// consumerSupplement lists consumer mail providers newer than the HubSpot
// free-domains list backing free.IsFreeDomain.
var consumerSupplement = map[string]bool{
	"proton.me":    true,
	"pm.me":        true,
	"hey.com":      true,
	"tutanota.com": true,
	"tuta.io":      true,
	"skiff.com":    true,
}

// LabelForEmail derives a friendly default account label from a login email:
// consumer providers keep the local part minus any +tag ("yasyfm@gmail.com" →
// "yasyfm"); org domains become the registrable domain's org name
// ("rebecca.fang@ucsf.edu" → "UCSF"). Anything unparseable is returned unchanged.
func LabelForEmail(email string) string {
	at := strings.LastIndex(email, "@")
	if at <= 0 || at == len(email)-1 {
		return email
	}
	local, domain := email[:at], strings.ToLower(email[at+1:])
	// publicsuffix derives garbage like "0.1" from IP literals; bail first.
	if net.ParseIP(strings.Trim(domain, "[]")) != nil {
		return email
	}
	etld1, err := publicsuffix.EffectiveTLDPlusOne(domain)
	if err != nil {
		return email
	}
	// Matching the registrable domain (not the raw one) lets subdomained and
	// ccTLD provider variants hit their canonical list entries.
	if free.IsFreeDomain(etld1) || consumerSupplement[etld1] {
		if name, _, _ := strings.Cut(local, "+"); name != "" {
			return name
		}
		return email
	}
	base, _, _ := strings.Cut(etld1, ".")
	return orgLabel(base)
}

// orgLabel renders a registrable domain's leftmost label as an org name:
// short unhyphenated labels read as acronyms ("ucsf" → "UCSF"); everything
// else capitalizes each hyphen-separated segment ("e-corp" → "E-Corp").
func orgLabel(base string) string {
	if !strings.Contains(base, "-") && utf8.RuneCountInString(base) <= 4 {
		return strings.ToUpper(base)
	}
	segs := strings.Split(base, "-")
	for i, seg := range segs {
		segs[i] = capitalizeFirst(seg)
	}
	return strings.Join(segs, "-")
}

func capitalizeFirst(s string) string {
	r, size := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError {
		return s
	}
	return string(unicode.ToUpper(r)) + s[size:]
}
