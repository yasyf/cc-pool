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
// ("rebecca.fang@ucsf.edu" → "UCSF"). Whole claude tokens are removed from the
// local part, with remaining org tokens appended as a suffix. Anything
// unparseable is returned unchanged.
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
	consumer := free.IsFreeDomain(etld1) || consumerSupplement[etld1]
	base, _, _ := strings.Cut(etld1, ".")
	local, _, _ = strings.Cut(local, "+")
	cleaned, foundClaude := dropClaudeTokens(local)
	if !foundClaude {
		if consumer {
			if local != "" {
				return local
			}
			return email
		}
		return orgLabel(base)
	}
	if consumer {
		if cleaned != "" {
			return cleaned
		}
		return local
	}
	return orgLabelWithSuffix(base, local)
}

type labelToken struct {
	value     string
	separator byte
}

func dropClaudeTokens(local string) (string, bool) {
	tokens := tokenizeLabel(local)
	found := false
	for i := 0; i < len(tokens); {
		if !strings.EqualFold(tokens[i].value, "claude") {
			i++
			continue
		}
		found = true
		if i == 0 && len(tokens) > 1 {
			tokens[1].separator = 0
		}
		tokens = append(tokens[:i], tokens[i+1:]...)
	}
	if !found {
		return local, false
	}

	var cleaned strings.Builder
	for _, token := range tokens {
		if token.separator != 0 {
			cleaned.WriteByte(token.separator)
		}
		cleaned.WriteString(token.value)
	}
	return cleaned.String(), true
}

// orgLabelWithSuffix appends the local part's distinguishing tokens to the org
// label; tokens matching any hyphen-segment of base merge into it (a hyphenated
// base would otherwise reappear segment by segment: e-corp-claude-10@e-corp.com
// must be E-Corp-10, not E-Corp-E-Corp-10).
func orgLabelWithSuffix(base, local string) string {
	baseSegs := strings.Split(base, "-")
	suffix := make([]string, 0)
	for _, token := range tokenizeLabel(local) {
		if token.value == "" || strings.EqualFold(token.value, "claude") || matchesSegment(baseSegs, token.value) {
			continue
		}
		suffix = append(suffix, capitalizeFirst(token.value))
	}
	label := orgLabel(base)
	if len(suffix) == 0 {
		return label
	}
	return label + "-" + strings.Join(suffix, "-")
}

func matchesSegment(segs []string, tok string) bool {
	for _, s := range segs {
		if strings.EqualFold(s, tok) {
			return true
		}
	}
	return false
}

func tokenizeLabel(local string) []labelToken {
	tokens := make([]labelToken, 0, 1)
	start := 0
	var separator byte
	for i := 0; i < len(local); i++ {
		if local[i] != '-' && local[i] != '.' && local[i] != '_' {
			continue
		}
		tokens = append(tokens, labelToken{value: local[start:i], separator: separator})
		separator = local[i]
		start = i + 1
	}
	return append(tokens, labelToken{value: local[start:], separator: separator})
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
