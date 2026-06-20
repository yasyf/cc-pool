package keychain

import (
	"bytes"
	"os/exec"
	"regexp"
)

// acctAttrRE matches the account attribute line in `security
// find-generic-password` attribute output:  "acct"<blob>="yasyf"
var acctAttrRE = regexp.MustCompile(`"acct"<blob>="([^"]*)"`)

// DiscoverAccount returns the Keychain account (-a) label actually stored on
// the item for the given service, by parsing the item's attribute dump (no
// secret is read). This is more robust than recomputing the label, because it
// reflects exactly what Claude wrote at /login time. Returns ErrNotFound if no
// item exists for the service.
func DiscoverAccount(service string) (string, error) {
	//nolint:gosec // G204: securityBin is the fixed /usr/bin/security path; service is a cc-pool-derived keychain service name
	cmd := exec.Command(securityBin, "find-generic-password", "-s", service)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		if isNotFound(errb.String()) {
			return "", ErrNotFound
		}
		return "", err
	}
	if m := acctAttrRE.FindStringSubmatch(out.String()); m != nil {
		return m[1], nil
	}
	// Fall back to the computed label if the attribute is absent.
	return AccountLabel(), nil
}
