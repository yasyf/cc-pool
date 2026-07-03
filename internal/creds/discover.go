package creds

import (
	"bytes"
	"os/exec"
	"regexp"
)

// acctAttrRE matches the account line in security(1) attribute output: "acct"<blob>="yasyf".
var acctAttrRE = regexp.MustCompile(`"acct"<blob>="([^"]*)"`)

// DiscoverAccount returns the account (-a) label stored on the service's item by
// parsing its attribute dump (no secret read). Returns ErrNotFound if absent.
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
	return AccountLabel(), nil
}
