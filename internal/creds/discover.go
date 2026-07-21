package creds

import (
	"context"
	"regexp"
)

// acctAttrRE matches the account line in security(1) attribute output: "acct"<blob>="yasyf".
var acctAttrRE = regexp.MustCompile(`"acct"<blob>="([^"]*)"`)

// DiscoverAccount returns the account (-a) label stored on the service's item by
// parsing its attribute dump (no secret read). Returns ErrNotFound if absent.
func DiscoverAccount(ctx context.Context, runner TaskRunner, service string) (string, error) {
	var out, errb boundedBuffer
	if err := runKeychainTask(
		ctx,
		runner,
		[]string{"find-generic-password", "-s", service},
		&out,
		&errb,
	); err != nil {
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
