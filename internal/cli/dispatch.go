package cli

var helpVersionFlags = map[string]bool{
	"-h": true, "--help": true,
	"-v": true, "--version": true,
}

// InjectRun rewrites `ccp <claude-flag> ...` into `ccp run <claude-flag> ...`;
// ccp's own help/version flags and non-flag args pass through unchanged.
func InjectRun(args []string) []string {
	if len(args) == 0 {
		return args
	}
	first := args[0]
	if first == "" || first[0] != '-' || helpVersionFlags[first] {
		return args
	}
	return append([]string{"run"}, args...)
}
