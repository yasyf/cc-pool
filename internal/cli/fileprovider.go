package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/version"
)

// fpElection is the three-way pluginkit reading of the File Provider
// extension: not registered, registered-but-unelected, or elected.
type fpElection int

const (
	fpNotRegistered fpElection = iota // pluginkit has never seen the appex
	fpNotElected                      // registered but not elected ('-', '?', '!')
	fpElected                         // '+': fpAvailable's true
)

// classifyFPElection parses `pluginkit -m -i <id>` output (one line per copy).
// Any elected copy counts — a stale duplicate in the Trash must not mask a live election.
func classifyFPElection(out string) fpElection {
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return fpNotRegistered
	}
	for _, line := range strings.Split(trimmed, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "+") {
			return fpElected
		}
	}
	return fpNotElected
}

// fpElectionState is the pluginkit seam behind the onboarding trichotomy:
// exit 1 with no stderr is a clean "not registered"; stderr means a real failure.
var fpElectionState = func(ctx context.Context) (fpElection, error) {
	out, err := exec.CommandContext(ctx, "pluginkit", "-m", "-i", pool.FPExtensionBundleID).Output()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && len(bytes.TrimSpace(exitErr.Stderr)) == 0 {
		return fpNotRegistered, nil
	}
	if err != nil {
		return fpNotRegistered, fmt.Errorf("pluginkit -m -i %s: %w", pool.FPExtensionBundleID, err)
	}
	return classifyFPElection(string(out)), nil
}

// fpElect elects the appex headlessly (`pluginkit -e use`). It cannot clear a
// user-disabled election — only the Settings toggle can — so callers treat a
// still-unelected reading afterwards as "send the user to Settings".
var fpElect = func(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "pluginkit", "-e", "use", "-i", pool.FPExtensionBundleID).CombinedOutput()
	if err != nil {
		return fmt.Errorf("pluginkit -e use -i %s: %w (%s)", pool.FPExtensionBundleID, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// fpOpenSettings deep-links the System Settings File Providers pane — a seam
// over the backend's Enablement URLs.
var fpOpenSettings = func(ctx context.Context) error {
	return fkoverlay.BackendFileProvider.OpenSettings(ctx)
}

// fpDaemonProbe reports the daemon's liveness and its consent-pending signal —
// the precise "bridge bind parked on the group-container TCC prompt" fact only
// the daemon can observe. A seam for tests.
var fpDaemonProbe = func() (alive, consentPending bool) {
	cl := daemon.NewClient()
	if h, err := cl.Health(); err != nil || !h.OK {
		return false, false
	}
	if st, err := cl.Status(); err == nil && st.OK {
		return true, st.FPConsentPending
	}
	return true, false
}

// fpCapabilityProbe reports whether the just-launched companion app can actually
// serve File Provider on this machine, over the app's throwaway-domain probe —
// the truthful consent gate that a pluginkit election is NOT. It NEVER spawns:
// onboard already launched the app, so a dead control socket must read as
// "app coming up", never mask a stall behind a spawn. A seam so onboarding tests
// never dial a real app.
var fpCapabilityProbe = func(ctx context.Context) (bool, error) {
	return fileproviderd.NewAppClient(pool.FPControlSocketPath()).Probe(ctx)
}

// widgetAppInstalled reports whether the CCPoolStatus app bundle is present. A
// seam so onboarding tests exercise both the install and already-present paths
// without touching Homebrew.
var widgetAppInstalled = func() bool {
	fi, err := os.Stat(pool.WidgetAppPath())
	return err == nil && fi.IsDir()
}

const (
	// fpOnboardPollInterval paces the election wait and the health-rung polls.
	fpOnboardPollInterval = 500 * time.Millisecond
	// fpRungAttempts bounds each health-rung poll: the app just launched and the
	// daemon binds with retry, so a healthy stack answers within seconds — a
	// rung still down after the window is stuck, not slow.
	fpRungAttempts = 20
)

func newFPCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fp",
		Short: "Manage the File Provider overlay",
	}
	cmd.AddCommand(newFPOnboardCmd())
	cmd.AddCommand(newFPRepairCmd())
	return cmd
}

func newFPRepairCmd() *cobra.Command {
	var account int
	var retreat bool
	cmd := &cobra.Command{
		Use:   "repair",
		Short: "Re-register wedged File Provider domains (control ops answer but reads hang)",
		Long: `repair re-registers a File Provider domain whose data plane has wedged —
control ops answer but every read hangs, the failure cc-pool's control-plane
health check cannot see. Re-registration (remove + re-add) discards
fileproviderd's poisoned replica state and forces a clean re-enumeration.

Without --account it repairs every domain the daemon currently reports wedged;
with --account it repairs that one regardless of its verdict. The daemon owns
the select gate a CLI-side re-register would race, so this routes through the
daemon when it is running and falls back to a direct provider repair only when
it is down.

--retreat forces the target domain(s) back to the symlink floor instead of
re-registering — the escape hatch for a domain File Provider can never serve
here (the daemon's auto-heal now parks a wedged-but-controllable domain rather
than retreating it behind your back). Pair it with --account.

Both re-registration and the retreat break any open file descriptors on the
domain, so relaunch sessions on a repaired account.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				return runFPRepair(cmd, m, account, retreat)
			})
		},
	}
	cmd.Flags().IntVar(&account, "account", 0, "repair only this account id (default: every wedged domain)")
	cmd.Flags().BoolVar(&retreat, "retreat", false, "force the target domain(s) back to the symlink floor instead of re-registering")
	return cmd
}

// runFPRepair routes a repair through the daemon when it is up (it owns the
// select gate a CLI-side re-register would race) and directly through the
// provider when it is down.
func runFPRepair(cmd *cobra.Command, m *pool.Manager, account int, retreat bool) error {
	if err := requireInit(m); err != nil {
		return err
	}
	var acct *int
	if account > 0 {
		acct = &account
	}
	cl := daemon.NewClient()
	health, err := cl.Health()
	switch {
	case errors.Is(err, daemon.ErrDaemonUnavailable):
		// Daemon down: no select to race, so act on the provider directly.
		return repairFPDirect(cmd, m, account, retreat)
	case err != nil:
		return fmt.Errorf("daemon health check: %w", err)
	case health.Version != version.String():
		return fmt.Errorf("the daemon is %s but this ccp is %s; restart it (`brew services restart cc-pool` or `ccp service install`) and re-run", health.Version, version.String())
	}
	resp, err := cl.FPRepair(acct, retreat)
	if err != nil {
		return fmt.Errorf("fp repair: %w", err)
	}
	return renderFPRepairs(cmd, resp, account > 0)
}

// renderFPRepairs prints the daemon's per-account repair outcomes.
func renderFPRepairs(cmd *cobra.Command, resp *daemon.Response, explicit bool) error {
	out := cmd.OutOrStdout()
	if resp.Error != "" {
		return errors.New(resp.Error)
	}
	if len(resp.FPRepairs) == 0 {
		if explicit {
			return errors.New("the requested account was not repaired")
		}
		note(out, "No wedged file provider domains to repair.")
		return nil
	}
	var repaired, failed int
	for _, r := range resp.FPRepairs {
		name := fmt.Sprintf("acct-%02d (%s)", r.ID, accountName(r.Label))
		switch r.Outcome {
		case daemon.FPRepairRepaired:
			repaired++
			success(out, "%s re-registered — the daemon re-verifies it on the next probe", name)
		case daemon.FPRepairRetreated:
			note(out, "%s fell back to symlink: %s", name, r.Detail)
		case daemon.FPRepairBusy:
			step(out, "%s skipped: %s", name, r.Detail)
		case daemon.FPRepairFailed:
			failed++
			step(out, "%s %s: %s", badStyle.Render("✗"), name, r.Detail)
		}
	}
	if repaired > 0 {
		step(out, "Re-registered %d domain(s); relaunch any sessions on them.", repaired)
	}
	if failed > 0 {
		return fmt.Errorf("%d domain(s) failed to repair", failed)
	}
	return nil
}

// repairFPDirect re-registers File Provider domains without the daemon (it is
// down, so there is no select to race). It resolves the provider itself and does
// Teardown+Setup per target — the one named with --account, else every File
// Provider row. Content stays unserved until the daemon (its content bridge) is
// back, so it warns about that.
func repairFPDirect(cmd *cobra.Command, m *pool.Manager, account int, retreat bool) error {
	out := cmd.OutOrStdout()
	if retreat {
		// The File-Provider→symlink retreat deregisters the domain, breaking any
		// live session's open fds; only the daemon gates that cutover against live
		// sessions (ConvertOverlay itself does not). Refuse to retreat blind with the
		// daemon down rather than yank a domain out from under a running session.
		return errors.New("`--retreat` needs the daemon: it gates the File-Provider→symlink cutover against live sessions (whose open descriptors break on the domain removal) — start it with `ccp service install`, then re-run `ccp fp repair --retreat`")
	}
	accts, err := m.Store.ListAccounts()
	if err != nil {
		return err
	}
	targets, err := fpRepairTargets(accts, account)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		note(out, "No file provider accounts to repair.")
		return nil
	}
	warn(out, "The daemon is not running; re-registering directly. File Provider domains cannot serve content until the daemon's bridge is back — start it with `ccp service install`.")
	prov, err := fpOverlayProvider(fkoverlay.BackendFileProvider)
	if err != nil {
		return fmt.Errorf("resolve file provider overlay: %w", err)
	}
	base := pool.ClaudeDir()
	var failed int
	for _, a := range targets {
		name := fmt.Sprintf("acct-%02d (%s)", a.ID, accountName(a.Label))
		if terr := prov.Teardown(base, a.ConfigDir); terr != nil {
			// A wedged domain may refuse a clean Teardown; the idempotent Setup
			// below re-adds regardless, so note and press on.
			step(out, "%s teardown: %v (continuing to re-add)", name, terr)
		}
		switch serr := prov.Setup(base, a.ConfigDir); {
		case serr == nil:
			success(out, "%s re-registered", name)
		case errors.Is(serr, fileproviderd.ErrCannotControl):
			failed++
			step(out, "%s %s: File Provider cannot serve on this machine — run `ccp migrate --account %d --to symlink`", badStyle.Render("✗"), name, a.ID)
		default:
			failed++
			step(out, "%s %s: %v", badStyle.Render("✗"), name, serr)
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d domain(s) failed to re-register", failed)
	}
	step(out, "Re-registered %d domain(s); relaunch any sessions on them, and restart the daemon.", len(targets)-failed)
	return nil
}

// fpRepairTargets picks the accounts a direct (daemon-down) repair re-registers:
// the one named by account (error if it is not a File Provider row or is
// unknown), else every File Provider row — the daemon-down path cannot tell a
// wedged domain from a healthy one.
func fpRepairTargets(accts []store.Account, account int) ([]store.Account, error) {
	if account > 0 {
		for _, a := range accts {
			if a.ID != account {
				continue
			}
			if !fileProviderRow(a.OverlayKind) {
				return nil, fmt.Errorf("acct-%02d is on %s, not file provider", a.ID, a.OverlayKind)
			}
			return []store.Account{a}, nil
		}
		return nil, fmt.Errorf("account %d not found", account)
	}
	var fp []store.Account
	for _, a := range accts {
		if fileProviderRow(a.OverlayKind) {
			fp = append(fp, a)
		}
	}
	return fp, nil
}

func newFPOnboardCmd() *cobra.Command {
	pane := fkoverlay.BackendFileProvider.Enablement().Pane
	return &cobra.Command{
		Use:   "onboard",
		Short: "Install, enable, and adopt the File Provider overlay end to end",
		Long: `onboard walks the File Provider overlay from zero to serving: it installs
the CCPoolStatus app — the extension's host — if it is missing (an existing
install is left as-is; the version check flags a stale one), launches it,
elects the extension with pluginkit, then probes whether it can actually serve.
Election is registration, not consent, so if macOS still holds the extension
disabled it opens System Settings ▸ ` + pane + `
(the one toggle no CLI can flip) and waits until the probe passes. Once serving
it verifies the rest of the ladder — control socket → daemon bridge socket —
naming the exact fix for whichever rung is stuck, then offers to migrate
your accounts onto File Provider.

Idempotent: steps already satisfied are skipped.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return runFPOnboard(cmd) },
	}
}

func runFPOnboard(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	if err := ensureWidgetInstalled(cmd); err != nil {
		return err
	}
	step(out, "Launching it so macOS registers the extension…")
	if err := launchWidgetApp(cmd); err != nil {
		return err
	}

	// Election first (the extension must be registered + elected), then the
	// data-plane capability probe — election is registration, NOT consent, so it
	// must never be reported as "enabled".
	if err := awaitFPElection(cmd.Context(), out, fpOnboardPollInterval); err != nil {
		return err
	}
	if err := awaitFPCapability(cmd.Context(), out, fpOnboardPollInterval); err != nil {
		return err
	}

	if err := checkFPRungs(cmd.Context(), out, fpOnboardPollInterval); err != nil {
		return err
	}
	success(out, "File Provider stack healthy.")

	return offerFPMigration(cmd)
}

// ensureWidgetInstalled installs the CCPoolStatus app (the extension's host) via
// Homebrew when it is absent. An already-installed app is left as-is — skipping
// the tap add and the slow `brew install`/`brew upgrade` entirely; checkFPRungs'
// widget-version floor catches a genuinely stale copy and points at the upgrade,
// so the common (already-current) case stays fast and needs no `brew` on PATH.
func ensureWidgetInstalled(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	if widgetAppInstalled() {
		step(out, "CCPoolStatus is already installed (%s); skipping the Homebrew install.", abbreviateHome(pool.WidgetAppPath()))
		return nil
	}
	if _, err := exec.LookPath("brew"); err != nil {
		return fmt.Errorf("the CCPoolStatus app installs via Homebrew, which isn't on PATH (to build from source instead, see widget/README.md): %w", err)
	}
	if err := ensureWidgetTap(cmd); err != nil {
		return err
	}
	step(out, "Installing the CCPoolStatus app (it hosts the File Provider extension)…")
	return brewInstallWidget(cmd)
}

// awaitFPElection drives the pluginkit election to elected (a user-disabled
// election only the Settings toggle clears). Unbounded like waitForLogin; ^C cancels.
func awaitFPElection(ctx context.Context, out io.Writer, interval time.Duration) error {
	en := fkoverlay.BackendFileProvider.Enablement()
	triedElect, openedSettings := false, false
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for i := 0; ; i++ {
		state, err := fpElectionState(ctx)
		if err != nil {
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			return err
		}
		switch state {
		case fpElected:
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			return nil
		case fpNotElected:
			if !triedElect {
				triedElect = true
				if err := fpElect(ctx); err != nil {
					_, _ = fmt.Fprint(out, "\r\x1b[K")
					warn(out, "headless election failed: %v", err)
				}
				continue // re-read at once: a working `pluginkit -e use` lands synchronously
			}
			if !openedSettings {
				openedSettings = true
				_, _ = fmt.Fprint(out, "\r\x1b[K")
				step(out, "macOS is holding the extension disabled; only the Settings toggle can enable it.")
				step(out, "Flip CCPoolStatus ON under System Settings ▸ %s.", en.Pane)
				if err := fpOpenSettings(ctx); err != nil {
					warn(out, "couldn't open System Settings (%v) — navigate there yourself", err)
				}
			}
		case fpNotRegistered:
			// The just-launched app registers within seconds; nothing to do but wait.
		}
		msg := "waiting for macOS to register the extension… press ctrl-c to abort"
		if state == fpNotElected {
			msg = "waiting for the File Providers toggle… press ctrl-c to abort"
		}
		_, _ = fmt.Fprintf(out, "\r%s %s", spinnerFrames[i%len(spinnerFrames)], dimStyle.Render(msg))
		select {
		case <-ctx.Done():
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// awaitFPCapability polls the companion app's throwaway-domain probe — the
// truthful consent gate that a pluginkit election is NOT — until File Provider
// can actually serve on this machine. Election puts the extension on the list;
// only macOS's System Settings toggle grants it, and no CLI can flip it, so the
// first time the app answers "can't serve" this narrates that lever, opens the
// pane, and then spins on the probe until it passes. A dead control socket is
// treated as "app still coming up" (quiet wait), never a false consent prompt.
// Unbounded like awaitFPElection; ^C cancels.
func awaitFPCapability(ctx context.Context, out io.Writer, interval time.Duration) error {
	en := fkoverlay.BackendFileProvider.Enablement()
	explained := false
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for i := 0; ; i++ {
		ok, err := fpCapabilityProbe(ctx)
		if ok && err == nil {
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			success(out, "File Provider extension enabled and serving.")
			return nil
		}
		// Open Settings ONLY on a definitive can't-serve verdict: a clean ok=false
		// (err==nil here implies ok==false) or ErrCannotControl (extension disabled /
		// no entitlement — the one lever the Settings toggle flips). Every other error
		// is a no-verdict transient (ErrAppUnavailable, ErrBusy, ErrRegisterFailed, or
		// any other shape) → quiet-wait, never a spurious deep-link. ErrOpUnsupported
		// can't reach here (ProbeDomain-only; checkFPRungs' version floor names it).
		cantServe := err == nil || errors.Is(err, fileproviderd.ErrCannotControl)
		msg := "waiting for the CCPoolStatus app to come up… press ctrl-c to abort"
		if cantServe {
			msg = "waiting for the File Provider extension to serve… press ctrl-c to abort"
			if !explained {
				explained = true
				_, _ = fmt.Fprint(out, "\r\x1b[K")
				step(out, "The extension is registered but macOS has not cleared it to serve — election is not consent.")
				step(out, "Turn CCPoolStatus ON under System Settings ▸ %s; only that toggle grants it.", en.Pane)
				if serr := fpOpenSettings(ctx); serr != nil {
					warn(out, "couldn't open System Settings (%v) — navigate there yourself", serr)
				}
			}
		}
		_, _ = fmt.Fprintf(out, "\r%s %s", spinnerFrames[i%len(spinnerFrames)], dimStyle.Render(msg))
		select {
		case <-ctx.Done():
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// checkFPRungs walks the remaining File Provider rungs (app control socket,
// then daemon bridge socket), naming the exact fix for whichever rung stays down.
func checkFPRungs(ctx context.Context, out io.Writer, interval time.Duration) error {
	ver, err := pollFPRung(ctx, out, interval, "waiting for the CCPoolStatus control socket…", func() (string, error) {
		return fpControlHealth(ctx)
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return fmt.Errorf("the CCPoolStatus control socket %s never answered (%w) — launch %s and re-run `ccp fp onboard`",
			abbreviateHome(pool.FPControlSocketPath()), err, pool.WidgetAppPath())
	}
	if !pool.WidgetVersionSupported(ver) {
		return fmt.Errorf("the CCPoolStatus app is %s but File Provider needs %s or newer to answer the probe-domain control op the wedge detector and migrate gate rely on — run `brew upgrade --cask %s`, relaunch it, then re-run `ccp fp onboard`",
			ver, pool.MinWidgetVersion, widgetCask)
	}
	step(out, "CCPoolStatus control socket answering (%s).", ver)

	_, err = pollFPRung(ctx, out, interval, "waiting for the daemon's bridge socket…", func() (string, error) {
		if fpBridgeReachable() {
			return "", nil
		}
		return "", errors.New("not accepting")
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		alive, pending := fpDaemonProbe()
		switch {
		case !alive:
			return fmt.Errorf("the daemon isn't running, so its bridge socket %s can't come up — start it with `ccp service install`, then re-run `ccp fp onboard`",
				abbreviateHome(pool.FPBridgeSocketPath()))
		case pending:
			return errors.New("the daemon is up but its bridge bind is parked on the one-time app group container consent prompt (macOS re-asks after every upgrade, and launchd never surfaces it) — approve the prompt, then restart the daemon (`brew services restart cc-pool`) and re-run `ccp fp onboard`")
		default:
			return errors.New("the daemon is up but its bridge socket " + abbreviateHome(pool.FPBridgeSocketPath()) +
				" isn't accepting — approve the app group container consent prompt if one is pending, restart the daemon (`brew services restart cc-pool`), and re-run `ccp fp onboard`; check " + abbreviateHome(pool.LogPath()))
		}
	}
	step(out, "Daemon bridge socket up.")
	return nil
}

// pollFPRung polls probe up to fpRungAttempts under a spinner, returning the
// probe's value on the first success or its last error once the window closes.
func pollFPRung(ctx context.Context, out io.Writer, interval time.Duration, msg string, probe func() (string, error)) (string, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var lastErr error
	for i := 0; i < fpRungAttempts; i++ {
		v, err := probe()
		if err == nil {
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			return v, nil
		}
		lastErr = err
		_, _ = fmt.Fprintf(out, "\r%s %s", spinnerFrames[i%len(spinnerFrames)], dimStyle.Render(msg))
		select {
		case <-ctx.Done():
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
	_, _ = fmt.Fprint(out, "\r\x1b[K")
	return "", lastErr
}

// offerFPMigration drives `ccp migrate --to fileprovider` (the requestMigration
// shape) once the stack is green; non-TTY prints the command instead of
// prompting.
func offerFPMigration(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	if !isTTY() {
		note(out, "Migrate accounts with: ccp migrate --to fileprovider")
		return nil
	}
	migrate := true
	switch err := huh.NewConfirm().
		Title("Migrate your accounts to the File Provider overlay now?").
		Value(&migrate).
		WithTheme(ccpTheme()).
		Run(); {
	case errors.Is(err, huh.ErrUserAborted):
		note(out, "Later: ccp migrate --to fileprovider")
		return nil
	case err != nil:
		return fmt.Errorf("confirm migration: %w", err)
	case !migrate:
		note(out, "Later: ccp migrate --to fileprovider")
		return nil
	}
	return withManager(func(m *pool.Manager) error {
		if ok, err := m.Initialized(); err != nil {
			return err
		} else if !ok {
			note(out, "Pool not set up yet — run `ccp add`, then `ccp migrate --to fileprovider`.")
			return nil
		}
		resp, err := requestMigration(cmd, m, "fileprovider", 0, false)
		if err != nil {
			return err
		}
		if len(resp.Migrations) == 0 {
			if resp.Error != "" {
				return errors.New(resp.Error)
			}
			note(out, "No accounts to migrate; fileprovider is now the default for new accounts.")
			return nil
		}
		summaryErr := renderMigrationSummary(cmd, resp, "fileprovider", false)
		// Prove each freshly-migrated domain actually serves reads over the app
		// control op — the truthful close a "converted" tally alone can't give.
		renderFPServeVerdicts(cmd, m, resp)
		if names := busyMigrationNames(resp); len(names) > 0 {
			note(out, "Still busy, not migrated: %s — close those sessions, then re-run `ccp fp onboard`.", strings.Join(names, ", "))
		}
		return summaryErr
	})
}

// renderFPServeVerdicts probes each freshly-migrated File Provider domain over
// the app control op (never a materializing filesystem read) and prints a
// per-account serve verdict: the migrate gate already proved identity-bearing
// domains before flipping the row, so this is the user-facing confirmation. A
// NoVerdict (companion app busy/unreachable/too old) prints "unverified" rather
// than a false ✗.
func renderFPServeVerdicts(cmd *cobra.Command, m *pool.Manager, resp *daemon.Response) {
	out := cmd.OutOrStdout()
	for _, r := range resp.Migrations {
		if r.Outcome != daemon.MigrationDone {
			continue
		}
		a, err := m.Store.GetAccount(r.ID)
		if err != nil {
			continue
		}
		name := fmt.Sprintf("acct-%02d (%s)", r.ID, accountName(r.Label))
		switch err := fpDomainProbeAt(a.ConfigDir); {
		case err == nil, errors.Is(err, overlay.ErrFPProbeMissing), errors.Is(err, overlay.ErrFPProbeEmpty):
			success(out, "%s domain serving", name)
		case errors.Is(err, overlay.ErrFPProbeNoVerdict):
			step(out, "%s domain unverified (companion app busy or unreachable): %v", name, err)
		default:
			step(out, "%s %s domain does not serve reads: %v", badStyle.Render("✗"), name, err)
		}
	}
}

// busyMigrationNames lists the accounts a migrate skipped as busy (live session
// or reservation), for the "close those sessions, then re-run" guidance.
func busyMigrationNames(resp *daemon.Response) []string {
	var names []string
	for _, r := range resp.Migrations {
		if r.Outcome == daemon.MigrationBusy {
			names = append(names, fmt.Sprintf("acct-%02d (%s)", r.ID, accountName(r.Label)))
		}
	}
	return names
}
