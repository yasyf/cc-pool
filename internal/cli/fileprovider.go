package cli

import (
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
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/version"
)

// tryEnableFP is fusekit's headless File Provider election (`pluginkit -e use`
// plus an enablement re-check), bound to a var so onboarding tests script it
// without a real extension. A nil error means the extension is now elected and
// enabled; ErrFileProviderElectionIneffective means the election command
// succeeded yet macOS still holds the extension disabled — the System
// Settings-managed case whose only lever is the File Providers toggle. Any other
// error is a real pluginkit failure.
var tryEnableFP = fkoverlay.TryEnableFileProvider

// fpElectSettleBudget and fpElectSettleInterval bound settleFPElection's retry
// window. On a fresh install pluginkit registers the just-launched appex
// asynchronously, so the first `pluginkit -e use` can fail — or the enablement
// re-check can read still-disabled — purely because registration hasn't landed
// yet. Vars so tests shrink them; the wait is always bounded, never a spin.
var (
	fpElectSettleBudget   = 15 * time.Second
	fpElectSettleInterval = 500 * time.Millisecond
)

// settleFPElection retries the headless election until it succeeds or the
// settle budget closes, absorbing the post-launch pluginkit registration race.
// It returns nil on the first success, ctx's error on cancellation, or the LAST
// election error once the budget is spent — callers classify that final error
// (ErrFileProviderElectionIneffective → Settings guidance, anything else →
// loud), exactly as they would a single attempt's.
func settleFPElection(ctx context.Context) error {
	deadline := time.Now().Add(fpElectSettleBudget)
	for {
		err := tryEnableFP(pool.FPExtensionBundleID)
		if err == nil {
			return nil
		}
		if time.Now().Add(fpElectSettleInterval).After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(fpElectSettleInterval):
		}
	}
}

// fpOpenSettings deep-links the System Settings File Providers pane — a seam
// over the backend's Enablement URLs.
var fpOpenSettings = func(ctx context.Context) error {
	return fkoverlay.BackendFileProvider.OpenSettings(ctx)
}

// fpDaemonProbe reports the daemon's liveness, its consent-pending signal, and
// whether its File Provider data bridge is up — facts only the daemon can
// observe (it alone dials the group-container bridge). bridgeUp is nil when the
// daemon predates bridge reporting (pre-v0.49.1) or status is unavailable, so
// callers can prescribe a restart instead of misreporting a down bridge. A seam
// for tests.
var fpDaemonProbe = func() (alive, consentPending bool, bridgeUp *bool) {
	cl := daemon.NewClient()
	if h, err := cl.Health(); err != nil || !h.OK {
		return false, false, nil
	}
	if st, err := cl.Status(); err == nil && st.OK {
		return true, st.FPConsentPending, st.FPBridgeUp
	}
	return true, false, nil
}

var (
	fpDaemonHealth = func() (*daemon.Response, error) {
		return daemon.NewClient().Health()
	}
	fpBridgeCheck = func() (*daemon.Response, error) {
		return daemon.NewClient().FPBridgeCheck()
	}
	fpCapabilityNow = time.Now
)

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
	// fpOnboardPollInterval paces the capability wait and the health-rung polls.
	fpOnboardPollInterval = 500 * time.Millisecond
	// fpRungAttempts bounds each health-rung poll: the app just launched and the
	// daemon binds with retry, so a healthy stack answers within seconds — a
	// rung still down after the window is stuck, not slow.
	fpRungAttempts = 20
	// fpCapabilityStallWindow bounds no-verdict capability failures while leaving
	// the definitive System Settings lane unbounded. Sized for the worst-case
	// legitimate path (a ~30s app spawn then two ~25s probe budgets) so a slow
	// but progressing onboard is not failed early; one clock, so a lane flap
	// cannot defer the bound indefinitely.
	fpCapabilityStallWindow = 120 * time.Second
)

func newFPCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fp",
		Short: "Manage the File Provider overlay",
	}
	cmd.AddCommand(newFPOnboardCmd())
	cmd.AddCommand(newFPRepairCmd())
	cmd.AddCommand(newFPConsentCmd())
	return cmd
}

func newFPConsentCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "consent",
		Short: "Diagnose the daemon's File Provider bridge (release daemons need no consent)",
		Long: `consent reports the File Provider data bridge's status. Release daemons ship
as the signed CCPoolDaemon.app bundle (app-group entitlement + embedded
Developer ID profile), so the daemon binds its bridge prompt-free and there is no
consent to grant. This command only diagnoses — it points at ` + "`ccp doctor`" + ` and
` + "`ccp service install`" + ` when the bridge is not up.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runFPConsent(cmd)
		},
	}
}

// runFPConsent is a pure diagnostic: a release daemon binds the File Provider
// bridge prompt-free from the signed CCPoolDaemon.app bundle, so there is nothing
// to grant. It reports the daemon's bridge status and names the concrete fix
// (`ccp doctor` / `ccp service install`) when the bridge is not up.
func runFPConsent(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	alive, consentPending, bridgeUp := fpDaemonProbe()
	switch {
	case bridgeUp != nil && *bridgeUp:
		success(out, "the daemon's File Provider bridge is up — release daemons ship as the signed CCPoolDaemon.app bundle (app-group entitlement + embedded Developer ID profile), so no consent is needed")
		return nil
	case !alive:
		return errors.New("the cc-pool daemon isn't running, so its File Provider bridge can't come up — start it with `ccp service install`, then run `ccp doctor`")
	case consentPending:
		return errors.New("the daemon is up but its File Provider bridge bind is still pending — release daemons bind prompt-free from the signed CCPoolDaemon.app bundle, so a pending bind means an unbundled build; reinstall with `ccp service install`, then run `ccp doctor`")
	default:
		return errors.New("the daemon is up but its File Provider bridge isn't accepting yet — run `ccp doctor` to diagnose; if it persists, reinstall with `ccp service install`")
	}
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
daemon and refuses when it is down.

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

// runFPRepair routes a repair through the daemon, which owns the select gate a
// CLI-side re-register would race.
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
		return errors.New("the daemon isn't running; File Provider repair requires its select gate — start it with `ccp service install`, then re-run `ccp fp repair`")
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

func newFPOnboardCmd() *cobra.Command {
	pane := fkoverlay.BackendFileProvider.Enablement().Pane
	var postInstall bool
	cmd := &cobra.Command{
		Use:   "onboard",
		Short: "Install, enable, and adopt the File Provider overlay end to end",
		Long: `onboard walks the File Provider overlay from zero to serving: it installs
the CCPoolStatus app — the extension's host — if it is missing (an existing
install is left as-is; the version check flags a stale one), launches it,
elects the extension headlessly (` + "`pluginkit -e use`" + `), then probes whether it
can actually serve. Election is registration, not consent, so if macOS still
holds the extension disabled it opens System Settings ▸ ` + pane + `
(the one toggle no CLI can flip) and waits until the probe passes. Once serving
it verifies the rest of the ladder — control socket → daemon bridge socket —
naming the exact fix for whichever rung is stuck, then offers to migrate
your accounts onto File Provider.

Idempotent: steps already satisfied are skipped.

--post-install is the non-interactive mode the Homebrew formula's post_install
runs: it installs the host app (if brew is available) and elects the extension,
then stops — no daemon, no domains, no prompts, and never a nonzero exit. A
later ` + "`ccp init`" + ` picks File Provider up.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if postInstall {
				return runFPPostInstall(cmd)
			}
			return runFPOnboard(cmd)
		},
	}
	cmd.Flags().BoolVar(&postInstall, "post-install", false, "non-interactive brew post_install mode: install the host app and elect the extension, then stop (no daemon, no domains, no prompts, always exit 0)")
	return cmd
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
	if err := electFPForOnboard(cmd.Context(), out); err != nil {
		return err
	}
	if err := awaitFPCapability(cmd.Context(), out, fpOnboardPollInterval); err != nil {
		return err
	}

	if err := checkFPRungs(cmd, fpOnboardPollInterval); err != nil {
		return err
	}
	success(out, "File Provider stack healthy.")

	return offerFPMigration(cmd)
}

// electFPForOnboard drives the headless election for interactive onboarding
// through the bounded settle wait (the just-launched appex may not be
// pluginkit-registered yet), then classifies the outcome. Success says so
// briefly and returns nil so onboard proceeds to the truthful consent gate
// (awaitFPCapability). When macOS holds election behind the System Settings
// File Providers pane (ErrFileProviderElectionIneffective) it prints the loud
// manual-toggle guidance and opens the pane — the ONLY fallback — then returns
// nil: awaitFPCapability polls until the toggle actually grants the extension.
// Any other election failure fails onboard loudly.
func electFPForOnboard(ctx context.Context, out io.Writer) error {
	switch err := settleFPElection(ctx); {
	case err == nil:
		success(out, "File Provider extension enabled; verifying it serves…")
		return nil
	case errors.Is(err, fkoverlay.ErrFileProviderElectionIneffective):
		en := fkoverlay.BackendFileProvider.Enablement()
		step(out, "macOS is holding the extension disabled; only the Settings toggle can enable it.")
		step(out, "Flip CCPoolStatus ON under System Settings ▸ %s.", en.Pane)
		if serr := fpOpenSettings(ctx); serr != nil {
			warn(out, "couldn't open System Settings (%v) — navigate there yourself", serr)
		}
		return nil
	default:
		return fmt.Errorf("enable file provider extension: %w", err)
	}
}

// runFPPostInstall is the non-interactive `--post-install` mode the Homebrew
// formula's post_install runs. It is idempotent, fast, and NEVER returns an error
// (a post_install must not fail the formula): every branch prints what to do and
// exits 0. It (a) no-ops when the extension is already enabled; (b) installs the
// host app when it is missing and brew is available, else prints the manual
// command; (c) elects the appex headlessly; then STOPS — no app launch, no
// daemon, no domains, no prompts. A later `ccp init` picks File Provider up.
func runFPPostInstall(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()

	// (a) Already enabled — the extension serves, so there is nothing to install or
	// elect. This is the common brew-upgrade / re-run path; exit fast.
	if fpAvailable(pool.OverlaySpec()) {
		note(out, "File Provider extension already enabled; nothing to do.")
		return nil
	}

	// (b) Ensure the host app (the extension's bundle) is present. An already-present
	// app skips brew entirely (durable task 2805fc4); missing-and-no-brew prints the
	// manual install command and still exits 0.
	if !widgetAppInstalled() {
		if _, err := exec.LookPath("brew"); err != nil {
			note(out, "Install the CCPoolStatus app (it hosts the File Provider extension), then run `ccp fp onboard`: brew install --cask %s/%s", widgetTap, widgetCask)
			return nil
		}
		if err := ensureWidgetInstalled(cmd); err != nil {
			note(out, "Couldn't install the CCPoolStatus app automatically — run `ccp fp onboard` to finish (%v).", err)
			return nil
		}
	}

	// (c) Elect the appex headlessly through the same bounded settle wait as
	// interactive onboard (the cask postflight usually pre-registered the appex,
	// so this settles on the first attempt). Unlike interactive onboard this never
	// opens System Settings or prompts — it prints the manual command instead.
	switch err := settleFPElection(cmd.Context()); {
	case err == nil:
		note(out, "File Provider extension enabled. Run `ccp init` to start pooling on it, or `ccp fp onboard` to migrate existing accounts.")
	case errors.Is(err, fkoverlay.ErrFileProviderElectionIneffective):
		en := fkoverlay.BackendFileProvider.Enablement()
		note(out, "Enable CCPoolStatus under System Settings ▸ %s, then run `ccp fp onboard` to finish.", en.Pane)
	default:
		note(out, "Run `ccp fp onboard` to finish enabling the File Provider extension (%v).", err)
	}
	return nil
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

// awaitFPCapability polls the companion app's throwaway-domain probe — the
// truthful consent gate that a pluginkit election is NOT — until File Provider
// can actually serve on this machine. Election puts the extension on the list;
// only macOS's System Settings toggle grants it, and no CLI can flip it, so the
// first time the app answers "can't serve" this narrates that lever, opens the
// pane, and then spins on the probe until it passes. Dial refusal means the app
// is still coming up; other errors mean the app answers but its probe is broken.
// Those two no-verdict lanes are bounded; the definitive Settings lane is not.
func awaitFPCapability(ctx context.Context, out io.Writer, interval time.Duration) error {
	en := fkoverlay.BackendFileProvider.Enablement()
	explained := false
	var stallStarted time.Time
	var lastErr error
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for i := 0; ; i++ {
		ok, err := fpCapabilityProbe(ctx)
		if ok && err == nil {
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			success(out, "File Provider extension enabled and serving.")
			return nil
		}
		cantServe := err == nil || errors.Is(err, fileproviderd.ErrCannotControl)
		msg := "waiting for the CCPoolStatus app to come up… press ctrl-c to abort"
		if cantServe {
			stallStarted = time.Time{}
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
		} else {
			now := fpCapabilityNow()
			if stallStarted.IsZero() {
				stallStarted = now
			}
			lastErr = err
			dialRefused := errors.Is(err, fileproviderd.ErrAppDialRefused)
			if !dialRefused {
				msg = "app answering but capability probe failing: " + err.Error() + "… press ctrl-c to abort"
			}
			if now.Sub(stallStarted) >= fpCapabilityStallWindow {
				_, _ = fmt.Fprint(out, "\r\x1b[K")
				if dialRefused {
					return fmt.Errorf("the CCPoolStatus app did not come up within %s (last error: %w) — launch %s and re-run `ccp fp onboard`", fpCapabilityStallWindow, lastErr, pool.WidgetAppPath())
				}
				return fmt.Errorf("the CCPoolStatus app is answering but its capability probe kept failing for %s (last error: %w) — run `ccp doctor`, then re-run `ccp fp onboard`", fpCapabilityStallWindow, lastErr)
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
func checkFPRungs(cmd *cobra.Command, interval time.Duration) error {
	ctx := cmd.Context()
	out := cmd.OutOrStdout()
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
		if _, _, bridgeUp := fpDaemonProbe(); bridgeUp != nil && *bridgeUp {
			return "", nil
		}
		return "", errors.New("not accepting")
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		alive, pending, bridgeUp := fpDaemonProbe()
		switch {
		case !alive:
			return fmt.Errorf("the daemon isn't running, so its bridge socket %s can't come up — start it with `ccp service install`, then re-run `ccp fp onboard`",
				abbreviateHome(pool.FPBridgeSocketPath()))
		case pending:
			return errors.New("the daemon is up but its File Provider bridge bind is still pending its app-group-container grant; a release daemon binds prompt-free from the signed CCPoolDaemon.app bundle, so this is an unprofiled build — reinstall with `ccp service install`, then re-run `ccp fp onboard`")
		case bridgeUp == nil:
			return errors.New("the daemon is up but predates bridge-health reporting — restart it (`brew services restart cc-pool`) so the upgraded daemon takes over, then re-run `ccp fp onboard`")
		default:
			return errors.New("the daemon is up but its bridge socket " + abbreviateHome(pool.FPBridgeSocketPath()) +
				" isn't accepting — run `ccp doctor` to diagnose, then re-run `ccp fp onboard` or check " + abbreviateHome(pool.LogPath()))
		}
	}
	step(out, "Daemon bridge socket up.")

	health, err := fpDaemonHealth()
	if err != nil {
		return fmt.Errorf("daemon health check before bridge self-test: %w", err)
	}
	if health.Version != version.String() {
		return fmt.Errorf("the daemon is %s but this ccp is %s; restart it (`brew services restart cc-pool` or `ccp service install`) so it can run the bridge self-test, then re-run `ccp fp onboard`", health.Version, version.String())
	}
	resp, err := fpBridgeCheck()
	if err != nil {
		return fmt.Errorf("daemon bridge self-test: %w", err)
	}
	if strings.HasPrefix(resp.Error, "unknown op:") {
		return errors.New("the running daemon predates the bridge self-test — restart it (`brew services restart cc-pool`) so the upgraded daemon takes over, then re-run `ccp fp onboard`")
	}
	if !resp.OK {
		return fmt.Errorf("daemon bridge self-test: %s", resp.Error)
	}
	if resp.FPBridge == nil {
		return errors.New("daemon bridge self-test returned no verdict — restart the daemon (`brew services restart cc-pool`), then re-run `ccp fp onboard`")
	}
	if resp.FPBridge.Verdict != daemon.FPBridgeServing {
		if resp.FPBridge.Detail != "" {
			return errors.New(resp.FPBridge.Detail)
		}
		return fmt.Errorf("daemon bridge self-test: %s", resp.FPBridge.Verdict)
	}
	step(out, "Daemon bridge serving.")
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
