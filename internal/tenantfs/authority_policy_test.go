package tenantfs

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/tenant"
)

func TestClaudeAuthorityPolicyPlansPrivateAndCanonicalDeltasWithConstantLookups(t *testing.T) {
	specs := testPolicyTenants()
	policy := testClaudePolicy()
	private := sourceauthority.PhysicalEntry{
		Root: privateRootID(specs[0].ID), Relative: claudeJSONFile,
		Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600,
	}
	view := &testIndexView{
		fence:   testPolicyFence(),
		tenants: specs,
		entries: map[testPolicyPath]sourceauthority.IndexedEntry{
			{private.Root, private.Relative}: {
				Physical: private,
				Logical:  []sourceauthority.LogicalID{syntheticLogical(specs[0].ID, "claude-json")},
			},
		},
	}
	plan, err := policy.PlanDelta(t.Context(), view, sourceauthority.EventBatch{
		Events: []sourceauthority.PathEvent{{Root: private.Root, Relative: private.Relative, Kind: sourceauthority.EventModified}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if view.entryCalls != 1 || len(plan.Reads) != 1 ||
		plan.Reads[0].Logical != syntheticLogical(specs[0].ID, "claude-json") ||
		len(plan.Reads[0].Inputs) != 1 || plan.Reads[0].Inputs[0].Root != private.Root {
		t.Fatalf("private delta = %+v, lookups=%d", plan, view.entryCalls)
	}

	canonical := sourceauthority.PhysicalEntry{
		Root: canonicalClaudeJSONRoot, Relative: ".",
		Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600,
	}
	view.entries = map[testPolicyPath]sourceauthority.IndexedEntry{
		{canonical.Root, canonical.Relative}: {Physical: canonical},
	}
	view.entryCalls = 0
	plan, err = policy.PlanDelta(t.Context(), view, sourceauthority.EventBatch{
		Events: []sourceauthority.PathEvent{{Root: canonical.Root, Relative: canonical.Relative, Kind: sourceauthority.EventModified}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if view.entryCalls != 1 || len(plan.Reads) != len(specs) {
		t.Fatalf("canonical delta reads=%d lookups=%d", len(plan.Reads), view.entryCalls)
	}
	for _, request := range plan.Reads {
		if len(request.Inputs) != 1 || request.Inputs[0] != (sourceauthority.PathRef{Root: canonical.Root, Relative: "."}) {
			t.Fatalf("canonical request = %+v", request)
		}
	}
}

func TestClaudeAuthorityPolicyPlansSettingsDeltaWithoutRootRebuild(t *testing.T) {
	specs := testPolicyTenants()
	entry := sourceauthority.PhysicalEntry{
		Root: sharedClaudeRoot, Relative: settingsFile,
		Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600,
	}
	view := &testIndexView{
		fence: testPolicyFence(), tenants: specs,
		entries: map[testPolicyPath]sourceauthority.IndexedEntry{
			{entry.Root, entry.Relative}: {Physical: entry},
		},
	}
	plan, err := testClaudePolicy().PlanDelta(t.Context(), view, sourceauthority.EventBatch{
		Events: []sourceauthority.PathEvent{{Root: entry.Root, Relative: entry.Relative, Kind: sourceauthority.EventModified}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if view.entryCalls != 1 || len(plan.Reads) != len(specs) || len(plan.Roots) != len(specs) {
		t.Fatalf("settings plan reads=%d roots=%d lookups=%d", len(plan.Reads), len(plan.Roots), view.entryCalls)
	}
}

func TestClaudeAuthorityDeclarationDigestTracksProductConfiguration(t *testing.T) {
	policy := testClaudePolicy()
	first, err := policy.DeclarationDigest()
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := policy.DeclarationDigest()
	if err != nil {
		t.Fatal(err)
	}
	if first == ([32]byte{}) || repeated != first {
		t.Fatalf("declaration digests = %x and %x", first, repeated)
	}
	const wantDigest = "13ff2fc2f7af667a6cbc1714290d25c35c29b88a4fea56c3e04d43cfc77705c3"
	if got := hex.EncodeToString(first[:]); got != wantDigest {
		t.Fatalf("declaration digest = %s, want %s", got, wantDigest)
	}
	changed := policy
	changed.ClaudeDir = "/Users/test/.claude-next"
	second, err := changed.DeclarationDigest()
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatal("product configuration change retained the declaration digest")
	}
}

func TestClaudeAuthorityPolicySnapshotIsBoundedAndDeterministic(t *testing.T) {
	specs := testPolicyTenants()
	entries := []sourceauthority.PhysicalEntry{
		{Root: canonicalClaudeJSONRoot, Relative: ".", Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600},
		{Root: privateRootID(specs[0].ID), Relative: "daemon", Exists: true, Kind: sourceauthority.PhysicalDirectory, Mode: 0o40700},
		{Root: privateRootID(specs[0].ID), Relative: "daemon/socket", Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600},
		{Root: privateRootID(specs[0].ID), Relative: claudeJSONFile, Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600},
		{Root: privateRootID(specs[1].ID), Relative: "backups", Exists: true, Kind: sourceauthority.PhysicalDirectory, Mode: 0o40700},
		{Root: privateRootID(specs[1].ID), Relative: claudeJSONFile, Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600},
		{Root: sharedClaudeRoot, Relative: "history.jsonl", Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600},
		{Root: sharedClaudeRoot, Relative: "plans", Exists: true, Kind: sourceauthority.PhysicalDirectory, Mode: 0o40700},
		{Root: sharedClaudeRoot, Relative: "plans/one.md", Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600},
		{Root: sharedClaudeRoot, Relative: settingsFile, Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600},
	}
	view := &testSnapshotView{fence: testPolicyFence(), tenants: specs, entries: entries}
	var cursor sourceauthority.SnapshotPlanCursor
	var logicals []sourceauthority.LogicalID
	pages := 0
	for {
		page, err := testClaudePolicy().PlanSnapshot(t.Context(), view, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Reads) > 2 || len(page.Roots) > 2 {
			t.Fatalf("unbounded page = %+v", page)
		}
		for _, request := range page.Reads {
			logicals = append(logicals, request.Logical)
		}
		pages++
		if page.Next == "" {
			break
		}
		if pages > 32 {
			t.Fatal("snapshot cursor did not terminate")
		}
		cursor = page.Next
	}
	if !slices.IsSorted(logicals) || len(logicals) != len(slices.Compact(slices.Clone(logicals))) {
		t.Fatalf("snapshot logical order = %v", logicals)
	}
	if pages < 5 || view.scanCalls == 0 {
		t.Fatalf("snapshot pages=%d scanCalls=%d", pages, view.scanCalls)
	}
}

func TestClaudeAuthorityPolicySuppressesPrivateStagingFromSnapshotAndDelta(t *testing.T) {
	specs := testPolicyTenants()
	privateRoot := privateRootID(specs[0].ID)
	canonicalName := ".credentials.json"
	canonical := sourceauthority.PhysicalEntry{
		Root: privateRoot, Relative: canonicalName,
		Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600,
	}
	entries := []sourceauthority.PhysicalEntry{
		{Root: canonicalClaudeJSONRoot, Relative: ".", Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600},
		{Root: sharedClaudeRoot, Relative: settingsFile, Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600},
		canonical,
	}
	deltaEntries := map[testPolicyPath]sourceauthority.IndexedEntry{
		{canonical.Root, canonical.Relative}: {Physical: canonical},
	}
	deltaEvents := make([]sourceauthority.PathEvent, 0, len(overlay.PrivateStagingPrefixes)+1)
	for _, prefix := range overlay.PrivateStagingPrefixes {
		staging := sourceauthority.PhysicalEntry{
			Root: privateRoot, Relative: prefix + "A1B2",
			Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600,
		}
		entries = append(entries, staging)
		deltaEntries[testPolicyPath{staging.Root, staging.Relative}] = sourceauthority.IndexedEntry{Physical: staging}
		deltaEvents = append(deltaEvents, sourceauthority.PathEvent{
			Root: staging.Root, Relative: staging.Relative, Kind: sourceauthority.EventModified,
		})
	}
	deltaEvents = append(deltaEvents, sourceauthority.PathEvent{
		Root: canonical.Root, Relative: canonical.Relative, Kind: sourceauthority.EventModified,
	})

	snapshot := &testSnapshotView{fence: testPolicyFence(), tenants: specs, entries: entries}
	var cursor sourceauthority.SnapshotPlanCursor
	canonicalSeen := false
	for {
		page, err := testClaudePolicy().PlanSnapshot(t.Context(), snapshot, cursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		for _, request := range page.Reads {
			for _, input := range request.Inputs {
				if input.Root == privateRoot && overlay.PrivateStagingEntry(topLevel(input.Relative)) {
					t.Fatalf("snapshot projected private staging input %+v", input)
				}
				if input == (sourceauthority.PathRef{Root: privateRoot, Relative: canonicalName}) {
					canonicalSeen = true
				}
			}
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	if !canonicalSeen {
		t.Fatal("snapshot omitted the canonical result")
	}

	index := &testIndexView{fence: testPolicyFence(), tenants: specs, entries: deltaEntries}
	plan, err := testClaudePolicy().PlanDelta(t.Context(), index, sourceauthority.EventBatch{Events: deltaEvents})
	if err != nil {
		t.Fatal(err)
	}
	if index.entryCalls != len(deltaEvents) || len(plan.Reads) != 1 || len(plan.Deletes) != 0 ||
		len(plan.Reads[0].Inputs) != 1 ||
		plan.Reads[0].Inputs[0] != (sourceauthority.PathRef{Root: privateRoot, Relative: canonicalName}) {
		t.Fatalf("delta plan = %+v, lookups=%d", plan, index.entryCalls)
	}
}

func TestClaudeAuthorityPolicyRejectsLegacyMutationArtifactAsPrivateContent(t *testing.T) {
	specs := testPolicyTenants()
	entry := sourceauthority.PhysicalEntry{
		Root: privateRootID(specs[0].ID), Relative: ".fuse_hidden.ccpool.orphan",
		Exists: true, Kind: sourceauthority.PhysicalFile, Mode: 0o100600,
	}
	request, included, err := physicalMaterializationRequest(entry, specs)
	if err == nil || included || request.Logical != "" || len(request.Inputs) != 0 || len(request.Payload) != 0 ||
		!strings.Contains(err.Error(), "private root contains non-private entry") {
		t.Fatalf("legacy mutation artifact = request %+v included=%t err=%v, want loud private-source rejection", request, included, err)
	}
}

func TestClaudeAuthorityMaterializesMergedClaudeJSONAndInjectedSettings(t *testing.T) {
	spec := testPolicyTenants()[0]
	policy := testClaudePolicy()
	claudeTask := sourceauthority.MaterializerTask{
		Fence:   testPolicyFence(),
		Tenants: []tenant.TenantSpec{spec},
		Logical: syntheticLogical(spec.ID, "claude-json"),
		Inputs: []sourceauthority.MaterializerInput{
			testMaterializerInput(canonicalClaudeJSONRoot, ".", `{"theme":"dark","oauthAccount":{"id":"plain"}}`),
			testMaterializerInput(privateRootID(spec.ID), claudeJSONFile, `{"theme":"old","oauthAccount":{"id":"pool"}}`),
		},
	}
	request, err := syntheticClaudeJSONRequest(spec)
	if err != nil {
		t.Fatal(err)
	}
	claudeTask.Payload = request.Payload
	value, err := policy.Materialize(t.Context(), claudeTask)
	if err != nil {
		t.Fatal(err)
	}
	body := readProjection(t, value.Objects[0])
	assertFileProviderOnlyProjection(t, value.Objects[0])
	if !bytes.Contains(body, []byte(`"theme":"dark"`)) || !bytes.Contains(body, []byte(`"id":"pool"`)) {
		t.Fatalf("merged Claude JSON = %s", body)
	}

	settingsTask := sourceauthority.MaterializerTask{
		Fence:   testPolicyFence(),
		Tenants: []tenant.TenantSpec{spec},
		Logical: syntheticLogical(spec.ID, "settings"),
		Inputs:  []sourceauthority.MaterializerInput{testMaterializerInput(sharedClaudeRoot, settingsFile, `{"enabled":true}`)},
	}
	settingsRequest, err := syntheticSettingsRequest(spec)
	if err != nil {
		t.Fatal(err)
	}
	settingsTask.Payload = settingsRequest.Payload
	value, err = policy.Materialize(t.Context(), settingsTask)
	if err != nil {
		t.Fatal(err)
	}
	body = readProjection(t, value.Objects[0])
	assertFileProviderOnlyProjection(t, value.Objects[0])
	if !bytes.Contains(body, []byte(`"enabled":true`)) ||
		!bytes.Contains(body, []byte(`"plansDirectory":"/Users/test/.claude/plans"`)) {
		t.Fatalf("settings = %s", body)
	}
}

func assertFileProviderOnlyProjection(t *testing.T, projection sourceauthority.Projection) {
	t.Helper()
	if !projection.Visibility.FileProvider || projection.Visibility.Mount {
		t.Fatalf("projection visibility = %+v, want File Provider only", projection.Visibility)
	}
}

func TestClaudeAuthorityRejectsEscapingProjectedSymlink(t *testing.T) {
	spec := testPolicyTenants()[0]
	request, err := newMaterializationRequest(
		physicalLogical(sharedClaudeRoot, "plans/link"),
		[]sourceauthority.PathRef{{Root: sharedClaudeRoot, Relative: "plans/link"}},
		claudeMaterializationPayload{Kind: materializePhysical, Root: sharedClaudeRoot, Relative: "plans/link"},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = testClaudePolicy().Materialize(t.Context(), sourceauthority.MaterializerTask{
		Fence: testPolicyFence(), Tenants: []tenant.TenantSpec{spec},
		Logical: request.Logical, Payload: request.Payload,
		Inputs: []sourceauthority.MaterializerInput{{Physical: sourceauthority.PhysicalEntry{
			Root: sharedClaudeRoot, Relative: "plans/link", Exists: true,
			Kind: sourceauthority.PhysicalSymlink, Mode: 0o120777, LinkTarget: "../../outside",
		}}},
	})
	if err == nil {
		t.Fatal("Materialize accepted escaping symlink")
	}
}

func TestClaudeAuthorityPlansAtomicReplacementWithoutIdentityCanonicalization(t *testing.T) {
	spec := testPolicyTenants()[0]
	source := testMutationBinding(privateRootID(spec.ID), ".claude.json.tmp.123", sourceauthority.PhysicalFile)
	target := testMutationBinding(privateRootID(spec.ID), claudeJSONFile, sourceauthority.PhysicalFile)
	plan, err := testClaudePolicy().PlanMutation(t.Context(), sourceauthority.MutationRequest{
		Step: tenant.SourceMutationStep{
			TenantID: spec.ID, Generation: spec.Generation, Kind: catalog.MutationReplace,
			Source: catalog.SourceMutationContext{Operation: catalog.SourceMutationOperation{
				Kind: catalog.MutationReplace, Name: claudeJSONFile,
				ObjectKind: catalog.KindFile, Mode: 0o600,
			}},
		},
		Object: testMutationLocator(source),
		Parent: testMutationLocator(),
		Target: testMutationLocator(target),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Program.Actions) != 1 || plan.Program.Actions[0].Kind != sourceauthority.MutationRename ||
		plan.Program.Actions[0].From == nil ||
		plan.Program.Actions[0].From.Relative != ".claude.json.tmp.123" ||
		plan.Program.Actions[0].Path.Relative != claudeJSONFile ||
		len(plan.Effects) != 2 {
		t.Fatalf("replacement plan = %+v", plan)
	}
}

func TestClaudeAuthorityRejectsForeignMutationLocator(t *testing.T) {
	spec := testPolicyTenants()[0]
	parent := testMutationLocator()
	parent.Source.SourceAuthority = "foreign"
	_, err := testClaudePolicy().PlanMutation(t.Context(), sourceauthority.MutationRequest{
		Step: tenant.SourceMutationStep{
			TenantID: spec.ID, Generation: spec.Generation, Kind: catalog.MutationCreate,
			Source: catalog.SourceMutationContext{Operation: catalog.SourceMutationOperation{
				Kind: catalog.MutationCreate, Name: "commands",
				ObjectKind: catalog.KindDirectory, Mode: 0o700,
			}},
		},
		Parent: parent,
	})
	if !errors.Is(err, sourceauthority.ErrMutationLocator) {
		t.Fatalf("PlanMutation foreign locator error = %v, want ErrMutationLocator", err)
	}
}

type testPolicyPath struct {
	root     sourceauthority.RootID
	relative string
}

type testIndexView struct {
	fence      sourceauthority.Fence
	tenants    []tenant.TenantSpec
	entries    map[testPolicyPath]sourceauthority.IndexedEntry
	entryCalls int
}

func (v *testIndexView) Fence() sourceauthority.Fence { return v.fence }
func (*testIndexView) Roots() []sourceauthority.RootSpec {
	return nil
}

func (v *testIndexView) Tenants() []tenant.TenantSpec {
	return append([]tenant.TenantSpec(nil), v.tenants...)
}

func (v *testIndexView) Entry(root sourceauthority.RootID, relative string) (sourceauthority.IndexedEntry, bool) {
	v.entryCalls++
	entry, found := v.entries[testPolicyPath{root, relative}]
	return entry, found
}

type testSnapshotView struct {
	fence     sourceauthority.Fence
	tenants   []tenant.TenantSpec
	entries   []sourceauthority.PhysicalEntry
	scanCalls int
}

func (v *testSnapshotView) Fence() sourceauthority.Fence { return v.fence }
func (*testSnapshotView) Roots() []sourceauthority.RootSpec {
	return nil
}

func (v *testSnapshotView) Tenants() []tenant.TenantSpec {
	return append([]tenant.TenantSpec(nil), v.tenants...)
}

func (v *testSnapshotView) Scan(
	_ context.Context,
	cursor sourceauthority.ScanCursor,
	limit int,
) (sourceauthority.ScanPage, error) {
	v.scanCalls++
	start := 0
	if cursor != "" {
		value, err := strconv.Atoi(string(cursor))
		if err != nil {
			return sourceauthority.ScanPage{}, err
		}
		start = value
	}
	end := min(start+limit, len(v.entries))
	page := sourceauthority.ScanPage{
		Entries: append([]sourceauthority.PhysicalEntry(nil), v.entries[start:end]...),
	}
	if end != len(v.entries) {
		page.Next = sourceauthority.ScanCursor(strconv.Itoa(end))
	}
	return page, nil
}

type testImmutableContent []byte

func (c testImmutableContent) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(c)), nil
}

func testMaterializerInput(
	root sourceauthority.RootID,
	relative string,
	body string,
) sourceauthority.MaterializerInput {
	return sourceauthority.MaterializerInput{
		Physical: sourceauthority.PhysicalEntry{
			Root: root, Relative: relative, Exists: true,
			Kind: sourceauthority.PhysicalFile, Mode: 0o100600,
		},
		Content: testImmutableContent(body),
	}
}

func testMutationBinding(
	root sourceauthority.RootID,
	relative string,
	kind sourceauthority.PhysicalKind,
) sourceauthority.PhysicalBinding {
	return sourceauthority.PhysicalBinding{Physical: sourceauthority.PhysicalEntry{
		Root: root, Relative: relative, Exists: true, Kind: kind,
		Mode: 0o100600, Identity: sourceauthority.FileIdentity{VolumeUUID: "volume", Inode: uint64(len(relative) + 1)},
	}}
}

func testMutationLocator(bindings ...sourceauthority.PhysicalBinding) *sourceauthority.PhysicalLocator {
	return &sourceauthority.PhysicalLocator{
		Source:   catalog.SourceLocator{SourceAuthority: ClaudeAuthorityID},
		Bindings: bindings,
	}
}

func testPolicyTenants() []tenant.TenantSpec {
	return []tenant.TenantSpec{
		{
			ID: "account-0123456789abcdef0123456789abcdef", Generation: 1,
			Backing: tenant.BackingSpec{Root: "/Users/test/accounts/one"},
			Content: tenant.ContentSource{ID: string(ClaudeAuthorityID)},
		},
		{
			ID: "account-fedcba9876543210fedcba9876543210", Generation: 2,
			Backing: tenant.BackingSpec{Root: "/Users/test/accounts/two"},
			Content: tenant.ContentSource{ID: string(ClaudeAuthorityID)},
		},
	}
}

func testClaudePolicy() ClaudeAuthorityPolicy {
	return ClaudeAuthorityPolicy{
		ClaudeDir: "/Users/test/.claude", ClaudeJSONPath: "/Users/test/.claude.json",
	}
}

func testPolicyFence() sourceauthority.Fence {
	return sourceauthority.Fence{Authority: ClaudeAuthorityID}
}

func readProjection(t *testing.T, projection sourceauthority.Projection) []byte {
	t.Helper()
	source, err := projection.Content.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(source)
	settleErr := source.Settle(readErr)
	waitErr := source.Wait(t.Context())
	closeErr := projection.Content.Close()
	if err := errorsJoin(readErr, settleErr, waitErr, closeErr); err != nil {
		t.Fatal(err)
	}
	return body
}

func errorsJoin(values ...error) error {
	var messages []string
	for _, err := range values {
		if err != nil {
			messages = append(messages, err.Error())
		}
	}
	if len(messages) == 0 {
		return nil
	}
	return &testJoinedError{message: strings.Join(messages, ": ")}
}

type testJoinedError struct{ message string }

func (e *testJoinedError) Error() string { return e.message }
