package tenantfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/tenant"
)

const (
	// ClaudeAuthorityID is cc-pool's one FuseKit source authority.
	ClaudeAuthorityID       causal.SourceAuthorityID = "com.yasyf.cc-pool.config"
	canonicalClaudeJSONRoot sourceauthority.RootID   = "canonical-json"
	sharedClaudeRoot        sourceauthority.RootID   = "shared"
	privateRootPrefix                                = "private:"
	affectedClaudeConfig    causal.LogicalKey        = "claude-config"
	claudeJSONFile                                   = ".claude.json"
	settingsFile                                     = "settings.json"
	criticalRoleClaudeJSON                           = "claude-json"
	criticalRoleSettings                             = "settings"
)

type snapshotPhase uint8

const (
	snapshotRoots snapshotPhase = iota + 1
	snapshotPhysical
	snapshotSynthetic
)

type claudeSnapshotCursor struct {
	Phase        snapshotPhase              `json:"p"`
	Offset       int                        `json:"o,omitempty"`
	Scan         sourceauthority.ScanCursor `json:"s,omitempty"`
	SettingsSeen bool                       `json:"t,omitempty"`
}

type materializationKind string

const (
	materializePhysical   materializationKind = "physical"
	materializeClaudeJSON materializationKind = criticalRoleClaudeJSON
	materializeSettings   materializationKind = criticalRoleSettings
	materializeDirectory  materializationKind = "directory"
)

type claudeMaterializationPayload struct {
	Kind     materializationKind    `json:"kind"`
	Tenant   catalog.TenantID       `json:"tenant,omitempty"`
	Root     sourceauthority.RootID `json:"root,omitempty"`
	Relative string                 `json:"relative,omitempty"`
	Name     string                 `json:"name,omitempty"`
}

// ClaudeAuthorityPolicy is cc-pool's complete Claude projection and mutation policy.
type ClaudeAuthorityPolicy struct {
	ClaudeDir      string
	ClaudeJSONPath string
}

// Roots declares the canonical shared state and one private account root per tenant.
func (p ClaudeAuthorityPolicy) Roots(_ context.Context, specs []tenant.TenantSpec) ([]sourceauthority.RootSpec, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	ordered, err := orderedClaudeTenants(specs)
	if err != nil {
		return nil, err
	}
	authority := ClaudeAuthorityID
	roots := []sourceauthority.RootSpec{
		{Authority: authority, ID: canonicalClaudeJSONRoot, Path: p.ClaudeJSONPath, Kind: sourceauthority.RootFile, Generation: 1},
		{Authority: authority, ID: sharedClaudeRoot, Path: p.ClaudeDir, Kind: sourceauthority.RootDirectory, Generation: 1},
	}
	for _, spec := range ordered {
		roots = append(roots, sourceauthority.RootSpec{
			Authority:  authority,
			ID:         privateRootID(spec.ID),
			Path:       spec.Backing.Root,
			Kind:       sourceauthority.RootDirectory,
			Generation: uint64(spec.Generation),
		})
	}
	slices.SortFunc(roots, func(left, right sourceauthority.RootSpec) int {
		return strings.Compare(string(left.ID), string(right.ID))
	})
	return roots, nil
}

// PlanSnapshot maps one bounded physical scan page into deterministic Claude projections.
func (p ClaudeAuthorityPolicy) PlanSnapshot(
	ctx context.Context,
	view sourceauthority.SnapshotView,
	cursor sourceauthority.SnapshotPlanCursor,
	limit int,
) (sourceauthority.SnapshotPlanPage, error) {
	if err := p.validate(); err != nil {
		return sourceauthority.SnapshotPlanPage{}, err
	}
	if limit <= 0 {
		return sourceauthority.SnapshotPlanPage{}, sourceauthority.ErrInvalidPlan
	}
	specs, err := orderedClaudeTenants(view.Tenants())
	if err != nil {
		return sourceauthority.SnapshotPlanPage{}, err
	}
	state, err := decodeClaudeSnapshotCursor(cursor)
	if err != nil {
		return sourceauthority.SnapshotPlanPage{}, err
	}
	for {
		switch state.Phase {
		case snapshotRoots:
			return p.planSnapshotRoots(view, cursor, specs, state, limit)
		case snapshotPhysical:
			page, next, exhausted, err := p.planSnapshotPhysical(ctx, view, specs, state, limit)
			if err != nil {
				return sourceauthority.SnapshotPlanPage{}, err
			}
			if len(page.Reads) != 0 || !exhausted {
				page.Fence = view.Fence()
				page.Next, err = encodeClaudeSnapshotCursor(next)
				return page, err
			}
			state = next
		case snapshotSynthetic:
			return p.planSnapshotSynthetic(view, specs, state, limit)
		default:
			return sourceauthority.SnapshotPlanPage{}, sourceauthority.ErrInvalidPlan
		}
	}
}

func (p ClaudeAuthorityPolicy) planSnapshotRoots(
	view sourceauthority.SnapshotView,
	cursor sourceauthority.SnapshotPlanCursor,
	specs []tenant.TenantSpec,
	state claudeSnapshotCursor,
	limit int,
) (sourceauthority.SnapshotPlanPage, error) {
	if state.Offset < 0 || state.Offset > len(specs) {
		return sourceauthority.SnapshotPlanPage{}, sourceauthority.ErrInvalidPlan
	}
	end := min(state.Offset+limit, len(specs))
	page := sourceauthority.SnapshotPlanPage{Fence: view.Fence()}
	if cursor == "" {
		page.AffectedKeys = []causal.LogicalKey{affectedClaudeConfig}
	}
	for _, spec := range specs[state.Offset:end] {
		page.Roots = append(page.Roots, tenantRoot(spec))
	}
	next := claudeSnapshotCursor{Phase: snapshotRoots, Offset: end}
	if end == len(specs) {
		next = claudeSnapshotCursor{Phase: snapshotPhysical}
	}
	encoded, err := encodeClaudeSnapshotCursor(next)
	if err != nil {
		return sourceauthority.SnapshotPlanPage{}, err
	}
	page.Next = encoded
	return page, nil
}

func (p ClaudeAuthorityPolicy) planSnapshotPhysical(
	ctx context.Context,
	view sourceauthority.SnapshotView,
	specs []tenant.TenantSpec,
	state claudeSnapshotCursor,
	limit int,
) (sourceauthority.SnapshotPlanPage, claudeSnapshotCursor, bool, error) {
	for {
		scan, err := view.Scan(ctx, state.Scan, min(limit, sourceauthority.SnapshotMaterializationLimit))
		if err != nil {
			return sourceauthority.SnapshotPlanPage{}, state, false, err
		}
		result := sourceauthority.SnapshotPlanPage{}
		for _, entry := range scan.Entries {
			if entry.Root == sharedClaudeRoot && entry.Relative == settingsFile {
				state.SettingsSeen = true
			}
			request, include, err := physicalMaterializationRequest(entry, specs)
			if err != nil {
				return sourceauthority.SnapshotPlanPage{}, state, false, err
			}
			if include {
				result.Reads = append(result.Reads, request)
			}
		}
		slices.SortFunc(result.Reads, compareMaterializationRequest)
		if scan.Next == "" {
			state.Phase = snapshotSynthetic
			state.Scan = ""
			state.Offset = 0
			return result, state, true, nil
		}
		state.Scan = scan.Next
		if len(result.Reads) != 0 {
			return result, state, false, nil
		}
	}
}

func (p ClaudeAuthorityPolicy) planSnapshotSynthetic(
	view sourceauthority.SnapshotView,
	specs []tenant.TenantSpec,
	state claudeSnapshotCursor,
	limit int,
) (sourceauthority.SnapshotPlanPage, error) {
	if !state.SettingsSeen {
		return sourceauthority.SnapshotPlanPage{}, errors.New("tenantfs: canonical settings.json is required")
	}
	requests, err := p.syntheticRequests(specs)
	if err != nil {
		return sourceauthority.SnapshotPlanPage{}, err
	}
	if state.Offset < 0 || state.Offset > len(requests) {
		return sourceauthority.SnapshotPlanPage{}, sourceauthority.ErrInvalidPlan
	}
	end := min(state.Offset+limit, len(requests))
	page := sourceauthority.SnapshotPlanPage{
		Fence: view.Fence(),
		Reads: append([]sourceauthority.MaterializationRequest(nil), requests[state.Offset:end]...),
	}
	if end != len(requests) {
		next, err := encodeClaudeSnapshotCursor(claudeSnapshotCursor{
			Phase: snapshotSynthetic, Offset: end, SettingsSeen: true,
		})
		if err != nil {
			return sourceauthority.SnapshotPlanPage{}, err
		}
		page.Next = next
	}
	return page, nil
}

// PlanDelta translates only event-related physical facts and retained logical inputs.
func (p ClaudeAuthorityPolicy) PlanDelta(
	_ context.Context,
	view sourceauthority.IndexView,
	batch sourceauthority.EventBatch,
) (sourceauthority.DeltaPlan, error) {
	if err := p.validate(); err != nil {
		return sourceauthority.DeltaPlan{}, err
	}
	specs, err := orderedClaudeTenants(view.Tenants())
	if err != nil {
		return sourceauthority.DeltaPlan{}, err
	}
	plan := sourceauthority.DeltaPlan{Fence: view.Fence()}
	reads := make(map[sourceauthority.LogicalID]sourceauthority.MaterializationRequest)
	deletes := make(map[sourceauthority.LogicalID]sourceauthority.Delete)
	affected := make(map[causal.LogicalKey]struct{})
	roots := make(map[catalog.TenantID]sourceauthority.TenantRoot)
	addRoot := func(spec tenant.TenantSpec) { roots[spec.ID] = tenantRoot(spec) }
	for _, event := range batch.Events {
		if event.Flags.RequiresSnapshot() {
			return sourceauthority.DeltaPlan{}, sourceauthority.ErrSnapshotRequired
		}
		entry, exists := view.Entry(event.Root, event.Relative)
		switch {
		case event.Root == canonicalClaudeJSONRoot:
			for _, spec := range specs {
				request, err := syntheticClaudeJSONRequest(spec)
				if err != nil {
					return sourceauthority.DeltaPlan{}, err
				}
				request.Inputs = []sourceauthority.PathRef{{Root: event.Root, Relative: event.Relative}}
				reads[request.Logical] = request
				addRoot(spec)
			}
			affected[causal.LogicalKey(claudeJSONFile)] = struct{}{}
		case event.Root == sharedClaudeRoot && topLevel(event.Relative) == settingsFile:
			if !exists || !entry.Physical.Exists {
				return sourceauthority.DeltaPlan{}, sourceauthority.ErrSnapshotRequired
			}
			for _, spec := range specs {
				request, err := syntheticSettingsRequest(spec)
				if err != nil {
					return sourceauthority.DeltaPlan{}, err
				}
				request.Inputs = []sourceauthority.PathRef{{Root: event.Root, Relative: event.Relative}}
				reads[request.Logical] = request
				addRoot(spec)
			}
			affected[causal.LogicalKey(settingsFile)] = struct{}{}
		case strings.HasPrefix(string(event.Root), privateRootPrefix) && topLevel(event.Relative) == claudeJSONFile:
			if event.Relative != claudeJSONFile || !exists || !entry.Physical.Exists {
				continue
			}
			spec, found := tenantForPrivateRoot(specs, event.Root)
			if !found {
				return sourceauthority.DeltaPlan{}, sourceauthority.ErrInvalidPlan
			}
			request, err := syntheticClaudeJSONRequest(spec)
			if err != nil {
				return sourceauthority.DeltaPlan{}, err
			}
			request.Inputs = []sourceauthority.PathRef{{Root: event.Root, Relative: event.Relative}}
			reads[request.Logical] = request
			addRoot(spec)
			affected[causal.LogicalKey(claudeJSONFile)] = struct{}{}
		default:
			request, include, err := deltaPhysicalRequest(event, entry, exists, specs)
			if err != nil {
				return sourceauthority.DeltaPlan{}, err
			}
			if !include {
				continue
			}
			if exists && entry.Physical.Exists {
				reads[request.Logical] = request
				delete(deletes, request.Logical)
			} else if _, replaced := reads[request.Logical]; !replaced {
				targets := projectionTenants(event.Root, specs)
				deletes[request.Logical] = sourceauthority.Delete{
					Logical: request.Logical,
					Tenants: tenantFences(targets),
				}
			}
			for _, spec := range projectionTenants(event.Root, specs) {
				addRoot(spec)
			}
			affected[affectedPathKey(event.Root, event.Relative)] = struct{}{}
		}
	}
	for key := range affected {
		plan.AffectedKeys = append(plan.AffectedKeys, key)
	}
	for _, root := range roots {
		plan.Roots = append(plan.Roots, root)
	}
	for _, request := range reads {
		plan.Reads = append(plan.Reads, request)
	}
	for logical, deletion := range deletes {
		if _, replaced := reads[logical]; !replaced {
			plan.Deletes = append(plan.Deletes, deletion)
		}
	}
	slices.Sort(plan.AffectedKeys)
	slices.SortFunc(plan.Roots, func(left, right sourceauthority.TenantRoot) int {
		return strings.Compare(string(left.Tenant), string(right.Tenant))
	})
	slices.SortFunc(plan.Reads, compareMaterializationRequest)
	slices.SortFunc(plan.Deletes, func(left, right sourceauthority.Delete) int {
		return strings.Compare(string(left.Logical), string(right.Logical))
	})
	return plan, nil
}

func (p ClaudeAuthorityPolicy) syntheticRequests(specs []tenant.TenantSpec) ([]sourceauthority.MaterializationRequest, error) {
	requests := make([]sourceauthority.MaterializationRequest, 0, len(specs)*(len(overlay.ExcludedEntries)+3))
	for _, spec := range specs {
		claudeJSON, err := syntheticClaudeJSONRequest(spec)
		if err != nil {
			return nil, err
		}
		requests = append(requests, claudeJSON)
		names := make([]string, 0, len(overlay.ExcludedEntries))
		for name := range overlay.ExcludedEntries {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			request, err := syntheticDirectoryRequest(spec, name, "private-directory:"+name)
			if err != nil {
				return nil, err
			}
			requests = append(requests, request)
		}
		plans, err := syntheticDirectoryRequest(spec, "plans", "plans")
		if err != nil {
			return nil, err
		}
		settings, err := syntheticSettingsRequest(spec)
		if err != nil {
			return nil, err
		}
		requests = append(requests, plans, settings)
	}
	slices.SortFunc(requests, compareMaterializationRequest)
	return requests, nil
}

func syntheticClaudeJSONRequest(spec tenant.TenantSpec) (sourceauthority.MaterializationRequest, error) {
	payload := claudeMaterializationPayload{Kind: materializeClaudeJSON, Tenant: spec.ID}
	return newMaterializationRequest(
		syntheticLogical(spec.ID, criticalRoleClaudeJSON),
		[]sourceauthority.PathRef{
			{Root: canonicalClaudeJSONRoot, Relative: "."},
			{Root: privateRootID(spec.ID), Relative: claudeJSONFile},
		},
		payload,
	)
}

func syntheticSettingsRequest(spec tenant.TenantSpec) (sourceauthority.MaterializationRequest, error) {
	payload := claudeMaterializationPayload{Kind: materializeSettings, Tenant: spec.ID}
	return newMaterializationRequest(
		syntheticLogical(spec.ID, criticalRoleSettings),
		[]sourceauthority.PathRef{{Root: sharedClaudeRoot, Relative: settingsFile}},
		payload,
	)
}

func syntheticDirectoryRequest(spec tenant.TenantSpec, name, role string) (sourceauthority.MaterializationRequest, error) {
	payload := claudeMaterializationPayload{
		Kind: materializeDirectory, Tenant: spec.ID, Name: name,
	}
	input := sourceauthority.PathRef{Root: privateRootID(spec.ID), Relative: name}
	if name == "plans" {
		input = sourceauthority.PathRef{Root: sharedClaudeRoot, Relative: name}
	}
	return newMaterializationRequest(
		syntheticLogical(spec.ID, role),
		[]sourceauthority.PathRef{input},
		payload,
	)
}

func physicalMaterializationRequest(
	entry sourceauthority.PhysicalEntry,
	specs []tenant.TenantSpec,
) (sourceauthority.MaterializationRequest, bool, error) {
	if entry.Root == canonicalClaudeJSONRoot {
		return sourceauthority.MaterializationRequest{}, false, nil
	}
	top := topLevel(entry.Relative)
	switch {
	case entry.Root == sharedClaudeRoot:
		if top == settingsFile || !overlay.SharedTopLevel(top) || (entry.Relative == "plans" && entry.Kind == sourceauthority.PhysicalDirectory) {
			return sourceauthority.MaterializationRequest{}, false, nil
		}
	case strings.HasPrefix(string(entry.Root), privateRootPrefix):
		if top == claudeJSONFile || top == settingsFile {
			return sourceauthority.MaterializationRequest{}, false, nil
		}
		if overlay.PrivateStagingEntry(top) {
			return sourceauthority.MaterializationRequest{}, false, nil
		}
		if !overlay.PrivateTopLevel(top) {
			return sourceauthority.MaterializationRequest{}, false, fmt.Errorf(
				"tenantfs: private root contains non-private entry %q", top,
			)
		}
		if overlay.ExcludedEntries[top] && entry.Relative == top && entry.Kind == sourceauthority.PhysicalDirectory {
			return sourceauthority.MaterializationRequest{}, false, nil
		}
		if _, found := tenantForPrivateRoot(specs, entry.Root); !found {
			return sourceauthority.MaterializationRequest{}, false, sourceauthority.ErrInvalidPlan
		}
	default:
		return sourceauthority.MaterializationRequest{}, false, sourceauthority.ErrInvalidPlan
	}
	payload := claudeMaterializationPayload{
		Kind: materializePhysical, Root: entry.Root, Relative: entry.Relative,
	}
	request, err := newMaterializationRequest(
		physicalLogical(entry.Root, entry.Relative),
		[]sourceauthority.PathRef{{Root: entry.Root, Relative: entry.Relative}},
		payload,
	)
	return request, err == nil, err
}

func deltaPhysicalRequest(
	event sourceauthority.PathEvent,
	entry sourceauthority.IndexedEntry,
	exists bool,
	specs []tenant.TenantSpec,
) (sourceauthority.MaterializationRequest, bool, error) {
	if exists {
		request, include, err := physicalMaterializationRequest(entry.Physical, specs)
		if err != nil || !include {
			return request, include, err
		}
		for _, logical := range entry.Logical {
			if strings.HasPrefix(string(logical), "p:") {
				request.Logical = logical
				break
			}
		}
		return request, true, nil
	}
	probe := sourceauthority.PhysicalEntry{
		Root: event.Root, Relative: event.Relative,
		Kind: sourceauthority.PhysicalFile,
	}
	request, include, err := physicalMaterializationRequest(probe, specs)
	if err != nil {
		return sourceauthority.MaterializationRequest{}, false, err
	}
	if !include {
		return sourceauthority.MaterializationRequest{}, false, nil
	}
	return request, true, nil
}

func newMaterializationRequest(
	logical sourceauthority.LogicalID,
	inputs []sourceauthority.PathRef,
	payload claudeMaterializationPayload,
) (sourceauthority.MaterializationRequest, error) {
	slices.SortFunc(inputs, func(left, right sourceauthority.PathRef) int {
		if left.Root != right.Root {
			return strings.Compare(string(left.Root), string(right.Root))
		}
		return strings.Compare(left.Relative, right.Relative)
	})
	encoded, err := json.Marshal(payload)
	if err != nil {
		return sourceauthority.MaterializationRequest{}, err
	}
	return sourceauthority.MaterializationRequest{Logical: logical, Inputs: inputs, Payload: encoded}, nil
}

func orderedClaudeTenants(specs []tenant.TenantSpec) ([]tenant.TenantSpec, error) {
	ordered := append([]tenant.TenantSpec(nil), specs...)
	slices.SortFunc(ordered, func(left, right tenant.TenantSpec) int {
		return strings.Compare(string(left.ID), string(right.ID))
	})
	for index, spec := range ordered {
		if spec.ID == "" || spec.Generation == 0 || !exactPolicyAbsolutePath(spec.Backing.Root) ||
			spec.Content.ID != string(ClaudeAuthorityID) ||
			(index > 0 && ordered[index-1].ID == spec.ID) {
			return nil, errors.New("tenantfs: Claude authority tenant fleet is invalid")
		}
	}
	return ordered, nil
}

func (p ClaudeAuthorityPolicy) validate() error {
	if !exactPolicyAbsolutePath(p.ClaudeDir) || !exactPolicyAbsolutePath(p.ClaudeJSONPath) ||
		filepath.Dir(p.ClaudeJSONPath) == p.ClaudeDir {
		return errors.New("tenantfs: Claude authority paths are invalid")
	}
	return nil
}

func tenantRoot(spec tenant.TenantSpec) sourceauthority.TenantRoot {
	return sourceauthority.TenantRoot{
		Tenant: spec.ID, Generation: spec.Generation,
		Logical: sourceauthority.LogicalID("root:" + spec.ID),
	}
}

func privateRootID(id catalog.TenantID) sourceauthority.RootID {
	return sourceauthority.RootID(privateRootPrefix + string(id))
}

func tenantForPrivateRoot(specs []tenant.TenantSpec, root sourceauthority.RootID) (tenant.TenantSpec, bool) {
	id := catalog.TenantID(strings.TrimPrefix(string(root), privateRootPrefix))
	index, found := slices.BinarySearchFunc(specs, id, func(spec tenant.TenantSpec, target catalog.TenantID) int {
		return strings.Compare(string(spec.ID), string(target))
	})
	if !found {
		return tenant.TenantSpec{}, false
	}
	return specs[index], true
}

func projectionTenants(root sourceauthority.RootID, specs []tenant.TenantSpec) []tenant.TenantSpec {
	if root == sharedClaudeRoot {
		return specs
	}
	spec, found := tenantForPrivateRoot(specs, root)
	if !found {
		return nil
	}
	return []tenant.TenantSpec{spec}
}

func tenantFences(specs []tenant.TenantSpec) []sourceauthority.TenantFence {
	result := make([]sourceauthority.TenantFence, len(specs))
	for index, spec := range specs {
		result[index] = sourceauthority.TenantFence{Tenant: spec.ID, Generation: spec.Generation}
	}
	return result
}

func physicalLogical(root sourceauthority.RootID, relative string) sourceauthority.LogicalID {
	return sourceauthority.LogicalID("p:" + string(root) + ":" + relative)
}

func syntheticLogical(tenantID catalog.TenantID, role string) sourceauthority.LogicalID {
	return sourceauthority.LogicalID("z:" + string(tenantID) + ":" + role)
}

func topLevel(relative string) string {
	if index := strings.IndexByte(relative, '/'); index >= 0 {
		return relative[:index]
	}
	return relative
}

func affectedPathKey(root sourceauthority.RootID, relative string) causal.LogicalKey {
	scope := "shared"
	if strings.HasPrefix(string(root), privateRootPrefix) {
		scope = string(root)
	}
	return causal.LogicalKey(scope + ":" + topLevel(relative))
}

func exactPolicyAbsolutePath(value string) bool {
	return value != "" && filepath.IsAbs(value) && filepath.Clean(value) == value &&
		!strings.ContainsRune(value, 0)
}

func compareMaterializationRequest(left, right sourceauthority.MaterializationRequest) int {
	return strings.Compare(string(left.Logical), string(right.Logical))
}

func decodeClaudeSnapshotCursor(cursor sourceauthority.SnapshotPlanCursor) (claudeSnapshotCursor, error) {
	if cursor == "" {
		return claudeSnapshotCursor{Phase: snapshotRoots}, nil
	}
	var value claudeSnapshotCursor
	if err := json.Unmarshal([]byte(cursor), &value); err != nil ||
		value.Phase < snapshotRoots || value.Phase > snapshotSynthetic ||
		value.Offset < 0 {
		return claudeSnapshotCursor{}, sourceauthority.ErrInvalidPlan
	}
	return value, nil
}

func encodeClaudeSnapshotCursor(cursor claudeSnapshotCursor) (sourceauthority.SnapshotPlanCursor, error) {
	if cursor.Phase == 0 {
		return "", nil
	}
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return sourceauthority.SnapshotPlanCursor(payload), nil
}

func safeProjectionLink(relative, target string) bool {
	if target == "" || len(target) > 4096 || path.IsAbs(target) || strings.ContainsRune(target, 0) {
		return false
	}
	resolved := path.Clean(path.Join(path.Dir(relative), target))
	return resolved != ".." && !strings.HasPrefix(resolved, "../")
}

var _ sourceauthority.AuthorityPolicy = ClaudeAuthorityPolicy{}
