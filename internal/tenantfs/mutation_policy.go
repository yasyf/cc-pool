package tenantfs

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/sourceauthority"
)

// PlanMutation maps one catalog semantic mutation to closed physical primitives.
func (p ClaudeAuthorityPolicy) PlanMutation(
	_ context.Context,
	request sourceauthority.MutationRequest,
) (sourceauthority.MutationPlan, error) {
	if err := p.validate(); err != nil {
		return sourceauthority.MutationPlan{}, err
	}
	for _, locator := range []*sourceauthority.PhysicalLocator{request.Object, request.Parent, request.Target} {
		if err := validateMutationLocator(locator); err != nil {
			return sourceauthority.MutationPlan{}, err
		}
	}
	if request.Step.Source.Operation.Kind != request.Step.Kind {
		return sourceauthority.MutationPlan{}, sourceauthority.ErrInvalidPlan
	}
	operation := request.Step.Source.Operation
	if !safeMutationName(operation.Name) || operation.Mode&^uint32(0o777) != 0 || operation.Mode&0o777 == 0 {
		return sourceauthority.MutationPlan{}, sourceauthority.ErrInvalidPlan
	}
	switch request.Step.Kind {
	case catalog.MutationCreate:
		return p.planCreateMutation(request)
	case catalog.MutationRevise:
		return p.planReviseMutation(request)
	case catalog.MutationDelete:
		return p.planDeleteMutation(request)
	case catalog.MutationReplace:
		return p.planReplaceMutation(request)
	default:
		return sourceauthority.MutationPlan{}, sourceauthority.ErrInvalidPlan
	}
}

func (p ClaudeAuthorityPolicy) planCreateMutation(
	request sourceauthority.MutationRequest,
) (sourceauthority.MutationPlan, error) {
	if request.Parent == nil || request.Object != nil || request.Target != nil {
		return sourceauthority.MutationPlan{}, sourceauthority.ErrInvalidPlan
	}
	operation := request.Step.Source.Operation
	target, err := mutationDestination(request.Step.TenantID, operation.Name, request.Parent)
	if err != nil {
		return sourceauthority.MutationPlan{}, err
	}
	action, err := createMutationAction(target, operation)
	if err != nil {
		return sourceauthority.MutationPlan{}, err
	}
	return sourceauthority.MutationPlan{
		Program: sourceauthority.MutationProgram{Actions: []sourceauthority.MutationAction{action}},
		Effects: []sourceauthority.ExpectedEffect{{
			Path: target, Outcome: sourceauthority.MutationPresent,
			Kind: physicalKind(operation.ObjectKind),
		}},
	}, nil
}

func (p ClaudeAuthorityPolicy) planReviseMutation(
	request sourceauthority.MutationRequest,
) (sourceauthority.MutationPlan, error) {
	if request.Object == nil || request.Parent == nil || request.Target != nil {
		return sourceauthority.MutationPlan{}, sourceauthority.ErrInvalidPlan
	}
	operation := request.Step.Source.Operation
	current, before, err := mutationObjectPath(request.Step.TenantID, operation.Name, request.Object)
	if err != nil {
		return sourceauthority.MutationPlan{}, err
	}
	target, err := mutationDestination(request.Step.TenantID, operation.Name, request.Parent)
	if err != nil {
		return sourceauthority.MutationPlan{}, err
	}
	if current != target {
		if operation.HasContent || before.Kind != physicalKind(operation.ObjectKind) ||
			before.Mode&0o777 != operation.Mode&0o777 {
			return sourceauthority.MutationPlan{}, errors.New("tenantfs: revise cannot combine rename with content or mode replacement")
		}
		from := current
		effects := []sourceauthority.ExpectedEffect{
			{Path: current, Before: before, Outcome: sourceauthority.MutationAbsent},
			{Path: target, Outcome: sourceauthority.MutationPresent, Kind: before.Kind},
		}
		sortMutationEffects(effects)
		return sourceauthority.MutationPlan{
			Program: sourceauthority.MutationProgram{Actions: []sourceauthority.MutationAction{{
				Kind: sourceauthority.MutationRename, Path: target, From: &from,
			}}},
			Effects: effects,
		}, nil
	}
	if operation.ObjectKind != catalog.KindFile || !operation.HasContent {
		return sourceauthority.MutationPlan{}, errors.New("tenantfs: revise has no representable physical change")
	}
	return sourceauthority.MutationPlan{
		Program: sourceauthority.MutationProgram{Actions: []sourceauthority.MutationAction{{
			Kind: sourceauthority.MutationAtomicWriteFile, Path: current,
			Mode: operation.Mode & 0o777, UseRequestContent: true,
		}}},
		Effects: []sourceauthority.ExpectedEffect{{
			Path: current, Before: before, Outcome: sourceauthority.MutationPresent,
			Kind: sourceauthority.PhysicalFile,
		}},
	}, nil
}

func (p ClaudeAuthorityPolicy) planDeleteMutation(
	request sourceauthority.MutationRequest,
) (sourceauthority.MutationPlan, error) {
	if request.Object == nil || request.Parent == nil || request.Target != nil ||
		request.Step.Source.Operation.HasContent {
		return sourceauthority.MutationPlan{}, sourceauthority.ErrInvalidPlan
	}
	current, before, err := mutationObjectPath(
		request.Step.TenantID, request.Step.Source.Operation.Name, request.Object,
	)
	if err != nil {
		return sourceauthority.MutationPlan{}, err
	}
	return sourceauthority.MutationPlan{
		Program: sourceauthority.MutationProgram{Actions: []sourceauthority.MutationAction{{
			Kind: sourceauthority.MutationRemove, Path: current,
		}}},
		Effects: []sourceauthority.ExpectedEffect{{
			Path: current, Before: before, Outcome: sourceauthority.MutationAbsent,
		}},
	}, nil
}

func (p ClaudeAuthorityPolicy) planReplaceMutation(
	request sourceauthority.MutationRequest,
) (sourceauthority.MutationPlan, error) {
	if request.Object == nil || request.Parent == nil || request.Target == nil ||
		request.Step.Source.Operation.HasContent {
		return sourceauthority.MutationPlan{}, sourceauthority.ErrInvalidPlan
	}
	operation := request.Step.Source.Operation
	source, sourceBefore, err := mutationObjectPath(request.Step.TenantID, operation.Name, request.Object)
	if err != nil {
		return sourceauthority.MutationPlan{}, err
	}
	targetBeforePath, targetBefore, err := mutationObjectPath(request.Step.TenantID, operation.Name, request.Target)
	if err != nil {
		return sourceauthority.MutationPlan{}, err
	}
	target, err := mutationDestination(request.Step.TenantID, operation.Name, request.Parent)
	if err != nil {
		return sourceauthority.MutationPlan{}, err
	}
	if target != targetBeforePath || source == target ||
		sourceBefore.Kind != physicalKind(operation.ObjectKind) ||
		sourceBefore.Mode&0o777 != operation.Mode&0o777 {
		return sourceauthority.MutationPlan{}, errors.New("tenantfs: replace changes an unsupported physical attribute")
	}
	from := source
	effects := []sourceauthority.ExpectedEffect{
		{Path: source, Before: sourceBefore, Outcome: sourceauthority.MutationAbsent},
		{Path: target, Before: targetBefore, Outcome: sourceauthority.MutationPresent, Kind: sourceBefore.Kind},
	}
	sortMutationEffects(effects)
	return sourceauthority.MutationPlan{
		Program: sourceauthority.MutationProgram{Actions: []sourceauthority.MutationAction{{
			Kind: sourceauthority.MutationRename, Path: target, From: &from,
		}}},
		Effects: effects,
	}, nil
}

func createMutationAction(
	target sourceauthority.PathRef,
	operation catalog.SourceMutationOperation,
) (sourceauthority.MutationAction, error) {
	switch operation.ObjectKind {
	case catalog.KindFile:
		if !operation.HasContent || operation.LinkTarget != "" {
			return sourceauthority.MutationAction{}, sourceauthority.ErrInvalidPlan
		}
		return sourceauthority.MutationAction{
			Kind: sourceauthority.MutationAtomicWriteFile, Path: target,
			Mode: operation.Mode & 0o777, UseRequestContent: true,
		}, nil
	case catalog.KindDirectory:
		if operation.HasContent || operation.LinkTarget != "" {
			return sourceauthority.MutationAction{}, sourceauthority.ErrInvalidPlan
		}
		return sourceauthority.MutationAction{
			Kind: sourceauthority.MutationCreateDirectory, Path: target, Mode: operation.Mode & 0o777,
		}, nil
	case catalog.KindSymlink:
		if operation.HasContent || !safeProjectionLink(target.Relative, operation.LinkTarget) {
			return sourceauthority.MutationAction{}, errors.New("tenantfs: unsafe source symlink mutation")
		}
		return sourceauthority.MutationAction{
			Kind: sourceauthority.MutationCreateSymlink, Path: target, LinkTarget: operation.LinkTarget,
		}, nil
	default:
		return sourceauthority.MutationAction{}, sourceauthority.ErrInvalidPlan
	}
}

func mutationDestination(
	tenantID catalog.TenantID,
	name string,
	parent *sourceauthority.PhysicalLocator,
) (sourceauthority.PathRef, error) {
	if parent == nil || !safeMutationName(name) {
		return sourceauthority.PathRef{}, sourceauthority.ErrInvalidPlan
	}
	if len(parent.Bindings) == 0 {
		root, err := topLevelMutationRoot(tenantID, name)
		if err != nil {
			return sourceauthority.PathRef{}, err
		}
		return sourceauthority.PathRef{Root: root, Relative: name}, nil
	}
	binding, err := exactMutationBinding(tenantID, name, parent.Bindings)
	if err != nil {
		return sourceauthority.PathRef{}, err
	}
	if binding.Physical.Kind != sourceauthority.PhysicalDirectory {
		return sourceauthority.PathRef{}, errors.New("tenantfs: source mutation parent is not a directory")
	}
	return sourceauthority.PathRef{
		Root:     binding.Physical.Root,
		Relative: path.Join(binding.Physical.Relative, name),
	}, nil
}

func mutationObjectPath(
	tenantID catalog.TenantID,
	name string,
	locator *sourceauthority.PhysicalLocator,
) (sourceauthority.PathRef, sourceauthority.ExpectedPhysicalState, error) {
	if locator == nil || len(locator.Bindings) == 0 {
		return sourceauthority.PathRef{}, sourceauthority.ExpectedPhysicalState{}, sourceauthority.ErrMutationLocator
	}
	binding, err := exactMutationBinding(tenantID, name, locator.Bindings)
	if err != nil {
		return sourceauthority.PathRef{}, sourceauthority.ExpectedPhysicalState{}, err
	}
	return sourceauthority.PathRef{
		Root: binding.Physical.Root, Relative: binding.Physical.Relative,
	}, expectedPhysicalState(binding.Physical), nil
}

func exactMutationBinding(
	tenantID catalog.TenantID,
	name string,
	bindings []sourceauthority.PhysicalBinding,
) (sourceauthority.PhysicalBinding, error) {
	if len(bindings) == 1 {
		return bindings[0], nil
	}
	private := privateRootID(tenantID)
	var matches []sourceauthority.PhysicalBinding
	for _, binding := range bindings {
		switch {
		case name == claudeJSONFile && binding.Physical.Root == private:
			matches = append(matches, binding)
		case name == settingsFile && binding.Physical.Root == sharedClaudeRoot:
			matches = append(matches, binding)
		}
	}
	if len(matches) != 1 {
		return sourceauthority.PhysicalBinding{}, sourceauthority.ErrMutationLocator
	}
	return matches[0], nil
}

func topLevelMutationRoot(tenantID catalog.TenantID, name string) (sourceauthority.RootID, error) {
	switch {
	case name == claudeJSONFile, overlay.PrivateTopLevel(name):
		return privateRootID(tenantID), nil
	case name == settingsFile, overlay.SharedTopLevel(name):
		return sharedClaudeRoot, nil
	default:
		return "", fmt.Errorf("tenantfs: top-level source name %q has no mutation policy", name)
	}
}

func safeMutationName(name string) bool {
	return name != "" && name != "." && name != ".." &&
		path.Base(name) == name && !strings.ContainsRune(name, 0)
}

func expectedPhysicalState(entry sourceauthority.PhysicalEntry) sourceauthority.ExpectedPhysicalState {
	if !entry.Exists {
		return sourceauthority.ExpectedPhysicalState{}
	}
	return sourceauthority.ExpectedPhysicalState{
		Exists: true, Kind: entry.Kind, Identity: entry.Identity,
		Mode: entry.Mode, UID: entry.UID, GID: entry.GID, Size: entry.Size,
		LinkTarget: entry.LinkTarget, MetadataFingerprint: entry.MetadataFingerprint,
		ContentFingerprint: entry.ContentFingerprint,
	}
}

func physicalKind(kind catalog.Kind) sourceauthority.PhysicalKind {
	switch kind {
	case catalog.KindDirectory:
		return sourceauthority.PhysicalDirectory
	case catalog.KindFile:
		return sourceauthority.PhysicalFile
	case catalog.KindSymlink:
		return sourceauthority.PhysicalSymlink
	default:
		return 0
	}
}

func sortMutationEffects(effects []sourceauthority.ExpectedEffect) {
	slices.SortFunc(effects, func(left, right sourceauthority.ExpectedEffect) int {
		if left.Path.Root != right.Path.Root {
			return strings.Compare(string(left.Path.Root), string(right.Path.Root))
		}
		return strings.Compare(left.Path.Relative, right.Path.Relative)
	})
}

func validateMutationLocator(locator *sourceauthority.PhysicalLocator) error {
	if locator == nil || locator.Source.SourceAuthority != ClaudeAuthorityID {
		return sourceauthority.ErrMutationLocator
	}
	return nil
}
