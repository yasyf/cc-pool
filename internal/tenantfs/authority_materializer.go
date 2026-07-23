package tenantfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"sync"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/tenant"
)

const maxClaudeDocumentBytes = 16 << 20

type projectionFingerprint struct {
	Tenant     catalog.TenantID
	Generation catalog.Generation
	Parent     sourceauthority.LogicalID
	Name       string
	Kind       catalog.Kind
	Mode       uint32
	LinkTarget string
	Mount      bool
	Provider   bool
}

// Materialize produces one exact tenant projection from immutable source inputs.
func (p ClaudeAuthorityPolicy) Materialize(
	ctx context.Context,
	task sourceauthority.MaterializerTask,
) (sourceauthority.Materialization, error) {
	if err := p.validate(); err != nil {
		return sourceauthority.Materialization{}, err
	}
	if task.Fence.Authority != ClaudeAuthorityID {
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
	specs, err := orderedClaudeTenants(task.Tenants)
	if err != nil {
		return sourceauthority.Materialization{}, err
	}
	var payload claudeMaterializationPayload
	decoder := json.NewDecoder(bytes.NewReader(task.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return sourceauthority.Materialization{}, fmt.Errorf("tenantfs: decode materialization policy: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return sourceauthority.Materialization{}, errors.New("tenantfs: materialization policy has trailing data")
	}
	switch payload.Kind {
	case materializePhysical:
		return materializePhysicalObject(task, specs, payload)
	case materializeClaudeJSON:
		return p.materializeClaudeJSON(ctx, task, specs, payload)
	case materializeSettings:
		return p.materializeSettings(ctx, task, specs, payload)
	case materializeDirectory:
		return materializeSyntheticDirectory(task, specs, payload)
	default:
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
}

func materializePhysicalObject(
	task sourceauthority.MaterializerTask,
	specs []tenant.TenantSpec,
	payload claudeMaterializationPayload,
) (sourceauthority.Materialization, error) {
	if payload.Root == "" || payload.Relative == "" || payload.Tenant != "" || payload.Name != "" ||
		task.Logical != physicalLogical(payload.Root, payload.Relative) {
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
	input, found := materializerInput(task.Inputs, payload.Root, payload.Relative)
	if !found {
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
	targets := projectionTenants(payload.Root, specs)
	if len(targets) == 0 {
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
	top := topLevel(payload.Relative)
	if payload.Root == sharedClaudeRoot {
		if !overlay.SharedTopLevel(top) || top == settingsFile {
			return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
		}
	} else if !overlay.PrivateTopLevel(top) || top == claudeJSONFile || top == settingsFile {
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
	kind, err := catalogKind(input.Physical.Kind)
	if err != nil {
		return sourceauthority.Materialization{}, err
	}
	if kind == catalog.KindSymlink && !safeProjectionLink(payload.Relative, input.Physical.LinkTarget) {
		return sourceauthority.Materialization{}, errors.New("tenantfs: source symlink escapes its projection root")
	}
	mode := input.Physical.Mode & 0o777
	if mode == 0 {
		return sourceauthority.Materialization{}, errors.New("tenantfs: source object has no permission bits")
	}
	objects := make([]sourceauthority.Projection, 0, len(targets))
	for _, spec := range targets {
		projection := sourceauthority.Projection{
			Tenant: spec.ID, Generation: spec.Generation,
			Parent: physicalParentLogical(payload.Root, payload.Relative, spec),
			Name:   path.Base(payload.Relative), Kind: kind, Mode: mode,
			LinkTarget: input.Physical.LinkTarget,
			Visibility: catalog.Visibility{FileProvider: true},
		}
		if kind == catalog.KindFile {
			if input.Content == nil {
				return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
			}
			projection.Content = immutableProjectionContent{input: input.Content}
		}
		objects = append(objects, projection)
	}
	fingerprint, err := materializationFingerprint(task.Logical, objects, input.Physical.ContentFingerprint, nil)
	if err != nil {
		return sourceauthority.Materialization{}, err
	}
	return sourceauthority.Materialization{
		Logical: task.Logical, Fingerprint: fingerprint, Objects: objects,
	}, nil
}

func (p ClaudeAuthorityPolicy) materializeClaudeJSON(
	ctx context.Context,
	task sourceauthority.MaterializerTask,
	specs []tenant.TenantSpec,
	payload claudeMaterializationPayload,
) (sourceauthority.Materialization, error) {
	spec, found := tenantByID(specs, payload.Tenant)
	if !found || payload.Root != "" || payload.Relative != "" || payload.Name != "" ||
		task.Logical != syntheticLogical(payload.Tenant, "claude-json") {
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
	private, found := materializerInput(task.Inputs, privateRootID(spec.ID), claudeJSONFile)
	if !found || private.Content == nil {
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
	canonical, found := materializerInput(task.Inputs, canonicalClaudeJSONRoot, ".")
	if !found || canonical.Content == nil {
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
	privateBytes, err := readImmutable(ctx, private.Content)
	if err != nil {
		return sourceauthority.Materialization{}, err
	}
	canonicalBytes, err := readImmutable(ctx, canonical.Content)
	if err != nil {
		return sourceauthority.Materialization{}, err
	}
	merged, _, err := overlay.MergeClaudeJSON(privateBytes, canonicalBytes)
	if err != nil {
		return sourceauthority.Materialization{}, fmt.Errorf("tenantfs: merge Claude state: %w", err)
	}
	object := sourceauthority.Projection{
		Tenant: spec.ID, Generation: spec.Generation, Parent: tenantRoot(spec).Logical,
		Name: claudeJSONFile, Kind: catalog.KindFile, Mode: 0o600,
		Content:    memoryProjectionContent{body: merged},
		Visibility: catalog.Visibility{FileProvider: true},
	}
	fingerprint, err := materializationFingerprint(task.Logical, []sourceauthority.Projection{object}, sourceauthority.Fingerprint{}, merged)
	if err != nil {
		return sourceauthority.Materialization{}, err
	}
	return sourceauthority.Materialization{
		Logical: task.Logical, Fingerprint: fingerprint,
		Objects: []sourceauthority.Projection{object},
	}, nil
}

func (p ClaudeAuthorityPolicy) materializeSettings(
	ctx context.Context,
	task sourceauthority.MaterializerTask,
	specs []tenant.TenantSpec,
	payload claudeMaterializationPayload,
) (sourceauthority.Materialization, error) {
	spec, found := tenantByID(specs, payload.Tenant)
	if !found || payload.Root != "" || payload.Relative != "" || payload.Name != "" ||
		task.Logical != syntheticLogical(payload.Tenant, "settings") {
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
	settings, found := materializerInput(task.Inputs, sharedClaudeRoot, settingsFile)
	if !found || settings.Content == nil {
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
	base, err := readImmutable(ctx, settings.Content)
	if err != nil {
		return sourceauthority.Materialization{}, err
	}
	served, err := overlay.InjectPlansDirectory(base, path.Join(p.ClaudeDir, "plans"))
	if err != nil {
		return sourceauthority.Materialization{}, fmt.Errorf("tenantfs: inject plans directory: %w", err)
	}
	object := sourceauthority.Projection{
		Tenant: spec.ID, Generation: spec.Generation, Parent: tenantRoot(spec).Logical,
		Name: settingsFile, Kind: catalog.KindFile, Mode: 0o600,
		Content:    memoryProjectionContent{body: served},
		Visibility: catalog.Visibility{FileProvider: true},
	}
	fingerprint, err := materializationFingerprint(task.Logical, []sourceauthority.Projection{object}, sourceauthority.Fingerprint{}, served)
	if err != nil {
		return sourceauthority.Materialization{}, err
	}
	return sourceauthority.Materialization{
		Logical: task.Logical, Fingerprint: fingerprint,
		Objects: []sourceauthority.Projection{object},
	}, nil
}

func materializeSyntheticDirectory(
	task sourceauthority.MaterializerTask,
	specs []tenant.TenantSpec,
	payload claudeMaterializationPayload,
) (sourceauthority.Materialization, error) {
	spec, found := tenantByID(specs, payload.Tenant)
	if !found || payload.Name == "" || payload.Root != "" || payload.Relative != "" {
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
	role := "plans"
	if payload.Name != "plans" {
		if !overlay.ExcludedEntries[payload.Name] {
			return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
		}
		role = "private-directory:" + payload.Name
	}
	if task.Logical != syntheticLogical(payload.Tenant, role) {
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
	root := privateRootID(spec.ID)
	if payload.Name == "plans" {
		root = sharedClaudeRoot
	}
	input, found := materializerInput(task.Inputs, root, payload.Name)
	if !found || input.Physical.Kind != sourceauthority.PhysicalDirectory {
		return sourceauthority.Materialization{}, sourceauthority.ErrInvalidPlan
	}
	object := sourceauthority.Projection{
		Tenant: spec.ID, Generation: spec.Generation, Parent: tenantRoot(spec).Logical,
		Name: payload.Name, Kind: catalog.KindDirectory, Mode: 0o700,
		Visibility: catalog.Visibility{FileProvider: true},
	}
	fingerprint, err := materializationFingerprint(task.Logical, []sourceauthority.Projection{object}, sourceauthority.Fingerprint{}, nil)
	if err != nil {
		return sourceauthority.Materialization{}, err
	}
	return sourceauthority.Materialization{
		Logical: task.Logical, Fingerprint: fingerprint,
		Objects: []sourceauthority.Projection{object},
	}, nil
}

func physicalParentLogical(
	root sourceauthority.RootID,
	relative string,
	spec tenant.TenantSpec,
) sourceauthority.LogicalID {
	parent := path.Dir(relative)
	if parent == "." {
		return tenantRoot(spec).Logical
	}
	top := topLevel(relative)
	if root == sharedClaudeRoot && parent == "plans" {
		return syntheticLogical(spec.ID, "plans")
	}
	if strings.HasPrefix(string(root), privateRootPrefix) &&
		parent == top && overlay.ExcludedEntries[top] {
		return syntheticLogical(spec.ID, "private-directory:"+top)
	}
	return physicalLogical(root, parent)
}

func catalogKind(kind sourceauthority.PhysicalKind) (catalog.Kind, error) {
	switch kind {
	case sourceauthority.PhysicalDirectory:
		return catalog.KindDirectory, nil
	case sourceauthority.PhysicalFile:
		return catalog.KindFile, nil
	case sourceauthority.PhysicalSymlink:
		return catalog.KindSymlink, nil
	default:
		return 0, sourceauthority.ErrInvalidPlan
	}
}

func materializerInput(
	inputs []sourceauthority.MaterializerInput,
	root sourceauthority.RootID,
	relative string,
) (sourceauthority.MaterializerInput, bool) {
	for _, input := range inputs {
		if input.Physical.Root == root && input.Physical.Relative == relative && input.Physical.Exists {
			return input, true
		}
	}
	return sourceauthority.MaterializerInput{}, false
}

func tenantByID(specs []tenant.TenantSpec, id catalog.TenantID) (tenant.TenantSpec, bool) {
	index, found := slices.BinarySearchFunc(specs, id, func(spec tenant.TenantSpec, target catalog.TenantID) int {
		return strings.Compare(string(spec.ID), string(target))
	})
	if !found {
		return tenant.TenantSpec{}, false
	}
	return specs[index], true
}

func readImmutable(ctx context.Context, content sourceauthority.ImmutableContent) ([]byte, error) {
	reader, err := content.Open(ctx)
	if err != nil {
		return nil, err
	}
	body, readErr := io.ReadAll(io.LimitReader(reader, maxClaudeDocumentBytes+1))
	closeErr := reader.Close()
	if len(body) > maxClaudeDocumentBytes {
		return nil, errors.Join(errors.New("tenantfs: Claude document exceeds its bounded size"), closeErr)
	}
	return body, errors.Join(readErr, closeErr)
}

func materializationFingerprint(
	logical sourceauthority.LogicalID,
	objects []sourceauthority.Projection,
	physical sourceauthority.Fingerprint,
	body []byte,
) (sourceauthority.Fingerprint, error) {
	metadata := make([]projectionFingerprint, len(objects))
	for index, object := range objects {
		metadata[index] = projectionFingerprint{
			Tenant: object.Tenant, Generation: object.Generation, Parent: object.Parent,
			Name: object.Name, Kind: object.Kind, Mode: object.Mode, LinkTarget: object.LinkTarget,
			Mount: object.Visibility.Mount, Provider: object.Visibility.FileProvider,
		}
	}
	payload, err := json.Marshal(struct {
		Logical  sourceauthority.LogicalID
		Objects  []projectionFingerprint
		Physical sourceauthority.Fingerprint
		Body     [sha256.Size]byte
	}{
		Logical: logical, Objects: metadata, Physical: physical, Body: sha256.Sum256(body),
	})
	if err != nil {
		return sourceauthority.Fingerprint{}, err
	}
	return sha256.Sum256(payload), nil
}

type immutableProjectionContent struct {
	input sourceauthority.ImmutableContent
}

func (c immutableProjectionContent) Open(ctx context.Context) (contentstream.Source, error) {
	reader, err := c.input.Open(ctx)
	if err != nil {
		return nil, err
	}
	return newProjectionStream(reader), nil
}

func (immutableProjectionContent) Close() error { return nil }

type memoryProjectionContent struct {
	body []byte
}

func (c memoryProjectionContent) Open(context.Context) (contentstream.Source, error) {
	return newProjectionStream(io.NopCloser(bytes.NewReader(c.body))), nil
}

func (memoryProjectionContent) Close() error { return nil }

type projectionStream struct {
	io.ReadCloser
	once sync.Once
	done chan struct{}
	err  error
}

func newProjectionStream(reader io.ReadCloser) *projectionStream {
	return &projectionStream{ReadCloser: reader, done: make(chan struct{})}
}

func (s *projectionStream) Settle(_ error) error {
	s.once.Do(func() {
		s.err = s.Close()
		close(s.done)
	})
	return s.err
}

func (s *projectionStream) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		return s.err
	case <-ctx.Done():
		_ = s.Settle(ctx.Err())
		return errors.Join(ctx.Err(), s.err)
	}
}

var (
	_ sourceauthority.ContentSource = immutableProjectionContent{}
	_ sourceauthority.ContentSource = memoryProjectionContent{}
	_ contentstream.Source          = (*projectionStream)(nil)
)
