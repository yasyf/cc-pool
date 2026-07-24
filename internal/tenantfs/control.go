package tenantfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/holder"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
)

const controlProtocol uint16 = 1

const (
	operationRuntimeReadiness wire.Op = "product.cc-pool.runtime-readiness.v1"
	operationTenantState      wire.Op = "product.cc-pool.tenant-state.v1"
	operationTenantProvision  wire.Op = "product.cc-pool.tenant-provision.v1"
	operationTenantReplace    wire.Op = "product.cc-pool.tenant-replace.v1"
	operationTenantRetire     wire.Op = "product.cc-pool.tenant-retire.v1"
	operationTenantPrepare    wire.Op = "product.cc-pool.tenant-prepare.v1"
	operationLeaseCommit      wire.Op = "product.cc-pool.file-provider-lease-commit.v1"
	operationLeaseRenew       wire.Op = "product.cc-pool.file-provider-lease-renew.v1"
	operationLeaseRelease     wire.Op = "product.cc-pool.file-provider-lease-release.v1"
)

// ControlErrorCode classifies an exact cc-pool holder operation failure.
type ControlErrorCode string

const (
	// ControlErrorOK reports a successful operation.
	ControlErrorOK ControlErrorCode = "ok"
	// ControlErrorInvalid rejects malformed or unauthenticated input.
	ControlErrorInvalid ControlErrorCode = "invalid_request"
	// ControlErrorNotFound reports an absent tenant or lease.
	ControlErrorNotFound ControlErrorCode = "not_found"
	// ControlErrorConflict reports a generation or identity fence mismatch.
	ControlErrorConflict ControlErrorCode = "conflict"
	// ControlErrorUnavailable reports a runtime that cannot currently admit work.
	ControlErrorUnavailable ControlErrorCode = "unavailable"
	// ControlErrorFailed reports an unclassified operation failure.
	ControlErrorFailed ControlErrorCode = "failed"
)

// ControlRemoteError is an exact holder business-operation rejection.
type ControlRemoteError struct {
	Code    ControlErrorCode
	Message string
}

func (e *ControlRemoteError) Error() string {
	return fmt.Sprintf("cc-pool holder: %s: %s", e.Code, e.Message)
}

// ControlTransportError reports a daemonkit session outcome without replaying
// a possibly dispatched mutation.
type ControlTransportError struct {
	Outcome wire.Outcome
	Message string
	cause   error
}

func (e *ControlTransportError) Error() string { return e.Message }
func (e *ControlTransportError) Unwrap() error { return e.cause }

type controlHeader struct {
	Protocol uint16           `json:"protocol"`
	Code     ControlErrorCode `json:"code"`
	Message  string           `json:"message,omitempty"`
}

type readinessRequest struct {
	Protocol uint16 `json:"protocol"`
}

type readinessResponse struct {
	controlHeader
	Readiness holder.LocalRuntimeReadiness `json:"readiness"`
}

type stateRequest struct {
	Protocol uint16           `json:"protocol"`
	Tenant   catalog.TenantID `json:"tenant"`
}

type stateResponse struct {
	controlHeader
	State tenant.TenantStatus `json:"state"`
}

type provisionRequest struct {
	Protocol uint16            `json:"protocol"`
	Spec     tenant.TenantSpec `json:"spec"`
}

type acknowledgementResponse struct {
	controlHeader
	Acknowledgement holder.LocalTenantAcknowledgement `json:"acknowledgement"`
}

type replaceRequest struct {
	Protocol uint16             `json:"protocol"`
	Expected catalog.Generation `json:"expected_generation"`
	Next     tenant.TenantSpec  `json:"next"`
}

type retireRequest struct {
	Protocol uint16             `json:"protocol"`
	Tenant   catalog.TenantID   `json:"tenant"`
	Expected catalog.Generation `json:"expected_generation"`
}

type retireResponse struct {
	controlHeader
	Proof holder.LocalTenantRetirementProof `json:"proof"`
}

type prepareRequest struct {
	Protocol    uint16                         `json:"protocol"`
	Tenant      catalog.TenantID               `json:"tenant"`
	Preparation holder.LocalPreparationRequest `json:"preparation"`
}

type prepareResponse struct {
	controlHeader
	Proof catalogproto.TenantPreparationProof `json:"proof"`
}

type leaseCommitRequest struct {
	Protocol uint16                              `json:"protocol"`
	Commit   holder.LocalFileProviderLeaseCommit `json:"commit"`
}

type leaseRenewRequest struct {
	Protocol uint16                             `json:"protocol"`
	Renew    holder.LocalFileProviderLeaseRenew `json:"renew"`
}

type leaseReleaseRequest struct {
	Protocol uint16                                `json:"protocol"`
	Receipt  catalogproto.FileProviderLeaseReceipt `json:"receipt"`
}

type leaseResponse struct {
	controlHeader
	Receipt catalogproto.FileProviderLeaseReceipt `json:"receipt"`
}

// ControlClient owns one persistent exact-build cc-pool holder session.
type ControlClient struct {
	wire *wire.Client
	done <-chan struct{}
}

// NewControlClient opens the holder's cc-pool-specific business protocol.
func NewControlClient(ctx context.Context, socket string) (*ControlClient, error) {
	if socket == "" {
		return nil, errors.New("tenantfs: FuseKit socket is empty")
	}
	session, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: wire.UnixDialer(socket), WireBuild: transportproto.WireBuild, Role: trust.UnprotectedRole,
	})
	if err != nil {
		return nil, err
	}
	return &ControlClient{wire: session, done: controlSessionDone(session.Events())}, nil
}

// Close settles and closes the persistent holder session.
func (c *ControlClient) Close() error { return c.wire.Close() }

// Done closes when the persistent holder session terminates.
func (c *ControlClient) Done() <-chan struct{} { return c.done }

// Readiness identifies the exact admitted holder publication.
func (c *ControlClient) Readiness(ctx context.Context) (holder.LocalRuntimeReadiness, error) {
	var response readinessResponse
	if err := c.call(ctx, operationRuntimeReadiness, readinessRequest{Protocol: controlProtocol}, &response); err != nil {
		return holder.LocalRuntimeReadiness{}, err
	}
	return response.Readiness, nil
}

// TenantState returns one exact owner-bound tenant state.
func (c *ControlClient) TenantState(ctx context.Context, id catalog.TenantID) (tenant.TenantStatus, error) {
	var response stateResponse
	if err := c.call(ctx, operationTenantState, stateRequest{Protocol: controlProtocol, Tenant: id}, &response); err != nil {
		return tenant.TenantStatus{}, err
	}
	return response.State, nil
}

// ProvisionTenant provisions one File Provider-only account without preparing it.
func (c *ControlClient) ProvisionTenant(ctx context.Context, account Account) (holder.LocalTenantAcknowledgement, error) {
	spec, err := account.Spec()
	if err != nil {
		return holder.LocalTenantAcknowledgement{}, err
	}
	var response acknowledgementResponse
	if err := c.call(ctx, operationTenantProvision, provisionRequest{Protocol: controlProtocol, Spec: spec}, &response); err != nil {
		return holder.LocalTenantAcknowledgement{}, err
	}
	return response.Acknowledgement, nil
}

// ReplaceTenant generation-fences one exact successor.
func (c *ControlClient) ReplaceTenant(ctx context.Context, account Account, expected uint64) (holder.LocalTenantAcknowledgement, error) {
	spec, err := account.Spec()
	if err != nil {
		return holder.LocalTenantAcknowledgement{}, err
	}
	var response acknowledgementResponse
	request := replaceRequest{Protocol: controlProtocol, Expected: catalog.Generation(expected), Next: spec}
	if err := c.call(ctx, operationTenantReplace, request, &response); err != nil {
		return holder.LocalTenantAcknowledgement{}, err
	}
	return response.Acknowledgement, nil
}

// RetireTenant generation-fences removal and proves File Provider absence.
func (c *ControlClient) RetireTenant(ctx context.Context, account Account, expected uint64) (holder.LocalTenantRetirementProof, error) {
	id, err := account.TenantID()
	if err != nil {
		return holder.LocalTenantRetirementProof{}, err
	}
	var response retireResponse
	request := retireRequest{Protocol: controlProtocol, Tenant: id, Expected: catalog.Generation(expected)}
	if err := c.call(ctx, operationTenantRetire, request, &response); err != nil {
		return holder.LocalTenantRetirementProof{}, err
	}
	return response.Proof, nil
}

// PrepareTenant converges one exact account generation and File Provider lease.
func (c *ControlClient) PrepareTenant(
	ctx context.Context,
	id catalog.TenantID,
	request holder.LocalPreparationRequest,
) (catalogproto.TenantPreparationProof, error) {
	var response prepareResponse
	payload := prepareRequest{Protocol: controlProtocol, Tenant: id, Preparation: request}
	if err := c.call(ctx, operationTenantPrepare, payload, &response); err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	return response.Proof, nil
}

// CommitFileProviderLease promotes one provisional lease to live demand.
func (c *ControlClient) CommitFileProviderLease(
	ctx context.Context,
	request holder.LocalFileProviderLeaseCommit,
) (catalogproto.FileProviderLeaseReceipt, error) {
	var response leaseResponse
	if err := c.call(ctx, operationLeaseCommit, leaseCommitRequest{Protocol: controlProtocol, Commit: request}, &response); err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	return response.Receipt, nil
}

// RenewFileProviderLease extends one committed live-demand lease.
func (c *ControlClient) RenewFileProviderLease(
	ctx context.Context,
	request holder.LocalFileProviderLeaseRenew,
) (catalogproto.FileProviderLeaseReceipt, error) {
	var response leaseResponse
	if err := c.call(ctx, operationLeaseRenew, leaseRenewRequest{Protocol: controlProtocol, Renew: request}, &response); err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	return response.Receipt, nil
}

// ReleaseFileProviderLease retires one exact provisional or committed receipt.
func (c *ControlClient) ReleaseFileProviderLease(
	ctx context.Context,
	receipt catalogproto.FileProviderLeaseReceipt,
) (catalogproto.FileProviderLeaseReceipt, error) {
	var response leaseResponse
	if err := c.call(ctx, operationLeaseRelease, leaseReleaseRequest{Protocol: controlProtocol, Receipt: receipt}, &response); err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	return response.Receipt, nil
}

func (c *ControlClient) call(ctx context.Context, operation wire.Op, request, response any) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	result, err := c.wire.Call(ctx, operation, "", payload)
	if err != nil {
		return err
	}
	if result.Outcome != wire.Delivered || result.Response.Rejected {
		message := result.Response.Reason
		if message == "" {
			message = "tenantfs: cc-pool holder request was not delivered"
		}
		return &ControlTransportError{Outcome: result.Outcome, Message: message, cause: result.Rejection()}
	}
	if result.Response.Err != "" {
		return &ControlTransportError{Outcome: result.Outcome, Message: result.Response.Err}
	}
	if len(result.Response.Payload) == 0 {
		return &ControlTransportError{Outcome: result.Outcome, Message: "tenantfs: cc-pool holder response has no payload"}
	}
	if err := decodeControl(result.Response.Payload, response); err != nil {
		return err
	}
	header, err := controlResponseHeader(response)
	if err != nil {
		return err
	}
	if header.Protocol != controlProtocol {
		return errors.New("tenantfs: cc-pool holder response protocol mismatch")
	}
	if header.Code == ControlErrorOK {
		if header.Message != "" {
			return errors.New("tenantfs: successful cc-pool holder response carries a message")
		}
		return nil
	}
	if header.Message == "" {
		return errors.New("tenantfs: failed cc-pool holder response has no message")
	}
	return &ControlRemoteError{Code: header.Code, Message: header.Message}
}

// BusinessHandlers returns the complete cc-pool control surface registered on
// the holder's existing daemonkit server.
func BusinessHandlers() []holder.BusinessHandlerSpec {
	return []holder.BusinessHandlerSpec{
		controlHandler(operationRuntimeReadiness, false, handleReadiness),
		controlHandler(operationTenantState, true, handleState),
		controlHandler(operationTenantProvision, true, handleProvision),
		controlHandler(operationTenantReplace, true, handleReplace),
		controlHandler(operationTenantRetire, true, handleRetire),
		controlHandler(operationTenantPrepare, true, handlePrepare),
		controlHandler(operationLeaseCommit, true, handleLeaseCommit),
		controlHandler(operationLeaseRenew, true, handleLeaseRenew),
		controlHandler(operationLeaseRelease, true, handleLeaseRelease),
	}
}

func controlHandler(
	op wire.Op,
	concurrent bool,
	handler func(context.Context, wire.Request, *holder.LocalTenantController) any,
) holder.BusinessHandlerSpec {
	return holder.BusinessHandlerSpec{
		Op: op, Concurrent: concurrent,
		Handler: func(ctx context.Context, request wire.Request, controller *holder.LocalTenantController) (any, error) {
			if request.Session == nil || request.Session.Protected() ||
				request.WireBuild != transportproto.WireBuild || request.Session.WireBuild() != transportproto.WireBuild {
				return controlFailure(ControlErrorInvalid, "cc-pool holder request has an invalid session"), nil
			}
			if request.Tenant != "" {
				return controlFailure(ControlErrorInvalid, "cc-pool holder request has an invalid route"), nil
			}
			return handler(ctx, request, controller), nil
		},
	}
}

func handleReadiness(ctx context.Context, request wire.Request, controller *holder.LocalTenantController) any {
	var input readinessRequest
	if err := decodeControlRequest(request.Payload, &input); err != nil {
		return readinessResponse{controlHeader: controlErrorHeader(err)}
	}
	value, err := controller.Readiness(ctx)
	return readinessResponse{controlHeader: controlErrorHeader(err), Readiness: value}
}

func handleState(ctx context.Context, request wire.Request, controller *holder.LocalTenantController) any {
	var input stateRequest
	if err := decodeControlRequest(request.Payload, &input); err != nil {
		return stateResponse{controlHeader: controlErrorHeader(err)}
	}
	value, err := controller.State(ctx, input.Tenant)
	return stateResponse{controlHeader: controlErrorHeader(err), State: value}
}

func handleProvision(ctx context.Context, request wire.Request, controller *holder.LocalTenantController) any {
	var input provisionRequest
	if err := decodeControlRequest(request.Payload, &input); err != nil {
		return acknowledgementResponse{controlHeader: controlErrorHeader(err)}
	}
	value, err := controller.Provision(ctx, input.Spec)
	return acknowledgementResponse{controlHeader: controlErrorHeader(err), Acknowledgement: value}
}

func handleReplace(ctx context.Context, request wire.Request, controller *holder.LocalTenantController) any {
	var input replaceRequest
	if err := decodeControlRequest(request.Payload, &input); err != nil {
		return acknowledgementResponse{controlHeader: controlErrorHeader(err)}
	}
	value, err := controller.Replace(ctx, input.Expected, input.Next)
	return acknowledgementResponse{controlHeader: controlErrorHeader(err), Acknowledgement: value}
}

func handleRetire(ctx context.Context, request wire.Request, controller *holder.LocalTenantController) any {
	var input retireRequest
	if err := decodeControlRequest(request.Payload, &input); err != nil {
		return retireResponse{controlHeader: controlErrorHeader(err)}
	}
	value, err := controller.Retire(ctx, input.Tenant, input.Expected)
	return retireResponse{controlHeader: controlErrorHeader(err), Proof: value}
}

func handlePrepare(ctx context.Context, request wire.Request, controller *holder.LocalTenantController) any {
	var input prepareRequest
	if err := decodeControlRequest(request.Payload, &input); err != nil {
		return prepareResponse{controlHeader: controlErrorHeader(err)}
	}
	value, err := controller.Prepare(ctx, input.Tenant, input.Preparation)
	return prepareResponse{controlHeader: controlErrorHeader(err), Proof: value}
}

func handleLeaseCommit(ctx context.Context, request wire.Request, controller *holder.LocalTenantController) any {
	var input leaseCommitRequest
	if err := decodeControlRequest(request.Payload, &input); err != nil {
		return leaseResponse{controlHeader: controlErrorHeader(err)}
	}
	value, err := controller.CommitFileProviderLease(ctx, input.Commit)
	return leaseResponse{controlHeader: controlErrorHeader(err), Receipt: value}
}

func handleLeaseRenew(ctx context.Context, request wire.Request, controller *holder.LocalTenantController) any {
	var input leaseRenewRequest
	if err := decodeControlRequest(request.Payload, &input); err != nil {
		return leaseResponse{controlHeader: controlErrorHeader(err)}
	}
	value, err := controller.RenewFileProviderLease(ctx, input.Renew)
	return leaseResponse{controlHeader: controlErrorHeader(err), Receipt: value}
}

func handleLeaseRelease(ctx context.Context, request wire.Request, controller *holder.LocalTenantController) any {
	var input leaseReleaseRequest
	if err := decodeControlRequest(request.Payload, &input); err != nil {
		return leaseResponse{controlHeader: controlErrorHeader(err)}
	}
	value, err := controller.ReleaseFileProviderLease(ctx, input.Receipt)
	return leaseResponse{controlHeader: controlErrorHeader(err), Receipt: value}
}

func decodeControlRequest(payload []byte, value any) error {
	if err := decodeControl(payload, value); err != nil {
		return fmt.Errorf("invalid cc-pool holder request: %w", err)
	}
	protocol, err := controlRequestProtocol(value)
	if err != nil {
		return err
	}
	if protocol != controlProtocol {
		return errors.New("invalid cc-pool holder request protocol")
	}
	return nil
}

func decodeControl(payload []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func controlRequestProtocol(value any) (uint16, error) {
	switch request := value.(type) {
	case *readinessRequest:
		return request.Protocol, nil
	case *stateRequest:
		return request.Protocol, nil
	case *provisionRequest:
		return request.Protocol, nil
	case *replaceRequest:
		return request.Protocol, nil
	case *retireRequest:
		return request.Protocol, nil
	case *prepareRequest:
		return request.Protocol, nil
	case *leaseCommitRequest:
		return request.Protocol, nil
	case *leaseRenewRequest:
		return request.Protocol, nil
	case *leaseReleaseRequest:
		return request.Protocol, nil
	default:
		return 0, fmt.Errorf("unsupported cc-pool holder request %T", value)
	}
}

func controlResponseHeader(value any) (controlHeader, error) {
	switch response := value.(type) {
	case *readinessResponse:
		return response.controlHeader, nil
	case *stateResponse:
		return response.controlHeader, nil
	case *acknowledgementResponse:
		return response.controlHeader, nil
	case *retireResponse:
		return response.controlHeader, nil
	case *prepareResponse:
		return response.controlHeader, nil
	case *leaseResponse:
		return response.controlHeader, nil
	default:
		return controlHeader{}, fmt.Errorf("unsupported cc-pool holder response %T", value)
	}
}

func controlErrorHeader(err error) controlHeader {
	if err == nil {
		return controlHeader{Protocol: controlProtocol, Code: ControlErrorOK}
	}
	return controlHeader{Protocol: controlProtocol, Code: classifyControlError(err), Message: err.Error()}
}

func controlFailure(code ControlErrorCode, message string) controlHeader {
	return controlHeader{Protocol: controlProtocol, Code: code, Message: message}
}

func classifyControlError(err error) ControlErrorCode {
	switch {
	case errors.Is(err, tenant.ErrTenantNotFound), errors.Is(err, catalog.ErrNotFound):
		return ControlErrorNotFound
	case errors.Is(err, tenant.ErrTenantConflict), errors.Is(err, tenant.ErrGenerationConflict),
		errors.Is(err, tenant.ErrTenantChanging), errors.Is(err, catalog.ErrGenerationMismatch),
		errors.Is(err, catalog.ErrTenantMutationConflict), errors.Is(err, catalog.ErrTenantTargetingChanged):
		return ControlErrorConflict
	case errors.Is(err, tenant.ErrInvalidSpec), errors.Is(err, tenant.ErrTenantOwnerMismatch),
		errors.Is(err, catalog.ErrInvalidObject):
		return ControlErrorInvalid
	case errors.Is(err, holder.ErrLocalTenantControllerUnavailable), errors.Is(err, tenant.ErrClosed),
		errors.Is(err, tenant.ErrRecovering), errors.Is(err, tenant.ErrRecoveryActive):
		return ControlErrorUnavailable
	default:
		return ControlErrorFailed
	}
}

func controlSessionDone(events <-chan wire.Event) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		for event := range events {
			_ = event
		}
		close(done)
	}()
	return done
}
