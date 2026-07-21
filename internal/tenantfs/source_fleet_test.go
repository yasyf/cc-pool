package tenantfs

import (
	"context"
	"reflect"
	"testing"

	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
)

type sourceFleetTestSession struct {
	response catalogproto.PublishDesiredSourceFleetResponse
	err      error
	closeErr error
	requests *[]catalogproto.PublishDesiredSourceFleetRequest
}

func (s sourceFleetTestSession) publishDesiredSourceFleet(
	_ context.Context,
	request catalogproto.PublishDesiredSourceFleetRequest,
) (catalogproto.PublishDesiredSourceFleetResponse, error) {
	*s.requests = append(*s.requests, request)
	return s.response, s.err
}

func (s sourceFleetTestSession) Close() error { return s.closeErr }

func TestClaudeSourceFleetPublicationRetriesOneExactLostResponse(t *testing.T) {
	policy := testClaudePolicy()
	request, expected, err := claudeSourceFleetRequest(policy)
	if err != nil {
		t.Fatal(err)
	}
	if request.Protocol != catalogproto.Version || request.ExpectedGeneration != 0 ||
		request.Generation != uint64(SourceAuthorityFleetGeneration) ||
		len(request.Declarations) != 1 || len(request.Declarations[0].DriverConfig) == 0 {
		t.Fatalf("Claude source fleet request = %+v", request)
	}
	var requests []catalogproto.PublishDesiredSourceFleetRequest
	sessions := []sourceFleetTestSession{
		{err: &catalogservice.TransportError{Message: "response lost"}, requests: &requests},
		{
			response: catalogproto.PublishDesiredSourceFleetResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, State: &expected,
			},
			requests: &requests,
		},
	}
	connections := 0
	err = publishClaudeSourceFleet(t.Context(), policy, func(context.Context) (sourceFleetSession, error) {
		defer func() { connections++ }()
		return sessions[connections], nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if connections != 2 || len(requests) != 2 || !reflect.DeepEqual(requests[0], requests[1]) ||
		!reflect.DeepEqual(requests[0], request) {
		t.Fatalf("source fleet replay = connections %d requests %+v", connections, requests)
	}
}

func TestClaudeSourceFleetPublicationDoesNotRetryRemoteConflict(t *testing.T) {
	policy := testClaudePolicy()
	var requests []catalogproto.PublishDesiredSourceFleetRequest
	connections := 0
	err := publishClaudeSourceFleet(t.Context(), policy, func(context.Context) (sourceFleetSession, error) {
		connections++
		return sourceFleetTestSession{
			err: &catalogservice.RemoteError{
				Code: catalogproto.ErrorCodeConflict, Message: "different v1 fleet",
			},
			requests: &requests,
		}, nil
	})
	if err == nil || connections != 1 {
		t.Fatalf("remote conflict = %v after %d connections", err, connections)
	}
}

func TestClaudeSourceFleetPublicationRejectsDifferentAppliedState(t *testing.T) {
	policy := testClaudePolicy()
	_, expected, err := claudeSourceFleetRequest(policy)
	if err != nil {
		t.Fatal(err)
	}
	expected.Generation++
	var requests []catalogproto.PublishDesiredSourceFleetRequest
	connections := 0
	err = publishClaudeSourceFleet(t.Context(), policy, func(context.Context) (sourceFleetSession, error) {
		connections++
		return sourceFleetTestSession{
			response: catalogproto.PublishDesiredSourceFleetResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, State: &expected,
			},
			requests: &requests,
		}, nil
	})
	if err == nil || connections != 1 {
		t.Fatalf("different applied state = %v after %d connections", err, connections)
	}
}
