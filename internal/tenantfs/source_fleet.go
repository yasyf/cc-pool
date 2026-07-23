package tenantfs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/causal"
)

const sourceFleetPublishAttempts = 2

type sourceFleetSession interface {
	publishDesiredSourceFleet(
		context.Context,
		catalogproto.PublishDesiredSourceFleetRequest,
	) (catalogproto.PublishDesiredSourceFleetResponse, error)
	Close() error
}

type sourceFleetConnector func(context.Context) (sourceFleetSession, error)

// PublishClaudeSourceFleet publishes cc-pool's complete v1 topology from the
// fixed signed runtime process.
func PublishClaudeSourceFleet(
	ctx context.Context,
	socket string,
	policy ClaudeAuthorityPolicy,
) error {
	return publishClaudeSourceFleet(ctx, policy, func(ctx context.Context) (sourceFleetSession, error) {
		return NewClient(ctx, socket)
	})
}

func publishClaudeSourceFleet(
	ctx context.Context,
	policy ClaudeAuthorityPolicy,
	connect sourceFleetConnector,
) error {
	if connect == nil {
		return errors.New("tenantfs: source fleet connector is required")
	}
	request, expected, err := claudeSourceFleetRequest(policy)
	if err != nil {
		return err
	}
	var attemptErr error
	for attempt := 0; attempt < sourceFleetPublishAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, attemptErr)
		}
		session, err := connect(ctx)
		if err != nil {
			attemptErr = fmt.Errorf("connect source fleet session: %w", err)
			continue
		}
		response, publishErr := session.publishDesiredSourceFleet(ctx, request)
		closeErr := session.Close()
		if publishErr != nil {
			attemptErr = fmt.Errorf("publish source fleet: %w", errors.Join(publishErr, closeErr))
			var transportErr *catalogservice.TransportError
			if !errors.As(publishErr, &transportErr) {
				return attemptErr
			}
			continue
		}
		if closeErr != nil {
			return fmt.Errorf("close source fleet session: %w", closeErr)
		}
		if response.State == nil || *response.State != expected {
			return errors.New("tenantfs: published source fleet state differs from exact v1 topology")
		}
		return nil
	}
	return attemptErr
}

func claudeSourceFleetRequest(
	policy ClaudeAuthorityPolicy,
) (catalogproto.PublishDesiredSourceFleetRequest, catalogproto.DesiredSourceFleetState, error) {
	declaration, err := ClaudeSourceAuthorityDeclaration(policy)
	if err != nil {
		return catalogproto.PublishDesiredSourceFleetRequest{}, catalogproto.DesiredSourceFleetState{}, err
	}
	authoritiesDigest, err := catalog.SourceAuthorityFleetDigest(
		[]causal.SourceAuthorityID{causal.SourceAuthorityID(declaration.Authority)},
	)
	if err != nil {
		return catalogproto.PublishDesiredSourceFleetRequest{}, catalogproto.DesiredSourceFleetState{}, err
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(
		[]catalog.SourceAuthorityDeclaration{declaration},
	)
	if err != nil {
		return catalogproto.PublishDesiredSourceFleetRequest{}, catalogproto.DesiredSourceFleetState{}, err
	}
	request := catalogproto.PublishDesiredSourceFleetRequest{
		Protocol: catalogproto.Version, Owner: string(SourceAuthorityFleetOwner),
		Generation: uint64(SourceAuthorityFleetGeneration),
		Declarations: []catalogproto.SourceAuthorityDeclaration{{
			Authority: catalogproto.SourceAuthorityID(declaration.Authority),
			DriverID:  declaration.DriverID, DriverConfig: append([]byte(nil), declaration.DriverConfig...),
			DeclarationDigest: hex.EncodeToString(declaration.DeclarationDigest[:]),
		}},
	}
	expected := catalogproto.DesiredSourceFleetState{
		Owner: string(SourceAuthorityFleetOwner), Generation: uint64(SourceAuthorityFleetGeneration),
		AuthorityCount: 1, AuthoritiesDigest: hex.EncodeToString(authoritiesDigest[:]),
		DeclarationsDigest: hex.EncodeToString(declarationsDigest[:]),
	}
	if err := catalogproto.Validate(request); err != nil {
		return catalogproto.PublishDesiredSourceFleetRequest{}, catalogproto.DesiredSourceFleetState{}, err
	}
	if err := catalogproto.Validate(expected); err != nil {
		return catalogproto.PublishDesiredSourceFleetRequest{}, catalogproto.DesiredSourceFleetState{}, err
	}
	return request, expected, nil
}

func (c *Client) publishDesiredSourceFleet(
	ctx context.Context,
	request catalogproto.PublishDesiredSourceFleetRequest,
) (catalogproto.PublishDesiredSourceFleetResponse, error) {
	return c.catalog.PublishDesiredSourceFleet(ctx, request)
}
