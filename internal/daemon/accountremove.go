package daemon

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

func (s *Server) handleAccountRemove(ctx context.Context, request Request) Response {
	if request.Account == nil || *request.Account <= 0 {
		return Response{OK: false, Error: "account removal requires a positive account id"}
	}
	id := *request.Account
	removal, err := s.beginAccountRemoval(id, request.DeleteCredential)
	if errors.Is(err, errAccountExclusive) {
		return Response{OK: false, Error: fmt.Sprintf("acct-%02d is busy", id)}
	}
	if err != nil {
		return Response{OK: false, Error: err.Error()}
	}
	if err := s.finishAccountRemoval(ctx, removal); err != nil {
		return Response{OK: false, Error: fmt.Sprintf("remove acct-%02d: %v", id, err)}
	}
	return Response{OK: true}
}

func (s *Server) beginAccountRemoval(id int, deleteCredential bool) (removal store.AccountRemoval, err error) {
	account, ok, err := s.ownFresh(id)
	if !ok {
		return store.AccountRemoval{}, errAccountExclusive
	}
	defer s.cl.releaseExclusive(id)
	if err != nil {
		return store.AccountRemoval{}, err
	}
	return s.beginFreshAccountRemoval(account, deleteCredential)
}

func (s *Server) beginFreshAccountRemoval(account store.Account, deleteCredential bool) (store.AccountRemoval, error) {
	if s.heartbeatFor().view().sessionCount(pool.AccountPresentationDir(account.ID)) != 0 {
		return store.AccountRemoval{}, fmt.Errorf("acct-%02d has a live session", account.ID)
	}
	removal, err := s.m.Store.BeginAccountRemoval(account.ID, deleteCredential)
	if err != nil {
		return store.AccountRemoval{}, fmt.Errorf("begin removal: %w", err)
	}
	return removal, nil
}

func (s *Server) finishAccountRemoval(ctx context.Context, removal store.AccountRemoval) error {
	if s.tenantCoordinator == nil {
		return fmt.Errorf("FuseKit tenant coordinator is unavailable")
	}
	return s.tenantCoordinator.finishRemoval(ctx, removal)
}
