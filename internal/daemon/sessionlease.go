package daemon

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/holder"
)

const (
	sessionLeaseTTL              = 30 * time.Second
	sessionLeaseOperationTimeout = 5 * time.Second
)

type sessionLeaseRuntime interface {
	CommitFileProviderLease(context.Context, holder.LocalFileProviderLeaseCommit) (catalogproto.FileProviderLeaseReceipt, error)
	RenewFileProviderLease(context.Context, holder.LocalFileProviderLeaseRenew) (catalogproto.FileProviderLeaseReceipt, error)
	ReleaseFileProviderLease(context.Context, catalogproto.FileProviderLeaseReceipt) (catalogproto.FileProviderLeaseReceipt, error)
}

type sessionLeaseManager interface {
	Commit(context.Context, store.FileProviderLeaseReceipt, int64, store.ProcessIdentity, time.Time) (store.FileProviderLeaseReceipt, error)
	Renew(context.Context, store.FileProviderLeaseReceipt, time.Time) (store.FileProviderLeaseReceipt, error)
	ReleaseProvisional(context.Context, store.FileProviderLeaseReceipt) (store.FileProviderLeaseReceipt, error)
	Release(context.Context, store.Session) (store.FileProviderLeaseReceipt, error)
}

type catalogSessionLeaseManager struct {
	runtime sessionLeaseRuntime
}

func (m catalogSessionLeaseManager) Commit(
	ctx context.Context,
	provisional store.FileProviderLeaseReceipt,
	sessionID int64,
	process store.ProcessIdentity,
	expires time.Time,
) (store.FileProviderLeaseReceipt, error) {
	lease, err := decodeSessionLease(provisional)
	if err != nil {
		return nil, err
	}
	if lease.State != catalogproto.FileProviderLeaseStateProvisional || sessionID <= 0 ||
		process.PID <= 0 || process.StartedAt.IsZero() || expires.IsZero() {
		return nil, errors.New("commit File Provider lease: invalid session identity")
	}
	response, err := m.runtime.CommitFileProviderLease(ctx, holder.LocalFileProviderLeaseCommit{
		Lease: lease, SessionID: strconv.FormatInt(sessionID, 10),
		ProcessIdentity: sessionProcessIdentity(process), ExpiresAt: expires.UTC(),
	})
	if err != nil {
		return nil, err
	}
	expected := lease
	expected.State = catalogproto.FileProviderLeaseStateCommitted
	expected.SessionID = strconv.FormatInt(sessionID, 10)
	expected.ProcessIdentity = sessionProcessIdentity(process)
	expected.ExpiresUnixNano = uint64(expires.UTC().UnixNano())
	return exactSessionLeaseResponse(response, expected)
}

func (m catalogSessionLeaseManager) Renew(
	ctx context.Context,
	current store.FileProviderLeaseReceipt,
	expires time.Time,
) (store.FileProviderLeaseReceipt, error) {
	lease, err := decodeSessionLease(current)
	if err != nil {
		return nil, err
	}
	if lease.State != catalogproto.FileProviderLeaseStateCommitted || expires.IsZero() {
		return nil, errors.New("renew File Provider lease: invalid committed receipt")
	}
	response, err := m.runtime.RenewFileProviderLease(ctx, holder.LocalFileProviderLeaseRenew{
		Lease: lease, ExpiresAt: expires.UTC(),
	})
	if err != nil {
		return nil, err
	}
	expected := lease
	expected.ExpiresUnixNano = uint64(expires.UTC().UnixNano())
	return exactSessionLeaseResponse(response, expected)
}

func (m catalogSessionLeaseManager) ReleaseProvisional(
	ctx context.Context,
	provisional store.FileProviderLeaseReceipt,
) (store.FileProviderLeaseReceipt, error) {
	lease, err := decodeSessionLease(provisional)
	if err != nil {
		return nil, err
	}
	if lease.State != catalogproto.FileProviderLeaseStateProvisional ||
		lease.SessionID != "" || lease.ProcessIdentity != "" {
		return nil, errors.New("release File Provider lease: invalid provisional receipt")
	}
	response, err := m.runtime.ReleaseFileProviderLease(ctx, lease)
	if err != nil {
		return nil, err
	}
	expected := lease
	expected.State = catalogproto.FileProviderLeaseStateReleased
	return exactSessionLeaseResponse(response, expected)
}

func (m catalogSessionLeaseManager) Release(
	ctx context.Context,
	session store.Session,
) (store.FileProviderLeaseReceipt, error) {
	lease, err := decodeSessionLease(session.FileProviderLease)
	if err != nil {
		return nil, err
	}
	if session.LeaseState == store.SessionLeasePending {
		lease.State = catalogproto.FileProviderLeaseStateCommitted
		lease.SessionID = strconv.FormatInt(session.ID, 10)
		lease.ProcessIdentity = sessionProcessIdentity(store.ProcessIdentity{
			PID: session.PID, StartedAt: session.ProcessStartedAt,
		})
		lease.ExpiresUnixNano = uint64(session.LeaseExpiresAt.UTC().UnixNano())
	} else if session.LeaseState != store.SessionLeaseActive {
		return nil, errors.New("release File Provider lease: invalid session state")
	}
	response, err := m.runtime.ReleaseFileProviderLease(ctx, lease)
	if err != nil {
		return nil, err
	}
	expected := lease
	expected.State = catalogproto.FileProviderLeaseStateReleased
	return exactSessionLeaseResponse(response, expected)
}

func exactSessionLeaseResponse(
	receipt catalogproto.FileProviderLeaseReceipt,
	expected catalogproto.FileProviderLeaseReceipt,
) (store.FileProviderLeaseReceipt, error) {
	if receipt != expected {
		return nil, errors.New("FuseKit File Provider lease response changed exact identity")
	}
	return encodeSessionLease(receipt)
}

func encodeSessionLease(receipt catalogproto.FileProviderLeaseReceipt) (store.FileProviderLeaseReceipt, error) {
	raw, err := catalogproto.Encode(receipt)
	if err != nil {
		return nil, err
	}
	return store.FileProviderLeaseReceipt(raw), nil
}

func decodeSessionLease(raw store.FileProviderLeaseReceipt) (catalogproto.FileProviderLeaseReceipt, error) {
	var receipt catalogproto.FileProviderLeaseReceipt
	if err := catalogproto.Decode(raw, &receipt); err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	return receipt, nil
}

func sessionProcessIdentity(process store.ProcessIdentity) string {
	return fmt.Sprintf("pid=%d;started_unix_micro=%d", process.PID, process.StartedAt.UnixMicro())
}

func (s *Server) renewSessionLease(
	ctx context.Context,
	session store.Session,
	expires time.Time,
) (store.Session, error) {
	leaseCtx, cancel := context.WithTimeout(ctx, sessionLeaseOperationTimeout)
	defer cancel()
	renewed, err := s.sessionLeases.Renew(leaseCtx, session.FileProviderLease, expires)
	if err != nil {
		return session, err
	}
	if err := s.m.Store.CompleteSessionLeaseRenewal(
		session.ID, session.FileProviderLease, renewed, expires,
	); err != nil {
		return session, err
	}
	session.FileProviderLease = renewed
	session.LeaseExpiresAt = expires.UTC()
	session.LeaseRenewalExpiresAt = nil
	return session, nil
}

func (s *Server) releaseSessionLease(
	ctx context.Context,
	session store.Session,
) (store.FileProviderLeaseReceipt, error) {
	leaseCtx, cancel := context.WithTimeout(ctx, sessionLeaseOperationTimeout)
	defer cancel()
	return s.sessionLeases.Release(leaseCtx, session)
}

func sessionEndedAt(session store.Session) time.Time {
	endedAt := session.StartedAt
	if session.LastSeenAt != nil && session.LastSeenAt.After(endedAt) {
		endedAt = *session.LastSeenAt
	}
	return endedAt
}
