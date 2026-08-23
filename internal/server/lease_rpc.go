package server

import (
	"context"
	"errors"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func (s *Server) handleMutationLeaseAcquire(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
		return failure(request.ID, "provenance_lagging", err)
	}
	value, err := params[protocol.MutationLeaseRequest](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	result, err := s.provenance.AcquireMutationLease(ctx, &value)
	if err != nil {
		return failure(request.ID, "lease_admission_failed", err)
	}
	return success(request.ID, result)
}

func (s *Server) handleMutationLeaseTakeover(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
		return failure(request.ID, "provenance_lagging", err)
	}
	value, err := params[protocol.MutationLeaseTakeoverRequest](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	result, err := s.provenance.TakeoverMutationLease(ctx, value)
	if err != nil {
		return failure(request.ID, "lease_takeover_failed", err)
	}
	// The takeover lifecycle event projects the notification into the durable
	// mailbox. Do not emit a second state-engine message: that would create a
	// competing event sequence and duplicate delivery.
	return success(request.ID, result)
}

func (s *Server) handleMutationLeaseAncestry(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	value, err := params[struct {
		LeaseUUID string `json:"lease_uuid"`
	}](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	result, err := s.provenance.MutationLeaseAncestry(ctx, value.LeaseUUID)
	if err != nil {
		return failure(request.ID, "lease_query_failed", err)
	}
	return success(request.ID, result)
}

func (s *Server) handleMutationLeaseList(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	value, err := params[struct {
		ActorUUID     string `json:"actor_uuid"`
		WorkspaceUUID string `json:"workspace_uuid"`
	}](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	leases, err := s.provenance.ListMutationLeases(ctx, value.ActorUUID, value.WorkspaceUUID)
	if err != nil {
		return failure(request.ID, "lease_query_failed", err)
	}
	return success(request.ID, map[string]any{"leases": leases})
}

func (s *Server) handleMutationLeaseRenew(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	value, err := params[protocol.MutationLeaseRequest](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	result, err := s.provenance.RenewMutationLeaseContext(ctx, value)
	if err != nil {
		return failure(request.ID, "lease_renew_failed", err)
	}
	return success(request.ID, result)
}

func (s *Server) handleMutationLeaseRelease(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	value, err := params[protocol.MutationLeaseReleaseRequest](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	// Release timestamps are authority-owned. Now is an internal test seam;
	// never accept an arbitrary client clock over RPC.
	value.Now = time.Now().UTC()
	released, err := s.provenance.ReleaseMutationLease(ctx, &value)
	if err != nil {
		return failure(request.ID, "lease_release_failed", err)
	}
	return success(request.ID, map[string]any{"released": released})
}
