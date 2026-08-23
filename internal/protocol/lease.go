package protocol

import "time"

// MutationLeaseRequest is the exact-path v1 mutation lease request.
type MutationLeaseRequest struct {
	LeaseUUID      string    `json:"lease_uuid"`
	FencingToken   string    `json:"fencing_token"`
	ActorUUID      string    `json:"actor_uuid"`
	Generation     uint64    `json:"generation"`
	RepositoryUUID string    `json:"repository_uuid"`
	WorkspaceUUID  string    `json:"workspace_uuid"`
	IntentID       string    `json:"intent_id"`
	ToolCallID     string    `json:"tool_call_id"`
	Paths          []string  `json:"paths"`
	Now            time.Time `json:"now,omitzero"`
}

// MutationLease is a durable operational lease snapshot.
type MutationLease struct {
	LeaseUUID             string     `json:"lease_uuid"`
	FencingToken          string     `json:"fencing_token"`
	ActorUUID             string     `json:"actor_uuid"`
	Generation            uint64     `json:"generation"`
	RepositoryUUID        string     `json:"repository_uuid"`
	WorkspaceUUID         string     `json:"workspace_uuid"`
	IntentID              string     `json:"intent_id"`
	ToolCallID            string     `json:"tool_call_id"`
	Paths                 []string   `json:"paths"`
	GrantedAt             time.Time  `json:"granted_at"`
	RenewedAt             time.Time  `json:"renewed_at"`
	ExpiresAt             time.Time  `json:"expires_at"`
	HardDeadline          time.Time  `json:"hard_deadline"`
	State                 string     `json:"state"`
	BlockingLeaseUUID     string     `json:"blocking_lease_uuid,omitempty"`
	BlockingRootLeaseUUID string     `json:"blocking_root_lease_uuid,omitempty"`
	CollisionID           string     `json:"collision_id,omitempty"`
	PredecessorLeaseUUID  string     `json:"predecessor_lease_uuid,omitempty"`
	RootLeaseUUID         string     `json:"root_lease_uuid,omitempty"`
	TakeoverDepth         uint64     `json:"takeover_depth,omitempty"`
	SupersededByLeaseUUID string     `json:"superseded_by_lease_uuid,omitempty"`
	TerminalAt            *time.Time `json:"terminal_at,omitempty"`
}

// MutationLeaseResult is admission-compatible with grant, wait, and block.
type MutationLeaseResult struct {
	Decision  string          `json:"decision"` // grant, wait, block
	Lease     *MutationLease  `json:"lease,omitempty"`
	Conflicts []MutationLease `json:"conflicts,omitempty"`
	Reason    string          `json:"reason,omitempty"`
}

// MutationLeaseLifecycleEvent is the journal payload for every lease state
// transition. The lease snapshot is the rebuild input; operational tables are
// only a projection/cache of these events.
type MutationLeaseLifecycleEvent struct {
	Lease MutationLease `json:"lease"`
	// Takeover snapshots and its notification are one authoritative event. The
	// projector and Engine apply all three together, never as half-state.
	PredecessorLease    *MutationLease `json:"predecessor_lease,omitempty"`
	SuccessorLease      *MutationLease `json:"successor_lease,omitempty"`
	Message             *Message       `json:"message,omitempty"`
	Action              string         `json:"action"`
	Reason              string         `json:"reason,omitempty"`
	Coalesced           bool           `json:"coalesced,omitempty"`
	PreviousHolderUUID  string         `json:"previous_holder_uuid,omitempty"`
	NotificationBody    string         `json:"notification_body,omitempty"`
	RequesterActorUUID  string         `json:"requester_actor_uuid,omitempty"`
	RequesterGeneration uint64         `json:"requester_generation,omitempty"`
	AcquisitionSource   string         `json:"acquisition_source,omitempty"`
	WorkUnitUUID        string         `json:"work_unit_uuid,omitempty"`
	CollisionID         string         `json:"collision_id,omitempty"`
}

// Lease decisions and lifecycle states.
const (
	LeaseGrant      = "grant"
	LeaseWait       = "wait"
	LeaseBlock      = "block"
	LeaseActive     = "active"
	LeaseReleased   = "released"
	LeaseExpired    = "expired"
	LeaseSuperseded = "superseded"
	LeaseRevoked    = "revoked"
	LeaseWaiting    = "waiting"
	LeaseCancelled  = "canceled"
)

// MutationLeaseTakeoverRequest requests an explicit lease successor.
type MutationLeaseTakeoverRequest struct {
	PredecessorLeaseUUID string    `json:"predecessor_lease_uuid"`
	LeaseUUID            string    `json:"lease_uuid"`
	FencingToken         string    `json:"fencing_token"`
	RequesterActorUUID   string    `json:"requester_actor_uuid"`
	RequesterGeneration  uint64    `json:"requester_generation"`
	AcquisitionSource    string    `json:"acquisition_source"` // agent or human
	Reason               string    `json:"reason"`
	WorkUnitUUID         string    `json:"work_unit_uuid,omitempty"`
	CollisionID          string    `json:"collision_id,omitempty"`
	Now                  time.Time `json:"now,omitzero"`
}

// MutationLeaseAncestryResult contains a lease's predecessor chain.
type MutationLeaseAncestryResult struct {
	Leases []MutationLease `json:"leases"`
}

// MutationLeaseTakeoverResult describes a successful takeover.
type MutationLeaseTakeoverResult struct {
	Lease              MutationLease `json:"lease"`
	PreviousHolderUUID string        `json:"previous_holder_uuid"`
	NotificationBody   string        `json:"notification_body"`
}

// MutationLeaseReleaseRequest identifies a lease to release.
type MutationLeaseReleaseRequest struct {
	LeaseUUID    string    `json:"lease_uuid"`
	FencingToken string    `json:"fencing_token"`
	ActorUUID    string    `json:"actor_uuid"`
	Generation   uint64    `json:"generation"`
	Now          time.Time `json:"now,omitzero"`
}
