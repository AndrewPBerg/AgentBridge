package protocol

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Version is the current protocol version.
const Version = 1

func validUUIDLayout(value string) bool {
	if len(value) != 36 {
		return false
	}
	return value[8] == '-' && value[13] == '-' && value[18] == '-' && value[23] == '-'
}

// ValidateUUID enforces the canonical UUID storage contract at protocol
// boundaries. Persisted UUIDs must be 16-byte values with a recognized
// version and RFC 4122 variant.
func ValidateUUID(value string) error {
	if !validUUIDLayout(value) {
		return fmt.Errorf("invalid UUID %q", value)
	}
	for _, character := range value {
		if character >= 'A' && character <= 'F' {
			return fmt.Errorf("invalid UUID %q", value)
		}
	}
	compact := value[:8] + value[9:13] + value[14:18] + value[19:23] + value[24:]
	decoded, err := hex.DecodeString(compact)
	if err != nil || len(decoded) != 16 {
		return fmt.Errorf("invalid UUID %q", value)
	}
	if decoded[8]&0xc0 != 0x80 || decoded[6]>>4 == 0 || decoded[6]>>4 > 5 {
		return fmt.Errorf("invalid UUID %q", value)
	}
	return nil
}

// Request is a JSON-RPC request.
type Request struct {
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

// Response is a JSON-RPC response.
type Response struct {
	ID     string    `json:"id"`
	Result any       `json:"result,omitempty"`
	Error  *RPCError `json:"error,omitempty"`
}

// RPCError describes a JSON-RPC error.
type RPCError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// GitContext is a protocol value.
type GitContext struct {
	RepoRoot     string `json:"repo_root"`
	WorktreeRoot string `json:"worktree_root"`
	GitDir       string `json:"git_dir"`
	CommonDir    string `json:"common_dir"`
	Branch       string `json:"branch,omitempty"`
	Head         string `json:"head,omitempty"`
	Detached     bool   `json:"detached"`
}

// JJContext is a protocol value.
type JJContext struct {
	RepoPath      string `json:"repo_path,omitempty"`
	WorkspaceName string `json:"workspace_name,omitempty"`
	WorkspaceRoot string `json:"workspace_root"`
	ChangeID      string `json:"change_id"`
	CommitID      string `json:"commit_id,omitempty"`
}

// Actor and presence kinds distinguish live sessions from synthetic evidence identities.
const (
	ActorKindAgent        = "agent"
	ActorKindUnknown      = "unknown"
	PresenceKindLease     = "lease"
	PresenceKindSynthetic = "synthetic"
)

// Actor describes a connected coordination participant.
type Actor struct {
	Address          string      `json:"address"`
	Harness          string      `json:"harness"`
	SessionUUID      string      `json:"session_uuid"`
	SessionFile      string      `json:"session_file,omitempty"`
	Alias            string      `json:"alias,omitempty"`
	CWD              string      `json:"cwd"`
	PaneID           string      `json:"pane_id,omitempty"`
	HerdrWorkspaceID string      `json:"herdr_workspace_id,omitempty"`
	State            string      `json:"state"`
	RepositoryUUID   string      `json:"repository_uuid"`
	RepositoryRoot   string      `json:"repository_root"`
	WorkspaceUUID    string      `json:"workspace_uuid"`
	WorkspaceRoot    string      `json:"workspace_root"`
	WorkspaceKind    string      `json:"workspace_kind"`
	Git              *GitContext `json:"git,omitempty"`
	JJ               *JJContext  `json:"jj,omitempty"`
	Capabilities     []string    `json:"capabilities,omitempty"`
	ActorKind        string      `json:"actor_kind,omitempty"`
	Addressable      bool        `json:"addressable"`
	PresenceKind     string      `json:"presence_kind,omitempty"`
	Generation       uint64      `json:"generation"`
	StartedAt        time.Time   `json:"started_at"`
	HeartbeatAt      time.Time   `json:"heartbeat_at"`
}

// MarshalJSON keeps the default repository workspace explicit as null so
// consumers can inherit repository_root and repository kind.
//
//nolint:gocritic // Keep a value receiver so Actor's JSON behavior remains available on values.
func (actor Actor) MarshalJSON() ([]byte, error) {
	type alias Actor
	encoded, err := json.Marshal(alias(actor))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, err
	}
	if actor.RepositoryRoot == "" {
		fields["repository_root"] = json.RawMessage("null")
	}
	if actor.WorkspaceRoot == "" {
		fields["workspace_root"] = json.RawMessage("null")
	}
	if actor.WorkspaceKind == "" {
		fields["workspace_kind"] = json.RawMessage("null")
	}
	return json.Marshal(fields)
}

// IntentContext is a protocol value.
type IntentContext struct {
	AssistantExcerpt string `json:"assistant_excerpt,omitempty"`
}

// FileSnapshot is a protocol value.
type FileSnapshot struct {
	Path       string `json:"path"`
	Exists     bool   `json:"exists"`
	Kind       string `json:"kind,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
	Size       int64  `json:"size,omitempty"`
	ModifiedAt string `json:"modified_at,omitempty"`
}

// ExternalChange is an authoritative observation of an external change.
type ExternalChange struct {
	ID                string        `json:"external_change_uuid"`
	RepositoryUUID    string        `json:"repository_uuid"`
	WorkspaceUUID     string        `json:"workspace_uuid"`
	Actor             string        `json:"actor"`
	UnknownActor      string        `json:"unknown_actor_uuid"`
	IntervalStartedAt time.Time     `json:"interval_started_at"`
	IntervalEndedAt   time.Time     `json:"interval_ended_at"`
	ContinuityState   string        `json:"continuity_state"`
	ChangeKind        string        `json:"change_kind"`
	Path              string        `json:"path"`
	Before            *FileSnapshot `json:"before,omitempty"`
	After             *FileSnapshot `json:"after,omitempty"`
	WatchmanClock     string        `json:"watchman_clock"`
	RelatedIntentIDs  []string      `json:"related_intent_ids,omitempty"`
}

// ExternalChangeObservation is the descriptive name used by transport adapters.
type ExternalChangeObservation = ExternalChange

// WatchContinuity records watch continuity for one workspace.
type WatchContinuity struct {
	RepositoryUUID string    `json:"repository_uuid"`
	WorkspaceUUID  string    `json:"workspace_uuid"`
	State          string    `json:"state"`
	At             time.Time `json:"at"`
	WatchmanClock  string    `json:"watchman_clock,omitempty"`
}

// Intent is a protocol value.
type Intent struct {
	ID                string         `json:"id"`
	Actor             string         `json:"actor"`
	SessionGeneration uint64         `json:"session_generation"`
	TurnID            string         `json:"turn_id,omitempty"`
	TurnIndex         *int           `json:"turn_index,omitempty"`
	ToolCallID        string         `json:"tool_call_id"`
	Tool              string         `json:"tool"`
	Operation         string         `json:"operation"`
	Paths             []string       `json:"paths"`
	RelativePaths     []string       `json:"relative_paths,omitempty"`
	CWD               string         `json:"cwd"`
	RepositoryUUID    string         `json:"repository_uuid"`
	RepositoryRoot    string         `json:"repository_root"`
	WorkspaceUUID     string         `json:"workspace_uuid"`
	WorkspaceRoot     string         `json:"workspace_root"`
	WorkspaceKind     string         `json:"workspace_kind"`
	WorkspaceKey      string         `json:"workspace_key"`
	Git               *GitContext    `json:"git,omitempty"`
	JJ                *JJContext     `json:"jj,omitempty"`
	Context           IntentContext  `json:"context"`
	Before            []FileSnapshot `json:"before,omitempty"`
	After             []FileSnapshot `json:"after,omitempty"`
	GitAfter          *GitContext    `json:"git_after,omitempty"`
	JJAfter           *JJContext     `json:"jj_after,omitempty"`
	Success           *bool          `json:"success,omitempty"`
	Error             string         `json:"error,omitempty"`
	StartedAt         time.Time      `json:"started_at"`
	ExpiresAt         time.Time      `json:"expires_at"`
	CompletedAt       *time.Time     `json:"completed_at,omitempty"`
}

// MarshalJSON applies the same sparse workspace representation to intents.
//
//nolint:gocritic // Keep a value receiver so Intent's JSON behavior remains available on values.
func (intent Intent) MarshalJSON() ([]byte, error) {
	type alias Intent
	encoded, err := json.Marshal(alias(intent))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, err
	}
	if intent.RepositoryRoot == "" {
		fields["repository_root"] = json.RawMessage("null")
	}
	if intent.WorkspaceRoot == "" {
		fields["workspace_root"] = json.RawMessage("null")
	}
	if intent.WorkspaceKind == "" {
		fields["workspace_kind"] = json.RawMessage("null")
	}
	return json.Marshal(fields)
}

// CollisionState is a protocol value.
type CollisionState string

// CollisionTransitionEvent is the durable state-machine event for a lifecycle
// transition. Collision snapshots remain read-model data; lifecycle changes
// are replayed from these events.
type CollisionTransitionEvent struct {
	CollisionID string         `json:"collision_id"`
	Actor       string         `json:"actor"`
	From        CollisionState `json:"from"`
	To          CollisionState `json:"to"`
	Owner       string         `json:"owner,omitempty"`
	YieldedBy   string         `json:"yielded_by,omitempty"`
	Resolution  string         `json:"resolution,omitempty"`
	At          time.Time      `json:"at"`
}

// CollisionActorDeadEvent is a protocol value.
type CollisionActorDeadEvent struct {
	CollisionID string    `json:"collision_id"`
	Actor       string    `json:"actor"`
	At          time.Time `json:"at"`
}

// CollisionOpen is a protocol constant.
const (
	CollisionOpen        CollisionState = "open"
	CollisionNegotiating CollisionState = "negotiating"
	CollisionYielded     CollisionState = "yielded"
	CollisionResolved    CollisionState = "resolved"
)

// Collision is a protocol value.
type Collision struct {
	ID         string         `json:"id"`
	Key        string         `json:"key"`
	Path       string         `json:"path"`
	Actors     [2]string      `json:"actors"`
	IntentIDs  [2]string      `json:"intent_ids"`
	State      CollisionState `json:"state"`
	Owner      string         `json:"owner,omitempty"`
	YieldedBy  string         `json:"yielded_by,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
	ResolvedAt *time.Time     `json:"resolved_at,omitempty"`
	Resolution string         `json:"resolution,omitempty"`
	ResolvedBy string         `json:"resolved_by,omitempty"`
	DeadActor  string         `json:"dead_actor,omitempty"`
}

// Message is a protocol value.
type Message struct {
	ID                string     `json:"id"`
	Kind              string     `json:"kind"`
	From              string     `json:"from"`
	To                string     `json:"to"`
	Body              string     `json:"body"`
	GlobalSequence    uint64     `json:"global_sequence"`
	SenderSequence    uint64     `json:"sender_sequence"`
	RecipientSequence uint64     `json:"recipient_sequence"`
	ClientSequence    uint64     `json:"client_sequence,omitempty"`
	SessionGeneration uint64     `json:"session_generation,omitempty"`
	CollisionID       string     `json:"collision_id,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	AcknowledgedAt    *time.Time `json:"acknowledged_at,omitempty"`
}

// Event is a protocol value.
type Event struct {
	Version  int             `json:"version"`
	Sequence uint64          `json:"sequence"`
	Type     string          `json:"type"`
	At       time.Time       `json:"at"`
	Data     json.RawMessage `json:"data"`
}

// RegisterParams is a protocol value.
type RegisterParams struct {
	Actor Actor `json:"actor"`
}

// ScopeFilter is a protocol value.
type ScopeFilter struct {
	RepositoryUUID string `json:"repository_uuid,omitempty"`
	WorkspaceUUID  string `json:"workspace_uuid,omitempty"`
	Directory      string `json:"directory,omitempty"`
}

// HeartbeatParams is a protocol value.
type HeartbeatParams struct {
	Address    string      `json:"address"`
	State      string      `json:"state"`
	CWD        string      `json:"cwd,omitempty"`
	Git        *GitContext `json:"git,omitempty"`
	JJ         *JJContext  `json:"jj,omitempty"`
	Generation uint64      `json:"generation,omitempty"`
}

// SendParams is a protocol value.
type SendParams struct {
	ID                string `json:"id,omitempty"`
	From              string `json:"from"`
	To                string `json:"to"`
	Body              string `json:"body"`
	ClientSequence    uint64 `json:"client_sequence,omitempty"`
	SessionGeneration uint64 `json:"session_generation,omitempty"`
}

// PollParams is a protocol value.
type PollParams struct {
	Actor string `json:"actor"`
	Limit int    `json:"limit,omitempty"`
}

// AckParams is a protocol value.
type AckParams struct {
	Actor      string   `json:"actor"`
	MessageIDs []string `json:"message_ids"`
}

// IntentBeginParams is a protocol value.
type IntentBeginParams struct {
	Intent Intent `json:"intent"`
}

// IntentEndParams is a protocol value.
type IntentEndParams struct {
	IntentID    string         `json:"intent_id"`
	Success     bool           `json:"success"`
	Error       string         `json:"error,omitempty"`
	After       []FileSnapshot `json:"after,omitempty"`
	GitAfter    *GitContext    `json:"git_after,omitempty"`
	JJAfter     *JJContext     `json:"jj_after,omitempty"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
}

// SessionEvent is a protocol value.
type SessionEvent struct {
	ID                string          `json:"id"`
	Actor             string          `json:"actor"`
	SessionGeneration uint64          `json:"session_generation"`
	Type              string          `json:"type"`
	At                time.Time       `json:"at"`
	TurnIndex         *int            `json:"turn_index,omitempty"`
	Summary           string          `json:"summary,omitempty"`
	Data              json.RawMessage `json:"data,omitempty"`
}

// SessionEventParams is a protocol value.
type SessionEventParams struct {
	Event SessionEvent `json:"event"`
}

// TestOutcome is a protocol value.
type TestOutcome string

// TestPassed is a protocol constant.
const (
	TestPassed  TestOutcome = "passed"
	TestFailed  TestOutcome = "failed"
	TestBlocked TestOutcome = "blocked"

	// Descriptive aliases keep call sites explicit while the short names remain
	// convenient alongside the other protocol state constants.
	TestOutcomePassed  = TestPassed
	TestOutcomeFailed  = TestFailed
	TestOutcomeBlocked = TestBlocked
)

// TestResult is a protocol value.
type TestResult struct {
	ID                string      `json:"id"`
	Actor             string      `json:"actor"`
	SessionGeneration uint64      `json:"session_generation"`
	TurnID            string      `json:"turn_id,omitempty"`
	TurnIndex         *int        `json:"turn_index,omitempty"`
	ToolCallID        string      `json:"tool_call_id,omitempty"`
	Command           string      `json:"command"`
	CWD               string      `json:"cwd"`
	ExitCode          *int        `json:"exit_code,omitempty"`
	Outcome           TestOutcome `json:"outcome"`
	StartedAt         time.Time   `json:"started_at"`
	CompletedAt       time.Time   `json:"completed_at"`
	DurationMillis    int64       `json:"duration_ms,omitempty"`
	OutputExcerpt     string      `json:"output_excerpt,omitempty"`
	OutputSHA256      string      `json:"output_sha256,omitempty"`
	OutputBytes       int64       `json:"output_bytes,omitempty"`
	OutputTruncated   bool        `json:"output_truncated,omitempty"`
	RepositoryUUID    string      `json:"repository_uuid"`
	WorkspaceUUID     string      `json:"workspace_uuid"`
	Git               *GitContext `json:"git,omitempty"`
	JJ                *JJContext  `json:"jj,omitempty"`
}

func inferTestOutcome(exitCode *int) TestOutcome {
	if exitCode == nil {
		return TestBlocked
	}
	if *exitCode == 0 {
		return TestPassed
	}
	return TestFailed
}

func validateTestOutcome(result *TestResult) error {
	switch result.Outcome {
	case TestPassed:
		if result.ExitCode == nil || *result.ExitCode != 0 {
			return fmt.Errorf("test outcome %q requires exit_code 0", result.Outcome)
		}
	case TestFailed:
		if result.ExitCode == nil || *result.ExitCode == 0 {
			return fmt.Errorf("test outcome %q requires a non-zero exit_code", result.Outcome)
		}
	case TestBlocked:
		if result.ExitCode != nil {
			return fmt.Errorf("test outcome %q requires no exit_code", result.Outcome)
		}
	default:
		return fmt.Errorf("invalid test outcome %q", result.Outcome)
	}
	return nil
}

// NormalizeTestResult fills the outcome for legacy events and enforces the
// unambiguous relationship between an outcome and process exit status.
func NormalizeTestResult(result *TestResult) error {
	if result.Outcome == "" {
		result.Outcome = inferTestOutcome(result.ExitCode)
	}
	return validateTestOutcome(result)
}

// CheckpointClaimStatus is a protocol value.
type CheckpointClaimStatus string

// ClaimAsserted is a protocol constant.
const (
	ClaimAsserted CheckpointClaimStatus = "asserted"
	ClaimVerified CheckpointClaimStatus = "verified"
	ClaimFailed   CheckpointClaimStatus = "failed"
	ClaimBlocked  CheckpointClaimStatus = "blocked"
)

// CheckpointEvidenceRef is a protocol value.
type CheckpointEvidenceRef struct {
	Kind    string `json:"kind"`
	Ordinal int    `json:"ordinal"`
}

// CheckpointClaim is a protocol value.
type CheckpointClaim struct {
	Kind      string                  `json:"kind"`
	Statement string                  `json:"statement"`
	Status    CheckpointClaimStatus   `json:"status"`
	Evidence  []CheckpointEvidenceRef `json:"evidence,omitempty"`
}

// ValidCheckpointClaimKind reports whether a claim kind is part of the stable protocol vocabulary.
func ValidCheckpointClaimKind(kind string) bool {
	switch kind {
	case "summary", "implementation", "test", "build", "runtime", "review", "decision", "blocked", "mutation", "message", "collision":
		return true
	default:
		return false
	}
}

// CheckpointRequest is a protocol value.
type CheckpointRequest struct {
	ID                string            `json:"id"`
	Actor             string            `json:"actor"`
	DeclaredBy        string            `json:"declared_by,omitempty"`
	SessionGeneration uint64            `json:"session_generation"`
	RepositoryUUID    string            `json:"repository_uuid"`
	WorkspaceUUID     string            `json:"workspace_uuid"`
	WorkUnitUUID      string            `json:"work_unit_uuid,omitempty"`
	CheckpointKind    string            `json:"checkpoint_kind"`
	JournalStart      uint64            `json:"journal_start_sequence"`
	JournalEnd        uint64            `json:"journal_end_sequence"`
	BoundaryEventID   string            `json:"boundary_event_id,omitempty"`
	BoundaryType      string            `json:"boundary_type,omitempty"`
	TurnID            string            `json:"turn_id,omitempty"`
	TurnIndex         *int              `json:"turn_index,omitempty"`
	CompactionEventID string            `json:"compaction_event_id,omitempty"`
	MutationIDs       []string          `json:"mutation_ids,omitempty"`
	MessageIDs        []string          `json:"message_ids,omitempty"`
	CollisionIDs      []string          `json:"collision_ids,omitempty"`
	TestResultIDs     []string          `json:"test_result_ids,omitempty"`
	Claims            []CheckpointClaim `json:"claims,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
	Git               *GitContext       `json:"git,omitempty"`
	JJ                *JJContext        `json:"jj,omitempty"`
}

// CheckpointRequestParams is a protocol value.
type CheckpointRequestParams struct {
	Request CheckpointRequest `json:"request"`
}

// DirectionState is the lifecycle state of a project-sized coordination
// record. A Direction intentionally has no repository or workspace scope: it
// may coordinate WorkUnits across either or both.
type DirectionState string

// DirectionDraft is a protocol constant.
const (
	DirectionDraft      DirectionState = "draft"
	DirectionActive     DirectionState = "active"
	DirectionPaused     DirectionState = "paused"
	DirectionConverging DirectionState = "converging"
	DirectionVerified   DirectionState = "verified"
	DirectionCompleted  DirectionState = "completed"
	DirectionAbandoned  DirectionState = "abandoned"
)

// Direction is a protocol value.
type Direction struct {
	UUID            string         `json:"direction_uuid"`
	Objective       string         `json:"objective"`
	SuccessCriteria string         `json:"success_criteria,omitempty"`
	Constraints     string         `json:"constraints,omitempty"`
	Context         string         `json:"context,omitempty"`
	State           DirectionState `json:"state"`
	CreatedBy       string         `json:"created_by"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
	CompletedAt     *time.Time     `json:"completed_at,omitempty"`
}

// DirectionActor is a protocol value.
type DirectionActor struct {
	DirectionUUID      string     `json:"direction_uuid"`
	Actor              string     `json:"actor"`
	JoinedAt           time.Time  `json:"joined_at"`
	LeftAt             *time.Time `json:"left_at,omitempty"`
	ParticipationState string     `json:"participation_state"`
}

// DirectionCreatedEvent is a protocol value.
type DirectionCreatedEvent struct {
	Direction Direction `json:"direction"`
}

// DirectionUpdatedEvent is a protocol value.
type DirectionUpdatedEvent struct {
	UUID     string    `json:"direction_uuid"`
	Actor    string    `json:"actor"`
	Previous Direction `json:"previous"`
	Result   Direction `json:"result"`
}

// DirectionActorEvent is a protocol value.
type DirectionActorEvent struct {
	DirectionUUID string          `json:"direction_uuid"`
	Actor         string          `json:"actor"`
	At            time.Time       `json:"at"`
	Previous      *DirectionActor `json:"previous,omitempty"`
	Result        DirectionActor  `json:"result"`
}

// DirectionTransitionEvent is a protocol value.
type DirectionTransitionEvent struct {
	DirectionUUID string         `json:"direction_uuid"`
	Actor         string         `json:"actor"`
	From          DirectionState `json:"from"`
	To            DirectionState `json:"to"`
	At            time.Time      `json:"at"`
}

// DirectionCreateParams is a protocol value.
type DirectionCreateParams struct {
	Direction Direction `json:"direction"`
}

// DirectionUpdateParams is a protocol value.
type DirectionUpdateParams struct {
	DirectionUUID   string  `json:"direction_uuid"`
	Actor           string  `json:"actor"`
	Objective       *string `json:"objective,omitempty"`
	SuccessCriteria *string `json:"success_criteria,omitempty"`
	Constraints     *string `json:"constraints,omitempty"`
	Context         *string `json:"context,omitempty"`
}

// DirectionActorParams is a protocol value.
type DirectionActorParams struct {
	DirectionUUID string `json:"direction_uuid"`
	Actor         string `json:"actor"`
}

// DirectionTransitionParams is a protocol value.
type DirectionTransitionParams struct {
	DirectionUUID string         `json:"direction_uuid"`
	Actor         string         `json:"actor"`
	State         DirectionState `json:"state"`
}

// WorkUnitState is a protocol value.
type WorkUnitState string

// WorkUnitProposed is a protocol constant.
const (
	WorkUnitProposed  WorkUnitState = "proposed"
	WorkUnitActive    WorkUnitState = "active"
	WorkUnitBlocked   WorkUnitState = "blocked"
	WorkUnitVerified  WorkUnitState = "verified"
	WorkUnitCompleted WorkUnitState = "completed"
	WorkUnitAbandoned WorkUnitState = "abandoned"
)

// WorkUnit is a protocol value.
type WorkUnit struct {
	UUID               string        `json:"work_unit_uuid"`
	DirectionUUID      string        `json:"direction_uuid,omitempty"`
	RepositoryUUID     string        `json:"repository_uuid"`
	WorkspaceUUID      string        `json:"workspace_uuid"`
	Objective          string        `json:"objective"`
	AcceptanceCriteria string        `json:"acceptance_criteria,omitempty"`
	Context            string        `json:"context,omitempty"`
	State              WorkUnitState `json:"state"`
	CreatedBy          string        `json:"created_by"`
	CreatedAt          time.Time     `json:"created_at"`
	UpdatedAt          time.Time     `json:"updated_at"`
	CompletedAt        *time.Time    `json:"completed_at,omitempty"`
}

// WorkUnitActor is a protocol value.
type WorkUnitActor struct {
	WorkUnitUUID       string     `json:"work_unit_uuid"`
	Actor              string     `json:"actor"`
	JoinedAt           time.Time  `json:"joined_at"`
	LeftAt             *time.Time `json:"left_at,omitempty"`
	ParticipationState string     `json:"participation_state"`
}

// WorkUnitCreatedEvent is a protocol value.
type WorkUnitCreatedEvent struct {
	WorkUnit WorkUnit `json:"work_unit"`
}

// WorkUnitUpdatedEvent is a protocol value.
type WorkUnitUpdatedEvent struct {
	UUID     string   `json:"work_unit_uuid"`
	Actor    string   `json:"actor"`
	Previous WorkUnit `json:"previous"`
	Result   WorkUnit `json:"result"`
}

// WorkUnitActorEvent is a protocol value.
type WorkUnitActorEvent struct {
	WorkUnitUUID string         `json:"work_unit_uuid"`
	Actor        string         `json:"actor"`
	At           time.Time      `json:"at"`
	Previous     *WorkUnitActor `json:"previous,omitempty"`
	Result       WorkUnitActor  `json:"result"`
}

// WorkUnitTransitionEvent is a protocol value.
type WorkUnitTransitionEvent struct {
	WorkUnitUUID string        `json:"work_unit_uuid"`
	Actor        string        `json:"actor"`
	From         WorkUnitState `json:"from"`
	To           WorkUnitState `json:"to"`
	At           time.Time     `json:"at"`
}

// WorkUnitCreateParams is a protocol value.
type WorkUnitCreateParams struct {
	WorkUnit WorkUnit `json:"work_unit"`
}

// WorkUnitUpdateParams is a protocol value.
type WorkUnitUpdateParams struct {
	WorkUnitUUID       string  `json:"work_unit_uuid"`
	Actor              string  `json:"actor"`
	Objective          *string `json:"objective,omitempty"`
	AcceptanceCriteria *string `json:"acceptance_criteria,omitempty"`
	Context            *string `json:"context,omitempty"`
}

// WorkUnitActorParams is a protocol value.
type WorkUnitActorParams struct {
	WorkUnitUUID string `json:"work_unit_uuid"`
	Actor        string `json:"actor"`
}

// WorkUnitTransitionParams is a protocol value.
type WorkUnitTransitionParams struct {
	WorkUnitUUID string        `json:"work_unit_uuid"`
	Actor        string        `json:"actor"`
	State        WorkUnitState `json:"state"`
}

// TransitionParams is a protocol value.
type TransitionParams struct {
	CollisionID string         `json:"collision_id"`
	Actor       string         `json:"actor"`
	State       CollisionState `json:"state"`
	Owner       string         `json:"owner,omitempty"`
	Resolution  string         `json:"resolution,omitempty"`
}
