package protocol

import (
	"encoding/json"
	"time"
)

const Version = 1

type Request struct {
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

type Response struct {
	ID     string    `json:"id"`
	Result any       `json:"result,omitempty"`
	Error  *RPCError `json:"error,omitempty"`
}

type RPCError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type GitContext struct {
	RepoRoot     string `json:"repo_root"`
	WorktreeRoot string `json:"worktree_root"`
	GitDir       string `json:"git_dir"`
	CommonDir    string `json:"common_dir"`
	Branch       string `json:"branch,omitempty"`
	Head         string `json:"head,omitempty"`
	Detached     bool   `json:"detached"`
}

type JJContext struct {
	RepoPath      string `json:"repo_path,omitempty"`
	WorkspaceName string `json:"workspace_name,omitempty"`
	WorkspaceRoot string `json:"workspace_root"`
	ChangeID      string `json:"change_id"`
	CommitID      string `json:"commit_id,omitempty"`
}

type Actor struct {
	Address          string      `json:"address"`
	Harness          string      `json:"harness"`
	SessionID        string      `json:"session_id"`
	SessionFile      string      `json:"session_file,omitempty"`
	Alias            string      `json:"alias,omitempty"`
	CWD              string      `json:"cwd"`
	PaneID           string      `json:"pane_id,omitempty"`
	HerdrWorkspaceID string      `json:"herdr_workspace_id,omitempty"`
	State            string      `json:"state"`
	RepositoryID     string      `json:"repository_id"`
	RepositoryRoot   string      `json:"repository_root"`
	WorkspaceID      string      `json:"workspace_id"`
	WorkspaceRoot    string      `json:"workspace_root"`
	WorkspaceKind    string      `json:"workspace_kind"`
	Git              *GitContext `json:"git,omitempty"`
	JJ               *JJContext  `json:"jj,omitempty"`
	Capabilities     []string    `json:"capabilities,omitempty"`
	Generation       uint64      `json:"generation"`
	StartedAt        time.Time   `json:"started_at"`
	HeartbeatAt      time.Time   `json:"heartbeat_at"`
}

type IntentContext struct {
	AssistantExcerpt string `json:"assistant_excerpt,omitempty"`
}

type FileSnapshot struct {
	Path       string `json:"path"`
	Exists     bool   `json:"exists"`
	Kind       string `json:"kind,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
	Size       int64  `json:"size,omitempty"`
	ModifiedAt string `json:"modified_at,omitempty"`
}

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
	RepositoryID      string         `json:"repository_id"`
	RepositoryRoot    string         `json:"repository_root"`
	WorkspaceID       string         `json:"workspace_id"`
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

type CollisionState string

const (
	CollisionOpen        CollisionState = "open"
	CollisionNegotiating CollisionState = "negotiating"
	CollisionYielded     CollisionState = "yielded"
	CollisionResolved    CollisionState = "resolved"
)

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
}

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

type Event struct {
	Version  int             `json:"version"`
	Sequence uint64          `json:"sequence"`
	Type     string          `json:"type"`
	At       time.Time       `json:"at"`
	Data     json.RawMessage `json:"data"`
}

type RegisterParams struct {
	Actor Actor `json:"actor"`
}

type ScopeFilter struct {
	RepositoryID string `json:"repository_id,omitempty"`
	WorkspaceID  string `json:"workspace_id,omitempty"`
	Directory    string `json:"directory,omitempty"`
}

type HeartbeatParams struct {
	Address    string      `json:"address"`
	State      string      `json:"state"`
	CWD        string      `json:"cwd,omitempty"`
	Git        *GitContext `json:"git,omitempty"`
	JJ         *JJContext  `json:"jj,omitempty"`
	Generation uint64      `json:"generation,omitempty"`
}

type SendParams struct {
	ID                string `json:"id,omitempty"`
	From              string `json:"from"`
	To                string `json:"to"`
	Body              string `json:"body"`
	ClientSequence    uint64 `json:"client_sequence,omitempty"`
	SessionGeneration uint64 `json:"session_generation,omitempty"`
}

type PollParams struct {
	Actor string `json:"actor"`
	Limit int    `json:"limit,omitempty"`
}

type AckParams struct {
	Actor      string   `json:"actor"`
	MessageIDs []string `json:"message_ids"`
}

type IntentBeginParams struct {
	Intent Intent `json:"intent"`
}

type IntentEndParams struct {
	IntentID    string         `json:"intent_id"`
	Success     bool           `json:"success"`
	Error       string         `json:"error,omitempty"`
	After       []FileSnapshot `json:"after,omitempty"`
	GitAfter    *GitContext    `json:"git_after,omitempty"`
	JJAfter     *JJContext     `json:"jj_after,omitempty"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
}

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

type SessionEventParams struct {
	Event SessionEvent `json:"event"`
}

type TransitionParams struct {
	CollisionID string         `json:"collision_id"`
	Actor       string         `json:"actor"`
	State       CollisionState `json:"state"`
	Owner       string         `json:"owner,omitempty"`
	Resolution  string         `json:"resolution,omitempty"`
}
