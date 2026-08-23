package provenance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

// MutationRecord describes a recorded mutation.
type MutationRecord struct {
	ID                string          `json:"id"`
	Actor             string          `json:"actor"`
	SessionGeneration uint64          `json:"session_generation"`
	TurnID            string          `json:"turn_id,omitempty"`
	TurnIndex         *int            `json:"turn_index,omitempty"`
	ToolCallID        string          `json:"tool_call_id"`
	Tool              string          `json:"tool"`
	Operation         string          `json:"operation"`
	CWD               string          `json:"cwd"`
	RepositoryUUID    string          `json:"repository_uuid,omitempty"`
	RepositoryRoot    string          `json:"repository_root,omitempty"`
	WorkspaceUUID     string          `json:"workspace_uuid,omitempty"`
	WorkspaceRoot     string          `json:"workspace_root,omitempty"`
	WorkspaceKind     string          `json:"workspace_kind,omitempty"`
	WorkspaceKey      string          `json:"workspace_key"`
	Paths             json.RawMessage `json:"paths"`
	RelativePaths     json.RawMessage `json:"relative_paths"`
	AssistantExcerpt  string          `json:"assistant_excerpt,omitempty"`
	StartedAt         string          `json:"started_at"`
	CompletedAt       string          `json:"completed_at,omitempty"`
	Success           *bool           `json:"success,omitempty"`
	Error             string          `json:"error,omitempty"`
	Before            json.RawMessage `json:"before"`
	After             json.RawMessage `json:"after"`
	GitBefore         json.RawMessage `json:"git_before,omitempty"`
	GitAfter          json.RawMessage `json:"git_after,omitempty"`
	JJBefore          json.RawMessage `json:"jj_before,omitempty"`
	JJAfter           json.RawMessage `json:"jj_after,omitempty"`
	UpdatedSequence   uint64          `json:"updated_sequence"`
}

// ActorScope identifies a repository and workspace scope.
type ActorScope struct {
	RepositoryUUID string
	WorkspaceUUID  string
}

func selectedScope(scopes []ActorScope) ActorScope {
	if len(scopes) > 0 {
		return scopes[0]
	}
	return ActorScope{}
}

// MutationFilter selects mutations for a query.
type MutationFilter struct {
	Actor          string
	Path           string
	RepositoryUUID string
	WorkspaceUUID  string
	Limit          int
	Failed         bool
}

// EventRecord describes a projected event.
type EventRecord struct {
	Sequence uint64          `json:"sequence"`
	Type     string          `json:"type"`
	At       string          `json:"at"`
	Data     json.RawMessage `json:"data"`
}

// Status reports provenance database projection counts.
type Status struct {
	DatabasePath      string `json:"database_path"`
	ProjectedSequence uint64 `json:"projected_sequence"`
	Events            int64  `json:"events"`
	Actors            int64  `json:"actors"`
	Repositories      int64  `json:"repositories"`
	Workspaces        int64  `json:"workspaces"`
	Mutations         int64  `json:"mutations"`
	SessionEvents     int64  `json:"session_events"`
	Checkpoints       int64  `json:"checkpoints"`
	Collisions        int64  `json:"collisions"`
}

// WorkUnitRecord describes a projected work unit.
type WorkUnitRecord struct {
	UUID               string                   `json:"work_unit_uuid"`
	DirectionUUID      string                   `json:"direction_uuid,omitempty"`
	RepositoryUUID     string                   `json:"repository_uuid"`
	RepositoryRoot     string                   `json:"repository_root,omitempty"`
	WorkspaceUUID      string                   `json:"workspace_uuid"`
	WorkspaceRoot      string                   `json:"workspace_root,omitempty"`
	WorkspaceKind      string                   `json:"workspace_kind,omitempty"`
	Objective          string                   `json:"objective"`
	AcceptanceCriteria string                   `json:"acceptance_criteria,omitempty"`
	Context            string                   `json:"context,omitempty"`
	State              protocol.WorkUnitState   `json:"state"`
	CreatedBy          string                   `json:"created_by"`
	CreatedAt          string                   `json:"created_at"`
	UpdatedAt          string                   `json:"updated_at"`
	CompletedAt        string                   `json:"completed_at,omitempty"`
	Tickets            protocol.Tickets         `json:"tickets,omitempty"`
	Participants       []protocol.WorkUnitActor `json:"participants"`
	Checkpoints        []CheckpointRecord       `json:"checkpoints"`
	CheckpointCount    int                      `json:"checkpoint_count"`
	LatestCheckpoint   *CheckpointRecord        `json:"latest_checkpoint,omitempty"`
}

// DirectionEvidenceSummary is derived from projected checkpoints and claims.
// It is informational and never changes lifecycle authority.
type DirectionEvidenceSummary struct {
	Checkpoints    int `json:"checkpoints"`
	Claims         int `json:"claims"`
	AssertedClaims int `json:"asserted_claims"`
	VerifiedClaims int `json:"verified_claims"`
	FailedClaims   int `json:"failed_claims"`
	BlockedClaims  int `json:"blocked_claims"`
}

// DirectionReadinessSummary describes whether projected evidence is sufficient
// for a direction to be considered ready. It is a derived query result.
type DirectionReadinessSummary struct {
	Ready           bool     `json:"ready"`
	WorkUnitsReady  int      `json:"work_units_ready"`
	BlockingReasons []string `json:"blocking_reasons,omitempty"`
}

// DirectionStatus is the deterministic, read-only rollup for a direction.
type DirectionStatus struct {
	Direction protocol.Direction        `json:"direction"`
	WorkUnits []WorkUnitRecord          `json:"work_units"`
	Evidence  DirectionEvidenceSummary  `json:"evidence"`
	Readiness DirectionReadinessSummary `json:"readiness"`
}

// Direction returns the direction identified by uuid.
func (d *DB) Direction(uuid string) (protocol.Direction, error) {
	var direction protocol.Direction
	var id, creator []byte
	var createdAt, updatedAt string
	var completed sql.NullString
	var tickets string
	err := d.db.QueryRowContext(context.Background(), `SELECT direction_uuid, objective, COALESCE(success_criteria, ''), COALESCE(constraints, ''), COALESCE(context, ''), state, created_by, created_at, updated_at, completed_at, tickets_json FROM directions WHERE direction_uuid = ?`, uuidBlob(uuid)).Scan(
		&id, &direction.Objective, &direction.SuccessCriteria, &direction.Constraints, &direction.Context,
		&direction.State, &creator, &createdAt, &updatedAt, &completed, &tickets)
	if errors.Is(err, sql.ErrNoRows) {
		return direction, fmt.Errorf("unknown direction %q", uuid)
	}
	if err != nil {
		return direction, err
	}
	direction.UUID, direction.CreatedBy = uuidString(id), uuidString(creator)
	direction.CreatedAt = parseTime(createdAt)
	direction.UpdatedAt = parseTime(updatedAt)
	if completed.Valid {
		at := parseTime(completed.String)
		direction.CompletedAt = &at
	}
	if err := json.Unmarshal([]byte(tickets), &direction.Tickets); err != nil {
		return direction, err
	}
	return direction, nil
}

// WorkUnit returns the work unit identified by uuid.
//
//nolint:cyclop // query hydration keeps the direct and rollup APIs consistent.
func (d *DB) WorkUnit(uuid string) (WorkUnitRecord, error) {
	var record WorkUnitRecord
	var id, direction, repo, workspace, creator []byte
	var repositoryRoot, workspaceRoot, workspaceKind sql.NullString
	err := d.db.QueryRowContext(context.Background(), `SELECT u.work_unit_uuid, u.direction_uuid, u.repository_uuid, u.workspace_uuid,
		r.root, w.root, w.kind, u.objective, COALESCE(u.acceptance_criteria,''), COALESCE(u.context,''), u.state,
		u.created_by, u.created_at, u.updated_at, COALESCE(u.completed_at,''), u.tickets_json
		FROM work_units u
		LEFT JOIN repositories r ON r.id = u.repository_uuid
		LEFT JOIN workspaces w ON w.id = u.workspace_uuid
		WHERE u.work_unit_uuid=?`, uuidBlob(uuid)).Scan(&id, &direction, &repo, &workspace,
		&repositoryRoot, &workspaceRoot, &workspaceKind, &record.Objective, &record.AcceptanceCriteria,
		&record.Context, &record.State, &creator, &record.CreatedAt, &record.UpdatedAt, &record.CompletedAt, &record.Tickets)
	if errors.Is(err, sql.ErrNoRows) {
		return record, fmt.Errorf("unknown work unit %q", uuid)
	}
	if err != nil {
		return record, err
	}
	record.UUID, record.DirectionUUID, record.RepositoryUUID, record.WorkspaceUUID, record.CreatedBy = uuidString(id), uuidString(direction), uuidString(repo), uuidString(workspace), uuidString(creator)
	if repositoryRoot.Valid {
		record.RepositoryRoot = repositoryRoot.String
	}
	if workspaceRoot.Valid {
		record.WorkspaceRoot = workspaceRoot.String
	}
	if workspaceKind.Valid {
		record.WorkspaceKind = workspaceKind.String
	}
	rows, err := d.db.QueryContext(context.Background(), `SELECT actor_uuid, joined_at, left_at, participation_state FROM work_unit_actors WHERE work_unit_uuid=? ORDER BY actor_uuid`, uuidBlob(uuid))
	if err != nil {
		return record, err
	}
	defer closeRows(rows)
	record.Participants = []protocol.WorkUnitActor{}
	for rows.Next() {
		var actor []byte
		var joined, state string
		var left sql.NullString
		if err := rows.Scan(&actor, &joined, &left, &state); err != nil {
			return record, err
		}
		value := protocol.WorkUnitActor{WorkUnitUUID: record.UUID, Actor: uuidString(actor), JoinedAt: parseTime(joined), ParticipationState: state}
		if left.Valid {
			at := parseTime(left.String)
			value.LeftAt = &at
		}
		record.Participants = append(record.Participants, value)
	}
	if err := rows.Err(); err != nil {
		return record, err
	}
	record.Checkpoints, err = d.ListCheckpoints(uuid, 1000)
	if err != nil {
		return record, err
	}
	record.CheckpointCount = len(record.Checkpoints)
	if record.CheckpointCount > 0 {
		record.LatestCheckpoint = &record.Checkpoints[record.CheckpointCount-1]
	}
	return record, nil
}

// DirectionStatus returns a deterministic, read-only rollup of a direction
// and all attached work units, including work units in other repositories.
func (d *DB) DirectionStatus(uuid string) (DirectionStatus, error) {
	result := DirectionStatus{WorkUnits: make([]WorkUnitRecord, 0)}
	direction, err := d.Direction(uuid)
	if err != nil {
		return result, err
	}
	result.Direction = direction
	workUnits, err := d.directionWorkUnitUUIDs(uuid)
	if err != nil {
		return result, err
	}
	for _, workUnit := range workUnits {
		record, evidence, err := d.directionWorkUnitRollup(workUnit)
		if err != nil {
			return result, err
		}
		result.WorkUnits = append(result.WorkUnits, record)
		mergeDirectionEvidence(&result.Evidence, &evidence)
	}
	result.Readiness = deriveDirectionReadiness(result.WorkUnits, result.Evidence)
	return result, nil
}

func (d *DB) directionWorkUnitUUIDs(directionUUID string) ([][]byte, error) {
	rows, err := d.db.QueryContext(context.Background(), `SELECT work_unit_uuid FROM work_units WHERE direction_uuid = ? ORDER BY repository_uuid, workspace_uuid, work_unit_uuid`, uuidBlob(directionUUID))
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	var result [][]byte
	for rows.Next() {
		var workUnit []byte
		if err := rows.Scan(&workUnit); err != nil {
			return nil, err
		}
		result = append(result, append([]byte(nil), workUnit...))
	}
	return result, rows.Err()
}

func (d *DB) directionWorkUnitRollup(workUnit []byte) (WorkUnitRecord, DirectionEvidenceSummary, error) {
	record, err := d.WorkUnit(uuidString(workUnit))
	if err != nil {
		return record, DirectionEvidenceSummary{}, err
	}
	if err := d.populateDirectionCheckpointSummary(workUnit, &record); err != nil {
		return record, DirectionEvidenceSummary{}, err
	}
	evidence, err := d.directionClaimEvidence(workUnit)
	if err != nil {
		return record, evidence, err
	}
	evidence.Checkpoints = record.CheckpointCount
	return record, evidence, nil
}

func (d *DB) populateDirectionCheckpointSummary(workUnit []byte, record *WorkUnitRecord) error {
	if err := d.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM checkpoint_requests WHERE work_unit_uuid = ?`, workUnit).Scan(&record.CheckpointCount); err != nil {
		return err
	}
	if record.CheckpointCount == 0 {
		return nil
	}
	if record.CheckpointCount <= len(record.Checkpoints) {
		record.LatestCheckpoint = &record.Checkpoints[len(record.Checkpoints)-1]
		return nil
	}
	row := d.db.QueryRowContext(context.Background(), `SELECT id, actor, declared_by, session_generation, repository_uuid, workspace_uuid,
		COALESCE(work_unit_uuid, ''), checkpoint_kind, journal_start_sequence, journal_end_sequence,
		data, event_sequence FROM checkpoint_requests WHERE work_unit_uuid = ? ORDER BY event_sequence DESC, id DESC LIMIT 1`, workUnit)
	latest, err := scanCheckpoint(row)
	if err != nil {
		return err
	}
	if err := d.hydrateCheckpoint(&latest); err != nil {
		return err
	}
	record.LatestCheckpoint = &latest
	return nil
}

func (d *DB) directionClaimEvidence(workUnit []byte) (DirectionEvidenceSummary, error) {
	var evidence DirectionEvidenceSummary
	rows, err := d.db.QueryContext(context.Background(), `SELECT c.status FROM checkpoint_claims c JOIN checkpoint_requests p ON p.id = c.checkpoint_id WHERE p.work_unit_uuid = ? ORDER BY p.event_sequence, p.id, c.ordinal`, workUnit)
	if err != nil {
		return evidence, err
	}
	defer closeRows(rows)
	for rows.Next() {
		var status protocol.CheckpointClaimStatus
		if err := rows.Scan(&status); err != nil {
			return evidence, err
		}
		evidence.Claims++
		incrementDirectionClaimStatus(&evidence, status)
	}
	return evidence, rows.Err()
}

func incrementDirectionClaimStatus(evidence *DirectionEvidenceSummary, status protocol.CheckpointClaimStatus) {
	switch status {
	case protocol.ClaimAsserted:
		evidence.AssertedClaims++
	case protocol.ClaimVerified:
		evidence.VerifiedClaims++
	case protocol.ClaimFailed:
		evidence.FailedClaims++
	case protocol.ClaimBlocked:
		evidence.BlockedClaims++
	}
}

func mergeDirectionEvidence(target, addition *DirectionEvidenceSummary) {
	target.Checkpoints += addition.Checkpoints
	target.Claims += addition.Claims
	target.AssertedClaims += addition.AssertedClaims
	target.VerifiedClaims += addition.VerifiedClaims
	target.FailedClaims += addition.FailedClaims
	target.BlockedClaims += addition.BlockedClaims
}

func deriveDirectionReadiness(workUnits []WorkUnitRecord, evidence DirectionEvidenceSummary) DirectionReadinessSummary {
	readiness := DirectionReadinessSummary{}
	if len(workUnits) == 0 {
		readiness.BlockingReasons = []string{"no work units attached"}
		return readiness
	}
	for index := range workUnits {
		unit := &workUnits[index]
		if unit.State == protocol.WorkUnitVerified || unit.State == protocol.WorkUnitCompleted {
			readiness.WorkUnitsReady++
		} else {
			readiness.BlockingReasons = append(readiness.BlockingReasons, "work unit "+unit.UUID+" is "+string(unit.State))
		}
	}
	if evidence.FailedClaims > 0 {
		readiness.BlockingReasons = append(readiness.BlockingReasons, "failed evidence claims")
	}
	if evidence.BlockedClaims > 0 {
		readiness.BlockingReasons = append(readiness.BlockingReasons, "blocked evidence claims")
	}
	readiness.Ready = readiness.WorkUnitsReady == len(workUnits) && len(readiness.BlockingReasons) == 0
	return readiness
}

func closeRows(rows *sql.Rows) {
	if err := rows.Close(); err != nil {
		return
	}
}

func parseTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

// CheckpointClaimRecord describes a checkpoint claim and its evidence.
type CheckpointClaimRecord struct {
	Kind      string                           `json:"kind"`
	Statement string                           `json:"statement"`
	Status    protocol.CheckpointClaimStatus   `json:"status"`
	Evidence  []protocol.CheckpointEvidenceRef `json:"evidence,omitempty"`
}

// CheckpointRecord describes a projected checkpoint.
type CheckpointRecord struct {
	ID                string                  `json:"id"`
	Actor             string                  `json:"actor"`
	DeclaredBy        string                  `json:"declared_by"`
	SessionGeneration uint64                  `json:"session_generation"`
	RepositoryUUID    string                  `json:"repository_uuid"`
	WorkspaceUUID     string                  `json:"workspace_uuid"`
	WorkUnitUUID      string                  `json:"work_unit_uuid,omitempty"`
	CheckpointKind    string                  `json:"checkpoint_kind"`
	JournalStart      uint64                  `json:"journal_start_sequence"`
	JournalEnd        uint64                  `json:"journal_end_sequence"`
	BoundaryEventID   string                  `json:"boundary_event_id,omitempty"`
	BoundaryType      string                  `json:"boundary_type,omitempty"`
	TurnID            string                  `json:"turn_id,omitempty"`
	TurnIndex         *int                    `json:"turn_index,omitempty"`
	CompactionEventID string                  `json:"compaction_event_id,omitempty"`
	MutationIDs       []string                `json:"mutation_ids,omitempty"`
	MessageIDs        []string                `json:"message_ids,omitempty"`
	CollisionIDs      []string                `json:"collision_ids,omitempty"`
	TestResultIDs     []string                `json:"test_result_ids,omitempty"`
	Claims            []CheckpointClaimRecord `json:"claims,omitempty"`
	Tickets           protocol.Tickets        `json:"tickets,omitempty"`
	Metadata          map[string]string       `json:"metadata,omitempty"`
	Git               *protocol.GitContext    `json:"git,omitempty"`
	JJ                *protocol.JJContext     `json:"jj,omitempty"`
	Data              json.RawMessage         `json:"data"`
	EventSequence     uint64                  `json:"event_sequence"`
}

// RepositoryRecord describes a projected repository.
type RepositoryRecord struct {
	ID           string `json:"id"`
	Root         string `json:"root"`
	Kind         string `json:"kind"`
	GitCommonDir string `json:"git_common_dir,omitempty"`
	JJRepoPath   string `json:"jj_repo_path,omitempty"`
}

// WorkspaceRecord describes a projected workspace.
type WorkspaceRecord struct {
	ID              string `json:"id"`
	RepositoryUUID  string `json:"repository_uuid"`
	Root            string `json:"root"`
	Kind            string `json:"kind"`
	GitBranch       string `json:"git_branch,omitempty"`
	GitHead         string `json:"git_head,omitempty"`
	JJWorkspaceName string `json:"jj_workspace_name,omitempty"`
	JJChangeID      string `json:"jj_change_id,omitempty"`
}

// ScopeRecords contains projected repositories and workspaces.
type ScopeRecords struct {
	Repositories []RepositoryRecord `json:"repositories"`
	Workspaces   []WorkspaceRecord  `json:"workspaces"`
}

// SessionRecord describes a projected session event.
type SessionRecord struct {
	ID                string          `json:"id"`
	Actor             string          `json:"actor"`
	SessionGeneration uint64          `json:"session_generation"`
	Type              string          `json:"type"`
	At                string          `json:"at"`
	TurnIndex         *int            `json:"turn_index,omitempty"`
	Summary           string          `json:"summary,omitempty"`
	Data              json.RawMessage `json:"data"`
	EventSequence     uint64          `json:"event_sequence"`
}

// Scopes returns all projected repositories and workspaces.
func (d *DB) Scopes() (ScopeRecords, error) {
	result := ScopeRecords{Repositories: make([]RepositoryRecord, 0), Workspaces: make([]WorkspaceRecord, 0)}
	repositories, err := d.db.QueryContext(context.Background(), `SELECT id, root, kind, COALESCE(git_common_dir, ''), COALESCE(jj_repo_path, '') FROM repositories ORDER BY root, id`)
	if err != nil {
		return result, err
	}
	defer closeRows(repositories)
	for repositories.Next() {
		var record RepositoryRecord
		var id []byte
		if err := repositories.Scan(&id, &record.Root, &record.Kind, &record.GitCommonDir, &record.JJRepoPath); err != nil {
			closeRows(repositories)
			return result, err
		}
		record.ID = uuidString(id)
		result.Repositories = append(result.Repositories, record)
	}
	if err := repositories.Err(); err != nil {
		closeRows(repositories)
		return result, err
	}
	workspaces, err := d.db.QueryContext(context.Background(), `SELECT id, repository_uuid, root, kind, COALESCE(git_branch, ''), COALESCE(git_head, ''),
		COALESCE(jj_workspace_name, ''), COALESCE(jj_change_id, '') FROM workspaces ORDER BY root, id`)
	if err != nil {
		return result, err
	}
	defer closeRows(workspaces)
	for workspaces.Next() {
		var record WorkspaceRecord
		var id, repositoryUUID []byte
		if err := workspaces.Scan(&id, &repositoryUUID, &record.Root, &record.Kind, &record.GitBranch,
			&record.GitHead, &record.JJWorkspaceName, &record.JJChangeID); err != nil {
			return result, err
		}
		record.ID = uuidString(id)
		record.RepositoryUUID = uuidString(repositoryUUID)
		result.Workspaces = append(result.Workspaces, record)
	}
	return result, workspaces.Err()
}

// ProjectedSequence returns the latest journal sequence reflected in this
// rebuildable projection.
func (d *DB) ProjectedSequence() (uint64, error) {
	var sequence uint64
	err := d.db.QueryRowContext(context.Background(), `SELECT COALESCE(MAX(sequence), 0) FROM events`).Scan(&sequence)
	return sequence, err
}

// Status returns projection status and record counts.
func (d *DB) Status() (Status, error) {
	status := Status{DatabasePath: d.path}
	err := d.db.QueryRowContext(context.Background(), `SELECT
		COALESCE((SELECT MAX(sequence) FROM events), 0),
		(SELECT COUNT(*) FROM events),
		(SELECT COUNT(*) FROM actors),
		(SELECT COUNT(*) FROM repositories),
		(SELECT COUNT(*) FROM workspaces),
		(SELECT COUNT(*) FROM mutations),
		(SELECT COUNT(*) FROM session_events),
		(SELECT COUNT(*) FROM checkpoint_requests),
		(SELECT COUNT(*) FROM collisions)`).Scan(
		&status.ProjectedSequence, &status.Events, &status.Actors, &status.Repositories, &status.Workspaces,
		&status.Mutations, &status.SessionEvents, &status.Checkpoints, &status.Collisions,
	)
	return status, err
}

// ListCheckpoints returns checkpoints, optionally filtered by work unit.
func (d *DB) ListCheckpoints(workUnitUUID string, limit int) ([]CheckpointRecord, error) {
	if limit <= 0 || limit > 1000 {
		limit = 50
	}
	query := `SELECT id, actor, declared_by, session_generation, repository_uuid, workspace_uuid,
		COALESCE(work_unit_uuid, ''), checkpoint_kind, journal_start_sequence, journal_end_sequence,
		data, event_sequence FROM checkpoint_requests`
	arguments := []any{}
	if workUnitUUID != "" {
		query += ` WHERE work_unit_uuid = ?`
		arguments = append(arguments, uuidBlob(workUnitUUID))
	}
	if workUnitUUID != "" {
		query += ` ORDER BY event_sequence ASC, id ASC LIMIT ?`
	} else {
		query += ` ORDER BY journal_end_sequence DESC, event_sequence DESC, id DESC LIMIT ?`
	}
	arguments = append(arguments, limit)
	rows, err := d.db.QueryContext(context.Background(), query, arguments...)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	result := make([]CheckpointRecord, 0)
	for rows.Next() {
		record, err := scanCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		if err := d.hydrateCheckpoint(&record); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

// Checkpoint returns the checkpoint identified by id.
func (d *DB) Checkpoint(id string) (CheckpointRecord, error) {
	row := d.db.QueryRowContext(context.Background(), `SELECT id, actor, declared_by, session_generation, repository_uuid, workspace_uuid,
		COALESCE(work_unit_uuid, ''), checkpoint_kind, journal_start_sequence, journal_end_sequence,
		data, event_sequence FROM checkpoint_requests WHERE id = ?`, id)
	record, err := scanCheckpoint(row)
	if errors.Is(err, sql.ErrNoRows) {
		return CheckpointRecord{}, fmt.Errorf("unknown checkpoint %q", id)
	}
	if err != nil {
		return CheckpointRecord{}, err
	}
	if err := d.hydrateCheckpoint(&record); err != nil {
		return CheckpointRecord{}, err
	}
	return record, nil
}

func (d *DB) hydrateCheckpoint(record *CheckpointRecord) error {
	record.MutationIDs = nil
	record.MessageIDs = nil
	record.CollisionIDs = nil
	record.TestResultIDs = nil
	record.Claims = nil
	if err := d.hydrateCheckpointEvidence(record); err != nil {
		return err
	}
	if err := d.hydrateCheckpointClaims(record); err != nil {
		return err
	}
	return d.hydrateCheckpointMetadata(record)
}

func (d *DB) hydrateCheckpointEvidence(record *CheckpointRecord) error {
	rows, err := d.db.QueryContext(context.Background(), `SELECT kind, ref_text, ref_uuid FROM checkpoint_evidence WHERE checkpoint_id = ? ORDER BY kind, ordinal`, record.ID)
	if err != nil {
		return err
	}
	defer closeRows(rows)
	for rows.Next() {
		var kind string
		var text sql.NullString
		var binary []byte
		if err := rows.Scan(&kind, &text, &binary); err != nil {
			return err
		}
		ref := text.String
		if len(binary) > 0 {
			ref = uuidString(binary)
		}
		switch kind {
		case "mutation":
			record.MutationIDs = append(record.MutationIDs, ref)
		case "message":
			record.MessageIDs = append(record.MessageIDs, ref)
		case "collision":
			record.CollisionIDs = append(record.CollisionIDs, ref)
		case "test_result":
			record.TestResultIDs = append(record.TestResultIDs, ref)
		}
	}
	return rows.Err()
}

func (d *DB) hydrateCheckpointClaims(record *CheckpointRecord) error {
	claimRows, err := d.db.QueryContext(context.Background(), `SELECT ordinal, kind, statement, status FROM checkpoint_claims WHERE checkpoint_id=? ORDER BY ordinal`, record.ID)
	if err != nil {
		return err
	}
	defer closeRows(claimRows)
	for claimRows.Next() {
		var ordinal int
		var claim CheckpointClaimRecord
		if err := claimRows.Scan(&ordinal, &claim.Kind, &claim.Statement, &claim.Status); err != nil {
			return err
		}
		evidenceRows, err := d.db.QueryContext(context.Background(), `SELECT evidence_kind, evidence_ordinal FROM checkpoint_claim_evidence WHERE checkpoint_id=? AND claim_ordinal=? ORDER BY evidence_kind, evidence_ordinal`, record.ID, ordinal)
		if err != nil {
			return err
		}
		for evidenceRows.Next() {
			var ref protocol.CheckpointEvidenceRef
			if err := evidenceRows.Scan(&ref.Kind, &ref.Ordinal); err != nil {
				closeRows(evidenceRows)
				return err
			}
			claim.Evidence = append(claim.Evidence, ref)
		}
		if err := evidenceRows.Err(); err != nil {
			closeRows(evidenceRows)
			return err
		}
		closeRows(evidenceRows)
		record.Claims = append(record.Claims, claim)
	}
	return claimRows.Err()
}

func (d *DB) hydrateCheckpointMetadata(record *CheckpointRecord) error {
	record.Metadata = map[string]string{}
	metadata, err := d.db.QueryContext(context.Background(), `SELECT key, value FROM checkpoint_metadata WHERE checkpoint_id = ? ORDER BY key`, record.ID)
	if err != nil {
		return err
	}
	defer closeRows(metadata)
	for metadata.Next() {
		var key, value string
		if err := metadata.Scan(&key, &value); err != nil {
			return err
		}
		record.Metadata[key] = value
	}
	return metadata.Err()
}

func scanCheckpoint(row scanner) (CheckpointRecord, error) {
	var record CheckpointRecord
	var actor, repository, workspace, workUnit []byte
	var data string
	err := row.Scan(&record.ID, &actor, &record.DeclaredBy, &record.SessionGeneration, &repository, &workspace,
		&workUnit, &record.CheckpointKind, &record.JournalStart, &record.JournalEnd, &data, &record.EventSequence)
	if err != nil {
		return CheckpointRecord{}, err
	}
	record.Actor = uuidString(actor)
	record.RepositoryUUID = uuidString(repository)
	record.WorkspaceUUID = uuidString(workspace)
	record.WorkUnitUUID = uuidString(workUnit)
	record.Data = json.RawMessage(data)
	var checkpoint protocol.CheckpointRequest
	if err := json.Unmarshal(record.Data, &checkpoint); err != nil {
		return CheckpointRecord{}, fmt.Errorf("decode checkpoint data: %w", err)
	}
	record.BoundaryEventID = checkpoint.BoundaryEventID
	record.BoundaryType = checkpoint.BoundaryType
	record.TurnID = checkpoint.TurnID
	record.TurnIndex = checkpoint.TurnIndex
	record.CompactionEventID = checkpoint.CompactionEventID
	record.MutationIDs = checkpoint.MutationIDs
	record.MessageIDs = checkpoint.MessageIDs
	record.CollisionIDs = checkpoint.CollisionIDs
	record.TestResultIDs = checkpoint.TestResultIDs
	record.Tickets = checkpoint.Tickets
	record.Metadata = checkpoint.Metadata
	record.Git = checkpoint.Git
	record.JJ = checkpoint.JJ
	return record, nil
}

// ListMutations returns mutations matching filter.
//
//nolint:gocritic // MutationFilter is a stable value-type API boundary.
func (d *DB) ListMutations(filter MutationFilter) ([]MutationRecord, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 1000 {
		limit = 50
	}
	conditions := []string{"1=1"}
	arguments := make([]any, 0, 4)
	if filter.Actor != "" {
		actor, err := d.ResolveActorScoped(filter.Actor, filter.RepositoryUUID, filter.WorkspaceUUID)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, "m.actor = ?")
		arguments = append(arguments, uuidBlob(actor))
	}
	if filter.RepositoryUUID != "" {
		conditions = append(conditions, "m.repository_uuid = ?")
		arguments = append(arguments, uuidBlob(filter.RepositoryUUID))
	}
	if filter.WorkspaceUUID != "" {
		conditions = append(conditions, "m.workspace_uuid = ?")
		arguments = append(arguments, uuidBlob(filter.WorkspaceUUID))
	}
	if filter.Path != "" {
		conditions = append(conditions, "EXISTS (SELECT 1 FROM mutation_paths p WHERE p.mutation_id = m.id AND p.path = ?)")
		arguments = append(arguments, filter.Path)
	}
	if filter.Failed {
		conditions = append(conditions, "m.success = 0")
	}
	arguments = append(arguments, limit)
	rows, err := d.db.QueryContext(context.Background(), `SELECT
		m.id, m.actor, m.session_generation, COALESCE(m.turn_id, ''), m.turn_index, m.tool_call_id, m.tool, m.operation, m.cwd,
		COALESCE(m.repository_uuid, ''), COALESCE(m.repository_root, ''), COALESCE(m.workspace_uuid, ''),
		COALESCE(m.workspace_root, ''), COALESCE(m.workspace_kind, ''), m.workspace_key,
		m.paths_json, m.relative_paths_json, COALESCE(m.assistant_excerpt, ''), m.started_at, COALESCE(m.completed_at, ''),
		m.success, COALESCE(m.error, ''), m.before_json, m.after_json,
		COALESCE(m.git_before_json, ''), COALESCE(m.git_after_json, ''),
		COALESCE(m.jj_before_json, ''), COALESCE(m.jj_after_json, ''), m.updated_sequence
	FROM mutations m WHERE `+strings.Join(conditions, " AND ")+` ORDER BY m.started_at DESC, m.updated_sequence DESC, m.id DESC LIMIT ?`, arguments...)
	if err != nil {
		return nil, err
	}
	defer func() { closeRows(rows) }()
	result := make([]MutationRecord, 0)
	for rows.Next() {
		record, err := scanMutation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

// Mutation returns the mutation identified by id.
func (d *DB) Mutation(id string) (MutationRecord, error) {
	row := d.db.QueryRowContext(context.Background(), `SELECT
		id, actor, session_generation, COALESCE(turn_id, ''), turn_index, tool_call_id, tool, operation, cwd,
		COALESCE(repository_uuid, ''), COALESCE(repository_root, ''), COALESCE(workspace_uuid, ''),
		COALESCE(workspace_root, ''), COALESCE(workspace_kind, ''), workspace_key,
		paths_json, relative_paths_json, COALESCE(assistant_excerpt, ''), started_at, COALESCE(completed_at, ''),
		success, COALESCE(error, ''), before_json, after_json,
		COALESCE(git_before_json, ''), COALESCE(git_after_json, ''),
		COALESCE(jj_before_json, ''), COALESCE(jj_after_json, ''), updated_sequence
	FROM mutations WHERE id = ?`, id)
	record, err := scanMutation(row)
	if errors.Is(err, sql.ErrNoRows) {
		return MutationRecord{}, fmt.Errorf("unknown mutation %q", id)
	}
	return record, err
}

type scanner interface {
	Scan(...any) error
}

func scanMutation(row scanner) (MutationRecord, error) {
	var record MutationRecord
	var actor, repositoryUUID, workspaceUUID []byte
	var paths, relativePaths, before, after string
	var gitBefore, gitAfter, jjBefore, jjAfter string
	var success sql.NullBool
	var turnIndex sql.NullInt64
	err := row.Scan(&record.ID, &actor, &record.SessionGeneration, &record.TurnID, &turnIndex, &record.ToolCallID, &record.Tool,
		&record.Operation, &record.CWD, &repositoryUUID, &record.RepositoryRoot, &workspaceUUID,
		&record.WorkspaceRoot, &record.WorkspaceKind, &record.WorkspaceKey, &paths, &relativePaths, &record.AssistantExcerpt,
		&record.StartedAt, &record.CompletedAt, &success, &record.Error, &before, &after,
		&gitBefore, &gitAfter, &jjBefore, &jjAfter, &record.UpdatedSequence)
	if err != nil {
		return MutationRecord{}, err
	}
	record.Actor = uuidString(actor)
	record.RepositoryUUID = uuidString(repositoryUUID)
	record.WorkspaceUUID = uuidString(workspaceUUID)
	record.Paths = json.RawMessage(paths)
	record.RelativePaths = json.RawMessage(relativePaths)
	record.Before = json.RawMessage(before)
	record.After = json.RawMessage(after)
	record.GitBefore = rawOptional(gitBefore)
	record.GitAfter = rawOptional(gitAfter)
	record.JJBefore = rawOptional(jjBefore)
	record.JJAfter = rawOptional(jjAfter)
	if turnIndex.Valid {
		value := int(turnIndex.Int64)
		record.TurnIndex = &value
	}
	if success.Valid {
		value := success.Bool
		record.Success = &value
	}
	return record, nil
}

func rawOptional(value string) json.RawMessage {
	if value == "" {
		return nil
	}
	return json.RawMessage(value)
}

// Timeline returns events matching actor and eventType.
func (d *DB) Timeline(actor, eventType string, limit int, scopes ...ActorScope) ([]EventRecord, error) {
	if limit <= 0 || limit > 5000 {
		limit = 100
	}
	conditions := []string{"1=1"}
	arguments := make([]any, 0, 3)
	if actor != "" {
		scope := selectedScope(scopes)
		resolved, err := d.ResolveActorScoped(actor, scope.RepositoryUUID, scope.WorkspaceUUID)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, "COALESCE(json_extract(data, '$.actor'), json_extract(data, '$.address')) = ?")
		arguments = append(arguments, resolved)
	}
	if eventType != "" {
		conditions = append(conditions, "type = ?")
		arguments = append(arguments, eventType)
	}
	arguments = append(arguments, limit)
	rows, err := d.db.QueryContext(context.Background(), `SELECT sequence, type, at, data FROM events WHERE `+strings.Join(conditions, " AND ")+` ORDER BY sequence DESC LIMIT ?`, arguments...)
	if err != nil {
		return nil, err
	}
	defer func() { closeRows(rows) }()
	result := make([]EventRecord, 0)
	for rows.Next() {
		var record EventRecord
		var data string
		if err := rows.Scan(&record.Sequence, &record.Type, &record.At, &data); err != nil {
			return nil, err
		}
		record.Data = json.RawMessage(data)
		result = append(result, record)
	}
	return result, rows.Err()
}

// SessionEvents returns session events for actor.
func (d *DB) SessionEvents(actor string, limit int, scopes ...ActorScope) ([]SessionRecord, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	scope := selectedScope(scopes)
	resolved, err := d.ResolveActorScoped(actor, scope.RepositoryUUID, scope.WorkspaceUUID)
	if err != nil {
		return nil, err
	}
	rows, err := d.db.QueryContext(context.Background(), `SELECT id, actor, session_generation, type, at, turn_index,
		COALESCE(summary, ''), data, event_sequence FROM session_events WHERE actor = ? ORDER BY at DESC, event_sequence DESC, id DESC LIMIT ?`, uuidBlob(resolved), limit)
	if err != nil {
		return nil, err
	}
	defer func() { closeRows(rows) }()
	result := make([]SessionRecord, 0)
	for rows.Next() {
		var record SessionRecord
		var actor []byte
		var turnIndex sql.NullInt64
		var data string
		if err := rows.Scan(&record.ID, &actor, &record.SessionGeneration, &record.Type, &record.At,
			&turnIndex, &record.Summary, &data, &record.EventSequence); err != nil {
			return nil, err
		}
		record.Actor = uuidString(actor)
		if turnIndex.Valid {
			value := int(turnIndex.Int64)
			record.TurnIndex = &value
		}
		record.Data = json.RawMessage(data)
		result = append(result, record)
	}
	return result, rows.Err()
}
