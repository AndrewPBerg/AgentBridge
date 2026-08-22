package provenance

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

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

type MutationFilter struct {
	Actor          string
	Path           string
	RepositoryUUID string
	WorkspaceUUID  string
	Limit          int
	Failed         bool
}

type EventRecord struct {
	Sequence uint64          `json:"sequence"`
	Type     string          `json:"type"`
	At       string          `json:"at"`
	Data     json.RawMessage `json:"data"`
}

type Status struct {
	DatabasePath      string `json:"database_path"`
	ProjectedSequence uint64 `json:"projected_sequence"`
	Events            int64  `json:"events"`
	Actors            int64  `json:"actors"`
	Repositories      int64  `json:"repositories"`
	Workspaces        int64  `json:"workspaces"`
	Mutations         int64  `json:"mutations"`
	SessionEvents     int64  `json:"session_events"`
	Collisions        int64  `json:"collisions"`
}

type RepositoryRecord struct {
	ID           string `json:"id"`
	Root         string `json:"root"`
	Kind         string `json:"kind"`
	GitCommonDir string `json:"git_common_dir,omitempty"`
	JJRepoPath   string `json:"jj_repo_path,omitempty"`
}

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

type ScopeRecords struct {
	Repositories []RepositoryRecord `json:"repositories"`
	Workspaces   []WorkspaceRecord  `json:"workspaces"`
}

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

func (d *DB) Scopes() (ScopeRecords, error) {
	result := ScopeRecords{Repositories: make([]RepositoryRecord, 0), Workspaces: make([]WorkspaceRecord, 0)}
	repositories, err := d.db.Query(`SELECT id, root, kind, COALESCE(git_common_dir, ''), COALESCE(jj_repo_path, '') FROM repositories ORDER BY root, id`)
	if err != nil {
		return result, err
	}
	for repositories.Next() {
		var record RepositoryRecord
		if err := repositories.Scan(&record.ID, &record.Root, &record.Kind, &record.GitCommonDir, &record.JJRepoPath); err != nil {
			repositories.Close()
			return result, err
		}
		result.Repositories = append(result.Repositories, record)
	}
	if err := repositories.Close(); err != nil {
		return result, err
	}
	workspaces, err := d.db.Query(`SELECT id, repository_uuid, root, kind, COALESCE(git_branch, ''), COALESCE(git_head, ''),
		COALESCE(jj_workspace_name, ''), COALESCE(jj_change_id, '') FROM workspaces ORDER BY root, id`)
	if err != nil {
		return result, err
	}
	defer workspaces.Close()
	for workspaces.Next() {
		var record WorkspaceRecord
		if err := workspaces.Scan(&record.ID, &record.RepositoryUUID, &record.Root, &record.Kind, &record.GitBranch,
			&record.GitHead, &record.JJWorkspaceName, &record.JJChangeID); err != nil {
			return result, err
		}
		result.Workspaces = append(result.Workspaces, record)
	}
	return result, workspaces.Err()
}

func (d *DB) Status() (Status, error) {
	status := Status{DatabasePath: d.path}
	err := d.db.QueryRow(`SELECT
		COALESCE((SELECT MAX(sequence) FROM events), 0),
		(SELECT COUNT(*) FROM events),
		(SELECT COUNT(*) FROM actors),
		(SELECT COUNT(*) FROM repositories),
		(SELECT COUNT(*) FROM workspaces),
		(SELECT COUNT(*) FROM mutations),
		(SELECT COUNT(*) FROM session_events),
		(SELECT COUNT(*) FROM collisions)`).Scan(
		&status.ProjectedSequence, &status.Events, &status.Actors, &status.Repositories, &status.Workspaces,
		&status.Mutations, &status.SessionEvents, &status.Collisions,
	)
	return status, err
}

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
	rows, err := d.db.Query(`SELECT
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
	defer rows.Close()
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

func (d *DB) Mutation(id string) (MutationRecord, error) {
	row := d.db.QueryRow(`SELECT
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
	var paths, relativePaths, before, after string
	var gitBefore, gitAfter, jjBefore, jjAfter string
	var success sql.NullBool
	var turnIndex sql.NullInt64
	err := row.Scan(&record.ID, &record.Actor, &record.SessionGeneration, &record.TurnID, &turnIndex, &record.ToolCallID, &record.Tool,
		&record.Operation, &record.CWD, &record.RepositoryUUID, &record.RepositoryRoot, &record.WorkspaceUUID,
		&record.WorkspaceRoot, &record.WorkspaceKind, &record.WorkspaceKey, &paths, &relativePaths, &record.AssistantExcerpt,
		&record.StartedAt, &record.CompletedAt, &success, &record.Error, &before, &after,
		&gitBefore, &gitAfter, &jjBefore, &jjAfter, &record.UpdatedSequence)
	if err != nil {
		return MutationRecord{}, err
	}
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
	rows, err := d.db.Query(`SELECT sequence, type, at, data FROM events WHERE `+strings.Join(conditions, " AND ")+` ORDER BY sequence DESC LIMIT ?`, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (d *DB) SessionEvents(actor string, limit int, scopes ...ActorScope) ([]SessionRecord, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	scope := selectedScope(scopes)
	resolved, err := d.ResolveActorScoped(actor, scope.RepositoryUUID, scope.WorkspaceUUID)
	if err != nil {
		return nil, err
	}
	rows, err := d.db.Query(`SELECT id, actor, session_generation, type, at, turn_index,
		COALESCE(summary, ''), data, event_sequence FROM session_events WHERE actor = ? ORDER BY at DESC, event_sequence DESC, id DESC LIMIT ?`, uuidBlob(resolved), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]SessionRecord, 0)
	for rows.Next() {
		var record SessionRecord
		var turnIndex sql.NullInt64
		var data string
		if err := rows.Scan(&record.ID, &record.Actor, &record.SessionGeneration, &record.Type, &record.At,
			&turnIndex, &record.Summary, &data, &record.EventSequence); err != nil {
			return nil, err
		}
		if turnIndex.Valid {
			value := int(turnIndex.Int64)
			record.TurnIndex = &value
		}
		record.Data = json.RawMessage(data)
		result = append(result, record)
	}
	return result, rows.Err()
}
