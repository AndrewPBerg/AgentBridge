package provenance

import (
	"database/sql"
	"encoding/json"
	"errors"
)

type CollisionRecord struct {
	ID         string          `json:"id"`
	Path       string          `json:"path"`
	State      string          `json:"state"`
	Actors     json.RawMessage `json:"actors"`
	CreatedAt  string          `json:"created_at"`
	UpdatedAt  string          `json:"updated_at"`
	Owner      string          `json:"owner,omitempty"`
	ResolvedAt string          `json:"resolved_at,omitempty"`
	Resolution string          `json:"resolution,omitempty"`
	ResolvedBy string          `json:"resolved_by,omitempty"`
	DeadActor  string          `json:"dead_actor,omitempty"`
	Data       json.RawMessage `json:"data"`
}

type FileAnswer struct {
	Path       string            `json:"path"`
	Mutations  []MutationRecord  `json:"mutations"`
	Collisions []CollisionRecord `json:"collisions"`
}

type WhyAnswer struct {
	Mutation      MutationRecord    `json:"mutation"`
	FileHistory   []MutationRecord  `json:"file_history"`
	Collisions    []CollisionRecord `json:"collisions"`
	SessionEvents []SessionRecord   `json:"session_events"`
}

type AgentAnswer struct {
	Actor         string           `json:"actor"`
	Mutations     []MutationRecord `json:"mutations"`
	SessionEvents []SessionRecord  `json:"session_events"`
}

type SinceCompactionAnswer struct {
	Actor         string           `json:"actor"`
	Compaction    *SessionRecord   `json:"compaction,omitempty"`
	Mutations     []MutationRecord `json:"mutations"`
	SessionEvents []SessionRecord  `json:"session_events"`
}

func (d *DB) WhoChanged(path string, limit int) (FileAnswer, error) {
	mutations, err := d.ListMutations(MutationFilter{Path: path, Limit: limit})
	if err != nil {
		return FileAnswer{}, err
	}
	collisions, err := d.collisionsForPath(path, limit)
	if err != nil {
		return FileAnswer{}, err
	}
	return FileAnswer{Path: path, Mutations: mutations, Collisions: collisions}, nil
}

func (d *DB) Why(id string, limit int) (WhyAnswer, error) {
	mutation, err := d.Mutation(id)
	if err != nil {
		return WhyAnswer{}, err
	}
	var paths []string
	if err := json.Unmarshal(mutation.Paths, &paths); err != nil {
		return WhyAnswer{}, err
	}
	answer := WhyAnswer{Mutation: mutation, FileHistory: make([]MutationRecord, 0), Collisions: make([]CollisionRecord, 0)}
	for _, path := range paths {
		history, err := d.ListMutations(MutationFilter{Path: path, Limit: limit})
		if err != nil {
			return WhyAnswer{}, err
		}
		answer.FileHistory = appendUniqueMutations(answer.FileHistory, history)
		collisions, err := d.collisionsForPath(path, limit)
		if err != nil {
			return WhyAnswer{}, err
		}
		answer.Collisions = appendUniqueCollisions(answer.Collisions, collisions)
	}
	answer.SessionEvents, err = d.SessionEvents(mutation.Actor, limit)
	return answer, err
}

func (d *DB) AgentSummary(actor string, limit int, scopes ...ActorScope) (AgentAnswer, error) {
	scope := selectedScope(scopes)
	resolved, err := d.ResolveActorScoped(actor, scope.RepositoryUUID, scope.WorkspaceUUID)
	if err != nil {
		return AgentAnswer{}, err
	}
	mutations, err := d.ListMutations(MutationFilter{Actor: resolved, Limit: limit})
	if err != nil {
		return AgentAnswer{}, err
	}
	events, err := d.SessionEvents(resolved, limit, scope)
	if err != nil {
		return AgentAnswer{}, err
	}
	return AgentAnswer{Actor: resolved, Mutations: mutations, SessionEvents: events}, nil
}

func (d *DB) SinceCompaction(actor string, limit int, scopes ...ActorScope) (SinceCompactionAnswer, error) {
	scope := selectedScope(scopes)
	resolved, err := d.ResolveActorScoped(actor, scope.RepositoryUUID, scope.WorkspaceUUID)
	if err != nil {
		return SinceCompactionAnswer{}, err
	}
	answer := SinceCompactionAnswer{Actor: resolved, Mutations: make([]MutationRecord, 0), SessionEvents: make([]SessionRecord, 0)}
	var compaction SessionRecord
	var turnIndex sql.NullInt64
	var summary, data string
	err = d.db.QueryRow(`SELECT id, actor, session_generation, type, at, turn_index,
		COALESCE(summary, ''), data, event_sequence FROM session_events
		WHERE actor = ? AND type = 'session.compacted' ORDER BY at DESC, event_sequence DESC, id DESC LIMIT 1`, uuidBlob(resolved)).Scan(
		&compaction.ID, &compaction.Actor, &compaction.SessionGeneration, &compaction.Type, &compaction.At,
		&turnIndex, &summary, &data, &compaction.EventSequence,
	)
	if errors.Is(err, sql.ErrNoRows) {
		mutations, mutationErr := d.ListMutations(MutationFilter{Actor: resolved, Limit: limit})
		if mutationErr != nil {
			return SinceCompactionAnswer{}, mutationErr
		}
		events, eventErr := d.SessionEvents(resolved, limit, scope)
		answer.Mutations, answer.SessionEvents = mutations, events
		return answer, eventErr
	}
	if err != nil {
		return SinceCompactionAnswer{}, err
	}
	if turnIndex.Valid {
		value := int(turnIndex.Int64)
		compaction.TurnIndex = &value
	}
	compaction.Summary = summary
	compaction.Data = json.RawMessage(data)
	answer.Compaction = &compaction

	rows, err := d.db.Query(`SELECT
		id, actor, session_generation, COALESCE(turn_id, ''), turn_index, tool_call_id, tool, operation, cwd,
		COALESCE(repository_uuid, ''), COALESCE(repository_root, ''), COALESCE(workspace_uuid, ''),
		COALESCE(workspace_root, ''), COALESCE(workspace_kind, ''), workspace_key, paths_json, relative_paths_json,
		COALESCE(assistant_excerpt, ''), started_at, COALESCE(completed_at, ''), success,
		COALESCE(error, ''), before_json, after_json, COALESCE(git_before_json, ''), COALESCE(git_after_json, ''),
		COALESCE(jj_before_json, ''), COALESCE(jj_after_json, ''), updated_sequence
		FROM mutations WHERE actor = ? AND started_at >= ? ORDER BY started_at DESC, updated_sequence DESC, id DESC LIMIT ?`, uuidBlob(resolved), compaction.At, normalizedLimit(limit))
	if err != nil {
		return SinceCompactionAnswer{}, err
	}
	for rows.Next() {
		record, scanErr := scanMutation(rows)
		if scanErr != nil {
			rows.Close()
			return SinceCompactionAnswer{}, scanErr
		}
		answer.Mutations = append(answer.Mutations, record)
	}
	if err := rows.Close(); err != nil {
		return SinceCompactionAnswer{}, err
	}
	eventRows, err := d.db.Query(`SELECT id, actor, session_generation, type, at, turn_index,
		COALESCE(summary, ''), data, event_sequence FROM session_events
		WHERE actor = ? AND at >= ? ORDER BY at DESC, event_sequence DESC, id DESC LIMIT ?`, uuidBlob(resolved), compaction.At, normalizedLimit(limit))
	if err != nil {
		return SinceCompactionAnswer{}, err
	}
	defer eventRows.Close()
	for eventRows.Next() {
		var record SessionRecord
		var index sql.NullInt64
		var eventData string
		if err := eventRows.Scan(&record.ID, &record.Actor, &record.SessionGeneration, &record.Type, &record.At,
			&index, &record.Summary, &eventData, &record.EventSequence); err != nil {
			return SinceCompactionAnswer{}, err
		}
		if index.Valid {
			value := int(index.Int64)
			record.TurnIndex = &value
		}
		record.Data = json.RawMessage(eventData)
		answer.SessionEvents = append(answer.SessionEvents, record)
	}
	return answer, eventRows.Err()
}

func (d *DB) collisionsForPath(path string, limit int) ([]CollisionRecord, error) {
	rows, err := d.db.Query(`SELECT id, path, state, created_at, updated_at,
		owner, COALESCE(resolved_at, ''), COALESCE(resolution, ''), resolved_by, dead_actor, data
		FROM collisions WHERE path = ? ORDER BY updated_at DESC, updated_sequence DESC, id DESC LIMIT ?`, path, normalizedLimit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]CollisionRecord, 0)
	for rows.Next() {
		var record CollisionRecord
		var collisionID, data []byte
		var owner, resolvedBy, deadActor []byte
		if err := rows.Scan(&collisionID, &record.Path, &record.State, &record.CreatedAt,
			&record.UpdatedAt, &owner, &record.ResolvedAt, &record.Resolution, &resolvedBy, &deadActor, &data); err != nil {
			return nil, err
		}
		record.ID = uuidString(collisionID)
		actorRows, err := d.db.Query(`SELECT session_uuid FROM collision_actors WHERE collision_id = ? ORDER BY ordinal`, collisionID)
		if err != nil {
			return nil, err
		}
		actors := make([]string, 0, 2)
		for actorRows.Next() {
			var actor []byte
			if err := actorRows.Scan(&actor); err != nil {
				actorRows.Close()
				return nil, err
			}
			actors = append(actors, uuidString(actor))
		}
		if err := actorRows.Close(); err != nil {
			return nil, err
		}
		encodedActors, err := json.Marshal(actors)
		if err != nil {
			return nil, err
		}
		record.Actors = encodedActors
		record.Owner = uuidString(owner)
		record.ResolvedBy = uuidString(resolvedBy)
		record.DeadActor = uuidString(deadActor)
		record.Data = json.RawMessage(data)
		result = append(result, record)
	}
	return result, rows.Err()
}

func normalizedLimit(limit int) int {
	if limit <= 0 || limit > 100 {
		return 20
	}
	return limit
}

func appendUniqueMutations(target, values []MutationRecord) []MutationRecord {
	seen := make(map[string]bool, len(target)+len(values))
	for _, value := range target {
		seen[value.ID] = true
	}
	for _, value := range values {
		if !seen[value.ID] {
			target = append(target, value)
			seen[value.ID] = true
		}
	}
	return target
}

func appendUniqueCollisions(target, values []CollisionRecord) []CollisionRecord {
	seen := make(map[string]bool, len(target)+len(values))
	for _, value := range target {
		seen[value.ID] = true
	}
	for _, value := range values {
		if !seen[value.ID] {
			target = append(target, value)
			seen[value.ID] = true
		}
	}
	return target
}
