package provenance

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
	_ "turso.tech/database/tursogo"
)

const projectionSchemaVersion = 8

type DB struct {
	db   *sql.DB
	path string
}

func Open(path string) (*DB, error) {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create provenance directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return nil, fmt.Errorf("secure provenance directory: %w", err)
	}
	database, err := sql.Open("turso", path)
	if err != nil {
		return nil, fmt.Errorf("open provenance database: %w", err)
	}
	database.SetMaxOpenConns(8)
	database.SetMaxIdleConns(4)
	result := &DB{db: database, path: path}
	if err := result.initialize(); err != nil {
		database.Close()
		return nil, err
	}
	if err := secureDatabaseFiles(path); err != nil {
		database.Close()
		return nil, err
	}
	return result, nil
}

func (d *DB) initialize() error {
	statements := []string{
		`PRAGMA foreign_keys=ON`,
		`CREATE TABLE IF NOT EXISTS events (
			sequence INTEGER PRIMARY KEY,
			type TEXT NOT NULL,
			at TEXT NOT NULL,
			data TEXT NOT NULL
		) STRICT`,
		`CREATE INDEX IF NOT EXISTS events_type_at ON events(type, at DESC)`,
		`CREATE TABLE IF NOT EXISTS repositories (
			id BLOB PRIMARY KEY,
			root TEXT NOT NULL,
			kind TEXT NOT NULL,
			git_common_dir TEXT,
			jj_repo_path TEXT,
			updated_sequence INTEGER NOT NULL
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS workspaces (
			id BLOB PRIMARY KEY,
			repository_uuid BLOB NOT NULL REFERENCES repositories(id),
			root TEXT NOT NULL,
			kind TEXT NOT NULL,
			git_branch TEXT,
			git_head TEXT,
			jj_workspace_name TEXT,
			jj_change_id TEXT,
			updated_sequence INTEGER NOT NULL
		) STRICT`,
		`CREATE INDEX IF NOT EXISTS workspaces_repository ON workspaces(repository_uuid, root)`,
		`CREATE TABLE IF NOT EXISTS actors (
			session_uuid BLOB PRIMARY KEY,
			harness TEXT NOT NULL,
			alias TEXT,
			cwd TEXT NOT NULL,
			repository_uuid BLOB,
			repository_root TEXT,
			workspace_uuid BLOB,
			workspace_root TEXT,
			workspace_kind TEXT,
			state TEXT NOT NULL,
			generation INTEGER NOT NULL,
			started_at TEXT NOT NULL,
			heartbeat_at TEXT NOT NULL,
			git_json TEXT,
			jj_json TEXT,
			capabilities_json TEXT NOT NULL,
			updated_sequence INTEGER NOT NULL
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS mutations (
			id TEXT PRIMARY KEY,
			actor BLOB NOT NULL,
			session_generation INTEGER NOT NULL,
			turn_id TEXT,
			turn_index INTEGER,
			tool_call_id TEXT NOT NULL,
			tool TEXT NOT NULL,
			operation TEXT NOT NULL,
			cwd TEXT NOT NULL,
			repository_uuid BLOB,
			repository_root TEXT,
			workspace_uuid BLOB,
			workspace_root TEXT,
			workspace_kind TEXT,
			workspace_key TEXT NOT NULL,
			paths_json TEXT NOT NULL,
			relative_paths_json TEXT NOT NULL,
			assistant_excerpt TEXT,
			started_at TEXT NOT NULL,
			completed_at TEXT,
			success INTEGER,
			error TEXT,
			before_json TEXT NOT NULL,
			after_json TEXT NOT NULL,
			git_before_json TEXT,
			git_after_json TEXT,
			jj_before_json TEXT,
			jj_after_json TEXT,
			updated_sequence INTEGER NOT NULL
		) STRICT`,
		`CREATE INDEX IF NOT EXISTS mutations_actor_started ON mutations(actor, started_at DESC)`,
		`CREATE INDEX IF NOT EXISTS mutations_started ON mutations(started_at DESC)`,
		`CREATE TABLE IF NOT EXISTS mutation_paths (
			mutation_id TEXT NOT NULL REFERENCES mutations(id) ON DELETE CASCADE,
			path TEXT NOT NULL,
			ordinal INTEGER NOT NULL,
			before_json TEXT,
			after_json TEXT,
			PRIMARY KEY(mutation_id, path)
		) STRICT`,
		`CREATE INDEX IF NOT EXISTS mutation_paths_path ON mutation_paths(path, mutation_id)`,
		`CREATE TABLE IF NOT EXISTS session_events (
			id TEXT PRIMARY KEY,
			actor BLOB NOT NULL,
			session_generation INTEGER NOT NULL,
			type TEXT NOT NULL,
			at TEXT NOT NULL,
			turn_index INTEGER,
			summary TEXT,
			data TEXT NOT NULL,
			event_sequence INTEGER NOT NULL
		) STRICT`,
		`CREATE INDEX IF NOT EXISTS session_events_actor_at ON session_events(actor, at DESC)`,
		`CREATE TABLE IF NOT EXISTS collisions (
			id BLOB PRIMARY KEY,
			path TEXT NOT NULL,
			state TEXT NOT NULL,
			owner BLOB,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			resolved_at TEXT,
			resolution TEXT,
			resolved_by BLOB,
			dead_actor BLOB,
			data TEXT NOT NULL,
			updated_sequence INTEGER NOT NULL
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS collision_actors (
			collision_id BLOB NOT NULL REFERENCES collisions(id) ON DELETE CASCADE,
			ordinal INTEGER NOT NULL,
			session_uuid BLOB NOT NULL,
			PRIMARY KEY(collision_id, ordinal)
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS messages (
			id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			from_actor BLOB NOT NULL,
			to_actor BLOB NOT NULL,
			body TEXT NOT NULL,
			global_sequence INTEGER NOT NULL,
			sender_sequence INTEGER NOT NULL,
			recipient_sequence INTEGER NOT NULL,
			client_sequence INTEGER,
			session_generation INTEGER,
			collision_id BLOB,
			created_at TEXT NOT NULL,
			acknowledged_at TEXT,
			data TEXT NOT NULL,
			event_sequence INTEGER NOT NULL
		) STRICT`,
		`CREATE INDEX IF NOT EXISTS messages_actor_created ON messages(to_actor, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS test_results (
			id TEXT PRIMARY KEY,
			actor BLOB NOT NULL,
			session_generation INTEGER NOT NULL,
			turn_id TEXT,
			turn_index INTEGER,
			tool_call_id TEXT,
			command TEXT NOT NULL,
			cwd TEXT NOT NULL,
			exit_code INTEGER,
			started_at TEXT NOT NULL,
			completed_at TEXT NOT NULL,
			duration_ms INTEGER,
			output_excerpt TEXT,
			output_sha256 TEXT,
			output_bytes INTEGER,
			output_truncated INTEGER NOT NULL,
			repository_uuid BLOB,
			workspace_uuid BLOB,
			data TEXT NOT NULL,
			event_sequence INTEGER NOT NULL
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS checkpoint_requests (
			id TEXT PRIMARY KEY,
			actor BLOB NOT NULL,
			declared_by TEXT NOT NULL DEFAULT 'agent',
			session_generation INTEGER NOT NULL,
			repository_uuid BLOB NOT NULL,
			workspace_uuid BLOB NOT NULL,
			work_unit_uuid BLOB,
			checkpoint_kind TEXT NOT NULL,
			journal_start_sequence INTEGER NOT NULL,
			journal_end_sequence INTEGER NOT NULL,
			data TEXT NOT NULL,
			event_sequence INTEGER NOT NULL
		) STRICT`,
	}
	for _, statement := range statements {
		if _, err := d.db.Exec(statement); err != nil {
			return fmt.Errorf("initialize provenance database: %w", err)
		}
	}
	if err := d.ensureColumn("mutations", "turn_id", "TEXT"); err != nil {
		return err
	}
	if err := d.ensureColumn("mutations", "turn_index", "INTEGER"); err != nil {
		return err
	}
	for _, column := range []struct{ table, name, definition string }{
		{"actors", "repository_uuid", "BLOB"},
		{"actors", "repository_root", "TEXT"},
		{"actors", "workspace_uuid", "BLOB"},
		{"actors", "workspace_root", "TEXT"},
		{"actors", "workspace_kind", "TEXT"},
		{"mutations", "repository_uuid", "BLOB"},
		{"mutations", "repository_root", "TEXT"},
		{"mutations", "workspace_uuid", "BLOB"},
		{"mutations", "workspace_root", "TEXT"},
		{"mutations", "workspace_kind", "TEXT"},
		{"mutations", "relative_paths_json", "TEXT NOT NULL DEFAULT '[]'"},
		{"collisions", "owner", "BLOB"},
		{"collisions", "resolved_at", "TEXT"},
		{"collisions", "resolved_by", "BLOB"},
		{"collisions", "dead_actor", "BLOB"},
		{"checkpoint_requests", "declared_by", "TEXT NOT NULL DEFAULT 'agent'"},
		{"checkpoint_requests", "work_unit_uuid", "BLOB"},
	} {
		if err := d.ensureColumn(column.table, column.name, column.definition); err != nil {
			return err
		}
	}
	return d.ensureProjectionVersion()
}

func (d *DB) ensureProjectionVersion() error {
	if _, err := d.db.Exec(`CREATE TABLE IF NOT EXISTS projection_meta (version INTEGER NOT NULL) STRICT`); err != nil {
		return err
	}
	var version int
	err := d.db.QueryRow(`SELECT version FROM projection_meta LIMIT 1`).Scan(&version)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if version >= projectionSchemaVersion {
		var columnType string
		err := d.db.QueryRow(`SELECT type FROM pragma_table_info('messages') WHERE name = 'from_actor'`).Scan(&columnType)
		if err == nil && strings.EqualFold(columnType, "BLOB") {
			return nil
		}
		// A prior version may have recorded the new projection version before
		// actually rebuilding legacy TEXT tables. Force the binary rebuild.
		version = projectionSchemaVersion - 1
	}
	transaction, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	if version < projectionSchemaVersion {
		if err := recreateBinaryProjectionTables(transaction); err != nil {
			return err
		}
	}
	tables := []string{"mutation_paths", "mutations", "session_events", "messages", "test_results", "checkpoint_requests", "workspaces", "repositories", "events"}
	if version == 0 || version >= 7 {
		tables = append(tables, "collisions", "collision_actors")
	}
	for _, table := range tables {
		if _, err := transaction.Exec(`DELETE FROM ` + table); err != nil {
			return fmt.Errorf("reset provenance projection table %s: %w", table, err)
		}
	}
	if _, err := transaction.Exec(`DELETE FROM projection_meta`); err != nil {
		return err
	}
	if _, err := transaction.Exec(`INSERT INTO projection_meta(version) VALUES (?)`, projectionSchemaVersion); err != nil {
		return err
	}
	return transaction.Commit()
}

func recreateBinaryProjectionTables(transaction *sql.Tx) error {
	// Projection state is disposable: the journal is the migration source. Drop
	// every UUID-bearing table together so old TEXT schemas cannot reject the
	// binary projection during backfill.
	for _, table := range []string{"checkpoint_requests", "test_results", "messages", "collision_actors", "collisions", "session_events", "mutation_paths", "mutations", "actors", "workspaces", "repositories"} {
		if _, err := transaction.Exec(`DROP TABLE IF EXISTS ` + table); err != nil {
			return fmt.Errorf("drop legacy projection table %s: %w", table, err)
		}
	}
	statements := []string{
		`CREATE TABLE repositories (id BLOB PRIMARY KEY, root TEXT NOT NULL, kind TEXT NOT NULL, git_common_dir TEXT, jj_repo_path TEXT, updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE workspaces (id BLOB PRIMARY KEY, repository_uuid BLOB NOT NULL REFERENCES repositories(id), root TEXT NOT NULL, kind TEXT NOT NULL, git_branch TEXT, git_head TEXT, jj_workspace_name TEXT, jj_change_id TEXT, updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE INDEX workspaces_repository ON workspaces(repository_uuid, root)`,
		`CREATE TABLE actors (session_uuid BLOB PRIMARY KEY, harness TEXT NOT NULL, alias TEXT, cwd TEXT NOT NULL, repository_uuid BLOB, repository_root TEXT, workspace_uuid BLOB, workspace_root TEXT, workspace_kind TEXT, state TEXT NOT NULL, generation INTEGER NOT NULL, started_at TEXT NOT NULL, heartbeat_at TEXT NOT NULL, git_json TEXT, jj_json TEXT, capabilities_json TEXT NOT NULL, updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE mutations (id TEXT PRIMARY KEY, actor BLOB NOT NULL, session_generation INTEGER NOT NULL, turn_id TEXT, turn_index INTEGER, tool_call_id TEXT NOT NULL, tool TEXT NOT NULL, operation TEXT NOT NULL, cwd TEXT NOT NULL, repository_uuid BLOB, repository_root TEXT, workspace_uuid BLOB, workspace_root TEXT, workspace_kind TEXT, workspace_key TEXT NOT NULL, paths_json TEXT NOT NULL, relative_paths_json TEXT NOT NULL, assistant_excerpt TEXT, started_at TEXT NOT NULL, completed_at TEXT, success INTEGER, error TEXT, before_json TEXT NOT NULL, after_json TEXT NOT NULL, git_before_json TEXT, git_after_json TEXT, jj_before_json TEXT, jj_after_json TEXT, updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE INDEX mutations_actor_started ON mutations(actor, started_at DESC)`,
		`CREATE INDEX mutations_started ON mutations(started_at DESC)`,
		`CREATE TABLE mutation_paths (mutation_id TEXT NOT NULL REFERENCES mutations(id) ON DELETE CASCADE, path TEXT NOT NULL, ordinal INTEGER NOT NULL, before_json TEXT, after_json TEXT, PRIMARY KEY(mutation_id, path)) STRICT`,
		`CREATE INDEX mutation_paths_path ON mutation_paths(path, mutation_id)`,
		`CREATE TABLE session_events (id TEXT PRIMARY KEY, actor BLOB NOT NULL, session_generation INTEGER NOT NULL, type TEXT NOT NULL, at TEXT NOT NULL, turn_index INTEGER, summary TEXT, data TEXT NOT NULL, event_sequence INTEGER NOT NULL) STRICT`,
		`CREATE INDEX session_events_actor_at ON session_events(actor, at DESC)`,
		`CREATE TABLE collisions (id BLOB PRIMARY KEY, path TEXT NOT NULL, state TEXT NOT NULL, owner BLOB, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, resolved_at TEXT, resolution TEXT, resolved_by BLOB, dead_actor BLOB, data TEXT NOT NULL, updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE collision_actors (collision_id BLOB NOT NULL REFERENCES collisions(id) ON DELETE CASCADE, ordinal INTEGER NOT NULL, session_uuid BLOB NOT NULL, PRIMARY KEY(collision_id, ordinal)) STRICT`,
		`CREATE TABLE messages (id TEXT PRIMARY KEY, kind TEXT NOT NULL, from_actor BLOB NOT NULL, to_actor BLOB NOT NULL, body TEXT NOT NULL, global_sequence INTEGER NOT NULL, sender_sequence INTEGER NOT NULL, recipient_sequence INTEGER NOT NULL, client_sequence INTEGER, session_generation INTEGER, collision_id BLOB, created_at TEXT NOT NULL, acknowledged_at TEXT, data TEXT NOT NULL, event_sequence INTEGER NOT NULL) STRICT`,
		`CREATE INDEX messages_actor_created ON messages(to_actor, created_at DESC)`,
		`CREATE TABLE test_results (id TEXT PRIMARY KEY, actor BLOB NOT NULL, session_generation INTEGER NOT NULL, turn_id TEXT, turn_index INTEGER, tool_call_id TEXT, command TEXT NOT NULL, cwd TEXT NOT NULL, exit_code INTEGER, started_at TEXT NOT NULL, completed_at TEXT NOT NULL, duration_ms INTEGER, output_excerpt TEXT, output_sha256 TEXT, output_bytes INTEGER, output_truncated INTEGER NOT NULL, repository_uuid BLOB, workspace_uuid BLOB, data TEXT NOT NULL, event_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE checkpoint_requests (id TEXT PRIMARY KEY, actor BLOB NOT NULL, declared_by TEXT NOT NULL, session_generation INTEGER NOT NULL, repository_uuid BLOB NOT NULL, workspace_uuid BLOB NOT NULL, work_unit_uuid BLOB, checkpoint_kind TEXT NOT NULL, journal_start_sequence INTEGER NOT NULL, journal_end_sequence INTEGER NOT NULL, data TEXT NOT NULL, event_sequence INTEGER NOT NULL) STRICT`,
	}
	for _, statement := range statements {
		if _, err := transaction.Exec(statement); err != nil {
			return fmt.Errorf("recreate binary projection: %w", err)
		}
	}
	return nil
}

func migrateCollisions(transaction *sql.Tx) error {
	columns, err := transaction.Query(`PRAGMA table_info(collisions)`)
	if err != nil {
		return err
	}
	legacy := false
	for columns.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := columns.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			columns.Close()
			return err
		}
		if name == "actors_json" {
			legacy = true
		}
	}
	if err := columns.Close(); err != nil {
		return err
	}
	if !legacy {
		return nil
	}
	type legacyCollision struct {
		id, path, state, actors, createdAt, updatedAt string
		owner, resolvedBy, deadActor                  []byte
		resolvedAt, resolution                        sql.NullString
		data                                          string
		sequence                                      int64
	}
	rows, err := transaction.Query(`SELECT id, path, state, actors_json, owner, created_at, updated_at, resolved_at, resolution, resolved_by, dead_actor, data, updated_sequence FROM collisions`)
	if err != nil {
		return fmt.Errorf("read legacy collisions: %w", err)
	}
	defer rows.Close()
	var values []legacyCollision
	for rows.Next() {
		var value legacyCollision
		if err := rows.Scan(&value.id, &value.path, &value.state, &value.actors, &value.owner, &value.createdAt, &value.updatedAt, &value.resolvedAt, &value.resolution, &value.resolvedBy, &value.deadActor, &value.data, &value.sequence); err != nil {
			return err
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := transaction.Exec(`DROP TABLE IF EXISTS collision_actors`); err != nil {
		return err
	}
	if _, err := transaction.Exec(`ALTER TABLE collisions RENAME TO collisions_legacy`); err != nil {
		return err
	}
	if _, err := transaction.Exec(`CREATE TABLE collisions (id BLOB PRIMARY KEY, path TEXT NOT NULL, state TEXT NOT NULL, owner BLOB, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, resolved_at TEXT, resolution TEXT, resolved_by BLOB, dead_actor BLOB, data TEXT NOT NULL, updated_sequence INTEGER NOT NULL) STRICT`); err != nil {
		return err
	}
	if _, err := transaction.Exec(`CREATE TABLE collision_actors (collision_id BLOB NOT NULL REFERENCES collisions(id) ON DELETE CASCADE, ordinal INTEGER NOT NULL, session_uuid BLOB NOT NULL, PRIMARY KEY(collision_id, ordinal)) STRICT`); err != nil {
		return err
	}
	for _, value := range values {
		id := uuidBlob(value.id)
		if _, err := transaction.Exec(`INSERT INTO collisions(id, path, state, owner, created_at, updated_at, resolved_at, resolution, resolved_by, dead_actor, data, updated_sequence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, id, value.path, value.state, value.owner, value.createdAt, value.updatedAt, nullableNullString(value.resolvedAt), nullableNullString(value.resolution), value.resolvedBy, value.deadActor, value.data, value.sequence); err != nil {
			return err
		}
		var actors []string
		if json.Unmarshal([]byte(value.actors), &actors) == nil {
			for ordinal, actor := range actors {
				if _, err := transaction.Exec(`INSERT INTO collision_actors(collision_id, ordinal, session_uuid) VALUES (?, ?, ?)`, id, ordinal, uuidBlob(actor)); err != nil {
					return err
				}
			}
		}
	}
	_, err = transaction.Exec(`DROP TABLE collisions_legacy`)
	return err
}

func nullableNullString(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return value.String
}

func (d *DB) ensureColumn(table, column, definition string) error {
	rows, err := d.db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := d.db.Exec(`ALTER TABLE ` + table + ` ADD COLUMN ` + column + ` ` + definition); err != nil {
		return fmt.Errorf("add provenance column %s.%s: %w", table, column, err)
	}
	return nil
}

func secureDatabaseFiles(path string) error {
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Chmod(candidate, 0o600); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("secure provenance database file %s: %w", candidate, err)
		}
	}
	return nil
}

func (d *DB) Close() error { return d.db.Close() }
func (d *DB) Path() string { return d.path }

// PruneNonUUIDActors removes legacy actor keys from the read model. The
// append-only journal remains untouched and can be reprojected later.
func (d *DB) PruneNonUUIDActors() error {
	statements := []string{
		`DELETE FROM actors WHERE length(session_uuid) != 16 OR (repository_uuid IS NOT NULL AND length(repository_uuid) != 16) OR (workspace_uuid IS NOT NULL AND length(workspace_uuid) != 16)`,
		`DELETE FROM mutations WHERE (repository_uuid IS NOT NULL AND length(repository_uuid) != 16) OR (workspace_uuid IS NOT NULL AND length(workspace_uuid) != 16)`,
		`DELETE FROM test_results WHERE (repository_uuid IS NOT NULL AND length(repository_uuid) != 16) OR (workspace_uuid IS NOT NULL AND length(workspace_uuid) != 16)`,
		`DELETE FROM checkpoint_requests WHERE length(repository_uuid) != 16 OR length(workspace_uuid) != 16 OR (work_unit_uuid IS NOT NULL AND length(work_unit_uuid) != 16)`,
		`DELETE FROM workspaces WHERE length(id) != 16 OR length(repository_uuid) != 16`,
		`DELETE FROM repositories WHERE length(id) != 16`,
	}
	for _, statement := range statements {
		if _, err := d.db.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

// Snapshot creates a compact, consistent database copy through the same Turso
// connection that owns the live file. Other processes can inspect the copy
// without competing for the live engine lock.
func (d *DB) Snapshot(path string) error {
	if path == "" || filepath.IsAbs(path) == false {
		return errors.New("snapshot path must be absolute")
	}
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("snapshot path already exists: %s", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}
	// VACUUM INTO accepts a string literal, not a bind parameter.
	literal := strings.ReplaceAll(path, "'", "''")
	if _, err := d.db.Exec(`VACUUM INTO '` + literal + `'`); err != nil {
		return fmt.Errorf("create provenance snapshot: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("secure provenance snapshot: %w", err)
	}
	return nil
}

func (d *DB) ProjectAll(events []protocol.Event) error {
	for _, event := range events {
		if err := d.Project(event); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) Project(event protocol.Event) error {
	transaction, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	result, err := transaction.Exec(`INSERT OR IGNORE INTO events(sequence, type, at, data) VALUES (?, ?, ?, ?)`,
		event.Sequence, event.Type, event.At.UTC().Format(time.RFC3339Nano), string(event.Data))
	if err != nil {
		return fmt.Errorf("record provenance event %d: %w", event.Sequence, err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 0 {
		var existingType, existingData string
		if err := transaction.QueryRow(`SELECT type, data FROM events WHERE sequence = ?`, event.Sequence).Scan(&existingType, &existingData); err != nil {
			return fmt.Errorf("check duplicate provenance event %d: %w", event.Sequence, err)
		}
		if existingType != event.Type || existingData != string(event.Data) {
			return fmt.Errorf("conflicting provenance event sequence %d", event.Sequence)
		}
		return nil
	}
	if err := d.projectDomain(transaction, event); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return err
	}
	return secureDatabaseFiles(d.path)
}

func (d *DB) projectDomain(transaction *sql.Tx, event protocol.Event) error {
	switch event.Type {
	case "actor.upserted":
		var actor protocol.Actor
		if err := json.Unmarshal(event.Data, &actor); err != nil {
			return err
		}
		if actor.RepositoryUUID == "" {
			actor.RepositoryUUID, actor.RepositoryRoot, actor.WorkspaceUUID, actor.WorkspaceRoot, actor.WorkspaceKind = deriveScope(actor.CWD, actor.Git, actor.JJ)
		}
		repositoryRoot := actor.RepositoryRoot
		if repositoryRoot == "" {
			repositoryRoot = actor.CWD
		}
		if err := projectScope(transaction, event.Sequence, actor.RepositoryUUID, repositoryRoot, actor.WorkspaceUUID,
			actor.WorkspaceRoot, actor.WorkspaceKind, actor.Git, actor.JJ); err != nil {
			return err
		}
		gitJSON, _ := marshalOptional(actor.Git)
		jjJSON, _ := marshalOptional(actor.JJ)
		capabilities, _ := json.Marshal(actor.Capabilities)
		sessionUUID := uuidBlob(actor.Address)
		_, err := transaction.Exec(`INSERT INTO actors(
			session_uuid, harness, alias, cwd, repository_uuid, repository_root, workspace_uuid, workspace_root, workspace_kind,
			state, generation, started_at, heartbeat_at, git_json, jj_json, capabilities_json, updated_sequence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(session_uuid) DO UPDATE SET
			alias=excluded.alias, cwd=excluded.cwd, repository_uuid=excluded.repository_uuid, repository_root=excluded.repository_root,
			workspace_uuid=excluded.workspace_uuid, workspace_root=excluded.workspace_root, workspace_kind=excluded.workspace_kind,
			state=excluded.state, generation=excluded.generation, heartbeat_at=excluded.heartbeat_at,
			git_json=excluded.git_json, jj_json=excluded.jj_json, capabilities_json=excluded.capabilities_json,
			updated_sequence=excluded.updated_sequence`,
			sessionUUID, actor.Harness, nullable(actor.Alias), actor.CWD,
			nullableUUID(actor.RepositoryUUID), nullable(actor.RepositoryRoot), nullableUUID(actor.WorkspaceUUID), nullable(actor.WorkspaceRoot), nullable(actor.WorkspaceKind),
			actor.State, actor.Generation, actor.StartedAt.UTC().Format(time.RFC3339Nano), actor.HeartbeatAt.UTC().Format(time.RFC3339Nano),
			gitJSON, jjJSON, string(capabilities), event.Sequence)
		return err
	case "intent.started", "intent.completed":
		var intent protocol.Intent
		if err := json.Unmarshal(event.Data, &intent); err != nil {
			return err
		}
		return projectIntent(transaction, event.Sequence, intent)
	case "message.enqueued":
		var message protocol.Message
		if err := json.Unmarshal(event.Data, &message); err != nil {
			return err
		}
		_, err := transaction.Exec(`INSERT OR IGNORE INTO messages(
			id, kind, from_actor, to_actor, body, global_sequence, sender_sequence, recipient_sequence,
			client_sequence, session_generation, collision_id, created_at, data, event_sequence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, message.ID, message.Kind, uuidBlob(message.From), uuidBlob(message.To), message.Body,
			message.GlobalSequence, message.SenderSequence, message.RecipientSequence, message.ClientSequence, message.SessionGeneration,
			nullableUUID(message.CollisionID), message.CreatedAt.UTC().Format(time.RFC3339Nano), string(event.Data), event.Sequence)
		return err
	case "message.acked":
		var ack struct {
			Actor      string    `json:"actor"`
			MessageIDs []string  `json:"message_ids"`
			At         time.Time `json:"at"`
		}
		if err := json.Unmarshal(event.Data, &ack); err != nil {
			return err
		}
		for _, id := range ack.MessageIDs {
			if _, err := transaction.Exec(`UPDATE messages SET acknowledged_at = ? WHERE id = ? AND to_actor = ?`, ack.At.UTC().Format(time.RFC3339Nano), id, uuidBlob(ack.Actor)); err != nil {
				return err
			}
		}
		return nil
	case "session.event":
		var sessionEvent protocol.SessionEvent
		if err := json.Unmarshal(event.Data, &sessionEvent); err != nil {
			return err
		}
		data := sessionEvent.Data
		if len(data) == 0 {
			data = json.RawMessage(`{}`)
		}
		_, err := transaction.Exec(`INSERT OR REPLACE INTO session_events(
			id, actor, session_generation, type, at, turn_index, summary, data, event_sequence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, sessionEvent.ID, uuidBlob(sessionEvent.Actor), sessionEvent.SessionGeneration,
			sessionEvent.Type, sessionEvent.At.UTC().Format(time.RFC3339Nano), sessionEvent.TurnIndex,
			nullable(sessionEvent.Summary), string(data), event.Sequence)
		return err
	case "test.result":
		var result protocol.TestResult
		if err := json.Unmarshal(event.Data, &result); err != nil {
			return err
		}
		_, err := transaction.Exec(`INSERT OR IGNORE INTO test_results(
			id, actor, session_generation, turn_id, turn_index, tool_call_id, command, cwd, exit_code,
			started_at, completed_at, duration_ms, output_excerpt, output_sha256, output_bytes, output_truncated,
			repository_uuid, workspace_uuid, data, event_sequence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, result.ID, uuidBlob(result.Actor),
			result.SessionGeneration, nullable(result.TurnID), result.TurnIndex, nullable(result.ToolCallID), result.Command, result.CWD,
			result.ExitCode, result.StartedAt.UTC().Format(time.RFC3339Nano), result.CompletedAt.UTC().Format(time.RFC3339Nano),
			result.DurationMillis, nullable(result.OutputExcerpt), nullable(result.OutputSHA256), result.OutputBytes, result.OutputTruncated,
			nullableUUID(result.RepositoryUUID), nullableUUID(result.WorkspaceUUID), string(event.Data), event.Sequence)
		return err
	case "checkpoint.requested":
		var checkpoint protocol.CheckpointRequest
		if err := json.Unmarshal(event.Data, &checkpoint); err != nil {
			return err
		}
		workUnitUUID, err := checkpointWorkUnitUUID(checkpoint.WorkUnitUUID)
		if err != nil {
			return err
		}
		_, err = transaction.Exec(`INSERT OR IGNORE INTO checkpoint_requests(
			id, actor, declared_by, session_generation, repository_uuid, workspace_uuid, work_unit_uuid, checkpoint_kind,
			journal_start_sequence, journal_end_sequence, data, event_sequence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, checkpoint.ID, uuidBlob(checkpoint.Actor), checkpoint.DeclaredBy, checkpoint.SessionGeneration,
			uuidBlob(checkpoint.RepositoryUUID), uuidBlob(checkpoint.WorkspaceUUID), workUnitUUID, checkpoint.CheckpointKind, checkpoint.JournalStart,
			checkpoint.JournalEnd, string(event.Data), event.Sequence)
		return err
	case "collision.actor_dead":
		var dead protocol.CollisionActorDeadEvent
		if err := json.Unmarshal(event.Data, &dead); err != nil {
			return err
		}
		result, err := transaction.Exec(`UPDATE collisions SET dead_actor = ?, updated_at = ?, data = ?, updated_sequence = ? WHERE id = ?`, nullableUUID(dead.Actor), dead.At.UTC().Format(time.RFC3339Nano), string(event.Data), event.Sequence, uuidBlob(dead.CollisionID))
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("collision actor-dead event references unknown collision %q", dead.CollisionID)
		}
		return nil
	case "collision.transitioned":
		var transition protocol.CollisionTransitionEvent
		if err := json.Unmarshal(event.Data, &transition); err != nil {
			return err
		}
		result, err := transaction.Exec(`UPDATE collisions SET state = ?, owner = ?, updated_at = ?, resolved_at = CASE WHEN ? = ? THEN ? ELSE resolved_at END, resolution = ?, resolved_by = CASE WHEN ? = ? THEN ? ELSE resolved_by END, data = ?, updated_sequence = ? WHERE id = ?`,
			transition.To, nullableUUID(transition.Owner), transition.At.UTC().Format(time.RFC3339Nano), transition.To, protocol.CollisionResolved, transition.At.UTC().Format(time.RFC3339Nano), nullable(transition.Resolution), transition.To, protocol.CollisionResolved, nullableUUID(transition.Actor), string(event.Data), event.Sequence, uuidBlob(transition.CollisionID))
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("collision transition references unknown collision %q", transition.CollisionID)
		}
		return nil
	case "collision.upserted":
		var collision protocol.Collision
		if err := json.Unmarshal(event.Data, &collision); err != nil {
			return err
		}
		collisionID := uuidBlob(collision.ID)
		_, err := transaction.Exec(`INSERT INTO collisions(
			id, path, state, owner, created_at, updated_at, resolved_at,
			resolution, resolved_by, dead_actor, data, updated_sequence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET state=excluded.state, owner=excluded.owner,
			updated_at=excluded.updated_at, resolved_at=excluded.resolved_at,
			resolution=excluded.resolution, resolved_by=excluded.resolved_by,
			dead_actor=excluded.dead_actor, data=excluded.data,
			updated_sequence=excluded.updated_sequence`,
			collisionID, collision.Path, collision.State, nullableUUID(collision.Owner),
			collision.CreatedAt.UTC().Format(time.RFC3339Nano), collision.UpdatedAt.UTC().Format(time.RFC3339Nano),
			nullableTime(collision.ResolvedAt), nullable(collision.Resolution), nullableUUID(collision.ResolvedBy),
			nullableUUID(collision.DeadActor), string(event.Data), event.Sequence)
		if err != nil {
			return err
		}
		if _, err := transaction.Exec(`DELETE FROM collision_actors WHERE collision_id = ?`, collisionID); err != nil {
			return err
		}
		for ordinal, actor := range collision.Actors {
			if _, err := transaction.Exec(`INSERT INTO collision_actors(collision_id, ordinal, session_uuid) VALUES (?, ?, ?)`, collisionID, ordinal, uuidBlob(actor)); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

func deriveScope(cwd string, git *protocol.GitContext, jj *protocol.JJContext) (string, string, string, string, string) {
	repositoryKey := "dir:" + cwd
	repositoryRoot := cwd
	workspaceRoot := cwd
	kind := "directory"
	if git != nil && git.CommonDir != "" {
		repositoryKey = "git:" + git.CommonDir
		repositoryRoot = git.RepoRoot
		workspaceRoot = git.WorktreeRoot
		kind = "git-worktree"
		if jj != nil {
			kind = "git-jj-workspace"
		}
	} else if jj != nil && jj.RepoPath != "" {
		repositoryKey = "jj:" + jj.RepoPath
		repositoryRoot = jj.WorkspaceRoot
		workspaceRoot = jj.WorkspaceRoot
		kind = "jj-workspace"
	}
	repositoryID := deterministicUUID(filepath.Clean(repositoryKey))
	workspaceID := deterministicUUID(filepath.Clean(repositoryID + "\x00" + workspaceRoot))
	return repositoryID, filepath.Clean(repositoryRoot), workspaceID, filepath.Clean(workspaceRoot), kind
}

func deterministicUUID(key string) string {
	sum := sha256.Sum256([]byte(key))
	bytes := sum[:16]
	bytes[6] = (bytes[6] & 0x0f) | 0x50
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(bytes)
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}

func projectScope(
	transaction *sql.Tx,
	sequence uint64,
	repositoryID, repositoryRoot, workspaceID, workspaceRoot, workspaceKind string,
	git *protocol.GitContext,
	jj *protocol.JJContext,
) error {
	if repositoryID == "" || workspaceID == "" {
		return nil
	}
	// Empty workspace fields inherit the repository scope. This keeps the
	// persisted protocol sparse without losing the effective workspace values
	// in the relational projection.
	if workspaceRoot == "" {
		workspaceRoot = repositoryRoot
	}
	if workspaceKind == "" {
		workspaceKind = "directory"
		if git != nil && jj != nil {
			workspaceKind = "git-jj-workspace"
		} else if git != nil {
			workspaceKind = "git-worktree"
		} else if jj != nil {
			workspaceKind = "jj-workspace"
		}
	}
	repositoryKind := "directory"
	if git != nil && jj != nil {
		repositoryKind = "git+jj"
	} else if git != nil {
		repositoryKind = "git"
	} else if jj != nil {
		repositoryKind = "jj"
	}
	var gitCommonDir, gitBranch, gitHead, jjRepoPath, jjWorkspaceName, jjChangeID string
	if git != nil {
		gitCommonDir, gitBranch, gitHead = git.CommonDir, git.Branch, git.Head
	}
	if jj != nil {
		jjRepoPath, jjWorkspaceName, jjChangeID = jj.RepoPath, jj.WorkspaceName, jj.ChangeID
	}
	if _, err := transaction.Exec(`INSERT INTO repositories(id, root, kind, git_common_dir, jj_repo_path, updated_sequence)
		VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET root=excluded.root, kind=excluded.kind,
		git_common_dir=excluded.git_common_dir, jj_repo_path=excluded.jj_repo_path, updated_sequence=excluded.updated_sequence`,
		uuidBlob(repositoryID), repositoryRoot, repositoryKind, nullable(gitCommonDir), nullable(jjRepoPath), sequence); err != nil {
		return err
	}
	_, err := transaction.Exec(`INSERT INTO workspaces(id, repository_uuid, root, kind, git_branch, git_head,
		jj_workspace_name, jj_change_id, updated_sequence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET repository_uuid=excluded.repository_uuid, root=excluded.root, kind=excluded.kind,
		git_branch=excluded.git_branch, git_head=excluded.git_head, jj_workspace_name=excluded.jj_workspace_name,
		jj_change_id=excluded.jj_change_id, updated_sequence=excluded.updated_sequence`,
		uuidBlob(workspaceID), uuidBlob(repositoryID), workspaceRoot, workspaceKind, nullable(gitBranch), nullable(gitHead),
		nullable(jjWorkspaceName), nullable(jjChangeID), sequence)
	return err
}

func projectIntent(transaction *sql.Tx, sequence uint64, intent protocol.Intent) error {
	if intent.RepositoryUUID == "" {
		intent.RepositoryUUID, intent.RepositoryRoot, intent.WorkspaceUUID, intent.WorkspaceRoot, intent.WorkspaceKind = deriveScope(intent.CWD, intent.Git, intent.JJ)
	}
	repositoryRoot := intent.RepositoryRoot
	if repositoryRoot == "" {
		repositoryRoot = intent.CWD
	}
	workspaceRoot := intent.WorkspaceRoot
	if workspaceRoot == "" {
		workspaceRoot = repositoryRoot
	}
	if len(intent.RelativePaths) == 0 {
		for _, path := range intent.Paths {
			relative, err := filepath.Rel(workspaceRoot, path)
			if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				intent.RelativePaths = append(intent.RelativePaths, filepath.Clean(path))
			} else {
				intent.RelativePaths = append(intent.RelativePaths, filepath.ToSlash(relative))
			}
		}
	}
	if err := projectScope(transaction, sequence, intent.RepositoryUUID, repositoryRoot, intent.WorkspaceUUID,
		intent.WorkspaceRoot, intent.WorkspaceKind, intent.Git, intent.JJ); err != nil {
		return err
	}
	paths, _ := json.Marshal(intent.Paths)
	relativePaths, _ := json.Marshal(intent.RelativePaths)
	before, _ := json.Marshal(intent.Before)
	after, _ := json.Marshal(intent.After)
	gitBefore, _ := marshalOptional(intent.Git)
	gitAfter, _ := marshalOptional(intent.GitAfter)
	jjBefore, _ := marshalOptional(intent.JJ)
	jjAfter, _ := marshalOptional(intent.JJAfter)
	var completedAt any
	if intent.CompletedAt != nil {
		completedAt = intent.CompletedAt.UTC().Format(time.RFC3339Nano)
	}
	var success any
	if intent.Success != nil {
		success = *intent.Success
	}
	_, err := transaction.Exec(`INSERT INTO mutations(
		id, actor, session_generation, turn_id, turn_index, tool_call_id, tool, operation, cwd,
		repository_uuid, repository_root, workspace_uuid, workspace_root, workspace_kind, workspace_key, paths_json, relative_paths_json,
		assistant_excerpt, started_at, completed_at, success, error, before_json, after_json,
		git_before_json, git_after_json, jj_before_json, jj_after_json, updated_sequence
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(id) DO UPDATE SET completed_at=excluded.completed_at, success=excluded.success,
		error=excluded.error, after_json=excluded.after_json, git_after_json=excluded.git_after_json,
		jj_after_json=excluded.jj_after_json, updated_sequence=excluded.updated_sequence`,
		intent.ID, uuidBlob(intent.Actor), intent.SessionGeneration, nullable(intent.TurnID), intent.TurnIndex,
		intent.ToolCallID, intent.Tool, intent.Operation, intent.CWD,
		nullableUUID(intent.RepositoryUUID), nullable(intent.RepositoryRoot), nullableUUID(intent.WorkspaceUUID), nullable(intent.WorkspaceRoot), nullable(intent.WorkspaceKind),
		intent.WorkspaceKey, string(paths), string(relativePaths), nullable(intent.Context.AssistantExcerpt),
		intent.StartedAt.UTC().Format(time.RFC3339Nano), completedAt, success, nullable(intent.Error),
		string(before), string(after), gitBefore, gitAfter, jjBefore, jjAfter, sequence)
	if err != nil {
		return fmt.Errorf("project mutation %s: %w", intent.ID, err)
	}
	if _, err := transaction.Exec(`DELETE FROM mutation_paths WHERE mutation_id = ?`, intent.ID); err != nil {
		return err
	}
	beforeByPath := snapshotsByPath(intent.Before)
	afterByPath := snapshotsByPath(intent.After)
	seen := make(map[string]bool)
	ordinal := 0
	for _, path := range append(append([]string(nil), intent.Paths...), snapshotPaths(intent.After)...) {
		if seen[path] {
			continue
		}
		seen[path] = true
		beforeJSON, _ := marshalOptional(beforeByPath[path])
		afterJSON, _ := marshalOptional(afterByPath[path])
		if _, err := transaction.Exec(`INSERT INTO mutation_paths(mutation_id, path, ordinal, before_json, after_json) VALUES (?, ?, ?, ?, ?)`,
			intent.ID, path, ordinal, beforeJSON, afterJSON); err != nil {
			return err
		}
		ordinal++
	}
	return nil
}

func snapshotsByPath(values []protocol.FileSnapshot) map[string]*protocol.FileSnapshot {
	result := make(map[string]*protocol.FileSnapshot, len(values))
	for index := range values {
		value := values[index]
		result[value.Path] = &value
	}
	return result
}

func snapshotPaths(values []protocol.FileSnapshot) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, value.Path)
	}
	return result
}

func marshalOptional(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if string(encoded) == "null" {
		return nil, nil
	}
	return string(encoded), nil
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

type ProjectionHealth struct {
	JournalSequence   uint64 `json:"journal_sequence"`
	ProjectedSequence uint64 `json:"projected_sequence"`
	Lag               uint64 `json:"lag"`
	QueueDepth        int    `json:"queue_depth"`
	LastError         string `json:"last_error,omitempty"`
}

type ProjectingAppender struct {
	primary           interface{ Append(protocol.Event) error }
	db                *DB
	queue             chan protocol.Event
	done              chan struct{}
	closing           chan struct{}
	stop              chan struct{}
	appenders         sync.WaitGroup
	mu                sync.Mutex
	closed            bool
	lastErr           error
	journalSequence   uint64
	projectedSequence uint64
	notify            chan struct{}
	once              sync.Once
}

func NewProjectingAppender(primary interface{ Append(protocol.Event) error }, database *DB, initialSequence ...uint64) *ProjectingAppender {
	initial := uint64(0)
	if len(initialSequence) > 0 {
		initial = initialSequence[0]
	}
	return newProjectingAppender(primary, database, 4096, initial)
}

func newProjectingAppender(primary interface{ Append(protocol.Event) error }, database *DB, capacity int, initialSequence ...uint64) *ProjectingAppender {
	initial := uint64(0)
	if len(initialSequence) > 0 {
		initial = initialSequence[0]
	}
	projected := uint64(0)
	if status, err := database.Status(); err == nil {
		projected = status.ProjectedSequence
	}
	appender := &ProjectingAppender{
		primary:           primary,
		db:                database,
		queue:             make(chan protocol.Event, capacity),
		done:              make(chan struct{}),
		closing:           make(chan struct{}),
		stop:              make(chan struct{}),
		journalSequence:   max(initial, projected),
		projectedSequence: projected,
		notify:            make(chan struct{}),
	}
	go appender.projectLoop()
	return appender
}

func (p *ProjectingAppender) Append(event protocol.Event) error {
	// Serialize the durable append with journalSequence publication. Register
	// the sender before releasing mu so Close cannot race it with shutdown.
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("provenance projection is closed")
	}
	p.appenders.Add(1)
	defer p.appenders.Done()
	// Keep the publication lock across the durable append. WaitForCurrent must
	// not observe a pre-append tail while an append is in flight.
	if err := p.primary.Append(event); err != nil {
		p.mu.Unlock()
		return err
	}
	p.journalSequence = max(p.journalSequence, event.Sequence)
	p.signalLocked()
	p.mu.Unlock()
	// The journal is already durable. If shutdown wins while this sender is
	// waiting for queue space, return without risking a send on a closed queue;
	// the next startup replays the journal and rebuilds the projection.
	select {
	case p.queue <- event:
		return nil
	case <-p.closing:
		return errors.New("provenance projection is shutting down")
	}
}

func (p *ProjectingAppender) projectLoop() {
	defer close(p.done)
	for event := range p.queue {
		backoff := 10 * time.Millisecond
		for {
			if err := p.db.Project(event); err != nil {
				p.recordProjectionError(event.Sequence, err)
				timer := time.NewTimer(backoff)
				select {
				case <-p.stop:
					if !timer.Stop() {
						<-timer.C
					}
					return
				case <-timer.C:
				}
				backoff = min(backoff*2, time.Second)
				continue
			}
			p.markProjected(event.Sequence)
			break
		}
	}
}

func (p *ProjectingAppender) recordProjectionError(sequence uint64, err error) {
	p.mu.Lock()
	p.lastErr = err
	p.mu.Unlock()
	log.Printf("agent-bridge: provenance projection lagging at event %d: %v", sequence, err)
}

func (p *ProjectingAppender) markProjected(sequence uint64) {
	p.mu.Lock()
	p.lastErr = nil
	p.projectedSequence = max(p.projectedSequence, sequence)
	p.signalLocked()
	p.mu.Unlock()
}

func (p *ProjectingAppender) signalLocked() {
	close(p.notify)
	p.notify = make(chan struct{})
}

func (p *ProjectingAppender) WaitForCurrent(ctx context.Context) error {
	p.mu.Lock()
	target := p.journalSequence
	for p.projectedSequence < target {
		notify := p.notify
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
		p.mu.Lock()
	}
	p.mu.Unlock()
	return nil
}

func (p *ProjectingAppender) Health() ProjectionHealth {
	p.mu.Lock()
	defer p.mu.Unlock()
	health := ProjectionHealth{
		JournalSequence:   p.journalSequence,
		ProjectedSequence: p.projectedSequence,
		QueueDepth:        len(p.queue),
	}
	if health.JournalSequence > health.ProjectedSequence {
		health.Lag = health.JournalSequence - health.ProjectedSequence
	}
	if p.lastErr != nil {
		health.LastError = p.lastErr.Error()
	}
	return health
}

func (p *ProjectingAppender) Close() {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.closing)
		p.mu.Unlock()
		p.appenders.Wait()
		close(p.queue)
		select {
		case <-p.done:
			return
		case <-time.After(5 * time.Second):
			// A permanently unprojectable event must not make shutdown hang.
			close(p.stop)
			<-p.done
		}
	})
}

func (p *ProjectingAppender) LastError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastErr
}

func (d *DB) ResolveActor(selector string) (string, error) {
	return d.ResolveActorScoped(selector, "", "")
}

func nullableUUID(value string) any {
	if value == "" {
		return nil
	}
	return uuidBlob(value)
}

func checkpointWorkUnitUUID(value string) (any, error) {
	if value == "" {
		return nil, nil
	}
	blob := uuidBlob(value)
	if len(blob) != 16 || blob[8]&0xc0 != 0x80 || blob[6]>>4 == 0 || blob[6]>>4 > 5 {
		return nil, fmt.Errorf("invalid checkpoint work_unit_uuid %q", value)
	}
	return blob, nil
}

func uuidBlob(value string) []byte {
	value = strings.TrimSpace(value)
	candidate := value
	for _, prefix := range []string{"pi:", "col:", "repo:", "workspace:"} {
		if strings.HasPrefix(candidate, prefix) && len(strings.ReplaceAll(candidate[len(prefix):], "-", "")) == 32 {
			candidate = candidate[len(prefix):]
			break
		}
	}
	compact := strings.ReplaceAll(candidate, "-", "")
	if len(compact) == 32 {
		if decoded, err := hex.DecodeString(compact); err == nil {
			return decoded
		}
	}
	return []byte(value)
}

func uuidString(value []byte) string {
	if len(value) != 16 {
		return string(value)
	}
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(value[:4]), hex.EncodeToString(value[4:6]), hex.EncodeToString(value[6:8]), hex.EncodeToString(value[8:10]), hex.EncodeToString(value[10:]))
}

func (d *DB) ResolveActorScoped(selector, repositoryID, workspaceID string) (string, error) {
	normalized := strings.TrimPrefix(strings.TrimSpace(selector), "@")
	var sessionUUID []byte
	err := d.db.QueryRow(`SELECT session_uuid FROM actors WHERE session_uuid = ?`, uuidBlob(normalized)).Scan(&sessionUUID)
	if err == nil {
		return uuidString(sessionUUID), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	query := `SELECT session_uuid FROM actors WHERE alias = ?`
	arguments := []any{normalized}
	if workspaceID != "" {
		query += ` AND workspace_uuid = ?`
		arguments = append(arguments, uuidBlob(workspaceID))
	} else if repositoryID != "" {
		query += ` AND repository_uuid = ?`
		arguments = append(arguments, uuidBlob(repositoryID))
	}
	query += ` ORDER BY session_uuid LIMIT 2`
	rows, err := d.db.Query(query, arguments...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var matches []string
	for rows.Next() {
		var match []byte
		if err := rows.Scan(&match); err != nil {
			return "", err
		}
		matches = append(matches, uuidString(match))
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("unknown persisted actor %q", selector)
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("persisted alias %q is ambiguous; provide a workspace/repository scope or canonical address", selector)
	}
	return matches[0], nil
}
