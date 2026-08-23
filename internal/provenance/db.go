package provenance

import (
	"bytes"
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
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
	_ "turso.tech/database/tursogo"
)

const projectionSchemaVersion = 18

// DB stores and projects provenance events.
type DB struct {
	db   *sql.DB
	path string
}

// Open opens or creates a provenance database at path.
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
		if closeErr := database.Close(); closeErr != nil {
			return nil, fmt.Errorf("initialize provenance database: %w (close database: %w)", err, closeErr)
		}
		return nil, err
	}
	if err := secureDatabaseFiles(path); err != nil {
		if closeErr := database.Close(); closeErr != nil {
			return nil, fmt.Errorf("secure provenance database: %w (close database: %w)", err, closeErr)
		}
		return nil, err
	}
	return result, nil
}

func initializeSchema(d *DB) error {
	for _, statements := range [][]string{
		initializeSchemaGroup0(),
		initializeSchemaGroup1(),
		initializeSchemaGroup2(),
	} {
		for _, statement := range statements {
			if _, err := d.db.ExecContext(context.Background(), statement); err != nil {
				return fmt.Errorf("initialize provenance database: %w", err)
			}
		}
	}
	return nil
}

func initializeSchemaGroup0() []string {
	return []string{
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
			actor_kind TEXT NOT NULL DEFAULT 'agent',
			addressable INTEGER NOT NULL DEFAULT 1,
			presence_kind TEXT NOT NULL DEFAULT 'lease',
			state TEXT NOT NULL,
			generation INTEGER NOT NULL,
			started_at TEXT NOT NULL,
			heartbeat_at TEXT NOT NULL,
			git_json TEXT,
			jj_json TEXT,
			capabilities_json TEXT NOT NULL,
			updated_sequence INTEGER NOT NULL
		) STRICT`,
	}
}

func initializeSchemaGroup1() []string {
	return []string{
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
		`CREATE INDEX IF NOT EXISTS mutations_sequence_id ON mutations(updated_sequence, id)`,
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
		`CREATE INDEX IF NOT EXISTS collisions_sequence_id ON collisions(updated_sequence, id)`,
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
	}
}

func initializeSchemaGroup2() []string {
	return []string{
		`CREATE INDEX IF NOT EXISTS messages_sequence_id ON messages(event_sequence, id)`,
		`CREATE INDEX IF NOT EXISTS collision_actors_actor ON collision_actors(session_uuid, collision_id)`,
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
			outcome TEXT NOT NULL DEFAULT 'blocked' CHECK(outcome IN ('passed','failed','blocked')),
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
		`CREATE INDEX IF NOT EXISTS test_results_sequence_id ON test_results(event_sequence, id)`,
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
		`CREATE TABLE IF NOT EXISTS checkpoint_evidence (
			checkpoint_id TEXT NOT NULL REFERENCES checkpoint_requests(id) ON DELETE CASCADE,
			kind TEXT NOT NULL,
			ordinal INTEGER NOT NULL,
			ref_text TEXT,
			ref_uuid BLOB,
			PRIMARY KEY(checkpoint_id, kind, ordinal),
			CHECK ((ref_text IS NOT NULL) != (ref_uuid IS NOT NULL))
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS checkpoint_metadata (
			checkpoint_id TEXT NOT NULL REFERENCES checkpoint_requests(id) ON DELETE CASCADE,
			key TEXT NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY(checkpoint_id, key)
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS checkpoint_claims (
			checkpoint_id TEXT NOT NULL REFERENCES checkpoint_requests(id) ON DELETE CASCADE,
			ordinal INTEGER NOT NULL,
			kind TEXT NOT NULL,
			statement TEXT NOT NULL,
			status TEXT NOT NULL CHECK(status IN ('asserted','verified','failed','blocked')),
			PRIMARY KEY(checkpoint_id, ordinal)
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS checkpoint_claim_evidence (
			checkpoint_id TEXT NOT NULL,
			claim_ordinal INTEGER NOT NULL,
			evidence_kind TEXT NOT NULL,
			evidence_ordinal INTEGER NOT NULL,
			PRIMARY KEY(checkpoint_id, claim_ordinal, evidence_kind, evidence_ordinal),
			FOREIGN KEY(checkpoint_id, claim_ordinal) REFERENCES checkpoint_claims(checkpoint_id, ordinal) ON DELETE CASCADE,
			FOREIGN KEY(checkpoint_id, evidence_kind, evidence_ordinal) REFERENCES checkpoint_evidence(checkpoint_id, kind, ordinal) ON DELETE CASCADE
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS work_units (
			work_unit_uuid BLOB PRIMARY KEY, direction_uuid BLOB REFERENCES directions(direction_uuid), repository_uuid BLOB NOT NULL, workspace_uuid BLOB NOT NULL,
			objective TEXT NOT NULL, acceptance_criteria TEXT, context TEXT, state TEXT NOT NULL,
			created_by BLOB NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT,
			updated_sequence INTEGER NOT NULL
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS directions (
			direction_uuid BLOB PRIMARY KEY, objective TEXT NOT NULL, success_criteria TEXT,
			constraints TEXT, context TEXT, state TEXT NOT NULL, created_by BLOB NOT NULL,
			created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT,
			updated_sequence INTEGER NOT NULL
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS work_unit_actors (
			work_unit_uuid BLOB NOT NULL REFERENCES work_units(work_unit_uuid) ON DELETE CASCADE,
			actor_uuid BLOB NOT NULL, joined_at TEXT NOT NULL, left_at TEXT, participation_state TEXT NOT NULL,
			PRIMARY KEY(work_unit_uuid, actor_uuid)
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS launches (launch_uuid BLOB PRIMARY KEY, child_actor_uuid BLOB, work_unit_uuid BLOB REFERENCES work_units(work_unit_uuid), created_at TEXT NOT NULL, child_attached_at TEXT, work_unit_attached_at TEXT, created_sequence INTEGER NOT NULL, updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE IF NOT EXISTS launch_parent_actors (launch_uuid BLOB NOT NULL REFERENCES launches(launch_uuid) ON DELETE CASCADE, ordinal INTEGER NOT NULL, actor_uuid BLOB NOT NULL, PRIMARY KEY(launch_uuid, ordinal), UNIQUE(launch_uuid, actor_uuid)) STRICT`,
		`CREATE INDEX IF NOT EXISTS launch_parent_actors_actor ON launch_parent_actors(actor_uuid, launch_uuid)`,
		`CREATE INDEX IF NOT EXISTS launches_child_actor ON launches(child_actor_uuid)`,
		`CREATE INDEX IF NOT EXISTS launches_work_unit ON launches(work_unit_uuid)`,
		`CREATE TABLE IF NOT EXISTS external_changes (external_change_uuid BLOB PRIMARY KEY, repository_uuid BLOB NOT NULL, workspace_uuid BLOB NOT NULL, unknown_actor_uuid BLOB NOT NULL, interval_started_at TEXT NOT NULL, interval_ended_at TEXT NOT NULL, continuity_state TEXT NOT NULL, change_kind TEXT NOT NULL, watchman_clock TEXT NOT NULL, before_json TEXT, after_json TEXT, data TEXT NOT NULL, event_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE IF NOT EXISTS external_change_paths (external_change_uuid BLOB NOT NULL REFERENCES external_changes(external_change_uuid) ON DELETE CASCADE, path TEXT NOT NULL, PRIMARY KEY(external_change_uuid, path)) STRICT`,
		`CREATE TABLE IF NOT EXISTS external_change_intents (external_change_uuid BLOB NOT NULL REFERENCES external_changes(external_change_uuid) ON DELETE CASCADE, intent_id TEXT NOT NULL, PRIMARY KEY(external_change_uuid, intent_id)) STRICT`,
		`CREATE TABLE IF NOT EXISTS workspace_continuity (workspace_uuid BLOB PRIMARY KEY, repository_uuid BLOB NOT NULL, state TEXT NOT NULL, at TEXT NOT NULL, watchman_clock TEXT, event_sequence INTEGER NOT NULL) STRICT`,
	}
}

func (d *DB) initialize() error {
	if err := initializeSchema(d); err != nil {
		return err
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
		{"actors", "actor_kind", "TEXT NOT NULL DEFAULT 'agent'"},
		{"actors", "addressable", "INTEGER NOT NULL DEFAULT 1"},
		{"actors", "presence_kind", "TEXT NOT NULL DEFAULT 'lease'"},
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
		{"checkpoint_requests", "tickets_json", "TEXT NOT NULL DEFAULT '[]'"},
		{"directions", "tickets_json", "TEXT NOT NULL DEFAULT '[]'"},
		{"work_units", "tickets_json", "TEXT NOT NULL DEFAULT '[]'"},
		{"test_results", "outcome", "TEXT NOT NULL DEFAULT 'blocked'"},
	} {
		if err := d.ensureColumn(column.table, column.name, column.definition); err != nil {
			return err
		}
	}
	return d.ensureProjectionVersion()
}

func (d *DB) ensureProjectionVersion() error {
	if err := d.createProjectionMeta(); err != nil {
		return err
	}
	version, err := d.projectionVersion()
	if err != nil {
		return err
	}
	if d.projectionIsCurrent(version) {
		return nil
	}
	if version >= projectionSchemaVersion {
		version = projectionSchemaVersion - 1
	}
	transaction, err := d.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer rollbackProjection(transaction)
	return resetProjection(transaction, version)
}

func (d *DB) createProjectionMeta() error {
	_, err := d.db.ExecContext(context.Background(), `CREATE TABLE IF NOT EXISTS projection_meta (version INTEGER NOT NULL) STRICT`)
	return err
}

func (d *DB) projectionVersion() (int, error) {
	var version int
	err := d.db.QueryRowContext(context.Background(), `SELECT version FROM projection_meta LIMIT 1`).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

func (d *DB) projectionIsCurrent(version int) bool {
	if version < projectionSchemaVersion {
		return false
	}
	var columnType string
	err := d.db.QueryRowContext(context.Background(), `SELECT type FROM pragma_table_info('messages') WHERE name = 'from_actor'`).Scan(&columnType)
	return err == nil && strings.EqualFold(columnType, "BLOB")
}

func rollbackProjection(transaction *sql.Tx) {
	if err := transaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		log.Printf("agent-bridge: rollback provenance projection transaction: %v", err)
	}
}

func resetProjection(transaction *sql.Tx, version int) error {
	if version < projectionSchemaVersion {
		if err := recreateBinaryProjectionTables(transaction); err != nil {
			return err
		}
	}
	tables := []string{"launch_parent_actors", "launches", "external_change_intents", "external_change_paths", "external_changes", "workspace_continuity", "checkpoint_claim_evidence", "checkpoint_claims", "checkpoint_metadata", "checkpoint_evidence", "checkpoint_requests", "work_unit_actors", "work_units", "directions", "mutation_paths", "mutations", "session_events", "messages", "test_results", "workspaces", "repositories", "events"}
	if version == 0 || version >= 7 {
		tables = append(tables, "collision_actors", "collisions")
	}
	for _, table := range tables {
		if _, err := transaction.ExecContext(context.Background(), `DELETE FROM `+table); err != nil {
			return fmt.Errorf("reset provenance projection table %s: %w", table, err)
		}
	}
	if _, err := transaction.ExecContext(context.Background(), `DELETE FROM projection_meta`); err != nil {
		return err
	}
	if _, err := transaction.ExecContext(context.Background(), `INSERT INTO projection_meta(version) VALUES (?)`, projectionSchemaVersion); err != nil {
		return err
	}
	return transaction.Commit()
}

func recreateBinaryProjectionTables(transaction *sql.Tx) error {
	// Projection state is disposable: the journal is the migration source. Drop
	// every UUID-bearing table together so old TEXT schemas cannot reject the
	// binary projection during backfill.
	for _, table := range []string{"launch_parent_actors", "launches", "external_change_intents", "external_change_paths", "external_changes", "workspace_continuity", "checkpoint_claim_evidence", "checkpoint_claims", "checkpoint_metadata", "checkpoint_evidence", "checkpoint_requests", "work_unit_actors", "work_units", "directions", "test_results", "messages", "collision_actors", "collisions", "session_events", "mutation_paths", "mutations", "actors", "workspaces", "repositories"} {
		if _, err := transaction.ExecContext(context.Background(), `DROP TABLE IF EXISTS `+table); err != nil {
			return fmt.Errorf("drop legacy projection table %s: %w", table, err)
		}
	}
	statements := []string{
		`CREATE TABLE repositories (id BLOB PRIMARY KEY, root TEXT NOT NULL, kind TEXT NOT NULL, git_common_dir TEXT, jj_repo_path TEXT, updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE workspaces (id BLOB PRIMARY KEY, repository_uuid BLOB NOT NULL REFERENCES repositories(id), root TEXT NOT NULL, kind TEXT NOT NULL, git_branch TEXT, git_head TEXT, jj_workspace_name TEXT, jj_change_id TEXT, updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE INDEX workspaces_repository ON workspaces(repository_uuid, root)`,
		`CREATE TABLE actors (session_uuid BLOB PRIMARY KEY, harness TEXT NOT NULL, alias TEXT, cwd TEXT NOT NULL, repository_uuid BLOB, repository_root TEXT, workspace_uuid BLOB, workspace_root TEXT, workspace_kind TEXT, actor_kind TEXT NOT NULL DEFAULT 'agent', addressable INTEGER NOT NULL DEFAULT 1, presence_kind TEXT NOT NULL DEFAULT 'lease', state TEXT NOT NULL, generation INTEGER NOT NULL, started_at TEXT NOT NULL, heartbeat_at TEXT NOT NULL, git_json TEXT, jj_json TEXT, capabilities_json TEXT NOT NULL, updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE mutations (id TEXT PRIMARY KEY, actor BLOB NOT NULL, session_generation INTEGER NOT NULL, turn_id TEXT, turn_index INTEGER, tool_call_id TEXT NOT NULL, tool TEXT NOT NULL, operation TEXT NOT NULL, cwd TEXT NOT NULL, repository_uuid BLOB, repository_root TEXT, workspace_uuid BLOB, workspace_root TEXT, workspace_kind TEXT, workspace_key TEXT NOT NULL, paths_json TEXT NOT NULL, relative_paths_json TEXT NOT NULL, assistant_excerpt TEXT, started_at TEXT NOT NULL, completed_at TEXT, success INTEGER, error TEXT, before_json TEXT NOT NULL, after_json TEXT NOT NULL, git_before_json TEXT, git_after_json TEXT, jj_before_json TEXT, jj_after_json TEXT, updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE INDEX mutations_actor_started ON mutations(actor, started_at DESC)`,
		`CREATE INDEX mutations_started ON mutations(started_at DESC)`,
		`CREATE INDEX mutations_sequence_id ON mutations(updated_sequence, id)`,
		`CREATE TABLE mutation_paths (mutation_id TEXT NOT NULL REFERENCES mutations(id) ON DELETE CASCADE, path TEXT NOT NULL, ordinal INTEGER NOT NULL, before_json TEXT, after_json TEXT, PRIMARY KEY(mutation_id, path)) STRICT`,
		`CREATE INDEX mutation_paths_path ON mutation_paths(path, mutation_id)`,
		`CREATE TABLE session_events (id TEXT PRIMARY KEY, actor BLOB NOT NULL, session_generation INTEGER NOT NULL, type TEXT NOT NULL, at TEXT NOT NULL, turn_index INTEGER, summary TEXT, data TEXT NOT NULL, event_sequence INTEGER NOT NULL) STRICT`,
		`CREATE INDEX session_events_actor_at ON session_events(actor, at DESC)`,
		`CREATE TABLE collisions (id BLOB PRIMARY KEY, path TEXT NOT NULL, state TEXT NOT NULL, owner BLOB, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, resolved_at TEXT, resolution TEXT, resolved_by BLOB, dead_actor BLOB, data TEXT NOT NULL, updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE collision_actors (collision_id BLOB NOT NULL REFERENCES collisions(id) ON DELETE CASCADE, ordinal INTEGER NOT NULL, session_uuid BLOB NOT NULL, PRIMARY KEY(collision_id, ordinal)) STRICT`,
		`CREATE INDEX collisions_sequence_id ON collisions(updated_sequence, id)`,
		`CREATE TABLE messages (id TEXT PRIMARY KEY, kind TEXT NOT NULL, from_actor BLOB NOT NULL, to_actor BLOB NOT NULL, body TEXT NOT NULL, global_sequence INTEGER NOT NULL, sender_sequence INTEGER NOT NULL, recipient_sequence INTEGER NOT NULL, client_sequence INTEGER, session_generation INTEGER, collision_id BLOB, created_at TEXT NOT NULL, acknowledged_at TEXT, data TEXT NOT NULL, event_sequence INTEGER NOT NULL) STRICT`,
		`CREATE INDEX messages_actor_created ON messages(to_actor, created_at DESC)`,
		`CREATE INDEX messages_sequence_id ON messages(event_sequence, id)`,
		`CREATE INDEX collision_actors_actor ON collision_actors(session_uuid, collision_id)`,
		`CREATE TABLE test_results (id TEXT PRIMARY KEY, actor BLOB NOT NULL, session_generation INTEGER NOT NULL, turn_id TEXT, turn_index INTEGER, tool_call_id TEXT, command TEXT NOT NULL, cwd TEXT NOT NULL, exit_code INTEGER, outcome TEXT NOT NULL DEFAULT 'blocked' CHECK(outcome IN ('passed','failed','blocked')), started_at TEXT NOT NULL, completed_at TEXT NOT NULL, duration_ms INTEGER, output_excerpt TEXT, output_sha256 TEXT, output_bytes INTEGER, output_truncated INTEGER NOT NULL, repository_uuid BLOB, workspace_uuid BLOB, data TEXT NOT NULL, event_sequence INTEGER NOT NULL) STRICT`,
		`CREATE INDEX test_results_sequence_id ON test_results(event_sequence, id)`,
		`CREATE TABLE checkpoint_requests (id TEXT PRIMARY KEY, actor BLOB NOT NULL, declared_by TEXT NOT NULL, session_generation INTEGER NOT NULL, repository_uuid BLOB NOT NULL, workspace_uuid BLOB NOT NULL, work_unit_uuid BLOB REFERENCES work_units(work_unit_uuid), checkpoint_kind TEXT NOT NULL, journal_start_sequence INTEGER NOT NULL, journal_end_sequence INTEGER NOT NULL, tickets_json TEXT NOT NULL DEFAULT '[]', data TEXT NOT NULL, event_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE checkpoint_evidence (checkpoint_id TEXT NOT NULL REFERENCES checkpoint_requests(id) ON DELETE CASCADE, kind TEXT NOT NULL, ordinal INTEGER NOT NULL, ref_text TEXT, ref_uuid BLOB, PRIMARY KEY(checkpoint_id, kind, ordinal), CHECK ((ref_text IS NOT NULL) != (ref_uuid IS NOT NULL))) STRICT`,
		`CREATE TABLE checkpoint_metadata (checkpoint_id TEXT NOT NULL REFERENCES checkpoint_requests(id) ON DELETE CASCADE, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(checkpoint_id, key)) STRICT`,
		`CREATE TABLE checkpoint_claims (checkpoint_id TEXT NOT NULL REFERENCES checkpoint_requests(id) ON DELETE CASCADE, ordinal INTEGER NOT NULL, kind TEXT NOT NULL, statement TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('asserted','verified','failed','blocked')), PRIMARY KEY(checkpoint_id, ordinal)) STRICT`,
		`CREATE TABLE checkpoint_claim_evidence (checkpoint_id TEXT NOT NULL, claim_ordinal INTEGER NOT NULL, evidence_kind TEXT NOT NULL, evidence_ordinal INTEGER NOT NULL, PRIMARY KEY(checkpoint_id, claim_ordinal, evidence_kind, evidence_ordinal), FOREIGN KEY(checkpoint_id, claim_ordinal) REFERENCES checkpoint_claims(checkpoint_id, ordinal) ON DELETE CASCADE, FOREIGN KEY(checkpoint_id, evidence_kind, evidence_ordinal) REFERENCES checkpoint_evidence(checkpoint_id, kind, ordinal) ON DELETE CASCADE) STRICT`,
		`CREATE TABLE work_units (work_unit_uuid BLOB PRIMARY KEY, direction_uuid BLOB REFERENCES directions(direction_uuid), repository_uuid BLOB NOT NULL, workspace_uuid BLOB NOT NULL, objective TEXT NOT NULL, acceptance_criteria TEXT, context TEXT, state TEXT NOT NULL, created_by BLOB NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT, tickets_json TEXT NOT NULL DEFAULT '[]', updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE directions (direction_uuid BLOB PRIMARY KEY, objective TEXT NOT NULL, success_criteria TEXT, constraints TEXT, context TEXT, state TEXT NOT NULL, created_by BLOB NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT, tickets_json TEXT NOT NULL DEFAULT '[]', updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE work_unit_actors (work_unit_uuid BLOB NOT NULL REFERENCES work_units(work_unit_uuid) ON DELETE CASCADE, actor_uuid BLOB NOT NULL, joined_at TEXT NOT NULL, left_at TEXT, participation_state TEXT NOT NULL, PRIMARY KEY(work_unit_uuid, actor_uuid)) STRICT`,
		`CREATE TABLE external_changes (external_change_uuid BLOB PRIMARY KEY, repository_uuid BLOB NOT NULL, workspace_uuid BLOB NOT NULL, unknown_actor_uuid BLOB NOT NULL, interval_started_at TEXT NOT NULL, interval_ended_at TEXT NOT NULL, continuity_state TEXT NOT NULL, change_kind TEXT NOT NULL, watchman_clock TEXT NOT NULL, before_json TEXT, after_json TEXT, data TEXT NOT NULL, event_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE external_change_paths (external_change_uuid BLOB NOT NULL REFERENCES external_changes(external_change_uuid) ON DELETE CASCADE, path TEXT NOT NULL, PRIMARY KEY(external_change_uuid, path)) STRICT`,
		`CREATE TABLE external_change_intents (external_change_uuid BLOB NOT NULL REFERENCES external_changes(external_change_uuid) ON DELETE CASCADE, intent_id TEXT NOT NULL, PRIMARY KEY(external_change_uuid, intent_id)) STRICT`,
		`CREATE TABLE launches (launch_uuid BLOB PRIMARY KEY, child_actor_uuid BLOB, work_unit_uuid BLOB REFERENCES work_units(work_unit_uuid), created_at TEXT NOT NULL, child_attached_at TEXT, work_unit_attached_at TEXT, created_sequence INTEGER NOT NULL, updated_sequence INTEGER NOT NULL) STRICT`,
		`CREATE TABLE launch_parent_actors (launch_uuid BLOB NOT NULL REFERENCES launches(launch_uuid) ON DELETE CASCADE, ordinal INTEGER NOT NULL, actor_uuid BLOB NOT NULL, PRIMARY KEY(launch_uuid, ordinal), UNIQUE(launch_uuid, actor_uuid)) STRICT`,
		`CREATE INDEX launch_parent_actors_actor ON launch_parent_actors(actor_uuid, launch_uuid)`,
		`CREATE INDEX launches_child_actor ON launches(child_actor_uuid)`,
		`CREATE INDEX launches_work_unit ON launches(work_unit_uuid)`,
		`CREATE TABLE workspace_continuity (workspace_uuid BLOB PRIMARY KEY, repository_uuid BLOB NOT NULL, state TEXT NOT NULL, at TEXT NOT NULL, watchman_clock TEXT, event_sequence INTEGER NOT NULL) STRICT`,
	}
	for _, statement := range statements {
		if _, err := transaction.ExecContext(context.Background(), statement); err != nil {
			return fmt.Errorf("recreate binary projection: %w", err)
		}
	}
	return nil
}

func (d *DB) ensureColumn(table, column, definition string) error {
	rows, err := d.db.QueryContext(context.Background(), `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("agent-bridge: close provenance rows: %v", err)
		}
	}()
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
	if _, err := d.db.ExecContext(context.Background(), `ALTER TABLE `+table+` ADD COLUMN `+column+` `+definition); err != nil {
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

// Close closes the provenance database.
func (d *DB) Close() error { return d.db.Close() }

// Path returns the provenance database path.
func (d *DB) Path() string { return d.path }

// PruneNonUUIDActors removes legacy actor keys from the read model. The
// append-only journal remains untouched and can be reprojected later.
// PruneNonUUIDActors removes projection rows with invalid UUID values.
func (d *DB) PruneNonUUIDActors() error {
	statements := []string{
		`DELETE FROM external_change_intents WHERE length(external_change_uuid) != 16`,
		`DELETE FROM external_change_paths WHERE length(external_change_uuid) != 16`,
		`DELETE FROM external_changes WHERE length(external_change_uuid) != 16 OR length(repository_uuid) != 16 OR length(workspace_uuid) != 16 OR length(unknown_actor_uuid) != 16`,
		`DELETE FROM workspace_continuity WHERE length(workspace_uuid) != 16 OR length(repository_uuid) != 16`,
		`DELETE FROM actors WHERE length(session_uuid) != 16 OR (repository_uuid IS NOT NULL AND length(repository_uuid) != 16) OR (workspace_uuid IS NOT NULL AND length(workspace_uuid) != 16)`,
		`DELETE FROM mutations WHERE (repository_uuid IS NOT NULL AND length(repository_uuid) != 16) OR (workspace_uuid IS NOT NULL AND length(workspace_uuid) != 16)`,
		`DELETE FROM test_results WHERE (repository_uuid IS NOT NULL AND length(repository_uuid) != 16) OR (workspace_uuid IS NOT NULL AND length(workspace_uuid) != 16)`,
		`DELETE FROM checkpoint_requests WHERE length(repository_uuid) != 16 OR length(workspace_uuid) != 16 OR (work_unit_uuid IS NOT NULL AND length(work_unit_uuid) != 16)`,
		`DELETE FROM workspaces WHERE length(id) != 16 OR length(repository_uuid) != 16`,
		`DELETE FROM repositories WHERE length(id) != 16`,
	}
	for _, statement := range statements {
		if _, err := d.db.ExecContext(context.Background(), statement); err != nil {
			return err
		}
	}
	return nil
}

// Snapshot creates a compact, consistent database copy through the same Turso
// connection that owns the live file. Other processes can inspect the copy
// without competing for the live engine lock.
// Snapshot creates a compact copy of the provenance database.
func (d *DB) Snapshot(path string) error {
	if path == "" || !filepath.IsAbs(path) {
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
	if _, err := d.db.ExecContext(context.Background(), `VACUUM INTO '`+literal+`'`); err != nil {
		return fmt.Errorf("create provenance snapshot: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("secure provenance snapshot: %w", err)
	}
	return nil
}

// ProjectAll projects events into the provenance database.
func (d *DB) ProjectAll(events []protocol.Event) error {
	for _, event := range events {
		if err := d.Project(event); err != nil {
			return err
		}
	}
	return nil
}

// Project projects one event into the provenance database.
//
//nolint:gocritic // public Appender-compatible value API
func (d *DB) Project(event protocol.Event) error {
	transaction, err := d.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := transaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Printf("agent-bridge: rollback provenance projection transaction: %v", err)
		}
	}()
	result, err := transaction.ExecContext(context.Background(), `INSERT OR IGNORE INTO events(sequence, type, at, data) VALUES (?, ?, ?, ?)`,
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
		if err := transaction.QueryRowContext(context.Background(), `SELECT type, data FROM events WHERE sequence = ?`, event.Sequence).Scan(&existingType, &existingData); err != nil {
			return fmt.Errorf("check duplicate provenance event %d: %w", event.Sequence, err)
		}
		if existingType != event.Type || existingData != string(event.Data) {
			return fmt.Errorf("conflicting provenance event sequence %d", event.Sequence)
		}
		return nil
	}
	if err := d.projectDomain(transaction, &event); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return err
	}
	return secureDatabaseFiles(d.path)
}

func (d *DB) projectDomain(transaction *sql.Tx, event *protocol.Event) error {
	handlers := map[string]func(*sql.Tx, *protocol.Event) error{
		"actor.upserted": projectActorUpserted, "intent.started": projectIntentStarted,
		"intent.completed": projectIntentStarted, "message.enqueued": projectMessageEnqueued,
		"message.acked": projectMessageAcked, "session.event": projectSessionEvent,
		"test.result": projectTestResult, "launch.created": projectLaunchCreated,
		"launch.child_attached": projectLaunchChildAttached, "launch.work_unit_attached": projectLaunchWorkUnitAttached,
		"direction.created":      projectDirectionCreated,
		"direction.updated":      projectDirectionUpdated,
		"direction.transitioned": projectDirectionTransitioned,
		"work_unit.created":      projectWorkUnitCreated, "work_unit.updated": projectWorkUnitUpdated,
		"work_unit.transitioned": projectWorkUnitTransitioned, "work_unit.actor_joined": projectWorkUnitActorJoined,
		"work_unit.actor_left": projectWorkUnitActorJoined, "checkpoint.requested": projectCheckpointRequested,
		"collision.actor_dead": projectCollisionActorDead, "collision.transitioned": projectCollisionTransitioned,
		"collision.upserted":        projectCollisionUpserted,
		"external_change.observed":  projectExternalChange,
		"watch.continuity_lost":     projectWatchContinuity,
		"watch.continuity_restored": projectWatchContinuity,
	}
	if handler := handlers[event.Type]; handler != nil {
		return handler(transaction, event)
	}
	return nil
}

func projectActorUpserted(transaction *sql.Tx, event *protocol.Event) error {
	var actor protocol.Actor
	if err := json.Unmarshal(event.Data, &actor); err != nil {
		return err
	}
	address, ok := normalizeProjectedActorUUID(actor.Address)
	if !ok {
		// Legacy actor facts are retained in the journal, but malformed or
		// unsupported identities must never enter the relational read model.
		return nil
	}
	sessionUUID, ok := normalizeProjectedActorUUID(actor.SessionUUID)
	if !ok || sessionUUID != address {
		return nil
	}
	actor.Address, actor.SessionUUID = address, sessionUUID
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
	gitJSON, err := marshalOptional(actor.Git)
	if err != nil {
		return err
	}
	jjJSON, err := marshalOptional(actor.JJ)
	if err != nil {
		return err
	}
	capabilities, err := json.Marshal(actor.Capabilities)
	if err != nil {
		return err
	}
	sessionBlob := uuidBlob(address)
	_, err = transaction.ExecContext(context.Background(), `INSERT INTO actors(
		session_uuid, harness, alias, cwd, repository_uuid, repository_root, workspace_uuid, workspace_root, workspace_kind,
		actor_kind, addressable, presence_kind, state, generation, started_at, heartbeat_at, git_json, jj_json, capabilities_json, updated_sequence
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(session_uuid) DO UPDATE SET
		alias=excluded.alias, cwd=excluded.cwd, repository_uuid=excluded.repository_uuid, repository_root=excluded.repository_root,
		workspace_uuid=excluded.workspace_uuid, workspace_root=excluded.workspace_root, workspace_kind=excluded.workspace_kind, actor_kind=excluded.actor_kind, addressable=excluded.addressable, presence_kind=excluded.presence_kind,
		state=excluded.state, generation=excluded.generation, heartbeat_at=excluded.heartbeat_at,
		git_json=excluded.git_json, jj_json=excluded.jj_json, capabilities_json=excluded.capabilities_json,
		updated_sequence=excluded.updated_sequence`,
		sessionBlob, actor.Harness, nullable(actor.Alias), actor.CWD,
		nullableUUID(actor.RepositoryUUID), nullable(actor.RepositoryRoot), nullableUUID(actor.WorkspaceUUID), nullable(actor.WorkspaceRoot), nullable(actor.WorkspaceKind),
		coalesceText(actor.ActorKind, "agent"), actor.Addressable, coalesceText(actor.PresenceKind, "lease"), actor.State, actor.Generation, actor.StartedAt.UTC().Format(time.RFC3339Nano), actor.HeartbeatAt.UTC().Format(time.RFC3339Nano),
		gitJSON, jjJSON, string(capabilities), event.Sequence)
	return err
}

func projectIntentStarted(transaction *sql.Tx, event *protocol.Event) error {
	var intent protocol.Intent
	if err := json.Unmarshal(event.Data, &intent); err != nil {
		return err
	}
	return projectIntent(transaction, event.Sequence, &intent)
}

func projectMessageEnqueued(transaction *sql.Tx, event *protocol.Event) error {
	var message protocol.Message
	if err := json.Unmarshal(event.Data, &message); err != nil {
		return err
	}
	_, err := transaction.ExecContext(context.Background(), `INSERT OR IGNORE INTO messages(
		id, kind, from_actor, to_actor, body, global_sequence, sender_sequence, recipient_sequence,
		client_sequence, session_generation, collision_id, created_at, data, event_sequence
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, message.ID, message.Kind, uuidBlob(message.From), uuidBlob(message.To), message.Body,
		message.GlobalSequence, message.SenderSequence, message.RecipientSequence, message.ClientSequence, message.SessionGeneration,
		nullableUUID(message.CollisionID), message.CreatedAt.UTC().Format(time.RFC3339Nano), string(event.Data), event.Sequence)
	return err
}

func projectMessageAcked(transaction *sql.Tx, event *protocol.Event) error {
	var ack struct {
		Actor      string    `json:"actor"`
		MessageIDs []string  `json:"message_ids"`
		At         time.Time `json:"at"`
	}
	if err := json.Unmarshal(event.Data, &ack); err != nil {
		return err
	}
	for _, id := range ack.MessageIDs {
		if _, err := transaction.ExecContext(context.Background(), `UPDATE messages SET acknowledged_at = ? WHERE id = ? AND to_actor = ?`, ack.At.UTC().Format(time.RFC3339Nano), id, uuidBlob(ack.Actor)); err != nil {
			return err
		}
	}
	return nil
}

func projectSessionEvent(transaction *sql.Tx, event *protocol.Event) error {
	var sessionEvent protocol.SessionEvent
	if err := json.Unmarshal(event.Data, &sessionEvent); err != nil {
		return err
	}
	data := sessionEvent.Data
	if len(data) == 0 {
		data = json.RawMessage(`{}`)
	}
	_, err := transaction.ExecContext(context.Background(), `INSERT OR REPLACE INTO session_events(
		id, actor, session_generation, type, at, turn_index, summary, data, event_sequence
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, sessionEvent.ID, uuidBlob(sessionEvent.Actor), sessionEvent.SessionGeneration,
		sessionEvent.Type, sessionEvent.At.UTC().Format(time.RFC3339Nano), sessionEvent.TurnIndex,
		nullable(sessionEvent.Summary), string(data), event.Sequence)
	return err
}

func projectTestResult(transaction *sql.Tx, event *protocol.Event) error {
	var result protocol.TestResult
	if err := json.Unmarshal(event.Data, &result); err != nil {
		return err
	}
	if result.RepositoryUUID != "" {
		if err := protocol.ValidateUUID(result.RepositoryUUID); err != nil {
			return fmt.Errorf("repository_uuid: %w", err)
		}
	}
	if result.WorkspaceUUID != "" {
		if err := protocol.ValidateUUID(result.WorkspaceUUID); err != nil {
			return fmt.Errorf("workspace_uuid: %w", err)
		}
	}
	if err := protocol.NormalizeTestResult(&result); err != nil {
		return err
	}
	_, err := transaction.ExecContext(context.Background(), `INSERT OR IGNORE INTO test_results(
		id, actor, session_generation, turn_id, turn_index, tool_call_id, command, cwd, exit_code, outcome,
		started_at, completed_at, duration_ms, output_excerpt, output_sha256, output_bytes, output_truncated,
		repository_uuid, workspace_uuid, data, event_sequence
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, result.ID, uuidBlob(result.Actor),
		result.SessionGeneration, nullable(result.TurnID), result.TurnIndex, nullable(result.ToolCallID), result.Command, result.CWD,
		result.ExitCode, result.Outcome, result.StartedAt.UTC().Format(time.RFC3339Nano), result.CompletedAt.UTC().Format(time.RFC3339Nano),
		result.DurationMillis, nullable(result.OutputExcerpt), nullable(result.OutputSHA256), result.OutputBytes, result.OutputTruncated,
		nullableUUID(result.RepositoryUUID), nullableUUID(result.WorkspaceUUID), string(event.Data), event.Sequence)
	return err
}

//nolint:cyclop // Creation validates every normalized UUID relation before persistence.
func projectLaunchCreated(transaction *sql.Tx, event *protocol.Event) error {
	var created protocol.LaunchCreatedEvent
	if err := json.Unmarshal(event.Data, &created); err != nil {
		return err
	}
	launch := created.Launch
	if err := protocol.ValidateUUID(launch.UUID); err != nil || launch.ChildActor != "" || launch.WorkUnitUUID != "" || launch.ChildAttachedAt != nil || launch.WorkUnitAttachedAt != nil {
		return errors.New("invalid launch creation")
	}
	parents := append([]string(nil), launch.ParentActors...)
	sort.Strings(parents)
	if !reflect.DeepEqual(parents, launch.ParentActors) {
		return errors.New("launch parents are not normalized")
	}
	for index, parent := range parents {
		if err := protocol.ValidateUUID(parent); err != nil || index > 0 && parent == parents[index-1] {
			return errors.New("invalid launch parent")
		}
	}
	if _, err := transaction.ExecContext(context.Background(), `INSERT INTO launches(launch_uuid, created_at, created_sequence, updated_sequence) VALUES (?, ?, ?, ?)`, uuidBlob(launch.UUID), launch.CreatedAt.UTC().Format(time.RFC3339Nano), event.Sequence, event.Sequence); err != nil {
		return err
	}
	for ordinal, parent := range parents {
		if _, err := transaction.ExecContext(context.Background(), `INSERT INTO launch_parent_actors(launch_uuid, ordinal, actor_uuid) VALUES (?, ?, ?)`, uuidBlob(launch.UUID), ordinal, uuidBlob(parent)); err != nil {
			return err
		}
	}
	return nil
}

func projectLaunchChildAttached(transaction *sql.Tx, event *protocol.Event) error {
	var attached protocol.LaunchChildAttachedEvent
	if err := json.Unmarshal(event.Data, &attached); err != nil {
		return err
	}
	if protocol.ValidateUUID(attached.LaunchUUID) != nil || protocol.ValidateUUID(attached.ChildActor) != nil || attached.At.IsZero() {
		return errors.New("invalid launch child attachment")
	}
	result, err := transaction.ExecContext(context.Background(), `UPDATE launches SET child_actor_uuid=?, child_attached_at=?, updated_sequence=? WHERE launch_uuid=? AND child_actor_uuid IS NULL`, uuidBlob(attached.ChildActor), attached.At.UTC().Format(time.RFC3339Nano), event.Sequence, uuidBlob(attached.LaunchUUID))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return errors.New("launch child attachment references unknown or attached launch")
	}
	return nil
}

func projectLaunchWorkUnitAttached(transaction *sql.Tx, event *protocol.Event) error {
	var attached protocol.LaunchWorkUnitAttachedEvent
	if err := json.Unmarshal(event.Data, &attached); err != nil {
		return err
	}
	if protocol.ValidateUUID(attached.LaunchUUID) != nil || protocol.ValidateUUID(attached.WorkUnitUUID) != nil || attached.At.IsZero() {
		return errors.New("invalid launch work unit attachment")
	}
	result, err := transaction.ExecContext(context.Background(), `UPDATE launches SET work_unit_uuid=?, work_unit_attached_at=?, updated_sequence=? WHERE launch_uuid=? AND work_unit_uuid IS NULL`, uuidBlob(attached.WorkUnitUUID), attached.At.UTC().Format(time.RFC3339Nano), event.Sequence, uuidBlob(attached.LaunchUUID))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return errors.New("launch work unit attachment references unknown or attached launch")
	}
	return nil
}

func ticketJSON(tickets protocol.Tickets) string {
	data, err := json.Marshal(tickets)
	if err != nil {
		return "[]"
	}
	return string(data)
}

func projectDirectionCreated(transaction *sql.Tx, event *protocol.Event) error {
	var created protocol.DirectionCreatedEvent
	if err := json.Unmarshal(event.Data, &created); err != nil {
		return err
	}
	direction := created.Direction
	for name, value := range map[string]string{"direction_uuid": direction.UUID, "created_by": direction.CreatedBy} {
		if err := protocol.ValidateUUID(value); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	result, err := transaction.ExecContext(context.Background(), `INSERT OR IGNORE INTO directions(
		direction_uuid, objective, success_criteria, constraints, context, state, created_by,
		created_at, updated_at, completed_at, tickets_json, updated_sequence
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, uuidBlob(direction.UUID), direction.Objective,
		nullable(direction.SuccessCriteria), nullable(direction.Constraints), nullable(direction.Context), direction.State,
		uuidBlob(direction.CreatedBy), direction.CreatedAt.UTC().Format(time.RFC3339Nano), direction.UpdatedAt.UTC().Format(time.RFC3339Nano),
		nullableTime(direction.CompletedAt), ticketJSON(direction.Tickets), event.Sequence)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count == 0 {
		var existing protocol.Direction
		var id, creator []byte
		var createdAt, updatedAt string
		var completed sql.NullString
		err := transaction.QueryRowContext(context.Background(), `SELECT direction_uuid, objective, COALESCE(success_criteria, ''), COALESCE(constraints, ''), COALESCE(context, ''), state, created_by, created_at, updated_at, completed_at FROM directions WHERE direction_uuid = ?`, uuidBlob(direction.UUID)).Scan(&id, &existing.Objective, &existing.SuccessCriteria, &existing.Constraints, &existing.Context, &existing.State, &creator, &createdAt, &updatedAt, &completed)
		if err != nil {
			return err
		}
		existing.UUID, existing.CreatedBy = uuidString(id), uuidString(creator)
		existing.CreatedAt = parseProjectionTime(createdAt)
		existing.UpdatedAt = parseProjectionTime(updatedAt)
		if completed.Valid {
			at := parseProjectionTime(completed.String)
			existing.CompletedAt = &at
		}
		if !reflect.DeepEqual(existing, direction) {
			return errors.New("conflicting direction creation")
		}
	}
	return nil
}

func projectDirectionUpdated(transaction *sql.Tx, event *protocol.Event) error {
	var updated protocol.DirectionUpdatedEvent
	if err := json.Unmarshal(event.Data, &updated); err != nil {
		return err
	}
	current, err := loadProjectedDirection(transaction, updated.UUID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, updated.Previous) || updated.Result.UUID != updated.UUID || updated.Result.State != current.State {
		return errors.New("invalid direction update projection")
	}
	_, err = transaction.ExecContext(context.Background(), `UPDATE directions SET objective=?, success_criteria=?, constraints=?, context=?, updated_at=?, tickets_json=?, updated_sequence=? WHERE direction_uuid=?`, updated.Result.Objective, nullable(updated.Result.SuccessCriteria), nullable(updated.Result.Constraints), nullable(updated.Result.Context), updated.Result.UpdatedAt.UTC().Format(time.RFC3339Nano), ticketJSON(updated.Result.Tickets), event.Sequence, uuidBlob(updated.UUID))
	return err
}
func loadProjectedDirection(transaction *sql.Tx, uuid string) (protocol.Direction, error) {
	var result protocol.Direction
	var id, creator []byte
	var created, updated string
	var completed sql.NullString
	var tickets string
	err := transaction.QueryRowContext(context.Background(), `SELECT direction_uuid, objective, COALESCE(success_criteria,''), COALESCE(constraints,''), COALESCE(context,''), state, created_by, created_at, updated_at, completed_at, tickets_json FROM directions WHERE direction_uuid=?`, uuidBlob(uuid)).Scan(&id, &result.Objective, &result.SuccessCriteria, &result.Constraints, &result.Context, &result.State, &creator, &created, &updated, &completed, &tickets)
	if err != nil {
		return result, err
	}
	result.UUID, result.CreatedBy = uuidString(id), uuidString(creator)
	result.CreatedAt, result.UpdatedAt = parseProjectionTime(created), parseProjectionTime(updated)
	if completed.Valid {
		at := parseProjectionTime(completed.String)
		result.CompletedAt = &at
	}
	if err := json.Unmarshal([]byte(tickets), &result.Tickets); err != nil {
		return result, err
	}
	return result, nil
}

func projectDirectionTransitioned(transaction *sql.Tx, event *protocol.Event) error {
	var transition protocol.DirectionTransitionEvent
	if err := json.Unmarshal(event.Data, &transition); err != nil {
		return err
	}
	if protocol.ValidateUUID(transition.DirectionUUID) != nil || protocol.ValidateUUID(transition.Actor) != nil || !validProjectedDirectionTransition(transition.From, transition.To) {
		return errors.New("invalid direction transition")
	}
	var state protocol.DirectionState
	if err := transaction.QueryRowContext(context.Background(), `SELECT state FROM directions WHERE direction_uuid = ?`, uuidBlob(transition.DirectionUUID)).Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("direction transition references unknown direction")
		}
		return err
	}
	// A duplicate journal event is filtered before domain projection. Keeping
	// the state check here makes a transition replay-safe without allowing a
	// stale transition to overwrite newer authority.
	if state != transition.From {
		return errors.New("invalid direction transition provenance")
	}
	var completed any
	if transition.To == protocol.DirectionCompleted {
		completed = transition.At.UTC().Format(time.RFC3339Nano)
	}
	result, err := transaction.ExecContext(context.Background(), `UPDATE directions SET state = ?, updated_at = ?, completed_at = ?, updated_sequence = ? WHERE direction_uuid = ? AND state = ?`,
		transition.To, transition.At.UTC().Format(time.RFC3339Nano), completed, event.Sequence, uuidBlob(transition.DirectionUUID), transition.From)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count != 1 {
		return errors.New("direction transition references unknown or stale direction")
	}
	return nil
}

func validProjectedDirectionTransition(from, to protocol.DirectionState) bool {
	allowed := map[protocol.DirectionState]map[protocol.DirectionState]bool{
		protocol.DirectionDraft:      {protocol.DirectionActive: true, protocol.DirectionAbandoned: true},
		protocol.DirectionActive:     {protocol.DirectionPaused: true, protocol.DirectionConverging: true, protocol.DirectionAbandoned: true},
		protocol.DirectionPaused:     {protocol.DirectionActive: true, protocol.DirectionAbandoned: true},
		protocol.DirectionConverging: {protocol.DirectionActive: true, protocol.DirectionVerified: true, protocol.DirectionAbandoned: true},
		protocol.DirectionVerified:   {protocol.DirectionCompleted: true, protocol.DirectionAbandoned: true},
	}
	return allowed[from][to]
}

func projectWorkUnitCreated(transaction *sql.Tx, event *protocol.Event) error {
	var created protocol.WorkUnitCreatedEvent
	if err := json.Unmarshal(event.Data, &created); err != nil {
		return err
	}
	unit := created.WorkUnit
	var ticketErr error
	unit.Tickets, ticketErr = protocol.NormalizeTickets(unit.Tickets)
	if ticketErr != nil {
		return ticketErr
	}
	for name, value := range map[string]string{"work_unit_uuid": unit.UUID, "repository_uuid": unit.RepositoryUUID, "workspace_uuid": unit.WorkspaceUUID, "created_by": unit.CreatedBy} {
		if err := protocol.ValidateUUID(value); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	if unit.DirectionUUID != "" {
		if err := protocol.ValidateUUID(unit.DirectionUUID); err != nil {
			return fmt.Errorf("direction_uuid: %w", err)
		}
	}
	result, err := transaction.ExecContext(context.Background(), `INSERT INTO work_units(work_unit_uuid, direction_uuid, repository_uuid, workspace_uuid, objective, acceptance_criteria, context, state, created_by, created_at, updated_at, tickets_json, updated_sequence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, uuidBlob(unit.UUID), nullableUUID(unit.DirectionUUID), uuidBlob(unit.RepositoryUUID), uuidBlob(unit.WorkspaceUUID), unit.Objective, nullable(unit.AcceptanceCriteria), nullable(unit.Context), unit.State, uuidBlob(unit.CreatedBy), unit.CreatedAt.UTC().Format(time.RFC3339Nano), unit.UpdatedAt.UTC().Format(time.RFC3339Nano), ticketJSON(unit.Tickets), event.Sequence)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return errors.New("conflicting work unit creation")
	}
	return nil
}

func projectWorkUnitUpdated(transaction *sql.Tx, event *protocol.Event) error {
	var updated protocol.WorkUnitUpdatedEvent
	if err := json.Unmarshal(event.Data, &updated); err != nil {
		return err
	}
	var ticketErr error
	updated.Previous.Tickets, ticketErr = protocol.NormalizeTickets(updated.Previous.Tickets)
	if ticketErr != nil {
		return ticketErr
	}
	updated.Result.Tickets, ticketErr = protocol.NormalizeTickets(updated.Result.Tickets)
	if ticketErr != nil {
		return ticketErr
	}
	if err := validateWorkUnitUpdate(transaction, &updated); err != nil {
		return err
	}
	return updateProjectedWorkUnit(transaction, event.Sequence, &updated.Result)
}

func validateWorkUnitUpdate(transaction *sql.Tx, updated *protocol.WorkUnitUpdatedEvent) error {
	if !validWorkUnitUpdateIdentity(updated) {
		return errors.New("invalid work unit update")
	}
	current, err := loadProjectedWorkUnit(transaction, updated.UUID)
	if err != nil {
		return err
	}
	if !validWorkUnitUpdateProvenance(transaction, updated, &current) {
		return errors.New("invalid work unit update provenance")
	}
	return nil
}

func validWorkUnitUpdateIdentity(updated *protocol.WorkUnitUpdatedEvent) bool {
	if validateProjectedWorkUnit(&updated.Previous) != nil || validateProjectedWorkUnit(&updated.Result) != nil {
		return false
	}
	return protocol.ValidateUUID(updated.UUID) == nil && protocol.ValidateUUID(updated.Actor) == nil && updated.UUID == updated.Previous.UUID && updated.UUID == updated.Result.UUID
}

func validWorkUnitUpdateProvenance(transaction *sql.Tx, updated *protocol.WorkUnitUpdatedEvent, current *protocol.WorkUnit) bool {
	if !reflect.DeepEqual(*current, updated.Previous) || updated.Result.RepositoryUUID != current.RepositoryUUID || updated.Result.WorkspaceUUID != current.WorkspaceUUID || updated.Result.State != current.State {
		return false
	}
	return projectedActiveParticipant(transaction, updated.UUID, updated.Actor, current.RepositoryUUID, current.WorkspaceUUID)
}

func updateProjectedWorkUnit(transaction *sql.Tx, sequence uint64, unit *protocol.WorkUnit) error {
	result, err := transaction.ExecContext(context.Background(), `UPDATE work_units SET objective=?, acceptance_criteria=?, context=?, state=?, updated_at=?, tickets_json=?, updated_sequence=? WHERE work_unit_uuid=?`, unit.Objective, nullable(unit.AcceptanceCriteria), nullable(unit.Context), unit.State, unit.UpdatedAt.UTC().Format(time.RFC3339Nano), ticketJSON(unit.Tickets), sequence, uuidBlob(unit.UUID))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return errors.New("work unit update references unknown unit")
	}
	return nil
}

func projectWorkUnitTransitioned(transaction *sql.Tx, event *protocol.Event) error {
	var transition protocol.WorkUnitTransitionEvent
	if err := json.Unmarshal(event.Data, &transition); err != nil {
		return err
	}
	if protocol.ValidateUUID(transition.WorkUnitUUID) != nil || protocol.ValidateUUID(transition.Actor) != nil || !validProjectedWorkUnitTransition(transition.From, transition.To) {
		return errors.New("invalid work unit transition")
	}
	current, err := loadProjectedWorkUnit(transaction, transition.WorkUnitUUID)
	if err != nil {
		return err
	}
	if current.State != transition.From || !projectedActiveParticipant(transaction, transition.WorkUnitUUID, transition.Actor, current.RepositoryUUID, current.WorkspaceUUID) {
		return errors.New("invalid work unit transition provenance")
	}
	var completed any
	if transition.To == protocol.WorkUnitCompleted {
		completed = transition.At.UTC().Format(time.RFC3339Nano)
	}
	result, err := transaction.ExecContext(context.Background(), `UPDATE work_units SET state=?, updated_at=?, completed_at=?, updated_sequence=? WHERE work_unit_uuid=?`, transition.To, transition.At.UTC().Format(time.RFC3339Nano), completed, event.Sequence, uuidBlob(transition.WorkUnitUUID))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return errors.New("work unit transition references unknown unit")
	}
	return nil
}

func projectWorkUnitActorJoined(transaction *sql.Tx, event *protocol.Event) error {
	var membership protocol.WorkUnitActorEvent
	if err := json.Unmarshal(event.Data, &membership); err != nil {
		return err
	}
	if !validWorkUnitMembershipIdentity(&membership) {
		return errors.New("invalid work unit membership UUID")
	}
	if err := requireProjectedWorkUnit(transaction, membership.WorkUnitUUID); err != nil {
		return err
	}
	existing, found, err := loadProjectedWorkUnitActor(transaction, &membership.Result)
	if err != nil {
		return err
	}
	if found {
		return projectExistingWorkUnitMembership(transaction, event.Type, &membership, &existing)
	}
	return projectNewWorkUnitMembership(transaction, event.Type, &membership)
}

func validWorkUnitMembershipIdentity(membership *protocol.WorkUnitActorEvent) bool {
	result := &membership.Result
	return protocol.ValidateUUID(membership.WorkUnitUUID) == nil && protocol.ValidateUUID(membership.Actor) == nil && result.WorkUnitUUID == membership.WorkUnitUUID && result.Actor == membership.Actor
}

func requireProjectedWorkUnit(transaction *sql.Tx, uuid string) error {
	var repository, workspace []byte
	if err := transaction.QueryRowContext(context.Background(), `SELECT repository_uuid, workspace_uuid FROM work_units WHERE work_unit_uuid = ?`, uuidBlob(uuid)).Scan(&repository, &workspace); err != nil {
		return fmt.Errorf("membership references unknown work unit: %w", err)
	}
	return nil
}

func loadProjectedWorkUnitActor(transaction *sql.Tx, result *protocol.WorkUnitActor) (protocol.WorkUnitActor, bool, error) {
	var joined, state string
	var left sql.NullString
	err := transaction.QueryRowContext(context.Background(), `SELECT joined_at, left_at, participation_state FROM work_unit_actors WHERE work_unit_uuid = ? AND actor_uuid = ?`, uuidBlob(result.WorkUnitUUID), uuidBlob(result.Actor)).Scan(&joined, &left, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.WorkUnitActor{}, false, nil
	}
	if err != nil {
		return protocol.WorkUnitActor{}, false, err
	}
	existing := protocol.WorkUnitActor{WorkUnitUUID: result.WorkUnitUUID, Actor: result.Actor, JoinedAt: parseProjectionTime(joined), ParticipationState: state}
	if left.Valid {
		at := parseProjectionTime(left.String)
		existing.LeftAt = &at
	}
	return existing, true, nil
}

func projectExistingWorkUnitMembership(transaction *sql.Tx, eventType string, membership *protocol.WorkUnitActorEvent, existing *protocol.WorkUnitActor) error {
	result := &membership.Result
	if reflect.DeepEqual(*existing, *result) {
		return nil
	}
	validLeave := eventType == "work_unit.actor_left" && membership.Previous != nil && reflect.DeepEqual(*existing, *membership.Previous) && result.LeftAt != nil && result.ParticipationState == "left" && result.JoinedAt.Equal(existing.JoinedAt)
	if !validLeave {
		return errors.New("conflicting work unit membership projection")
	}
	update, err := transaction.ExecContext(context.Background(), `UPDATE work_unit_actors SET left_at = ?, participation_state = ? WHERE work_unit_uuid = ? AND actor_uuid = ?`, nullableTime(result.LeftAt), result.ParticipationState, uuidBlob(result.WorkUnitUUID), uuidBlob(result.Actor))
	if err != nil {
		return err
	}
	count, err := update.RowsAffected()
	if err != nil || count != 1 {
		return errors.New("work unit actor leave references unknown membership")
	}
	return nil
}

func projectNewWorkUnitMembership(transaction *sql.Tx, eventType string, membership *protocol.WorkUnitActorEvent) error {
	result := &membership.Result
	if eventType == "work_unit.actor_left" {
		return errors.New("work unit actor leave references unknown membership")
	}
	if membership.Previous != nil || result.LeftAt != nil || result.ParticipationState != "active" {
		return errors.New("invalid joined work unit membership")
	}
	_, err := transaction.ExecContext(context.Background(), `INSERT INTO work_unit_actors(work_unit_uuid, actor_uuid, joined_at, left_at, participation_state) VALUES (?, ?, ?, ?, ?)`, uuidBlob(result.WorkUnitUUID), uuidBlob(result.Actor), result.JoinedAt.UTC().Format(time.RFC3339Nano), nil, result.ParticipationState)
	return err
}

func projectCheckpointRequested(transaction *sql.Tx, event *protocol.Event) error {
	var checkpoint protocol.CheckpointRequest
	if err := json.Unmarshal(event.Data, &checkpoint); err != nil {
		return err
	}
	if checkpoint.RepositoryUUID == "" || checkpoint.WorkspaceUUID == "" {
		return nil // Legacy pre-scope facts remain journal-only.
	}
	if err := validateProjectedCheckpointIdentity(&checkpoint); err != nil {
		return err
	}
	workUnitUUID, err := resolveProjectedCheckpointWorkUnit(transaction, &checkpoint)
	if err != nil {
		return err
	}
	duplicate, err := projectedCheckpointExists(transaction, &checkpoint, event.Data)
	if err != nil || duplicate {
		return err
	}
	if err := insertProjectedCheckpoint(transaction, &checkpoint, workUnitUUID, event); err != nil {
		return err
	}
	return projectCheckpointChildren(transaction, &checkpoint)
}

func validateProjectedCheckpointIdentity(checkpoint *protocol.CheckpointRequest) error {
	for name, value := range map[string]string{"actor": checkpoint.Actor, "repository_uuid": checkpoint.RepositoryUUID, "workspace_uuid": checkpoint.WorkspaceUUID} {
		if err := protocol.ValidateUUID(value); err != nil {
			return fmt.Errorf("checkpoint %s: %w", name, err)
		}
	}
	return nil
}

func resolveProjectedCheckpointWorkUnit(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest) (any, error) {
	workUnitUUID, err := checkpointWorkUnitUUID(checkpoint.WorkUnitUUID)
	if err != nil || checkpoint.WorkUnitUUID == "" {
		return workUnitUUID, err
	}
	var repository, workspace []byte
	err = transaction.QueryRowContext(context.Background(), `SELECT repository_uuid, workspace_uuid FROM work_units WHERE work_unit_uuid = ?`, workUnitUUID).Scan(&repository, &workspace)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // Legacy provisional relation.
	}
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(repository, uuidBlob(checkpoint.RepositoryUUID)) || !bytes.Equal(workspace, uuidBlob(checkpoint.WorkspaceUUID)) {
		return nil, errors.New("checkpoint work unit scope mismatch")
	}
	var active int
	if err := transaction.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM work_unit_actors WHERE work_unit_uuid = ? AND actor_uuid = ? AND left_at IS NULL AND participation_state = 'active'`, workUnitUUID, uuidBlob(checkpoint.Actor)).Scan(&active); err != nil {
		return nil, err
	}
	if active != 1 {
		return nil, errors.New("checkpoint declarer is not an active work unit participant")
	}
	return workUnitUUID, nil
}

func projectedCheckpointExists(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest, data json.RawMessage) (bool, error) {
	var existing string
	err := transaction.QueryRowContext(context.Background(), `SELECT data FROM checkpoint_requests WHERE id = ?`, checkpoint.ID).Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if existing != string(data) {
		return false, fmt.Errorf("conflicting checkpoint ID %q", checkpoint.ID)
	}
	return true, nil
}

func insertProjectedCheckpoint(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest, workUnitUUID any, event *protocol.Event) error {
	_, err := transaction.ExecContext(context.Background(), `INSERT INTO checkpoint_requests(
		id, actor, declared_by, session_generation, repository_uuid, workspace_uuid, work_unit_uuid, checkpoint_kind,
		journal_start_sequence, journal_end_sequence, tickets_json, data, event_sequence
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, checkpoint.ID, uuidBlob(checkpoint.Actor), checkpoint.DeclaredBy, checkpoint.SessionGeneration,
		uuidBlob(checkpoint.RepositoryUUID), uuidBlob(checkpoint.WorkspaceUUID), workUnitUUID, checkpoint.CheckpointKind, checkpoint.JournalStart,
		checkpoint.JournalEnd, ticketJSON(checkpoint.Tickets), string(event.Data), event.Sequence)
	return err
}

func projectCheckpointChildren(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest) error {
	if err := normalizeProjectedCheckpointEvidence(transaction, checkpoint); err != nil {
		return err
	}
	if err := projectCheckpointEvidencePtr(transaction, checkpoint); err != nil {
		return err
	}
	if err := projectCheckpointClaimsPtr(transaction, checkpoint); err != nil {
		return err
	}
	return projectCheckpointMetadataPtr(transaction, checkpoint)
}

func projectCollisionActorDead(transaction *sql.Tx, event *protocol.Event) error {
	var dead protocol.CollisionActorDeadEvent
	if err := json.Unmarshal(event.Data, &dead); err != nil {
		return err
	}
	result, err := transaction.ExecContext(context.Background(), `UPDATE collisions SET dead_actor = ?, updated_at = ?, data = ?, updated_sequence = ? WHERE id = ?`, nullableUUID(dead.Actor), dead.At.UTC().Format(time.RFC3339Nano), string(event.Data), event.Sequence, uuidBlob(dead.CollisionID))
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
}

func projectCollisionTransitioned(transaction *sql.Tx, event *protocol.Event) error {
	var transition protocol.CollisionTransitionEvent
	if err := json.Unmarshal(event.Data, &transition); err != nil {
		return err
	}
	result, err := transaction.ExecContext(context.Background(), `UPDATE collisions SET state = ?, owner = ?, updated_at = ?, resolved_at = CASE WHEN ? = ? THEN ? ELSE resolved_at END, resolution = ?, resolved_by = CASE WHEN ? = ? THEN ? ELSE resolved_by END, data = ?, updated_sequence = ? WHERE id = ?`,
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
}

func projectCollisionUpserted(transaction *sql.Tx, event *protocol.Event) error {
	var collision protocol.Collision
	if err := json.Unmarshal(event.Data, &collision); err != nil {
		return err
	}
	collisionID := uuidBlob(collision.ID)
	_, err := transaction.ExecContext(context.Background(), `INSERT INTO collisions(
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
	if _, err := transaction.ExecContext(context.Background(), `DELETE FROM collision_actors WHERE collision_id = ?`, collisionID); err != nil {
		return err
	}
	for ordinal, actor := range collision.Actors {
		if _, err := transaction.ExecContext(context.Background(), `INSERT INTO collision_actors(collision_id, ordinal, session_uuid) VALUES (?, ?, ?)`, collisionID, ordinal, uuidBlob(actor)); err != nil {
			return err
		}
	}
	return nil
}

func parseProjectionTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func validateProjectedWorkUnit(unit *protocol.WorkUnit) error {
	for _, value := range []string{unit.UUID, unit.RepositoryUUID, unit.WorkspaceUUID, unit.CreatedBy} {
		if err := protocol.ValidateUUID(value); err != nil {
			return err
		}
	}
	if unit.Objective == "" || unit.State == "" || unit.CreatedAt.IsZero() || unit.UpdatedAt.IsZero() {
		return errors.New("invalid work unit")
	}
	return nil
}

func loadProjectedWorkUnit(transaction *sql.Tx, uuid string) (protocol.WorkUnit, error) {
	var unit protocol.WorkUnit
	var id, direction, repo, workspace, creator []byte
	var created, updated string
	var completed sql.NullString
	var tickets string
	err := transaction.QueryRowContext(context.Background(), `SELECT work_unit_uuid, direction_uuid, repository_uuid, workspace_uuid, objective, COALESCE(acceptance_criteria,''), COALESCE(context,''), state, created_by, created_at, updated_at, completed_at, tickets_json FROM work_units WHERE work_unit_uuid = ?`, uuidBlob(uuid)).Scan(&id, &direction, &repo, &workspace, &unit.Objective, &unit.AcceptanceCriteria, &unit.Context, &unit.State, &creator, &created, &updated, &completed, &tickets)
	if errors.Is(err, sql.ErrNoRows) {
		return unit, errors.New("work unit event references unknown unit")
	}
	if err != nil {
		return unit, err
	}
	unit.UUID, unit.DirectionUUID, unit.RepositoryUUID, unit.WorkspaceUUID, unit.CreatedBy = uuidString(id), uuidString(direction), uuidString(repo), uuidString(workspace), uuidString(creator)
	unit.CreatedAt = parseProjectionTime(created)
	unit.UpdatedAt = parseProjectionTime(updated)
	if completed.Valid {
		at := parseProjectionTime(completed.String)
		unit.CompletedAt = &at
	}
	if err := json.Unmarshal([]byte(tickets), &unit.Tickets); err != nil {
		return unit, err
	}
	return unit, nil
}

func projectedActiveParticipant(transaction *sql.Tx, workUnit, actor, repository, workspace string) bool {
	var count int
	err := transaction.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM work_unit_actors a JOIN actors s ON s.session_uuid = a.actor_uuid WHERE a.work_unit_uuid = ? AND a.actor_uuid = ? AND a.left_at IS NULL AND a.participation_state = 'active' AND s.repository_uuid = ? AND s.workspace_uuid = ?`, uuidBlob(workUnit), uuidBlob(actor), uuidBlob(repository), uuidBlob(workspace)).Scan(&count)
	return err == nil && count == 1
}

func validProjectedWorkUnitTransition(from, to protocol.WorkUnitState) bool {
	switch from {
	case protocol.WorkUnitProposed:
		return to == protocol.WorkUnitActive || to == protocol.WorkUnitAbandoned
	case protocol.WorkUnitActive:
		return to == protocol.WorkUnitBlocked || to == protocol.WorkUnitVerified || to == protocol.WorkUnitAbandoned
	case protocol.WorkUnitBlocked:
		return to == protocol.WorkUnitActive || to == protocol.WorkUnitVerified || to == protocol.WorkUnitAbandoned
	case protocol.WorkUnitVerified:
		return to == protocol.WorkUnitCompleted || to == protocol.WorkUnitAbandoned
	}
	return false
}

func deriveScope(cwd string, git *protocol.GitContext, jj *protocol.JJContext) (repositoryID, repositoryRoot, workspaceID, workspaceRoot, kind string) {
	repositoryKey := "dir:" + cwd
	repositoryRoot = cwd
	workspaceRoot = cwd
	kind = "directory"
	switch {
	case git != nil && git.CommonDir != "":
		repositoryKey = "git\x00" + git.CommonDir
		repositoryRoot = git.RepoRoot
		workspaceRoot = git.WorktreeRoot
		kind = "git-worktree"
		if jj != nil {
			kind = "git-jj-workspace"
		}
	case jj != nil && jj.RepoPath != "":
		repositoryKey = "jj:" + jj.RepoPath
		repositoryRoot = jj.WorkspaceRoot
		workspaceRoot = jj.WorkspaceRoot
		kind = "jj-workspace"
	}
	repositoryID = deterministicUUID(filepath.Clean(repositoryKey))
	workspaceID = deterministicUUID(filepath.Clean(repositoryID + "\x00" + workspaceRoot))
	return repositoryID, filepath.Clean(repositoryRoot), workspaceID, filepath.Clean(workspaceRoot), kind
}

func scopeWorkspaceKind(git *protocol.GitContext, jj *protocol.JJContext) string {
	switch {
	case git != nil && jj != nil:
		return "git-jj-workspace"
	case git != nil:
		return "git-worktree"
	case jj != nil:
		return "jj-workspace"
	default:
		return "directory"
	}
}

func scopeRepositoryKind(git *protocol.GitContext, jj *protocol.JJContext) string {
	switch {
	case git != nil && jj != nil:
		return "git+jj"
	case git != nil:
		return "git"
	case jj != nil:
		return "jj"
	default:
		return "directory"
	}
}

func deterministicUUID(key string) string {
	sum := sha256.Sum256([]byte(key))
	digest := sum[:16]
	digest[6] = (digest[6] & 0x0f) | 0x50
	digest[8] = (digest[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(digest)
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
		workspaceKind = scopeWorkspaceKind(git, jj)
	}
	repositoryKind := scopeRepositoryKind(git, jj)
	var gitCommonDir, gitBranch, gitHead, jjRepoPath, jjWorkspaceName, jjChangeID string
	if git != nil {
		gitCommonDir, gitBranch, gitHead = git.CommonDir, git.Branch, git.Head
	}
	if jj != nil {
		jjRepoPath, jjWorkspaceName, jjChangeID = jj.RepoPath, jj.WorkspaceName, jj.ChangeID
	}
	if _, err := transaction.ExecContext(context.Background(), `INSERT INTO repositories(id, root, kind, git_common_dir, jj_repo_path, updated_sequence)
		VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET root=excluded.root, kind=excluded.kind,
		git_common_dir=excluded.git_common_dir, jj_repo_path=excluded.jj_repo_path, updated_sequence=excluded.updated_sequence`,
		uuidBlob(repositoryID), repositoryRoot, repositoryKind, nullable(gitCommonDir), nullable(jjRepoPath), sequence); err != nil {
		return err
	}
	_, err := transaction.ExecContext(context.Background(), `INSERT INTO workspaces(id, repository_uuid, root, kind, git_branch, git_head,
		jj_workspace_name, jj_change_id, updated_sequence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET repository_uuid=excluded.repository_uuid, root=excluded.root, kind=excluded.kind,
		git_branch=excluded.git_branch, git_head=excluded.git_head, jj_workspace_name=excluded.jj_workspace_name,
		jj_change_id=excluded.jj_change_id, updated_sequence=excluded.updated_sequence`,
		uuidBlob(workspaceID), uuidBlob(repositoryID), workspaceRoot, workspaceKind, nullable(gitBranch), nullable(gitHead),
		nullable(jjWorkspaceName), nullable(jjChangeID), sequence)
	return err
}

func projectIntent(transaction *sql.Tx, sequence uint64, intent *protocol.Intent) error {
	repositoryRoot := normalizeProjectedIntent(intent)
	if err := projectScope(transaction, sequence, intent.RepositoryUUID, repositoryRoot, intent.WorkspaceUUID, intent.WorkspaceRoot, intent.WorkspaceKind, intent.Git, intent.JJ); err != nil {
		return err
	}
	data, err := marshalProjectedIntent(intent)
	if err != nil {
		return err
	}
	if err := upsertProjectedIntent(transaction, sequence, intent, &data); err != nil {
		return err
	}
	return projectIntentPaths(transaction, intent)
}

type projectedIntentData struct {
	paths, relativePaths, before, after    string
	gitBefore, gitAfter, jjBefore, jjAfter any
	completedAt, success                   any
}

func normalizeProjectedIntent(intent *protocol.Intent) string {
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
		intent.RelativePaths = relativeIntentPaths(intent.Paths, workspaceRoot)
	}
	return repositoryRoot
}

func relativeIntentPaths(paths []string, workspaceRoot string) []string {
	result := make([]string, 0, len(paths))
	for _, path := range paths {
		relative, err := filepath.Rel(workspaceRoot, path)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			result = append(result, filepath.Clean(path))
			continue
		}
		result = append(result, filepath.ToSlash(relative))
	}
	return result
}

func marshalProjectedIntent(intent *protocol.Intent) (projectedIntentData, error) {
	var data projectedIntentData
	values := []struct {
		value any
		dest  *string
	}{{intent.Paths, &data.paths}, {intent.RelativePaths, &data.relativePaths}, {intent.Before, &data.before}, {intent.After, &data.after}}
	for _, value := range values {
		encoded, err := json.Marshal(value.value)
		if err != nil {
			return data, err
		}
		*value.dest = string(encoded)
	}
	optional := []struct {
		value any
		dest  *any
	}{{intent.Git, &data.gitBefore}, {intent.GitAfter, &data.gitAfter}, {intent.JJ, &data.jjBefore}, {intent.JJAfter, &data.jjAfter}}
	for _, value := range optional {
		encoded, err := marshalOptional(value.value)
		if err != nil {
			return data, err
		}
		*value.dest = encoded
	}
	if intent.CompletedAt != nil {
		data.completedAt = intent.CompletedAt.UTC().Format(time.RFC3339Nano)
	}
	if intent.Success != nil {
		data.success = *intent.Success
	}
	return data, nil
}

func upsertProjectedIntent(transaction *sql.Tx, sequence uint64, intent *protocol.Intent, data *projectedIntentData) error {
	_, err := transaction.ExecContext(context.Background(), `INSERT INTO mutations(
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
		intent.WorkspaceKey, data.paths, data.relativePaths, nullable(intent.Context.AssistantExcerpt),
		intent.StartedAt.UTC().Format(time.RFC3339Nano), data.completedAt, data.success, nullable(intent.Error),
		data.before, data.after, data.gitBefore, data.gitAfter, data.jjBefore, data.jjAfter, sequence)
	if err != nil {
		return fmt.Errorf("project mutation %s: %w", intent.ID, err)
	}
	return nil
}

func projectIntentPaths(transaction *sql.Tx, intent *protocol.Intent) error {
	if _, err := transaction.ExecContext(context.Background(), `DELETE FROM mutation_paths WHERE mutation_id = ?`, intent.ID); err != nil {
		return err
	}
	beforeByPath, afterByPath := snapshotsByPath(intent.Before), snapshotsByPath(intent.After)
	seen := make(map[string]bool)
	ordinal := 0
	for _, path := range append(append([]string(nil), intent.Paths...), snapshotPaths(intent.After)...) {
		if seen[path] {
			continue
		}
		seen[path] = true
		beforeJSON, err := marshalOptional(beforeByPath[path])
		if err != nil {
			return err
		}
		afterJSON, err := marshalOptional(afterByPath[path])
		if err != nil {
			return err
		}
		if _, err := transaction.ExecContext(context.Background(), `INSERT INTO mutation_paths(mutation_id, path, ordinal, before_json, after_json) VALUES (?, ?, ?, ?, ?)`, intent.ID, path, ordinal, beforeJSON, afterJSON); err != nil {
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

func coalesceText(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
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

// ProjectionHealth describes the current projection state.
type ProjectionHealth struct {
	JournalSequence   uint64 `json:"journal_sequence"`
	ProjectedSequence uint64 `json:"projected_sequence"`
	Lag               uint64 `json:"lag"`
	QueueDepth        int    `json:"queue_depth"`
	LastError         string `json:"last_error,omitempty"`
}

// ProjectingAppender appends events and projects them asynchronously.
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

// NewProjectingAppender creates an appender that projects appended events.
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

// Append appends an event and schedules its projection.
//
//nolint:gocritic // Appender requires value semantics
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

// WaitForCurrent waits until all currently queued projections finish.
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

// Health returns the current projection health.
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

// Close stops the projecting appender.
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

// LastError returns the most recent projection error.
func (p *ProjectingAppender) LastError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastErr
}

// ResolveActor resolves an actor selector to a canonical UUID.
func (d *DB) ResolveActor(selector string) (string, error) {
	return d.ResolveActorScoped(selector, "", "")
}

func nullableUUID(value string) any {
	if value == "" {
		return nil
	}
	return uuidBlob(value)
}

func normalizeProjectedCheckpointEvidence(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest) error {
	groups := map[string]*[]string{"mutation": &checkpoint.MutationIDs, "message": &checkpoint.MessageIDs, "collision": &checkpoint.CollisionIDs, "test_result": &checkpoint.TestResultIDs}
	for kind, ids := range groups {
		if len(*ids) == 0 {
			if err := deriveProjectedCheckpointEvidence(transaction, checkpoint, kind, ids); err != nil {
				return err
			}
			continue
		}
		if err := validateProjectedCheckpointEvidence(transaction, checkpoint, kind, *ids); err != nil {
			return err
		}
	}
	return nil
}

func deriveProjectedCheckpointEvidence(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest, kind string, ids *[]string) error {
	actor := uuidBlob(checkpoint.Actor)
	query, arguments := projectedCheckpointEvidenceQuery(checkpoint, kind, actor)
	rows, err := transaction.QueryContext(context.Background(), query, arguments...)
	if err != nil {
		return err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("agent-bridge: close provenance rows: %v", err)
		}
	}()
	for rows.Next() {
		var id any
		if err := rows.Scan(&id); err != nil {
			return err
		}
		switch value := id.(type) {
		case string:
			*ids = append(*ids, value)
		case []byte:
			*ids = append(*ids, uuidString(value))
		}
	}
	return rows.Err()
}

func projectedCheckpointEvidenceQuery(checkpoint *protocol.CheckpointRequest, kind string, actor []byte) (query string, arguments []any) {
	rangeArgs := []any{checkpoint.JournalStart, checkpoint.JournalEnd}
	scopeArgs := []any{actor, uuidBlob(checkpoint.RepositoryUUID), uuidBlob(checkpoint.WorkspaceUUID), checkpoint.JournalStart, checkpoint.JournalEnd}
	switch kind {
	case "message":
		return `SELECT id FROM messages WHERE (from_actor=? OR to_actor=?) AND event_sequence BETWEEN ? AND ? ORDER BY event_sequence, id`, append([]any{actor, actor}, rangeArgs...)
	case "collision":
		return `SELECT c.id FROM collisions c JOIN collision_actors a ON a.collision_id=c.id WHERE a.session_uuid=? AND c.updated_sequence BETWEEN ? AND ? ORDER BY c.updated_sequence, c.id`, append([]any{actor}, rangeArgs...)
	case "test_result":
		return `SELECT id FROM test_results WHERE actor=? AND repository_uuid=? AND workspace_uuid=? AND event_sequence BETWEEN ? AND ? ORDER BY event_sequence, id`, scopeArgs
	default:
		return `SELECT id FROM mutations WHERE completed_at IS NOT NULL AND actor=? AND repository_uuid=? AND workspace_uuid=? AND updated_sequence BETWEEN ? AND ? ORDER BY updated_sequence, id`, scopeArgs
	}
}

func validateProjectedCheckpointEvidence(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest, kind string, ids []string) error {
	for _, id := range ids {
		count, err := projectedCheckpointEvidenceCount(transaction, checkpoint, kind, id)
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("checkpoint %s reference %q is unknown, out of scope, or outside range", kind, id)
		}
	}
	return nil
}

func projectedCheckpointEvidenceCount(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest, kind, id string) (int, error) {
	actor := uuidBlob(checkpoint.Actor)
	var row *sql.Row
	switch kind {
	case "mutation":
		row = transaction.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM mutations WHERE id=? AND completed_at IS NOT NULL AND actor=? AND repository_uuid=? AND workspace_uuid=? AND updated_sequence BETWEEN ? AND ?`, id, actor, uuidBlob(checkpoint.RepositoryUUID), uuidBlob(checkpoint.WorkspaceUUID), checkpoint.JournalStart, checkpoint.JournalEnd)
	case "message":
		row = transaction.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM messages WHERE id=? AND (from_actor=? OR to_actor=?) AND event_sequence BETWEEN ? AND ?`, id, actor, actor, checkpoint.JournalStart, checkpoint.JournalEnd)
	case "collision":
		row = transaction.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM collisions c JOIN collision_actors a ON a.collision_id=c.id WHERE c.id=? AND a.session_uuid=? AND c.updated_sequence BETWEEN ? AND ?`, uuidBlob(id), actor, checkpoint.JournalStart, checkpoint.JournalEnd)
	default:
		row = transaction.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM test_results WHERE id=? AND actor=? AND repository_uuid=? AND workspace_uuid=? AND event_sequence BETWEEN ? AND ?`, id, actor, uuidBlob(checkpoint.RepositoryUUID), uuidBlob(checkpoint.WorkspaceUUID), checkpoint.JournalStart, checkpoint.JournalEnd)
	}
	var count int
	err := row.Scan(&count)
	return count, err
}

func projectCheckpointEvidencePtr(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest) error {
	groups := []struct {
		kind string
		refs []string
	}{
		{"mutation", checkpoint.MutationIDs},
		{"message", checkpoint.MessageIDs},
		{"collision", checkpoint.CollisionIDs},
		{"test_result", checkpoint.TestResultIDs},
	}
	for _, group := range groups {
		for ordinal, ref := range group.refs {
			text, binary := checkpointReference(ref)
			var existingText sql.NullString
			var existingUUID []byte
			err := transaction.QueryRowContext(context.Background(), `SELECT ref_text, ref_uuid FROM checkpoint_evidence WHERE checkpoint_id = ? AND kind = ? AND ordinal = ?`, checkpoint.ID, group.kind, ordinal).Scan(&existingText, &existingUUID)
			if err == nil {
				if existingText.String != text || !reflect.DeepEqual(existingUUID, binary) {
					return fmt.Errorf("conflicting checkpoint %s evidence at ordinal %d", group.kind, ordinal)
				}
				continue
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if _, err := transaction.ExecContext(context.Background(), `INSERT INTO checkpoint_evidence(checkpoint_id, kind, ordinal, ref_text, ref_uuid) VALUES (?, ?, ?, ?, ?)`, checkpoint.ID, group.kind, ordinal, text, binary); err != nil {
				return fmt.Errorf("project checkpoint %s evidence: %w", group.kind, err)
			}
		}
	}
	return nil
}

func projectCheckpointClaimsPtr(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest) error {
	claims := checkpoint.Claims
	if len(claims) == 0 && strings.TrimSpace(checkpoint.Metadata["summary"]) != "" {
		claims = []protocol.CheckpointClaim{{Kind: "summary", Statement: strings.TrimSpace(checkpoint.Metadata["summary"]), Status: protocol.ClaimAsserted}}
	}
	for ordinal := range claims {
		claim := claims[ordinal]
		if err := validateProjectedCheckpointClaim(transaction, checkpoint, ordinal, &claim); err != nil {
			return err
		}
		normalizeProjectedVerificationClaim(transaction, checkpoint, &claim)
		if err := upsertProjectedCheckpointClaim(transaction, checkpoint.ID, ordinal, &claim); err != nil {
			return err
		}
		if err := linkProjectedCheckpointClaimEvidence(transaction, checkpoint.ID, ordinal, claim.Evidence); err != nil {
			return err
		}
	}
	return nil
}

func validateProjectedCheckpointClaim(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest, ordinal int, claim *protocol.CheckpointClaim) error {
	claim.Kind = strings.TrimSpace(claim.Kind)
	if !protocol.ValidCheckpointClaimKind(claim.Kind) || strings.TrimSpace(claim.Statement) == "" {
		return fmt.Errorf("invalid checkpoint claim at ordinal %d", ordinal)
	}
	switch claim.Status {
	case protocol.ClaimAsserted, protocol.ClaimVerified, protocol.ClaimFailed, protocol.ClaimBlocked:
	default:
		return fmt.Errorf("invalid checkpoint claim status %q", claim.Status)
	}
	validEvidence := map[string]bool{"mutation": true, "message": true, "collision": true, "test_result": true}
	for _, ref := range claim.Evidence {
		if !validEvidence[ref.Kind] || ref.Ordinal < 0 {
			return fmt.Errorf("invalid checkpoint claim evidence at ordinal %d", ordinal)
		}
		var count int
		if err := transaction.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM checkpoint_evidence WHERE checkpoint_id=? AND kind=? AND ordinal=?`, checkpoint.ID, ref.Kind, ref.Ordinal).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("checkpoint claim %d references unknown evidence %s[%d]", ordinal, ref.Kind, ref.Ordinal)
		}
	}
	return nil
}

func normalizeProjectedVerificationClaim(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest, claim *protocol.CheckpointClaim) {
	if claim.Kind != "test" && claim.Kind != "build" && claim.Kind != "runtime" {
		return
	}
	required := map[protocol.CheckpointClaimStatus]protocol.TestOutcome{
		protocol.ClaimVerified: protocol.TestPassed,
		protocol.ClaimFailed:   protocol.TestFailed,
		protocol.ClaimBlocked:  protocol.TestBlocked,
	}[claim.Status]
	if required == "" || projectedClaimHasOutcome(transaction, checkpoint, claim.Evidence, required) {
		return
	}
	claim.Status = protocol.ClaimAsserted
}

func projectedClaimHasOutcome(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest, evidence []protocol.CheckpointEvidenceRef, required protocol.TestOutcome) bool {
	for _, ref := range evidence {
		if ref.Kind != "test_result" || ref.Ordinal < 0 || ref.Ordinal >= len(checkpoint.TestResultIDs) {
			continue
		}
		var outcome protocol.TestOutcome
		if err := transaction.QueryRowContext(context.Background(), `SELECT outcome FROM test_results WHERE id=?`, checkpoint.TestResultIDs[ref.Ordinal]).Scan(&outcome); err == nil && outcome == required {
			return true
		}
	}
	return false
}

func upsertProjectedCheckpointClaim(transaction *sql.Tx, checkpointID string, ordinal int, claim *protocol.CheckpointClaim) error {
	var existingKind, existingStatement, existingStatus string
	err := transaction.QueryRowContext(context.Background(), `SELECT kind, statement, status FROM checkpoint_claims WHERE checkpoint_id=? AND ordinal=?`, checkpointID, ordinal).Scan(&existingKind, &existingStatement, &existingStatus)
	if err == nil {
		if existingKind != claim.Kind || existingStatement != claim.Statement || existingStatus != string(claim.Status) {
			return fmt.Errorf("conflicting checkpoint claim at ordinal %d", ordinal)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = transaction.ExecContext(context.Background(), `INSERT INTO checkpoint_claims(checkpoint_id, ordinal, kind, statement, status) VALUES (?, ?, ?, ?, ?)`, checkpointID, ordinal, claim.Kind, claim.Statement, claim.Status)
	return err
}

func linkProjectedCheckpointClaimEvidence(transaction *sql.Tx, checkpointID string, ordinal int, evidence []protocol.CheckpointEvidenceRef) error {
	for _, ref := range evidence {
		if _, err := transaction.ExecContext(context.Background(), `INSERT INTO checkpoint_claim_evidence(checkpoint_id, claim_ordinal, evidence_kind, evidence_ordinal) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING`, checkpointID, ordinal, ref.Kind, ref.Ordinal); err != nil {
			return err
		}
	}
	return nil
}

func projectCheckpointMetadataPtr(transaction *sql.Tx, checkpoint *protocol.CheckpointRequest) error {
	for key, value := range checkpoint.Metadata {
		var existing string
		err := transaction.QueryRowContext(context.Background(), `SELECT value FROM checkpoint_metadata WHERE checkpoint_id = ? AND key = ?`, checkpoint.ID, key).Scan(&existing)
		if err == nil {
			if existing != value {
				return fmt.Errorf("conflicting checkpoint metadata key %q", key)
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err := transaction.ExecContext(context.Background(), `INSERT INTO checkpoint_metadata(checkpoint_id, key, value) VALUES (?, ?, ?)`, checkpoint.ID, key, value); err != nil {
			return fmt.Errorf("project checkpoint metadata: %w", err)
		}
	}
	return nil
}

func checkpointReference(value string) (text, binary any) {
	if err := protocol.ValidateUUID(value); err == nil {
		return nil, uuidBlob(value)
	}
	return value, nil
}

func checkpointWorkUnitUUID(value string) (any, error) {
	if value == "" {
		return nil, nil
	}
	if err := protocol.ValidateUUID(value); err != nil {
		return nil, fmt.Errorf("invalid checkpoint work_unit_uuid %q", value)
	}
	return uuidBlob(value), nil
}

func uuidBlob(value string) []byte {
	value = strings.TrimSpace(value)
	candidate := value
	// Legacy journal/API input may still carry a namespace marker. Strip it at
	// the database boundary; no newly generated or returned identifier uses one.
	for _, prefix := range []string{"pi:", "col:", "repo:", "workspace:"} {
		if strings.HasPrefix(candidate, prefix) {
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

// normalizeProjectedActorUUID admits canonical UUIDs and the one legacy actor
// spelling still supported by the projection: pi:<canonical UUID>. It never
// returns a variable-length fallback, so callers can safely persist its result
// in actor-bearing BLOB columns.
func normalizeProjectedActorUUID(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if err := protocol.ValidateUUID(value); err == nil {
		return value, true
	}
	if candidate, ok := strings.CutPrefix(value, "pi:"); ok {
		if err := protocol.ValidateUUID(candidate); err == nil {
			return candidate, true
		}
	}
	return "", false
}

func uuidString(value []byte) string {
	if len(value) != 16 {
		return string(value)
	}
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(value[:4]), hex.EncodeToString(value[4:6]), hex.EncodeToString(value[6:8]), hex.EncodeToString(value[8:10]), hex.EncodeToString(value[10:]))
}

// ResolveActorScoped resolves an actor selector within repository and workspace scope.
func (d *DB) ResolveActorScoped(selector, repositoryID, workspaceID string) (string, error) {
	normalized := strings.TrimPrefix(strings.TrimSpace(selector), "@")
	var sessionUUID []byte
	err := d.db.QueryRowContext(context.Background(), `SELECT session_uuid FROM actors WHERE session_uuid = ?`, uuidBlob(normalized)).Scan(&sessionUUID)
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
	rows, err := d.db.QueryContext(context.Background(), query, arguments...)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("agent-bridge: close provenance rows: %v", err)
		}
	}()
	var matches []string
	for rows.Next() {
		var match []byte
		if err := rows.Scan(&match); err != nil {
			return "", err
		}
		matches = append(matches, uuidString(match))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("unknown persisted actor %q", selector)
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("persisted alias %q is ambiguous; provide a workspace/repository scope or canonical address", selector)
	}
	return matches[0], nil
}
