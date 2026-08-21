package provenance

import (
	"context"
	"crypto/sha256"
	"database/sql"
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

const projectionSchemaVersion = 3

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
			id TEXT PRIMARY KEY,
			root TEXT NOT NULL,
			kind TEXT NOT NULL,
			git_common_dir TEXT,
			jj_repo_path TEXT,
			updated_sequence INTEGER NOT NULL
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS workspaces (
			id TEXT PRIMARY KEY,
			repository_id TEXT NOT NULL REFERENCES repositories(id),
			root TEXT NOT NULL,
			kind TEXT NOT NULL,
			git_branch TEXT,
			git_head TEXT,
			jj_workspace_name TEXT,
			jj_change_id TEXT,
			updated_sequence INTEGER NOT NULL
		) STRICT`,
		`CREATE INDEX IF NOT EXISTS workspaces_repository ON workspaces(repository_id, root)`,
		`CREATE TABLE IF NOT EXISTS actors (
			address TEXT PRIMARY KEY,
			harness TEXT NOT NULL,
			session_id TEXT NOT NULL,
			alias TEXT,
			cwd TEXT NOT NULL,
			repository_id TEXT,
			repository_root TEXT,
			workspace_id TEXT,
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
			actor TEXT NOT NULL,
			session_generation INTEGER NOT NULL,
			turn_id TEXT,
			turn_index INTEGER,
			tool_call_id TEXT NOT NULL,
			tool TEXT NOT NULL,
			operation TEXT NOT NULL,
			cwd TEXT NOT NULL,
			repository_id TEXT,
			repository_root TEXT,
			workspace_id TEXT,
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
			actor TEXT NOT NULL,
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
			id TEXT PRIMARY KEY,
			path TEXT NOT NULL,
			state TEXT NOT NULL,
			actors_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			resolution TEXT,
			data TEXT NOT NULL,
			updated_sequence INTEGER NOT NULL
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
		{"actors", "repository_id", "TEXT"}, {"actors", "repository_root", "TEXT"},
		{"actors", "workspace_id", "TEXT"}, {"actors", "workspace_root", "TEXT"}, {"actors", "workspace_kind", "TEXT"},
		{"mutations", "repository_id", "TEXT"}, {"mutations", "repository_root", "TEXT"},
		{"mutations", "workspace_id", "TEXT"}, {"mutations", "workspace_root", "TEXT"}, {"mutations", "workspace_kind", "TEXT"},
		{"mutations", "relative_paths_json", "TEXT NOT NULL DEFAULT '[]'"},
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
		return nil
	}
	transaction, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	for _, table := range []string{"mutation_paths", "mutations", "session_events", "collisions", "actors", "workspaces", "repositories", "events"} {
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
		if actor.RepositoryID == "" {
			actor.RepositoryID, actor.RepositoryRoot, actor.WorkspaceID, actor.WorkspaceRoot, actor.WorkspaceKind = deriveScope(actor.CWD, actor.Git, actor.JJ)
		}
		if err := projectScope(transaction, event.Sequence, actor.RepositoryID, actor.RepositoryRoot, actor.WorkspaceID,
			actor.WorkspaceRoot, actor.WorkspaceKind, actor.Git, actor.JJ); err != nil {
			return err
		}
		gitJSON, _ := marshalOptional(actor.Git)
		jjJSON, _ := marshalOptional(actor.JJ)
		capabilities, _ := json.Marshal(actor.Capabilities)
		_, err := transaction.Exec(`INSERT INTO actors(
			address, harness, session_id, alias, cwd, repository_id, repository_root, workspace_id, workspace_root, workspace_kind,
			state, generation, started_at, heartbeat_at, git_json, jj_json, capabilities_json, updated_sequence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(address) DO UPDATE SET
			alias=excluded.alias, cwd=excluded.cwd, repository_id=excluded.repository_id, repository_root=excluded.repository_root,
			workspace_id=excluded.workspace_id, workspace_root=excluded.workspace_root, workspace_kind=excluded.workspace_kind,
			state=excluded.state, generation=excluded.generation, heartbeat_at=excluded.heartbeat_at,
			git_json=excluded.git_json, jj_json=excluded.jj_json, capabilities_json=excluded.capabilities_json,
			updated_sequence=excluded.updated_sequence`,
			actor.Address, actor.Harness, actor.SessionID, nullable(actor.Alias), actor.CWD,
			nullable(actor.RepositoryID), nullable(actor.RepositoryRoot), nullable(actor.WorkspaceID), nullable(actor.WorkspaceRoot), nullable(actor.WorkspaceKind),
			actor.State, actor.Generation, actor.StartedAt.UTC().Format(time.RFC3339Nano), actor.HeartbeatAt.UTC().Format(time.RFC3339Nano),
			gitJSON, jjJSON, string(capabilities), event.Sequence)
		return err
	case "intent.started", "intent.completed":
		var intent protocol.Intent
		if err := json.Unmarshal(event.Data, &intent); err != nil {
			return err
		}
		return projectIntent(transaction, event.Sequence, intent)
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
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, sessionEvent.ID, sessionEvent.Actor, sessionEvent.SessionGeneration,
			sessionEvent.Type, sessionEvent.At.UTC().Format(time.RFC3339Nano), sessionEvent.TurnIndex,
			nullable(sessionEvent.Summary), string(data), event.Sequence)
		return err
	case "collision.upserted":
		var collision protocol.Collision
		if err := json.Unmarshal(event.Data, &collision); err != nil {
			return err
		}
		actors, _ := json.Marshal(collision.Actors)
		_, err := transaction.Exec(`INSERT INTO collisions(
			id, path, state, actors_json, created_at, updated_at, resolution, data, updated_sequence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET state=excluded.state, updated_at=excluded.updated_at,
			resolution=excluded.resolution, data=excluded.data, updated_sequence=excluded.updated_sequence`,
			collision.ID, collision.Path, collision.State, string(actors), collision.CreatedAt.UTC().Format(time.RFC3339Nano),
			collision.UpdatedAt.UTC().Format(time.RFC3339Nano), nullable(collision.Resolution), string(event.Data), event.Sequence)
		return err
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
	repositorySum := sha256.Sum256([]byte(filepath.Clean(repositoryKey)))
	repositoryID := fmt.Sprintf("repo:%x", repositorySum[:16])
	workspaceSum := sha256.Sum256([]byte(filepath.Clean(repositoryID + "\x00" + workspaceRoot)))
	workspaceID := fmt.Sprintf("workspace:%x", workspaceSum[:16])
	return repositoryID, filepath.Clean(repositoryRoot), workspaceID, filepath.Clean(workspaceRoot), kind
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
		repositoryID, repositoryRoot, repositoryKind, nullable(gitCommonDir), nullable(jjRepoPath), sequence); err != nil {
		return err
	}
	_, err := transaction.Exec(`INSERT INTO workspaces(id, repository_id, root, kind, git_branch, git_head,
		jj_workspace_name, jj_change_id, updated_sequence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET repository_id=excluded.repository_id, root=excluded.root, kind=excluded.kind,
		git_branch=excluded.git_branch, git_head=excluded.git_head, jj_workspace_name=excluded.jj_workspace_name,
		jj_change_id=excluded.jj_change_id, updated_sequence=excluded.updated_sequence`,
		workspaceID, repositoryID, workspaceRoot, workspaceKind, nullable(gitBranch), nullable(gitHead),
		nullable(jjWorkspaceName), nullable(jjChangeID), sequence)
	return err
}

func projectIntent(transaction *sql.Tx, sequence uint64, intent protocol.Intent) error {
	if intent.RepositoryID == "" {
		intent.RepositoryID, intent.RepositoryRoot, intent.WorkspaceID, intent.WorkspaceRoot, intent.WorkspaceKind = deriveScope(intent.CWD, intent.Git, intent.JJ)
	}
	if len(intent.RelativePaths) == 0 {
		for _, path := range intent.Paths {
			relative, err := filepath.Rel(intent.WorkspaceRoot, path)
			if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				intent.RelativePaths = append(intent.RelativePaths, filepath.Clean(path))
			} else {
				intent.RelativePaths = append(intent.RelativePaths, filepath.ToSlash(relative))
			}
		}
	}
	if err := projectScope(transaction, sequence, intent.RepositoryID, intent.RepositoryRoot, intent.WorkspaceID,
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
		repository_id, repository_root, workspace_id, workspace_root, workspace_kind, workspace_key, paths_json, relative_paths_json,
		assistant_excerpt, started_at, completed_at, success, error, before_json, after_json,
		git_before_json, git_after_json, jj_before_json, jj_after_json, updated_sequence
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(id) DO UPDATE SET completed_at=excluded.completed_at, success=excluded.success,
		error=excluded.error, after_json=excluded.after_json, git_after_json=excluded.git_after_json,
		jj_after_json=excluded.jj_after_json, updated_sequence=excluded.updated_sequence`,
		intent.ID, intent.Actor, intent.SessionGeneration, nullable(intent.TurnID), intent.TurnIndex,
		intent.ToolCallID, intent.Tool, intent.Operation, intent.CWD,
		nullable(intent.RepositoryID), nullable(intent.RepositoryRoot), nullable(intent.WorkspaceID), nullable(intent.WorkspaceRoot), nullable(intent.WorkspaceKind),
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
	mu                sync.Mutex
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
		journalSequence:   max(initial, projected),
		projectedSequence: projected,
		notify:            make(chan struct{}),
	}
	go appender.projectLoop()
	return appender
}

func (p *ProjectingAppender) Append(event protocol.Event) error {
	// Serialize the durable append with journalSequence publication. Queries
	// taking the same lock can observe either the old tail before persistence or
	// the new tail after persistence, never the ambiguous window between them.
	p.mu.Lock()
	if err := p.primary.Append(event); err != nil {
		p.mu.Unlock()
		return err
	}
	p.journalSequence = max(p.journalSequence, event.Sequence)
	p.signalLocked()
	p.mu.Unlock()
	// Backpressure rather than drop: the journal is already durable, and every
	// projected sequence must remain contiguous so CLI answers are trustworthy.
	p.queue <- event
	return nil
}

func (p *ProjectingAppender) projectLoop() {
	defer close(p.done)
	for event := range p.queue {
		backoff := 10 * time.Millisecond
		for {
			if err := p.db.Project(event); err != nil {
				p.recordProjectionError(event.Sequence, err)
				time.Sleep(backoff)
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
		close(p.queue)
		<-p.done
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

func (d *DB) ResolveActorScoped(selector, repositoryID, workspaceID string) (string, error) {
	normalized := strings.TrimPrefix(strings.TrimSpace(selector), "@")
	var address string
	err := d.db.QueryRow(`SELECT address FROM actors WHERE address = ?`, normalized).Scan(&address)
	if err == nil {
		return address, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	query := `SELECT address FROM actors WHERE alias = ?`
	arguments := []any{normalized}
	if workspaceID != "" {
		query += ` AND workspace_id = ?`
		arguments = append(arguments, workspaceID)
	} else if repositoryID != "" {
		query += ` AND repository_id = ?`
		arguments = append(arguments, repositoryID)
	}
	query += ` ORDER BY address LIMIT 2`
	rows, err := d.db.Query(query, arguments...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var matches []string
	for rows.Next() {
		var match string
		if err := rows.Scan(&match); err != nil {
			return "", err
		}
		matches = append(matches, match)
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("unknown persisted actor %q", selector)
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("persisted alias %q is ambiguous; provide a workspace/repository scope or canonical address", selector)
	}
	return matches[0], nil
}
