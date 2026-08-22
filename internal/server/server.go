package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
	"github.com/AndrewPBerg/agent-bridge/internal/provenance"
	"github.com/AndrewPBerg/agent-bridge/internal/state"
)

type Server struct {
	engine     *state.Engine
	provenance *provenance.DB
	projection *provenance.ProjectingAppender
	path       string

	mu          sync.Mutex
	listener    net.Listener
	connections map[net.Conn]struct{}
	closed      bool
	wg          sync.WaitGroup
}

func New(engine *state.Engine, socketPath string) *Server {
	return NewWithProvenance(engine, nil, socketPath)
}

func NewWithProvenance(
	engine *state.Engine,
	database *provenance.DB,
	socketPath string,
	projection ...*provenance.ProjectingAppender,
) *Server {
	var projector *provenance.ProjectingAppender
	if len(projection) > 0 {
		projector = projection[0]
	}
	return &Server{
		engine: engine, provenance: database, projection: projector, path: socketPath, connections: make(map[net.Conn]struct{}),
	}
}

func (s *Server) Serve(ctx context.Context) error {
	directory := filepath.Dir(s.path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create socket directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fmt.Errorf("secure socket directory: %w", err)
	}
	if err := removeStaleSocket(s.path); err != nil {
		return err
	}
	listener, err := net.Listen("unix", s.path)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.path, err)
	}
	if err := os.Chmod(s.path, 0o600); err != nil {
		listener.Close()
		return fmt.Errorf("secure socket: %w", err)
	}
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		_ = s.Close()
	}()

	for {
		connection, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept connection: %w", err)
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			connection.Close()
			continue
		}
		s.connections[connection] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handle(connection)
	}
}

func removeStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("refusing to remove non-socket path %s", path)
	}
	connection, dialErr := net.Dial("unix", path)
	if dialErr == nil {
		connection.Close()
		return fmt.Errorf("agent-bridge daemon is already listening on %s", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale socket: %w", err)
	}
	return nil
}

func (s *Server) handle(connection net.Conn) {
	defer s.wg.Done()
	defer func() {
		connection.Close()
		s.mu.Lock()
		delete(s.connections, connection)
		s.mu.Unlock()
	}()
	scanner := bufio.NewScanner(connection)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	encoder := json.NewEncoder(connection)
	for scanner.Scan() {
		var request protocol.Request
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			_ = encoder.Encode(protocol.Response{Error: &protocol.RPCError{Code: "invalid_request", Message: err.Error()}})
			continue
		}
		_ = encoder.Encode(s.dispatch(request))
	}
}

func params[T any](request protocol.Request) (T, error) {
	var value T
	if len(request.Params) == 0 {
		return value, nil
	}
	err := json.Unmarshal(request.Params, &value)
	return value, err
}

func success(id string, result any) protocol.Response {
	return protocol.Response{ID: id, Result: result}
}

func failure(id, code string, err error) protocol.Response {
	return protocol.Response{ID: id, Error: &protocol.RPCError{Code: code, Message: err.Error()}}
}

func (s *Server) waitForProvenance() error {
	if s.projection == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.projection.WaitForCurrent(ctx); err != nil {
		return fmt.Errorf("provenance projection has not caught up: %w (health: %+v)", err, s.projection.Health())
	}
	return nil
}

func (s *Server) dispatch(request protocol.Request) protocol.Response {
	switch request.Method {
	case "ping":
		return success(request.ID, map[string]any{"version": protocol.Version})
	case "daemon.shutdown":
		go func() {
			time.Sleep(25 * time.Millisecond)
			_ = s.Close()
		}()
		return success(request.ID, map[string]any{"stopping": true})
	case "actor.register":
		value, err := params[protocol.RegisterParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		actor, err := s.engine.Register(value.Actor)
		if err != nil {
			return failure(request.ID, "register_failed", err)
		}
		return success(request.ID, actor)
	case "actor.heartbeat":
		value, err := params[protocol.HeartbeatParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		actor, err := s.engine.Heartbeat(value)
		if err != nil {
			return failure(request.ID, "heartbeat_failed", err)
		}
		return success(request.ID, actor)
	case "actor.alias":
		value, err := params[struct {
			Address string `json:"address"`
			Alias   string `json:"alias"`
		}](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		actor, err := s.engine.SetAlias(value.Address, value.Alias)
		if err != nil {
			return failure(request.ID, "alias_failed", err)
		}
		return success(request.ID, actor)
	case "sessions.list":
		value, err := params[struct {
			IncludeStale   bool   `json:"include_stale"`
			RepositoryUUID string `json:"repository_uuid"`
			WorkspaceUUID  string `json:"workspace_uuid"`
			Directory      string `json:"directory"`
		}](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		actors := s.engine.SessionsScoped(value.IncludeStale, protocol.ScopeFilter{
			RepositoryUUID: value.RepositoryUUID, WorkspaceUUID: value.WorkspaceUUID, Directory: value.Directory,
		})
		return success(request.ID, map[string]any{"actors": actors})
	case "message.send":
		value, err := params[protocol.SendParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		message, err := s.engine.Send(value)
		if err != nil {
			return failure(request.ID, "send_failed", err)
		}
		return success(request.ID, message)
	case "mailbox.poll":
		value, err := params[protocol.PollParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		messages, err := s.engine.Poll(value.Actor, value.Limit)
		if err != nil {
			return failure(request.ID, "poll_failed", err)
		}
		return success(request.ID, map[string]any{"messages": messages})
	case "mailbox.ack":
		value, err := params[protocol.AckParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		if err := s.engine.Ack(value); err != nil {
			return failure(request.ID, "ack_failed", err)
		}
		return success(request.ID, map[string]any{"acknowledged": len(value.MessageIDs)})
	case "intent.begin":
		value, err := params[protocol.IntentBeginParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		collisions, err := s.engine.BeginIntent(value.Intent)
		if err != nil {
			return failure(request.ID, "intent_failed", err)
		}
		return success(request.ID, map[string]any{"intent": value.Intent, "collisions": collisions})
	case "intent.end":
		value, err := params[protocol.IntentEndParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		intent, err := s.engine.EndIntent(value)
		if err != nil {
			return failure(request.ID, "intent_failed", err)
		}
		return success(request.ID, intent)
	case "session.event":
		value, err := params[protocol.SessionEventParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		event, err := s.engine.RecordSessionEvent(value.Event)
		if err != nil {
			return failure(request.ID, "session_event_failed", err)
		}
		return success(request.ID, event)
	case "test.result":
		value, err := params[struct {
			Result protocol.TestResult `json:"result"`
		}](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		result, err := s.engine.RecordTestResult(value.Result)
		if err != nil {
			return failure(request.ID, "test_result_failed", err)
		}
		return success(request.ID, result)
	case "work_unit.get":
		value, err := params[struct {
			UUID string `json:"work_unit_uuid"`
		}](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		unit, actors, err := s.engine.WorkUnit(value.UUID)
		if err != nil {
			return failure(request.ID, "work_unit_get_failed", err)
		}
		return success(request.ID, struct {
			Unit   protocol.WorkUnit        `json:"work_unit"`
			Actors []protocol.WorkUnitActor `json:"actors"`
		}{unit, actors})
	case "work_unit.create":
		value, err := params[protocol.WorkUnitCreateParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		unit, err := s.engine.CreateWorkUnit(value.WorkUnit)
		if err != nil {
			return failure(request.ID, "work_unit_create_failed", err)
		}
		return success(request.ID, unit)
	case "work_unit.update":
		value, err := params[protocol.WorkUnitUpdateParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		unit, err := s.engine.UpdateWorkUnit(value)
		if err != nil {
			return failure(request.ID, "work_unit_update_failed", err)
		}
		return success(request.ID, unit)
	case "work_unit.join":
		value, err := params[protocol.WorkUnitActorParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		member, err := s.engine.JoinWorkUnit(value)
		if err != nil {
			return failure(request.ID, "work_unit_join_failed", err)
		}
		return success(request.ID, member)
	case "work_unit.leave":
		value, err := params[protocol.WorkUnitActorParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		member, err := s.engine.LeaveWorkUnit(value)
		if err != nil {
			return failure(request.ID, "work_unit_leave_failed", err)
		}
		return success(request.ID, member)
	case "work_unit.transition":
		value, err := params[protocol.WorkUnitTransitionParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		unit, err := s.engine.TransitionWorkUnit(value)
		if err != nil {
			return failure(request.ID, "work_unit_transition_failed", err)
		}
		return success(request.ID, unit)
	case "provenance.work_unit":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		if err := s.waitForProvenance(); err != nil {
			return failure(request.ID, "provenance_lagging", err)
		}
		value, err := params[struct {
			UUID string `json:"work_unit_uuid"`
		}](request)
		if err != nil || value.UUID == "" {
			if err == nil {
				err = errors.New("work unit UUID is required")
			}
			return failure(request.ID, "invalid_params", err)
		}
		unit, err := s.provenance.WorkUnit(value.UUID)
		if err != nil {
			return failure(request.ID, "provenance_query_failed", err)
		}
		return success(request.ID, unit)
	case "checkpoint.request":
		value, err := params[protocol.CheckpointRequestParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		checkpoint, err := s.engine.RequestCheckpoint(value.Request)
		if err != nil {
			return failure(request.ID, "checkpoint_request_failed", err)
		}
		return success(request.ID, checkpoint)
	case "provenance.checkpoint":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		if err := s.waitForProvenance(); err != nil {
			return failure(request.ID, "provenance_lagging", err)
		}
		value, err := params[struct {
			ID string `json:"id"`
		}](request)
		if err != nil || value.ID == "" {
			if err == nil {
				err = errors.New("checkpoint ID is required")
			}
			return failure(request.ID, "invalid_params", err)
		}
		checkpoint, err := s.provenance.Checkpoint(value.ID)
		if err != nil {
			return failure(request.ID, "provenance_query_failed", err)
		}
		return success(request.ID, checkpoint)
	case "provenance.checkpoints":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		if err := s.waitForProvenance(); err != nil {
			return failure(request.ID, "provenance_lagging", err)
		}
		value, err := params[struct {
			WorkUnitUUID string `json:"work_unit_uuid,omitempty"`
			Limit        int    `json:"limit,omitempty"`
		}](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		checkpoints, err := s.provenance.ListCheckpoints(value.WorkUnitUUID, value.Limit)
		if err != nil {
			return failure(request.ID, "provenance_query_failed", err)
		}
		return success(request.ID, map[string]any{"checkpoints": checkpoints})
	case "provenance.scopes":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		if err := s.waitForProvenance(); err != nil {
			return failure(request.ID, "provenance_lagging", err)
		}
		scopes, err := s.provenance.Scopes()
		if err != nil {
			return failure(request.ID, "provenance_query_failed", err)
		}
		return success(request.ID, scopes)
	case "provenance.snapshot":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		if err := s.waitForProvenance(); err != nil {
			return failure(request.ID, "provenance_lagging", err)
		}
		value, err := params[struct {
			Path string `json:"path"`
		}](request)
		if err != nil || value.Path == "" {
			if err == nil {
				err = errors.New("snapshot path is required")
			}
			return failure(request.ID, "invalid_params", err)
		}
		if err := s.provenance.Snapshot(value.Path); err != nil {
			return failure(request.ID, "provenance_snapshot_failed", err)
		}
		return success(request.ID, map[string]any{"path": value.Path})
	case "provenance.status":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		status, err := s.provenance.Status()
		if err != nil {
			return failure(request.ID, "provenance_query_failed", err)
		}
		var health any
		if s.projection != nil {
			health = s.projection.Health()
		}
		return success(request.ID, map[string]any{"database": status, "projection": health})
	case "provenance.who_changed":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		if err := s.waitForProvenance(); err != nil {
			return failure(request.ID, "provenance_lagging", err)
		}
		value, err := params[struct {
			Path  string `json:"path"`
			Limit int    `json:"limit"`
		}](request)
		if err != nil || value.Path == "" {
			if err == nil {
				err = errors.New("path is required")
			}
			return failure(request.ID, "invalid_params", err)
		}
		answer, err := s.provenance.WhoChanged(value.Path, value.Limit)
		if err != nil {
			return failure(request.ID, "provenance_query_failed", err)
		}
		return success(request.ID, answer)
	case "provenance.why":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		if err := s.waitForProvenance(); err != nil {
			return failure(request.ID, "provenance_lagging", err)
		}
		value, err := params[struct {
			ID    string `json:"id"`
			Limit int    `json:"limit"`
		}](request)
		if err != nil || value.ID == "" {
			if err == nil {
				err = errors.New("mutation id is required")
			}
			return failure(request.ID, "invalid_params", err)
		}
		answer, err := s.provenance.Why(value.ID, value.Limit)
		if err != nil {
			return failure(request.ID, "provenance_query_failed", err)
		}
		return success(request.ID, answer)
	case "provenance.agent":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		if err := s.waitForProvenance(); err != nil {
			return failure(request.ID, "provenance_lagging", err)
		}
		value, err := params[struct {
			Actor          string `json:"actor"`
			RepositoryUUID string `json:"repository_uuid"`
			WorkspaceUUID  string `json:"workspace_uuid"`
			Limit          int    `json:"limit"`
		}](request)
		if err != nil || value.Actor == "" {
			if err == nil {
				err = errors.New("actor is required")
			}
			return failure(request.ID, "invalid_params", err)
		}
		answer, err := s.provenance.AgentSummary(value.Actor, value.Limit, provenance.ActorScope{
			RepositoryUUID: value.RepositoryUUID, WorkspaceUUID: value.WorkspaceUUID,
		})
		if err != nil {
			return failure(request.ID, "provenance_query_failed", err)
		}
		return success(request.ID, answer)
	case "provenance.since_compaction":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		if err := s.waitForProvenance(); err != nil {
			return failure(request.ID, "provenance_lagging", err)
		}
		value, err := params[struct {
			Actor          string `json:"actor"`
			RepositoryUUID string `json:"repository_uuid"`
			WorkspaceUUID  string `json:"workspace_uuid"`
			Limit          int    `json:"limit"`
		}](request)
		if err != nil || value.Actor == "" {
			if err == nil {
				err = errors.New("actor is required")
			}
			return failure(request.ID, "invalid_params", err)
		}
		answer, err := s.provenance.SinceCompaction(value.Actor, value.Limit, provenance.ActorScope{
			RepositoryUUID: value.RepositoryUUID, WorkspaceUUID: value.WorkspaceUUID,
		})
		if err != nil {
			return failure(request.ID, "provenance_query_failed", err)
		}
		return success(request.ID, answer)
	case "provenance.mutations":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		if err := s.waitForProvenance(); err != nil {
			return failure(request.ID, "provenance_lagging", err)
		}
		value, err := params[struct {
			Actor          string `json:"actor"`
			Path           string `json:"path"`
			RepositoryUUID string `json:"repository_uuid"`
			WorkspaceUUID  string `json:"workspace_uuid"`
			Limit          int    `json:"limit"`
			Failed         bool   `json:"failed"`
		}](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		records, err := s.provenance.ListMutations(provenance.MutationFilter{
			Actor: value.Actor, Path: value.Path, RepositoryUUID: value.RepositoryUUID, WorkspaceUUID: value.WorkspaceUUID,
			Limit: value.Limit, Failed: value.Failed,
		})
		if err != nil {
			return failure(request.ID, "provenance_query_failed", err)
		}
		return success(request.ID, map[string]any{"mutations": records})
	case "provenance.explain":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		if err := s.waitForProvenance(); err != nil {
			return failure(request.ID, "provenance_lagging", err)
		}
		value, err := params[struct {
			ID string `json:"id"`
		}](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		record, err := s.provenance.Mutation(value.ID)
		if err != nil {
			return failure(request.ID, "provenance_query_failed", err)
		}
		return success(request.ID, record)
	case "provenance.timeline":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		if err := s.waitForProvenance(); err != nil {
			return failure(request.ID, "provenance_lagging", err)
		}
		value, err := params[struct {
			Actor          string `json:"actor"`
			RepositoryUUID string `json:"repository_uuid"`
			WorkspaceUUID  string `json:"workspace_uuid"`
			Type           string `json:"type"`
			Limit          int    `json:"limit"`
		}](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		records, err := s.provenance.Timeline(value.Actor, value.Type, value.Limit, provenance.ActorScope{
			RepositoryUUID: value.RepositoryUUID, WorkspaceUUID: value.WorkspaceUUID,
		})
		if err != nil {
			return failure(request.ID, "provenance_query_failed", err)
		}
		return success(request.ID, map[string]any{"events": records})
	case "provenance.session":
		if s.provenance == nil {
			return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
		}
		if err := s.waitForProvenance(); err != nil {
			return failure(request.ID, "provenance_lagging", err)
		}
		value, err := params[struct {
			Actor          string `json:"actor"`
			RepositoryUUID string `json:"repository_uuid"`
			WorkspaceUUID  string `json:"workspace_uuid"`
			Limit          int    `json:"limit"`
		}](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		records, err := s.provenance.SessionEvents(value.Actor, value.Limit, provenance.ActorScope{
			RepositoryUUID: value.RepositoryUUID, WorkspaceUUID: value.WorkspaceUUID,
		})
		if err != nil {
			return failure(request.ID, "provenance_query_failed", err)
		}
		return success(request.ID, map[string]any{"session_events": records})
	case "collision.transition":
		value, err := params[protocol.TransitionParams](request)
		if err != nil {
			return failure(request.ID, "invalid_params", err)
		}
		collision, err := s.engine.Transition(value)
		if err != nil {
			return failure(request.ID, "transition_failed", err)
		}
		return success(request.ID, collision)
	default:
		return failure(request.ID, "method_not_found", fmt.Errorf("unknown method %q", request.Method))
	}
}

func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	listener := s.listener
	connections := make([]net.Conn, 0, len(s.connections))
	for connection := range s.connections {
		connections = append(connections, connection)
	}
	s.mu.Unlock()
	var err error
	if listener != nil {
		err = listener.Close()
	}
	for _, connection := range connections {
		_ = connection.Close()
	}
	s.wg.Wait()
	_ = os.Remove(s.path)
	return err
}
