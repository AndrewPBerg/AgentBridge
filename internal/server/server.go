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

func ignoreError(error) {}

// Server serves Agent Bridge requests over a Unix socket.
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

// New creates a server without provenance queries.
func New(engine *state.Engine, socketPath string) *Server {
	return NewWithProvenance(engine, nil, socketPath)
}

// NewWithProvenance creates a server with optional provenance projection support.
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

// Serve listens for requests until ctx is canceled or the server closes.
func (s *Server) Serve(ctx context.Context) error {
	directory := filepath.Dir(s.path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create socket directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fmt.Errorf("secure socket directory: %w", err)
	}
	if err := removeStaleSocket(ctx, s.path); err != nil {
		return err
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "unix", s.path)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.path, err)
	}
	if err := os.Chmod(s.path, 0o600); err != nil {
		ignoreError(listener.Close())
		return fmt.Errorf("secure socket: %w", err)
	}
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		ignoreError(s.Close())
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
			ignoreError(connection.Close())
			continue
		}
		s.connections[connection] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handle(ctx, connection)
	}
}

func removeStaleSocket(ctx context.Context, path string) error {
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
	connection, dialErr := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if dialErr == nil {
		ignoreError(connection.Close())
		return fmt.Errorf("agent-bridge daemon is already listening on %s", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale socket: %w", err)
	}
	return nil
}

func (s *Server) handle(ctx context.Context, connection net.Conn) {
	defer s.wg.Done()
	defer func() {
		ignoreError(connection.Close())
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
			ignoreError(encoder.Encode(protocol.Response{Error: &protocol.RPCError{Code: "invalid_request", Message: err.Error()}}))
			continue
		}
		ignoreError(encoder.Encode(s.dispatchContext(ctx, request)))
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

func (s *Server) waitForProvenance(ctx context.Context) error {
	if s.projection == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.projection.WaitForCurrent(ctx); err != nil {
		return fmt.Errorf("provenance projection has not caught up: %w (health: %+v)", err, s.projection.Health())
	}
	return nil
}

type requestHandler func(*Server, context.Context, protocol.Request) protocol.Response

var requestHandlers = map[string]requestHandler{
	"ping":                        (*Server).handlePing,
	"daemon.shutdown":             (*Server).handleDaemonShutdown,
	"actor.register":              (*Server).handleActorRegister,
	"actor.heartbeat":             (*Server).handleActorHeartbeat,
	"actor.alias":                 (*Server).handleActorAlias,
	"sessions.list":               (*Server).handleSessionsList,
	"message.send":                (*Server).handleMessageSend,
	"mailbox.poll":                (*Server).handleMailboxPoll,
	"mailbox.ack":                 (*Server).handleMailboxAck,
	"intent.begin":                (*Server).handleIntentBegin,
	"intent.end":                  (*Server).handleIntentEnd,
	"activity.report":             (*Server).handleLegacyActivityReport,
	"activities.list":             (*Server).handleLegacyActivitiesList,
	"session.event":               (*Server).handleSessionEvent,
	"test.result":                 (*Server).handleTestResult,
	"launch.create":               (*Server).handleLaunchCreate,
	"launch.attach_child":         (*Server).handleLaunchAttachChild,
	"launch.attach_work_unit":     (*Server).handleLaunchAttachWorkUnit,
	"launch.get":                  (*Server).handleLaunchGet,
	"direction.create":            (*Server).handleDirectionCreate,
	"direction.get":               (*Server).handleDirectionGet,
	"direction.update":            (*Server).handleDirectionUpdate,
	"direction.status":            (*Server).handleDirectionStatus,
	"direction.transition":        (*Server).handleDirectionTransition,
	"work_unit.get":               (*Server).handleWorkUnitGet,
	"work_unit.create":            (*Server).handleWorkUnitCreate,
	"work_unit.update":            (*Server).handleWorkUnitUpdate,
	"work_unit.join":              (*Server).handleWorkUnitJoin,
	"work_unit.leave":             (*Server).handleWorkUnitLeave,
	"work_unit.transition":        (*Server).handleWorkUnitTransition,
	"provenance.work_unit":        (*Server).handleProvenanceWorkUnit,
	"checkpoint.request":          (*Server).handleCheckpointRequest,
	"provenance.checkpoint":       (*Server).handleProvenanceCheckpoint,
	"provenance.checkpoints":      (*Server).handleProvenanceCheckpoints,
	"provenance.scopes":           (*Server).handleProvenanceScopes,
	"provenance.snapshot":         (*Server).handleProvenanceSnapshot,
	"provenance.status":           (*Server).handleProvenanceStatus,
	"provenance.who_changed":      (*Server).handleProvenanceWhoChanged,
	"provenance.why":              (*Server).handleProvenanceWhy,
	"provenance.agent":            (*Server).handleProvenanceAgent,
	"provenance.since_compaction": (*Server).handleProvenanceSinceCompaction,
	"provenance.mutations":        (*Server).handleProvenanceMutations,
	"provenance.explain":          (*Server).handleProvenanceExplain,
	"provenance.timeline":         (*Server).handleProvenanceTimeline,
	"provenance.session":          (*Server).handleProvenanceSession,
	"collision.transition":        (*Server).handleCollisionTransition,
}

func (s *Server) dispatchContext(ctx context.Context, request protocol.Request) protocol.Response {
	handler, ok := requestHandlers[request.Method]
	if !ok {
		return failure(request.ID, "method_not_found", fmt.Errorf("unknown method %q", request.Method))
	}
	return handler(s, ctx, request)
}

func (s *Server) handlePing(_ context.Context, request protocol.Request) protocol.Response {
	return success(request.ID, map[string]any{"version": protocol.Version})
}

func (s *Server) handleDaemonShutdown(_ context.Context, request protocol.Request) protocol.Response {
	go func() {
		time.Sleep(25 * time.Millisecond)
		ignoreError(s.Close())
	}()
	return success(request.ID, map[string]any{"stopping": true})
}

func (s *Server) handleActorRegister(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.RegisterParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	actor, err := s.engine.Register(value.Actor)
	if err != nil {
		return failure(request.ID, "register_failed", err)
	}
	if value.LaunchUUID != "" {
		if _, err := s.engine.AttachLaunchChild(protocol.LaunchChildAttachParams{LaunchUUID: value.LaunchUUID, ChildActor: actor.Address}); err != nil {
			return failure(request.ID, "launch_attach_child_failed", err)
		}
	}
	return success(request.ID, actor)
}

func (s *Server) handleActorHeartbeat(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.HeartbeatParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	actor, err := s.engine.Heartbeat(value)
	if err != nil {
		return failure(request.ID, "heartbeat_failed", err)
	}
	return success(request.ID, actor)
}

func (s *Server) handleActorAlias(_ context.Context, request protocol.Request) protocol.Response {
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
}

func (s *Server) handleSessionsList(_ context.Context, request protocol.Request) protocol.Response {
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
}

func (s *Server) handleMessageSend(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.SendParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	message, err := s.engine.Send(value)
	if err != nil {
		return failure(request.ID, "send_failed", err)
	}
	return success(request.ID, message)
}

func (s *Server) handleMailboxPoll(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.PollParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	messages, err := s.engine.Poll(value.Actor, value.Limit)
	if err != nil {
		return failure(request.ID, "poll_failed", err)
	}
	return success(request.ID, map[string]any{"messages": messages})
}

func (s *Server) handleMailboxAck(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.AckParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	if err := s.engine.Ack(value); err != nil {
		return failure(request.ID, "ack_failed", err)
	}
	return success(request.ID, map[string]any{"acknowledged": len(value.MessageIDs)})
}

func (s *Server) handleIntentBegin(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.IntentBeginParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	collisions, err := s.engine.BeginIntent(value.Intent)
	if err != nil {
		return failure(request.ID, "intent_failed", err)
	}
	return success(request.ID, map[string]any{"intent": value.Intent, "collisions": collisions})
}

func (s *Server) handleIntentEnd(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.IntentEndParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	intent, err := s.engine.EndIntent(value)
	if err != nil {
		return failure(request.ID, "intent_failed", err)
	}
	return success(request.ID, intent)
}

// handleLegacyActivityReport is a transition shim for Pi sessions that have
// not yet reloaded after removal of the abandoned activity feature.
func (s *Server) handleLegacyActivityReport(_ context.Context, request protocol.Request) protocol.Response {
	return success(request.ID, map[string]any{"ignored": true})
}

func (s *Server) handleLegacyActivitiesList(_ context.Context, request protocol.Request) protocol.Response {
	return success(request.ID, map[string]any{"activities": []any{}})
}

func (s *Server) handleSessionEvent(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.SessionEventParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	event, err := s.engine.RecordSessionEvent(value.Event)
	if err != nil {
		return failure(request.ID, "session_event_failed", err)
	}
	return success(request.ID, event)
}

func (s *Server) handleTestResult(_ context.Context, request protocol.Request) protocol.Response {
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
}

func (s *Server) handleDirectionCreate(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.DirectionCreateParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	direction, err := s.engine.CreateDirection(value.Direction)
	if err != nil {
		return failure(request.ID, "direction_create_failed", err)
	}
	return success(request.ID, direction)
}

func (s *Server) handleDirectionUpdate(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.DirectionUpdateParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	direction, err := s.engine.UpdateDirection(value)
	if err != nil {
		return failure(request.ID, "direction_update_failed", err)
	}
	return success(request.ID, direction)
}

func (s *Server) handleDirectionGet(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[struct {
		UUID string `json:"direction_uuid"`
	}](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	direction, err := s.engine.Direction(value.UUID)
	if err != nil {
		return failure(request.ID, "direction_get_failed", err)
	}
	return success(request.ID, direction)
}

func (s *Server) handleDirectionStatus(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
		return failure(request.ID, "provenance_lagging", err)
	}
	value, err := params[struct {
		UUID string `json:"direction_uuid"`
	}](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	//nolint:contextcheck // provenance query API currently owns its bounded contexts
	status, err := s.provenance.DirectionStatus(value.UUID)
	if err != nil {
		return failure(request.ID, "direction_status_failed", err)
	}
	return success(request.ID, status)
}

func (s *Server) handleLaunchCreate(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.LaunchCreateParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	launch, err := s.engine.CreateLaunch(value)
	if err != nil {
		return failure(request.ID, "launch_create_failed", err)
	}
	return success(request.ID, launch)
}

func (s *Server) handleLaunchAttachChild(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.LaunchChildAttachParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	launch, err := s.engine.AttachLaunchChild(value)
	if err != nil {
		return failure(request.ID, "launch_attach_child_failed", err)
	}
	return success(request.ID, launch)
}

func (s *Server) handleLaunchAttachWorkUnit(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.LaunchWorkUnitAttachParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	launch, err := s.engine.AttachLaunchWorkUnit(value)
	if err != nil {
		return failure(request.ID, "launch_attach_work_unit_failed", err)
	}
	return success(request.ID, launch)
}

func (s *Server) handleLaunchGet(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[struct {
		UUID string `json:"launch_uuid"`
	}](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	launch, err := s.engine.Launch(value.UUID)
	if err != nil {
		return failure(request.ID, "launch_get_failed", err)
	}
	return success(request.ID, launch)
}

func (s *Server) handleDirectionTransition(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.DirectionTransitionParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	direction, err := s.engine.TransitionDirection(value)
	if err != nil {
		return failure(request.ID, "direction_transition_failed", err)
	}
	return success(request.ID, direction)
}

func (s *Server) handleWorkUnitGet(_ context.Context, request protocol.Request) protocol.Response {
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
}

func (s *Server) handleWorkUnitCreate(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.WorkUnitCreateParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	unit, err := s.engine.CreateWorkUnit(value.WorkUnit)
	if err != nil {
		return failure(request.ID, "work_unit_create_failed", err)
	}
	return success(request.ID, unit)
}

func (s *Server) handleWorkUnitUpdate(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.WorkUnitUpdateParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	unit, err := s.engine.UpdateWorkUnit(value)
	if err != nil {
		return failure(request.ID, "work_unit_update_failed", err)
	}
	return success(request.ID, unit)
}

func (s *Server) handleWorkUnitJoin(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.WorkUnitActorParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	member, err := s.engine.JoinWorkUnit(value)
	if err != nil {
		return failure(request.ID, "work_unit_join_failed", err)
	}
	return success(request.ID, member)
}

func (s *Server) handleWorkUnitLeave(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.WorkUnitActorParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	member, err := s.engine.LeaveWorkUnit(value)
	if err != nil {
		return failure(request.ID, "work_unit_leave_failed", err)
	}
	return success(request.ID, member)
}

func (s *Server) handleWorkUnitTransition(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.WorkUnitTransitionParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	unit, err := s.engine.TransitionWorkUnit(value)
	if err != nil {
		return failure(request.ID, "work_unit_transition_failed", err)
	}
	return success(request.ID, unit)
}

func (s *Server) handleProvenanceWorkUnit(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
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
	//nolint:contextcheck // provenance APIs currently accept no context
	unit, err := s.provenance.WorkUnit(value.UUID)
	if err != nil {
		return failure(request.ID, "provenance_query_failed", err)
	}
	return success(request.ID, unit)
}

func (s *Server) handleCheckpointRequest(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.CheckpointRequestParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	checkpoint, err := s.engine.RequestCheckpoint(value.Request)
	if err != nil {
		return failure(request.ID, "checkpoint_request_failed", err)
	}
	return success(request.ID, checkpoint)
}

func (s *Server) handleProvenanceCheckpoint(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
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
	//nolint:contextcheck // provenance APIs currently accept no context
	checkpoint, err := s.provenance.Checkpoint(value.ID)
	if err != nil {
		return failure(request.ID, "provenance_query_failed", err)
	}
	return success(request.ID, checkpoint)
}

func (s *Server) handleProvenanceCheckpoints(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
		return failure(request.ID, "provenance_lagging", err)
	}
	value, err := params[struct {
		WorkUnitUUID string `json:"work_unit_uuid,omitempty"`
		Limit        int    `json:"limit,omitempty"`
	}](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	//nolint:contextcheck // provenance APIs currently accept no context
	checkpoints, err := s.provenance.ListCheckpoints(value.WorkUnitUUID, value.Limit)
	if err != nil {
		return failure(request.ID, "provenance_query_failed", err)
	}
	return success(request.ID, map[string]any{"checkpoints": checkpoints})
}

func (s *Server) handleProvenanceScopes(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
		return failure(request.ID, "provenance_lagging", err)
	}
	//nolint:contextcheck // provenance APIs currently accept no context
	scopes, err := s.provenance.Scopes()
	if err != nil {
		return failure(request.ID, "provenance_query_failed", err)
	}
	return success(request.ID, scopes)
}

func (s *Server) handleProvenanceSnapshot(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
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
	//nolint:contextcheck // provenance APIs currently accept no context
	if err := s.provenance.Snapshot(value.Path); err != nil {
		return failure(request.ID, "provenance_snapshot_failed", err)
	}
	return success(request.ID, map[string]any{"path": value.Path})
}

func (s *Server) handleProvenanceStatus(_ context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	//nolint:contextcheck // provenance APIs currently accept no context
	status, err := s.provenance.Status()
	if err != nil {
		return failure(request.ID, "provenance_query_failed", err)
	}
	var health any
	if s.projection != nil {
		health = s.projection.Health()
	}
	return success(request.ID, map[string]any{"database": status, "projection": health})
}

func (s *Server) handleProvenanceWhoChanged(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
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
	//nolint:contextcheck // provenance APIs currently accept no context
	answer, err := s.provenance.WhoChanged(value.Path, value.Limit)
	if err != nil {
		return failure(request.ID, "provenance_query_failed", err)
	}
	return success(request.ID, answer)
}

func (s *Server) handleProvenanceWhy(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
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
	//nolint:contextcheck // provenance APIs currently accept no context
	answer, err := s.provenance.Why(value.ID, value.Limit)
	if err != nil {
		return failure(request.ID, "provenance_query_failed", err)
	}
	return success(request.ID, answer)
}

func (s *Server) handleProvenanceAgent(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
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
	//nolint:contextcheck // provenance APIs currently accept no context
	answer, err := s.provenance.AgentSummary(value.Actor, value.Limit, provenance.ActorScope{
		RepositoryUUID: value.RepositoryUUID, WorkspaceUUID: value.WorkspaceUUID,
	})
	if err != nil {
		return failure(request.ID, "provenance_query_failed", err)
	}
	return success(request.ID, answer)
}

func (s *Server) handleProvenanceSinceCompaction(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
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
	//nolint:contextcheck // provenance APIs currently accept no context
	answer, err := s.provenance.SinceCompaction(value.Actor, value.Limit, provenance.ActorScope{
		RepositoryUUID: value.RepositoryUUID, WorkspaceUUID: value.WorkspaceUUID,
	})
	if err != nil {
		return failure(request.ID, "provenance_query_failed", err)
	}
	return success(request.ID, answer)
}

func (s *Server) handleProvenanceMutations(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
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
	//nolint:contextcheck // provenance APIs currently accept no context
	records, err := s.provenance.ListMutations(provenance.MutationFilter{
		Actor: value.Actor, Path: value.Path, RepositoryUUID: value.RepositoryUUID, WorkspaceUUID: value.WorkspaceUUID,
		Limit: value.Limit, Failed: value.Failed,
	})
	if err != nil {
		return failure(request.ID, "provenance_query_failed", err)
	}
	return success(request.ID, map[string]any{"mutations": records})
}

func (s *Server) handleProvenanceExplain(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
		return failure(request.ID, "provenance_lagging", err)
	}
	value, err := params[struct {
		ID string `json:"id"`
	}](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	//nolint:contextcheck // provenance APIs currently accept no context
	record, err := s.provenance.Mutation(value.ID)
	if err != nil {
		return failure(request.ID, "provenance_query_failed", err)
	}
	return success(request.ID, record)
}

func (s *Server) handleProvenanceTimeline(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
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
	//nolint:contextcheck // provenance APIs currently accept no context
	records, err := s.provenance.Timeline(value.Actor, value.Type, value.Limit, provenance.ActorScope{
		RepositoryUUID: value.RepositoryUUID, WorkspaceUUID: value.WorkspaceUUID,
	})
	if err != nil {
		return failure(request.ID, "provenance_query_failed", err)
	}
	return success(request.ID, map[string]any{"events": records})
}

func (s *Server) handleProvenanceSession(ctx context.Context, request protocol.Request) protocol.Response {
	if s.provenance == nil {
		return failure(request.ID, "provenance_unavailable", errors.New("provenance database is unavailable"))
	}
	if err := s.waitForProvenance(ctx); err != nil {
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
	//nolint:contextcheck // provenance APIs currently accept no context
	records, err := s.provenance.SessionEvents(value.Actor, value.Limit, provenance.ActorScope{
		RepositoryUUID: value.RepositoryUUID, WorkspaceUUID: value.WorkspaceUUID,
	})
	if err != nil {
		return failure(request.ID, "provenance_query_failed", err)
	}
	return success(request.ID, map[string]any{"session_events": records})
}

func (s *Server) handleCollisionTransition(_ context.Context, request protocol.Request) protocol.Response {
	value, err := params[protocol.TransitionParams](request)
	if err != nil {
		return failure(request.ID, "invalid_params", err)
	}
	collision, err := s.engine.Transition(value)
	if err != nil {
		return failure(request.ID, "transition_failed", err)
	}
	return success(request.ID, collision)
}

// Close stops accepting connections and closes active connections.
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
		ignoreError(connection.Close())
	}
	s.wg.Wait()
	ignoreError(os.Remove(s.path))
	return err
}
