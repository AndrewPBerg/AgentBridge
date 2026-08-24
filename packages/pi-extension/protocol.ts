export const BRIDGE_MESSAGE_TYPE = "agent-bridge";

export type GitContext = {
  repo_root: string;
  worktree_root: string;
  git_dir: string;
  common_dir: string;
  branch?: string;
  head?: string;
  detached: boolean;
};

export type JjContext = {
  repo_path?: string;
  workspace_name?: string;
  workspace_root?: string;
  change_id: string;
  commit_id?: string;
};

export type ActorRecord = {
  address: string;
  harness: "pi";
  session_uuid: string;
  session_file?: string;
  alias?: string;
  cwd: string;
  pane_id?: string;
  herdr_workspace_id?: string;
  state: "active" | "waiting" | "stealth" | "dead";
  repository_uuid?: string;
  repository_root?: string | null;
  workspace_uuid?: string;
  workspace_root?: string | null;
  workspace_kind?: string | null;
  git?: GitContext;
  jj?: JjContext;
  capabilities: string[];
  generation: number;
  started_at: string;
  heartbeat_at: string;
};

export type IntentContext = {
  assistant_excerpt?: string;
};

export type FileSnapshot = {
  path: string;
  exists: boolean;
  kind?: string;
  sha256?: string;
  size?: number;
  modified_at?: string;
};

export type MutationIntent = {
  id: string;
  actor: ActorRecord["address"];
  session_generation: number;
  turn_id?: string;
  turn_index?: number;
  tool_call_id: string;
  tool: string;
  operation: "edit" | "write" | "restore" | "delete" | "move" | "copy";
  paths: string[];
  relative_paths?: string[];
  cwd: string;
  repository_uuid?: string;
  repository_root?: string | null;
  workspace_uuid?: string;
  workspace_root?: string | null;
  workspace_kind?: string | null;
  workspace_key: string;
  git?: GitContext;
  jj?: JjContext;
  context: IntentContext;
  before?: FileSnapshot[];
  after?: FileSnapshot[];
  git_after?: GitContext;
  jj_after?: JjContext;
  success?: boolean;
  error?: string;
  started_at: string;
  expires_at: string;
  completed_at?: string;
};

export type MutationLease = {
  lease_uuid: string;
  fencing_token: string;
  actor_uuid: string;
  generation: number;
  repository_uuid: string;
  workspace_uuid: string;
  intent_id: string;
  tool_call_id: string;
  paths: string[];
  granted_at: string;
  renewed_at: string;
  expires_at: string;
  hard_deadline: string;
  state: "active" | "waiting" | string;
  blocking_lease_uuid?: string;
  blocking_root_lease_uuid?: string;
  collision_id?: string;
};

export type MutationLeaseResult = {
  decision: "grant" | "wait" | "block" | string;
  lease?: MutationLease;
  conflicts?: MutationLease[];
  reason?: string;
};

export type SessionEvent = {
  id: string;
  actor: ActorRecord["address"];
  session_generation: number;
  type: string;
  at: string;
  turn_index?: number;
  summary?: string;
  data?: Record<string, unknown>;
};

export type TestResult = {
  id: string;
  actor: ActorRecord["address"];
  session_generation: number;
  turn_id?: string;
  turn_index?: number;
  tool_call_id?: string;
  command: string;
  cwd: string;
  exit_code?: number;
  outcome?: "passed" | "failed" | "blocked";
  started_at: string;
  completed_at: string;
  duration_ms?: number;
  output_excerpt?: string;
  output_sha256?: string;
  output_bytes?: number;
  output_truncated?: boolean;
  repository_uuid: string;
  workspace_uuid: string;
  git?: GitContext;
  jj?: JjContext;
};

export type DirectionState = "draft" | "active" | "paused" | "converging" | "verified" | "completed" | "abandoned";
export type DirectionTransition = Exclude<DirectionState, "draft">;

/** Directions are project-level and intentionally have no repository/workspace scope. */
export type Direction = {
  direction_uuid: string;
  objective: string;
  success_criteria?: string;
  constraints?: string;
  context?: string;
  state: DirectionState | string;
  created_by: string;
  created_at?: string;
  updated_at?: string;
  completed_at?: string;
  tickets?: Array<Record<string, unknown>>;
};

export type WorkUnit = {
  work_unit_uuid: string;
  repository_uuid: string;
  repository_root?: string | null;
  workspace_uuid: string;
  workspace_root?: string | null;
  workspace_kind?: string | null;
  objective: string;
  state: string;
  participants?: unknown[];
  checkpoints?: unknown[];
  tickets?: Array<Record<string, unknown>>;
};

export type DirectionStatus = {
  direction: Direction;
  work_units: WorkUnit[];
  participants?: Array<{
    actor: string;
    alias?: string;
    state?: string;
    live: boolean;
    recent_activity?: { type: string; at: string; summary?: string };
  }>;
  open_collisions?: number;
  latest_checkpoints?: Array<{
    id: string;
    work_unit_uuid: string;
    kind: string;
    journal_end_sequence: number;
    claims?: Array<{ kind: string; status: string }>;
  }>;
};

export type CheckpointClaim = {
  kind: string;
  statement: string;
  status: "asserted" | "verified" | "failed" | "blocked";
  evidence: Array<{ kind: string; ordinal: number }>;
};

export type CheckpointRequest = {
  id: string;
  actor: ActorRecord["address"];
  declared_by?: "agent" | "human" | "system";
  session_generation: number;
  repository_uuid: string;
  workspace_uuid: string;
  work_unit_uuid?: string;
  tickets?: Array<Record<string, unknown>>;
  checkpoint_kind: string;
  claims?: CheckpointClaim[];
  journal_start_sequence: number;
  journal_end_sequence: number;
  boundary_event_id?: string;
  boundary_type?: string;
  turn_id?: string;
  turn_index?: number;
  compaction_event_id?: string;
  mutation_ids?: string[];
  message_ids?: string[];
  collision_ids?: string[];
  test_result_ids?: string[];
  metadata?: Record<string, string>;
  git?: GitContext;
  jj?: JjContext;
};

export type CollisionState = "open" | "negotiating" | "yielded" | "resolved";

export type Collision = {
  id: string;
  key: string;
  path: string;
  actors: [ActorRecord["address"], ActorRecord["address"]];
  intent_ids: [string, string];
  state: CollisionState;
  owner?: ActorRecord["address"];
  yielded_by?: ActorRecord["address"];
  created_at: string;
  updated_at: string;
  resolved_at?: string;
  resolution?: string;
};

export type BridgeMessage = {
  id: string;
  kind: "message" | "collision" | "external_change";
  from: string;
  to: ActorRecord["address"];
  body: string;
  global_sequence: number;
  sender_sequence: number;
  recipient_sequence: number;
  client_sequence?: number;
  session_generation?: number;
  collision_id?: string;
  created_at: string;
  acknowledged_at?: string;
};

export type HerdrAgent = {
  agent: string;
  agentSession?: string;
  status: string;
  cwd?: string;
  paneId: string;
  workspaceId?: string;
  title?: string;
};
