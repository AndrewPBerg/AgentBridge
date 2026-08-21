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
  address: `pi:${string}`;
  harness: "pi";
  session_id: string;
  session_file?: string;
  alias?: string;
  cwd: string;
  pane_id?: string;
  herdr_workspace_id?: string;
  state: "active" | "waiting" | "dead";
  repository_id?: string;
  repository_root?: string;
  workspace_id?: string;
  workspace_root?: string;
  workspace_kind?: string;
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
  repository_id?: string;
  repository_root?: string;
  workspace_id?: string;
  workspace_root?: string;
  workspace_kind?: string;
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
  kind: "message" | "collision";
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
