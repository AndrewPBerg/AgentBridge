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
  workspace_root: string;
  change_id: string;
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

export type MutationIntent = {
  id: string;
  actor: ActorRecord["address"];
  tool_call_id: string;
  tool: string;
  operation: "edit" | "write" | "restore" | "delete" | "move" | "copy";
  paths: string[];
  cwd: string;
  workspace_key: string;
  git?: GitContext;
  jj?: JjContext;
  context: IntentContext;
  started_at: string;
  expires_at: string;
  completed_at?: string;
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
