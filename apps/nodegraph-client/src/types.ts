// ── Connection ──

export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected';

// ── Metrics (from metrics channel) ──

export interface MetricsSnapshot {
  frameInProgress: number;
  framesPerSession: number;
  lookahead: number;
  pauseState: PauseState;
  ops: { current: number; average: number; max: number };
  buffer: { count: number; framesBuffered: number; highestFrame: number };
}

export type PauseState = 'UNPAUSED' | 'REST' | 'SOFT' | 'HARD';

// ── Entities (from down socket updates) ──

export interface EntityState {
  id: string;
  type: string;
  version: number;
  state: Record<string, unknown>;
}

export interface EntityMap {
  [id: string]: EntityState;
}

// ── Operations (from metrics channel manifests) ──

export interface ManifestOperation {
  id: string;
  entityId: string;
  entityType: string;
  action: string;
  timestamp: number;
  frame: number;
}

// ── Selection (local UI state) ──

export interface SelectionState {
  selectedOperationId: string | null;
  selectedNodeId: string | null;
}

export type SelectionAction =
  | { type: 'SELECT_OPERATION'; operationId: string; entityId: string | null }
  | { type: 'SELECT_NODE'; nodeId: string }
  | { type: 'CLEAR_SELECTION' };

// ── Server messages (down socket) ──

export interface MetricsFrameMessage {
  type: 'metrics_frame';
  frameInProgress: number;
  framesPerSession: number;
  lookahead: number;
  pauseState: PauseState;
  opsCurrentFrame: number;
  opsAverage: number;
  opsMax: number;
  bufferCount: number;
  framesBuffered: number;
  highestFrameBuffered: number;
  operations: Array<Record<string, unknown>>;
}

export interface MetricsPauseMessage {
  type: 'metrics_pause';
  pauseState: PauseState;
  frameInProgress: number;
  sessionId: string;
}

export interface EntityUpdateMessage {
  type: 'update';
  timestamp: number;
  updates: Record<string, {
    _id?: string;
    type?: string;
    version?: number;
    state?: Record<string, unknown>;
    delta?: Record<string, unknown>;
    operation?: string;
    changedAt?: number;
  }>;
}

export interface SyncMessage {
  type: 'sync';
  data: {
    entities: Array<{
      _id?: string;
      id?: string;
      type?: string;
      version?: number;
      state?: Record<string, unknown>;
    }>;
  };
}

export type DownSocketMessage =
  | MetricsFrameMessage
  | MetricsPauseMessage
  | EntityUpdateMessage
  | SyncMessage
  | { type: string; [key: string]: unknown };