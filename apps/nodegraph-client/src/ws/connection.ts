import { config } from '../config';
import type { ConnectionStatus, DownSocketMessage } from '../types';

let upSocket: WebSocket | null = null;
let downSocket: WebSocket | null = null;
let connectionId: string | null = null;
let status: ConnectionStatus = 'disconnected';

// ── Listener registries ──

type StatusListener = (status: ConnectionStatus) => void;
type MessageListener = (msg: DownSocketMessage) => void;

const statusListeners = new Set<StatusListener>();
const downMessageListeners = new Set<MessageListener>();

function notifyStatus(): void {
  for (const fn of statusListeners) fn(status);
}

// ── Public API ──

export function getStatus(): ConnectionStatus {
  return status;
}

export function getConnectionId(): string | null {
  return connectionId;
}

export function onStatusChange(fn: StatusListener): () => void {
  statusListeners.add(fn);
  return () => { statusListeners.delete(fn); };
}

export function onDownMessage(fn: MessageListener): () => void {
  downMessageListeners.add(fn);
  return () => { downMessageListeners.delete(fn); };
}

export function connect(): void {
  if (status !== 'disconnected') return;
  status = 'connecting';
  notifyStatus();
  openUpSocket();
}

export function disconnect(): void {
  try { upSocket?.close(); } catch { /* noop */ }
  try { downSocket?.close(); } catch { /* noop */ }
  upSocket = null;
  downSocket = null;
  connectionId = null;
  status = 'disconnected';
  notifyStatus();
}

export function sendCommand(cmd: Record<string, unknown>): boolean {
  if (!upSocket || upSocket.readyState !== WebSocket.OPEN) return false;
  upSocket.send(JSON.stringify(cmd));
  return true;
}

// ── Internals ──

function openUpSocket(): void {
  const { protocol, host, upPort, upPath } = config.ws;
  const url = `${protocol}${host}:${upPort}${upPath}`;
  upSocket = new WebSocket(url);

  upSocket.onopen = () => {
    upSocket!.send(JSON.stringify({
      type: 'connect',
      meta: { enable_metrics: 'true' },
    }));
  };

upSocket.onmessage = (event: MessageEvent) => {
  try {
    const msg = JSON.parse(event.data);

    if (msg.type === 'connection_confirm' && msg.connectionId) {
      connectionId = msg.connectionId;
      openDownSocket(msg.connectionId);
      return;
    }

    if (msg.type === 'sync') {
      for (const fn of downMessageListeners) fn(msg);
      return;
    }

    if (msg.type === 'initial_sync_complete') {
      status = 'connected';
      notifyStatus();
      return;
    }

    if (msg.type === 'heartbeat') {
      upSocket!.send(JSON.stringify({
        type: 'heartbeat_response',
        timestamp: msg.timestamp,
        clientTimestamp: Date.now(),
      }));
      return;
    }

    // Forward anything else (ack, mutation responses, etc.) to listeners
    for (const fn of downMessageListeners) fn(msg);

    } catch { /* ignore malformed */ }
  };

  upSocket.onerror = () => {
    status = 'disconnected';
    notifyStatus();
  };

  upSocket.onclose = () => {
    if (status !== 'disconnected') {
      status = 'disconnected';
      connectionId = null;
      notifyStatus();
    }
  };
}

function openDownSocket(connId: string): void {
  const { protocol, host, downPort, downPath } = config.ws;
  const url = `${protocol}${host}:${downPort}${downPath}`;
  downSocket = new WebSocket(url);

  downSocket.onopen = () => {
    downSocket!.send(JSON.stringify({ connectionId: connId }));
    // status stays 'connecting' — wait for initial_sync_complete on up socket
  };

  downSocket.onmessage = (event: MessageEvent) => {
    try {
      const msg = JSON.parse(event.data) as DownSocketMessage;
      for (const fn of downMessageListeners) fn(msg);
    } catch { /* ignore */ }
  };

  downSocket.onerror = () => {};

  downSocket.onclose = () => {
    if (status === 'connected') {
      status = 'disconnected';
      notifyStatus();
    }
  };
}
