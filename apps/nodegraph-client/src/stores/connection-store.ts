import { getStatus, onStatusChange } from '../ws/connection';
import type { ConnectionStatus } from '../types';

let snapshot: ConnectionStatus = getStatus();

const listeners = new Set<() => void>();

onStatusChange((newStatus) => {
  snapshot = newStatus;
  for (const fn of listeners) fn();
});

export function subscribe(fn: () => void): () => void {
  listeners.add(fn);
  return () => { listeners.delete(fn); };
}

export function getSnapshot(): ConnectionStatus {
  return snapshot;
}