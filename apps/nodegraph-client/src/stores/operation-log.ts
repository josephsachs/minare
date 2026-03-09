import { onDownMessage } from '../ws/connection';
import type { ManifestOperation, MetricsFrameMessage } from '../types';

let log: ManifestOperation[] = [];
let snapshot: ManifestOperation[] = log;

const listeners = new Set<() => void>();

function emit(): void {
  snapshot = [...log];
  for (const fn of listeners) fn();
}

onDownMessage((msg) => {
  if (msg.type !== 'metrics_frame') return;
  const m = msg as MetricsFrameMessage;
  const ops = m.operations;
  if (!Array.isArray(ops) || ops.length === 0) return;

  for (const op of ops) {
    log.push({
      id: (op.id as string) ?? `unknown-${log.length}`,
      entityId: (op.entity as string) ?? (op.entityId as string) ?? '',
      entityType: (op.entityType as string) ?? 'Node',
      action: (op.action as string) ?? 'MUTATE',
      timestamp: (op.timestamp as number) ?? Date.now(),
      frame: (op.frame as number) ?? m.frameInProgress ?? 0,
    });
  }

  emit();
});

export function subscribe(fn: () => void): () => void {
  listeners.add(fn);
  return () => { listeners.delete(fn); };
}

export function getSnapshot(): ManifestOperation[] {
  return snapshot;
}

export function reset(): void {
  log = [];
  snapshot = log;
  emit();
}