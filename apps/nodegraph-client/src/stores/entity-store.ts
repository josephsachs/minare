import { onDownMessage } from '../ws/connection';
import type { EntityMap, EntityUpdateMessage, SyncMessage } from '../types';

let entities: EntityMap = {};
let snapshot: EntityMap = entities;

const listeners = new Set<() => void>();

function emit(): void {
  snapshot = { ...entities };
  for (const fn of listeners) fn();
}

onDownMessage((msg) => {
  // Standard entity updates
  if (msg.type === 'update' && 'updates' in msg) {
    const m = msg as EntityUpdateMessage;
    let changed = false;

    for (const [entityId, update] of Object.entries(m.updates)) {
      const existing = entities[entityId];
      const newVersion = update.version ?? 0;

      if (!existing || newVersion > existing.version) {
        if (update.delta && existing) {
          // Delta merge
          entities[entityId] = {
            ...existing,
            version: newVersion,
            state: { ...existing.state, ...update.delta },
          };
        } else {
          // Full update
          entities[entityId] = {
            id: update._id ?? entityId,
            type: update.type ?? existing?.type ?? 'Unknown',
            version: newVersion,
            state: update.state ?? existing?.state ?? {},
          };
        }
        changed = true;
      }
    }

    if (changed) emit();
    return;
  }

  // Initial sync (entity array)
  if (msg.type === 'sync' && 'data' in msg) {
    const m = msg as SyncMessage;
    let changed = false;

    for (const entity of m.data.entities) {
      const id = entity._id ?? entity.id;
      if (!id) continue;
      entities[id] = {
        id,
        type: entity.type ?? 'Unknown',
        version: entity.version ?? 1,
        state: entity.state ?? {},
      };
      changed = true;
    }

    if (changed) emit();
  }
});

export function subscribe(fn: () => void): () => void {
  listeners.add(fn);
  return () => { listeners.delete(fn); };
}

export function getSnapshot(): EntityMap {
  return snapshot;
}

export function reset(): void {
  entities = {};
  snapshot = entities;
  emit();
}