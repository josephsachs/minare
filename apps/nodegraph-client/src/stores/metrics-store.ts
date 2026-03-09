import { onDownMessage } from '../ws/connection';
import type { MetricsSnapshot, MetricsFrameMessage, MetricsPauseMessage } from '../types';

let snapshot: MetricsSnapshot = {
  frameInProgress: 0,
  framesPerSession: 0,
  lookahead: 0,
  pauseState: 'SOFT',
  ops: { current: 0, average: 0, max: 0 },
  buffer: { count: 0, framesBuffered: 0, highestFrame: -1 },
};

const listeners = new Set<() => void>();

function emit(): void {
  for (const fn of listeners) fn();
}

// Wire to down socket
onDownMessage((msg) => {
  if (msg.type === 'metrics_frame') {
    const m = msg as MetricsFrameMessage;
    snapshot = {
      frameInProgress: m.frameInProgress ?? snapshot.frameInProgress,
      framesPerSession: m.framesPerSession ?? snapshot.framesPerSession,
      lookahead: m.lookahead ?? snapshot.lookahead,
      pauseState: m.pauseState ?? snapshot.pauseState,
      ops: {
        current: m.opsCurrentFrame ?? 0,
        average: m.opsAverage ?? 0,
        max: m.opsMax ?? 0,
      },
      buffer: {
        count: m.bufferCount ?? 0,
        framesBuffered: m.framesBuffered ?? 0,
        highestFrame: m.highestFrameBuffered ?? -1,
      },
    };
    emit();
  }

  if (msg.type === 'metrics_pause') {
    const m = msg as MetricsPauseMessage;
    snapshot = {
      ...snapshot,
      pauseState: m.pauseState ?? snapshot.pauseState,
      frameInProgress: m.frameInProgress ?? snapshot.frameInProgress,
    };
    emit();
  }
});

export function subscribe(fn: () => void): () => void {
  listeners.add(fn);
  return () => { listeners.delete(fn); };
}

export function getSnapshot(): MetricsSnapshot {
  return snapshot;
}