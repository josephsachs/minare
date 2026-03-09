import { useSyncExternalStore } from 'react';
import { config } from '../config';
import * as metricsStore from '../stores/metrics-store';
import * as connectionStore from '../stores/connection-store';
import type { PauseState } from '../types';

async function togglePause(currentPause: PauseState): Promise<void> {
  const mode = currentPause === 'UNPAUSED' ? 'pause' : 'resume';
  try {
    const resp = await fetch(config.api.timelineUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode }),
    });
    if (!resp.ok) console.warn('Timeline API returned', resp.status);
  } catch (e) {
    console.error('Timeline API error:', e);
  }
}

export function PlayControls() {
  const metrics = useSyncExternalStore(metricsStore.subscribe, metricsStore.getSnapshot);
  const connStatus = useSyncExternalStore(connectionStore.subscribe, connectionStore.getSnapshot);

  const disabled = connStatus !== 'connected';
  const isPaused = metrics.pauseState !== 'UNPAUSED';

  return (
    <div className="panel play-controls">
      <button disabled title="Step back (not yet supported)">◀</button>
      <button
        className={`play-controls__main${isPaused && !disabled ? ' primary' : ''}`}
        disabled={disabled}
        onClick={() => togglePause(metrics.pauseState)}
        title={isPaused ? 'Resume' : 'Pause'}
      >
        {isPaused ? '▶ PLAY' : '❚❚ PAUSE'}
      </button>
      <button disabled title="Step forward (not yet supported)">▶</button>
      <span className="mono play-controls__hint">
        Back/Forward stepping not yet supported
      </span>
    </div>
  );
}