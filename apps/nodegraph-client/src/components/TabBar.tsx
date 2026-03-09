import { useSyncExternalStore, Fragment } from 'react';
import * as metricsStore from '../stores/metrics-store';
import * as entityStore from '../stores/entity-store';

// ── StateTab ──

function StateTab() {
  const metrics = useSyncExternalStore(metricsStore.subscribe, metricsStore.getSnapshot);
  const entities = useSyncExternalStore(entityStore.subscribe, entityStore.getSnapshot);

  const nodeCount = Object.values(entities).filter(e => e.type === 'Node').length;

  const rows: [string, string | number][] = [
    ['Session pause', metrics.pauseState],
    ['Frame in progress', metrics.frameInProgress],
    ['Frames per session', metrics.framesPerSession],
    ['Lookahead', metrics.lookahead],
    ['Entities tracked', nodeCount],
    ['Buffer ops', metrics.buffer.count],
  ];

  return (
    <div className="state-tab">
      {rows.map(([k, v]) => (
        <Fragment key={k}>
          <span className="state-tab__label">{k}</span>
          <span className="state-tab__value">{String(v)}</span>
        </Fragment>
      ))}
    </div>
  );
}

// ── TabBar ──

export function TabBar() {
  return (
    <div className="panel tab-bar">
      <div className="tab-bar__row">
        <button className="tab-bar__tab tab-bar__tab--active">
          State
        </button>
      </div>
      <div className="tab-bar__body">
        <StateTab />
      </div>
    </div>
  );
}
