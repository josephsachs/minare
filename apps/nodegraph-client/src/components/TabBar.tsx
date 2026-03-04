import { useSyncExternalStore, useState, Fragment } from 'react';
import * as metricsStore from '../stores/metrics-store';
import * as entityStore from '../stores/entity-store';

type TabId = 'state';

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
    <div style={{
      fontFamily: 'var(--mono)',
      fontSize: 11,
      padding: '10px 0',
      display: 'grid',
      gridTemplateColumns: '1fr 1fr',
      gap: '4px 20px',
    }}>
      {rows.map(([k, v]) => (
        <Fragment key={k}>
          <span style={{ color: 'var(--text-mid)' }}>{k}</span>
          <span style={{ color: 'var(--text-hi)' }}>{String(v)}</span>
        </Fragment>
      ))}
    </div>
  );
}

export function TabBar() {
  const [activeTab, setActiveTab] = useState<TabId>('state');

  const tabStyle = (isActive: boolean): React.CSSProperties => ({
    fontFamily: 'var(--mono)',
    fontSize: 11,
    padding: '7px 16px',
    background: isActive ? 'var(--bg-2)' : 'transparent',
    color: isActive ? 'var(--text-hi)' : 'var(--text-mid)',
    border: 'none',
    borderBottom: isActive ? '2px solid var(--blue)' : '2px solid transparent',
    cursor: 'pointer',
    textTransform: 'uppercase',
    letterSpacing: '0.06em',
    transition: 'all 0.12s',
    borderRadius: 0,
  });

  return (
    <div className="panel" style={{ overflow: 'hidden', padding: 0 }}>
      <div style={{
        display: 'flex',
        borderBottom: '1px solid var(--border-subtle)',
      }}>
        <button
          style={tabStyle(activeTab === 'state')}
          onClick={() => setActiveTab('state')}
        >
          State
        </button>
      </div>
      <div style={{ padding: '0 14px 10px' }}>
        {activeTab === 'state' && <StateTab />}
      </div>
    </div>
  );
}