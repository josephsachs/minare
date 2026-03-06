import { useSyncExternalStore, useContext, useRef, useEffect } from 'react';
import { SelectionContext } from '../App';
import * as metricsStore from '../stores/metrics-store';
import * as opLogStore from '../stores/operation-log';
import type { PauseState } from '../types';

// ── Pause state maps ──

const PAUSE_COLOR: Record<PauseState, string> = {
  UNPAUSED: 'var(--blue)',
  REST: 'var(--yellow)',
  SOFT: 'var(--orange)',
  HARD: 'var(--red)',
};

const PAUSE_LABEL: Record<PauseState, string> = {
  UNPAUSED: 'RUNNING',
  REST: 'REST',
  SOFT: 'SOFT PAUSE',
  HARD: 'HARD PAUSE',
};

// ── ProgressBar ──

function ProgressBar() {
  const m = useSyncExternalStore(metricsStore.subscribe, metricsStore.getSnapshot);
  const pct = m.framesPerSession > 0
    ? Math.min((m.frameInProgress / m.framesPerSession) * 100, 100)
    : 0;
  const color = PAUSE_COLOR[m.pauseState] ?? 'var(--blue)';

  return (
    <div className="progress-bar">
      <div className="progress-bar__fill" style={{ width: `${pct}%`, background: color }} />
      <div className="progress-bar__tick" style={{ left: `${pct}%`, background: color }} />
      <div className="mono progress-bar__text">
        <span>
          <span className="progress-bar__label" style={{ color }}>{PAUSE_LABEL[m.pauseState]}</span>
          <span className="progress-bar__sep">·</span>
          Frame {m.frameInProgress}
        </span>
        <span>{Math.round(pct)}% of {m.framesPerSession}</span>
      </div>
    </div>
  );
}

// ── FrameStats ──

function FrameStats() {
  const m = useSyncExternalStore(metricsStore.subscribe, metricsStore.getSnapshot);

  const rows: [string, string | number][] = [
    ['Ops (frame)', m.ops.current],
    ['Ops (avg)', m.ops.average.toFixed(1)],
    ['Ops (max)', m.ops.max],
    ['Buffer ops', m.buffer.count],
    ['Frames buffered', m.buffer.framesBuffered],
    ['Highest buffered', m.buffer.highestFrame],
  ];

  return (
    <div className="col">
      <div className="section-label">Frame Stats</div>
      {rows.map(([label, value]) => (
        <div className="stat-row" key={label}>
          <span className="stat-label">{label}</span>
          <span className="stat-value">{value}</span>
        </div>
      ))}
    </div>
  );
}

// ── OperationList ──

function OperationList() {
  const opLog = useSyncExternalStore(opLogStore.subscribe, opLogStore.getSnapshot);
  const { state, dispatch } = useContext(SelectionContext);
  const listRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = listRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [opLog.length]);

  return (
    <div className="op-list">
      <div className="section-label">Operations ({opLog.length})</div>
      <div ref={listRef} className="op-list__scroll">
        {opLog.length === 0 ? (
          <div className="mono op-list__empty">No operations yet</div>
        ) : (
          opLog.map((op, i) => {
            const selected = state.selectedOperationId === op.id;
            return (
              <div
                key={`${op.id}-${i}`}
                className={`mono op-item${selected ? ' op-item--selected' : ''}`}
                onClick={() => dispatch({
                  type: 'SELECT_OPERATION',
                  operationId: op.id,
                  entityId: op.entityId,
                })}
              >
                <span className="op-item__frame">F{op.frame}</span>
                <span className="op-item__action">{op.action}</span>
                <span className="op-item__entity">{op.entityId.substring(0, 20)}</span>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}

// ── OperationDetail ──

function OperationDetail() {
  const opLog = useSyncExternalStore(opLogStore.subscribe, opLogStore.getSnapshot);
  const { state } = useContext(SelectionContext);

  const op = state.selectedOperationId
    ? opLog.find((o) => o.id === state.selectedOperationId)
    : null;

  if (!op) {
    return (
      <div>
        <div className="section-label">Detail</div>
        <div className="mono op-detail__empty">Select an operation</div>
      </div>
    );
  }

  const fields: [string, string | number][] = [
    ['ID', op.id],
    ['Entity', op.entityId],
    ['Type', op.entityType],
    ['Action', op.action],
    ['Frame', op.frame],
    ['Time', new Date(op.timestamp).toISOString().slice(11, 23)],
  ];

  return (
    <div className="fade-in">
      <div className="section-label">Detail</div>
      {fields.map(([label, value]) => (
        <div className="stat-row" key={label}>
          <span className="stat-label">{label}</span>
          <span className="stat-value-truncate">{value}</span>
        </div>
      ))}
    </div>
  );
}

// ── SessionPanel ──

export function SessionPanel() {
  return (
    <div className="panel session-panel">
      <ProgressBar />
      <div className="stats-grid">
        <div className="stats-grid__col">
          <FrameStats />
        </div>
        <div className="stats-grid__col--ops">
          <OperationList />
        </div>
        <div className="stats-grid__col">
          <OperationDetail />
        </div>
      </div>
    </div>
  );
}