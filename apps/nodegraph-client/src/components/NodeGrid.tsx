import { useSyncExternalStore, useContext, memo } from 'react';
import { SelectionContext, NavigationContext } from '../App';
import * as entityStore from '../stores/entity-store';
import type { EntityState } from '../types';

// ── NodeCell ──

interface NodeCellProps {
  entity: EntityState;
  isSelected: boolean;
  onSelect: (id: string) => void;
}

const NodeCell = memo(function NodeCell({ entity, isSelected, onSelect }: NodeCellProps) {
  const color = (entity.state?.color as string) ?? '#CCCCCC';
  const label = (entity.state?.label as string) ?? entity.id.substring(0, 8);
  const version = entity.version ?? 0;

  // Compute text color from background luminance
  const r = parseInt(color.slice(1, 3), 16) || 200;
  const g = parseInt(color.slice(3, 5), 16) || 200;
  const b = parseInt(color.slice(5, 7), 16) || 200;
  const brightness = (r * 299 + g * 587 + b * 114) / 1000;
  const textColor = brightness > 128 ? '#1a1a1e' : '#e8e8ec';

  return (
    <div
      className={`node-cell${isSelected ? ' node-cell--selected' : ''}`}
      onClick={() => onSelect(entity.id)}
      title={`${entity.id}\nv${version}\n${color}`}
      style={{ background: color, color: textColor }}
    >
      <div className="node-cell__label">{label}</div>
      <div className="node-cell__version">v{version}</div>
      <div className="node-cell__id">{entity.id.substring(0, 8)}</div>
    </div>
  );
}, (prev, next) =>
  prev.entity.version === next.entity.version
  && prev.isSelected === next.isSelected
);

// ── NodeHoverPanel ──

function NodeHoverPanel({ entity }: { entity: EntityState }) {
  const { goToOperationHistory } = useContext(NavigationContext);
  const lastOp = entity.state?.lastOperation as Record<string, unknown> | undefined;

  const fields: [string, string | number][] = [
    ['ID', entity.id],
    ['Type', entity.type],
    ['Version', entity.version],
    ['Color', (entity.state?.color as string) ?? 'N/A'],
    ['Label', (entity.state?.label as string) ?? 'N/A'],
  ];

  return (
    <div className="hover-panel">
      <div className="hover-panel__title">
        {(entity.state?.label as string) ?? entity.id.substring(0, 12)}
      </div>
      {fields.map(([k, v]) => (
        <div className="stat-row" key={k}>
          <span className="stat-label">{k}</span>
          <span className="stat-value hover-panel__value-truncate">{String(v)}</span>
        </div>
      ))}
      {lastOp && (
        <div className="hover-panel__divider">
          <div className="section-label hover-panel__op-label">Last Operation</div>
          <div className="stat-row">
            <span className="stat-label">Action</span>
            <span className="hover-panel__action">{String(lastOp.action ?? '—')}</span>
          </div>
          <div className="stat-row">
            <span className="stat-label">ID</span>
            <span className="stat-value hover-panel__value-truncate">{String(lastOp.id ?? '—')}</span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Time</span>
            <span className="stat-value">
              {lastOp.timestamp
                ? new Date(lastOp.timestamp as number).toISOString().slice(11, 23)
                : '—'}
            </span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Version</span>
            <span className="stat-value">{String(lastOp.version ?? '—')}</span>
          </div>
        </div>
      )}
      <div className="hover-panel__history-link">
        <button
          className="hover-panel__history-btn"
          onClick={() => goToOperationHistory((lastOp?.id as string) ?? null)}
        >
          · See Operation History
        </button>
      </div>
    </div>
  );
}

// ── NodeGrid ──

export function NodeGrid() {
  const entities = useSyncExternalStore(entityStore.subscribe, entityStore.getSnapshot);
  const { state, dispatch } = useContext(SelectionContext);

  const nodes = Object.values(entities)
    .filter(e => e.type === 'Node')
    .sort((a, b) =>
      ((a.state?.label as string) ?? '').localeCompare((b.state?.label as string) ?? '')
    );

  const selectedEntity = state.selectedNodeId ? entities[state.selectedNodeId] : null;

  function handleSelect(id: string) {
    if (state.selectedNodeId === id) {
      dispatch({ type: 'CLEAR_SELECTION' });
    } else {
      dispatch({ type: 'SELECT_NODE', nodeId: id });
    }
  }

  return (
    <div className="panel node-grid">
      <div className="section-label">Nodes ({nodes.length})</div>
      {nodes.length === 0 ? (
        <div className="mono node-grid__empty">Connect to see nodes</div>
      ) : (
        <div className="node-grid__wrap">
          {nodes.map(entity => (
            <NodeCell
              key={entity.id}
              entity={entity}
              isSelected={state.selectedNodeId === entity.id}
              onSelect={handleSelect}
            />
          ))}
        </div>
      )}
      {selectedEntity && <NodeHoverPanel entity={selectedEntity} />}
    </div>
  );
}
