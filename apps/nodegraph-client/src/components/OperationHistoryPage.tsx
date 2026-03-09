import { useSyncExternalStore, useContext, useState, useEffect } from 'react';
import { NavigationContext } from '../App';
import * as entityStore from '../stores/entity-store';
import * as opLogStore from '../stores/operation-log';
import type { ManifestOperation, OperationRecord } from '../types';

// ── Detail entries ──

interface EntityOpEntry extends OperationRecord {
  _entityId: string;
  _entityType: string;
}

function FrameOpDetail({ op }: { op: ManifestOperation | null }) {
  if (!op) return null;
  const fields: [string, string | number][] = [
    ['ID', op.id],
    ['Entity', op.entityId],
    ['Type', op.entityType],
    ['Action', op.action],
    ['Frame', op.frame],
    ['Time', new Date(op.timestamp).toISOString().slice(11, 23)],
  ];
  return (
    <div className="op-history-detail__entry fade-in">
      <div className="op-history-detail__source">Frame Loop</div>
      {fields.map(([k, v]) => (
        <div className="stat-row" key={k}>
          <span className="stat-label">{k}</span>
          <span className="stat-value-truncate">{v}</span>
        </div>
      ))}
    </div>
  );
}

function EntityOpDetail({ op }: { op: EntityOpEntry | null }) {
  if (!op) return null;
  const fields: [string, string | number][] = [
    ['ID', op.id ?? '—'],
    ['Entity', op._entityId],
    ['Type', op._entityType],
    ['Action', op.action ?? '—'],
    ['Time', op.timestamp ? new Date(op.timestamp).toISOString().slice(11, 23) : '—'],
    ['Version', op.version ?? '—'],
  ];
  return (
    <div className="op-history-detail__entry fade-in">
      <div className="op-history-detail__source">Entity Update</div>
      {fields.map(([k, v]) => (
        <div className="stat-row" key={k}>
          <span className="stat-label">{k}</span>
          <span className="stat-value-truncate">{v}</span>
        </div>
      ))}
    </div>
  );
}

// ── OperationHistoryPage ──

export function OperationHistoryPage() {
  const { historyFocusId, setActivePage } = useContext(NavigationContext);
  const opLog = useSyncExternalStore(opLogStore.subscribe, opLogStore.getSnapshot);
  const entities = useSyncExternalStore(entityStore.subscribe, entityStore.getSnapshot);

  const entityOps: EntityOpEntry[] = Object.values(entities).flatMap((e) =>
    (e.operationHistory ?? []).map((op) => ({
      ...op,
      _entityId: e.id,
      _entityType: e.type,
    }))
  ).sort((a, b) => (a.timestamp ?? 0) - (b.timestamp ?? 0));

  const [frameSelId, setFrameSelId] = useState<string | null>(null);
  const [entitySelId, setEntitySelId] = useState<string | null>(null);

  useEffect(() => {
    if (!historyFocusId) return;
    if (opLog.find((o) => o.id === historyFocusId)) setFrameSelId(historyFocusId);
    if (entityOps.find((o) => o.id === historyFocusId)) setEntitySelId(historyFocusId);
  }, [historyFocusId]); // eslint-disable-line react-hooks/exhaustive-deps

  const selectedFrameOp = frameSelId ? opLog.find((o) => o.id === frameSelId) ?? null : null;
  const selectedEntityOp = entitySelId ? entityOps.find((o) => o.id === entitySelId) ?? null : null;
  const hasDetail = selectedFrameOp || selectedEntityOp;

  return (
    <div className="panel op-history-page">
      {/* Page header */}
      <div className="op-history-page__header">
        <button className="op-history-page__back" onClick={() => setActivePage('nodegraph')}>
          ← NodeGraph v2
        </button>
        <span className="mono op-history-page__title">Operation History</span>
      </div>

      {/* 3-column body */}
      <div className="op-history-tab">
        {/* Column 1: Frame loop operations */}
        <div className="op-history-col">
          <div className="section-label">Frame Operations ({opLog.length})</div>
          <div className="op-history-col__scroll">
            {opLog.length === 0 ? (
              <div className="mono op-list__empty">No frame operations yet</div>
            ) : (
              opLog.map((op, i) => {
                const sel = frameSelId === op.id;
                return (
                  <div
                    key={`${op.id}-${i}`}
                    className={`mono op-item${sel ? ' op-item--selected' : ''}`}
                    onClick={() => setFrameSelId(sel ? null : op.id)}
                  >
                    <span className="op-item__frame">F{op.frame}</span>
                    <span className="op-item__action">{op.action}</span>
                    <span className="op-item__entity">{op.entityId}</span>
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* Column 2: Entity update operations */}
        <div className="op-history-col">
          <div className="section-label">Update Operations ({entityOps.length})</div>
          <div className="op-history-col__scroll">
            {entityOps.length === 0 ? (
              <div className="mono op-list__empty">No entity updates yet</div>
            ) : (
              entityOps.map((op, i) => {
                const sel = entitySelId === op.id;
                return (
                  <div
                    key={`${op.id ?? 'noid'}-${i}`}
                    className={`mono op-item${sel ? ' op-item--selected' : ''}`}
                    onClick={() => setEntitySelId(sel ? null : (op.id ?? null))}
                  >
                    <span className="op-item__frame op-item__frame--ts">
                      {op.timestamp ? new Date(op.timestamp).toISOString().slice(11, 19) : '—'}
                    </span>
                    <span className="op-item__action">{op.action ?? '—'}</span>
                    <span className="op-item__entity">{op._entityId}</span>
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* Column 3: Detail panel */}
        <div className="op-history-detail">
          <div className="section-label">Detail</div>
          {!hasDetail ? (
            <div className="mono op-detail__empty">Select an operation</div>
          ) : (
            <>
              <FrameOpDetail op={selectedFrameOp} />
              {selectedFrameOp && selectedEntityOp && (
                <div className="op-history-detail__divider" />
              )}
              <EntityOpDetail op={selectedEntityOp} />
            </>
          )}
        </div>
      </div>
    </div>
  );
}
