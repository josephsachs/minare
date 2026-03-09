import { useReducer, createContext, useState } from 'react';
import type { SelectionState, SelectionAction } from './types';
import { ConnectionBar } from './components/ConnectionBar';
import { SessionPanel } from './components/SessionPanel';
import { PlayControls } from './components/PlayControls';
import { NodeGrid } from './components/NodeGrid';
import { TabBar } from './components/TabBar';
import { OperationHistoryPage } from './components/OperationHistoryPage';
import './App.css';

// ── Selection reducer ──

const initialSelection: SelectionState = {
  selectedOperationId: null,
  selectedNodeId: null,
};

function selectionReducer(state: SelectionState, action: SelectionAction): SelectionState {
  switch (action.type) {
    case 'SELECT_OPERATION':
      return {
        selectedOperationId: action.operationId,
        selectedNodeId: action.entityId,
      };
    case 'SELECT_NODE':
      return {
        selectedOperationId: null,
        selectedNodeId: action.nodeId,
      };
    case 'CLEAR_SELECTION':
      return initialSelection;
    default:
      return state;
  }
}

// ── Context ──

export const SelectionContext = createContext<{
  state: SelectionState;
  dispatch: React.Dispatch<SelectionAction>;
}>({
  state: initialSelection,
  dispatch: () => {},
});

export type PageId = 'nodegraph' | 'operation-history';

export const NavigationContext = createContext<{
  activePage: PageId;
  historyFocusId: string | null;
  setActivePage: (page: PageId) => void;
  goToOperationHistory: (operationId: string | null) => void;
}>({
  activePage: 'nodegraph',
  historyFocusId: null,
  setActivePage: () => {},
  goToOperationHistory: () => {},
});

// ── App ──

export default function App() {
  const [selectionState, dispatch] = useReducer(selectionReducer, initialSelection);
  const [activePage, setActivePage] = useState<PageId>('nodegraph');
  const [historyFocusId, setHistoryFocusId] = useState<string | null>(null);

  function goToOperationHistory(operationId: string | null) {
    setHistoryFocusId(operationId);
    setActivePage('operation-history');
  }

  return (
    <SelectionContext.Provider value={{ state: selectionState, dispatch }}>
      <NavigationContext.Provider value={{ activePage, historyFocusId, setActivePage, goToOperationHistory }}>
        <div className="app-layout">
          <ConnectionBar />
          {activePage === 'nodegraph' && (
            <>
              <SessionPanel />
              <PlayControls />
              <NodeGrid />
              <TabBar />
            </>
          )}
          {activePage === 'operation-history' && (
            <OperationHistoryPage />
          )}
        </div>
      </NavigationContext.Provider>
    </SelectionContext.Provider>
  );
}
