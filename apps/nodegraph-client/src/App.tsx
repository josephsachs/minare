import { useReducer, createContext, useState } from 'react';
import type { SelectionState, SelectionAction } from './types';
import { ConnectionBar } from './components/ConnectionBar';
import { SessionPanel } from './components/SessionPanel';
import { PlayControls } from './components/PlayControls';
import { NodeGrid } from './components/NodeGrid';
import { TabBar } from './components/TabBar';
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

export type TabId = 'state' | 'operation-history';

export const NavigationContext = createContext<{
  activeTab: TabId;
  historyFocusId: string | null;
  setActiveTab: (tab: TabId) => void;
  goToOperationHistory: (operationId: string | null) => void;
}>({
  activeTab: 'state',
  historyFocusId: null,
  setActiveTab: () => {},
  goToOperationHistory: () => {},
});

// ── App ──

export default function App() {
  const [selectionState, dispatch] = useReducer(selectionReducer, initialSelection);
  const [activeTab, setActiveTab] = useState<TabId>('state');
  const [historyFocusId, setHistoryFocusId] = useState<string | null>(null);

  function goToOperationHistory(operationId: string | null) {
    setHistoryFocusId(operationId);
    setActiveTab('operation-history');
  }

  return (
    <SelectionContext.Provider value={{ state: selectionState, dispatch }}>
      <NavigationContext.Provider value={{ activeTab, historyFocusId, setActiveTab, goToOperationHistory }}>
        <div className="app-layout">
          <ConnectionBar />
          <SessionPanel />
          <PlayControls />
          <NodeGrid />
          <TabBar />
        </div>
      </NavigationContext.Provider>
    </SelectionContext.Provider>
  );
}
