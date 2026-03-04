import { useReducer, createContext } from 'react';
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

// ── App ──

export default function App() {
  const [selectionState, dispatch] = useReducer(selectionReducer, initialSelection);

  return (
    <SelectionContext.Provider value={{ state: selectionState, dispatch }}>
      <div className="app-layout">
        <ConnectionBar />
        <SessionPanel />
        <PlayControls />
        <NodeGrid />
        <TabBar />
      </div>
    </SelectionContext.Provider>
  );
}