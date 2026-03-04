import { useSyncExternalStore } from 'react';
import { connect, disconnect } from '../ws/connection';
import * as connectionStore from '../stores/connection-store';

export function ConnectionBar() {
  const status = useSyncExternalStore(connectionStore.subscribe, connectionStore.getSnapshot);
  const isConnected = status === 'connected';
  const isConnecting = status === 'connecting';

  const dotClass = `conn-bar__dot conn-bar__dot--${status}`;

  return (
    <div className="panel conn-bar">
      <div className="conn-bar__brand">
        <span className="mono conn-bar__title">NODEGRAPH</span>
        <span className="mono conn-bar__version">v2</span>
      </div>
      <div className="conn-bar__controls">
        <span className={dotClass} />
        <span className="mono conn-bar__status">{status}</span>
        <button
          className={isConnected ? undefined : 'primary'}
          disabled={isConnecting}
          onClick={() => isConnected ? disconnect() : connect()}
        >
          {isConnected ? 'DISCONNECT' : 'CONNECT'}
        </button>
      </div>
    </div>
  );
}