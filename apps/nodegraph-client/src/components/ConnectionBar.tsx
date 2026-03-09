import { useSyncExternalStore, useContext } from 'react';
import { connect, disconnect } from '../ws/connection';
import * as connectionStore from '../stores/connection-store';
import { NavigationContext } from '../App';
import type { PageId } from '../App';

const PAGE_LABELS: Record<PageId, string> = {
  'nodegraph': 'Frameloop',
  'operation-history': 'Operations',
};

const PAGES: PageId[] = ['nodegraph', 'operation-history'];

export function ConnectionBar() {
  const status = useSyncExternalStore(connectionStore.subscribe, connectionStore.getSnapshot);
  const { activePage, setActivePage } = useContext(NavigationContext);
  const isConnected = status === 'connected';
  const isConnecting = status === 'connecting';

  const dotClass = `conn-bar__dot conn-bar__dot--${status}`;

  return (
    <div className="panel conn-bar">
      <div className="conn-bar__left">
        <div
          className="conn-bar__brand"
          onClick={() => setActivePage('nodegraph')}
          style={{ cursor: activePage !== 'nodegraph' ? 'pointer' : 'default' }}
        >
          <span className="conn-bar__logo-container"><img className="mono conn-bar__logo" src="src/logo.svg" /></span>
          <span className="mono conn-bar__title-1">MINARE</span> <span className="mono conn-bar__title-2">NodeGraph</span>
          <span className="mono conn-bar__version">v2</span>
        </div>
        <div className="conn-bar__sep" />
        <nav className="conn-bar__nav">
          {PAGES.map((page) => (
            <button
              key={page}
              className={`conn-bar__nav-tab${activePage === page ? ' conn-bar__nav-tab--active' : ''}`}
              onClick={() => setActivePage(page)}
            >
              {PAGE_LABELS[page]}
            </button>
          ))}
        </nav>
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
