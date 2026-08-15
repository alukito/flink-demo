import type { ReactNode } from 'react';
import { SignalTrace } from './ui/SignalTrace';

export interface RoleLayoutProps {
  roleLabel: 'Shop' | 'Sell' | 'Deliver';
  participantName: string;
  pulseKey: string;
  onLogout: () => void;
  children: ReactNode;
}

export function RoleLayout({
  roleLabel,
  participantName,
  pulseKey,
  onLogout,
  children,
}: RoleLayoutProps) {
  return (
    <div className="role-layout">
      <header className="role-header">
        <div className="role-header__title">
          <SignalTrace pulseKey={pulseKey} />
          <h1>{roleLabel}</h1>
        </div>

        <details className="identity-menu">
          <summary aria-label={`Open account menu for ${participantName}`}>
            <span>{participantName}</span>
            <span aria-hidden="true">•••</span>
          </summary>
          <div className="identity-menu__panel">
            <span className="identity-menu__label">Participant</span>
            <strong>{participantName}</strong>
            <button className="button button--ghost" type="button" onClick={onLogout}>
              Log out
            </button>
          </div>
        </details>
      </header>

      <main className="role-layout__main">{children}</main>
    </div>
  );
}
