import { createContext, useContext, useState, type ReactNode } from 'react';
import {
  clearSession as clearStoredSession,
  readSession,
  writeSession,
  type Role,
} from '../lib/session';

export type { Role } from '../lib/session';

interface SessionState {
  token: string | null;
  name: string | null;
  role: Role | null;
  setSession: (token: string, name: string, role: Role) => void;
  clearSession: () => void;
}

const SessionContext = createContext<SessionState | undefined>(undefined);

export function SessionProvider({ children }: { children: ReactNode }) {
  const [session, setSessionState] = useState(() => readSession());

  const setSession = (newToken: string, newName: string, newRole: Role) => {
    const nextSession = { token: newToken, name: newName, role: newRole };
    setSessionState(nextSession);
    writeSession(nextSession);
  };

  const clearSession = () => {
    setSessionState(null);
    clearStoredSession();
  };

  return (
    <SessionContext.Provider value={{
      token: session?.token ?? null,
      name: session?.name ?? null,
      role: session?.role ?? null,
      setSession,
      clearSession,
    }}>
      {children}
    </SessionContext.Provider>
  );
}

export function useSession(): SessionState {
  const ctx = useContext(SessionContext);
  if (!ctx) {
    throw new Error('useSession must be used within SessionProvider');
  }
  return ctx;
}
