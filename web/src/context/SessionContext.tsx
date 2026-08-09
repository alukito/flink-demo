import { createContext, useContext, useState, type ReactNode } from 'react';
import {
  clearSession as clearStoredSession,
  readSession,
  writeSession,
  type Role,
} from '../lib/session';

export type { Role } from '../lib/session';

interface SessionState {
  id: string | null;
  token: string | null;
  name: string | null;
  role: Role | null;
  setSession: (id: string, token: string, name: string, role: Role) => void;
  clearSession: () => void;
}

const SessionContext = createContext<SessionState | undefined>(undefined);

export function SessionProvider({ children }: { children: ReactNode }) {
  const [session, setSessionState] = useState(() => readSession());

  const setSession = (newID: string, newToken: string, newName: string, newRole: Role) => {
    const nextSession = { id: newID, token: newToken, name: newName, role: newRole };
    setSessionState(nextSession);
    writeSession(nextSession);
  };

  const clearSession = () => {
    setSessionState(null);
    clearStoredSession();
  };

  return (
    <SessionContext.Provider value={{
      id: session?.id ?? null,
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
