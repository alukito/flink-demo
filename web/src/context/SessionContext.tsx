import { createContext, useContext, useState, type ReactNode } from 'react';

export type Role = 'buyer' | 'seller' | 'shipper' | 'dashboard';

interface SessionState {
  token: string | null;
  name: string | null;
  role: Role | null;
  setSession: (token: string, name: string, role: Role) => void;
  clearSession: () => void;
}

const SessionContext = createContext<SessionState | undefined>(undefined);

export function SessionProvider({ children }: { children: ReactNode }) {
  const [token, setToken] = useState<string | null>(() => {
    return localStorage.getItem('token');
  });
  const [name, setName] = useState<string | null>(() => {
    return localStorage.getItem('name');
  });
  const [role, setRole] = useState<Role | null>(() => {
    return localStorage.getItem('role') as Role | null;
  });

  const setSession = (newToken: string, newName: string, newRole: Role) => {
    setToken(newToken);
    setName(newName);
    setRole(newRole);
    localStorage.setItem('token', newToken);
    localStorage.setItem('name', newName);
    localStorage.setItem('role', newRole);
  };

  const clearSession = () => {
    setToken(null);
    setName(null);
    setRole(null);
    localStorage.removeItem('token');
    localStorage.removeItem('name');
    localStorage.removeItem('role');
  };

  return (
    <SessionContext.Provider value={{ token, name, role, setSession, clearSession }}>
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
