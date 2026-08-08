import { Navigate } from 'react-router-dom';
import type { ReactNode } from 'react';
import { useSession, type Role } from '../context/SessionContext';

interface RequireRoleProps {
  role: Role;
  children: ReactNode;
}

export default function RequireRole({ role, children }: RequireRoleProps) {
  const { token, name, role: sessionRole } = useSession();

  if (!token || !name || sessionRole !== role) {
    return <Navigate to="/" replace />;
  }

  return <>{children}</>;
}
