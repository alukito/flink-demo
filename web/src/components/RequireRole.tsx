import { Navigate } from 'react-router-dom';
import type { ReactNode } from 'react';
import { useSession, type Role } from '../context/SessionContext';

interface RequireRoleProps {
  role: Role;
  children: ReactNode;
}

export default function RequireRole({ role, children }: RequireRoleProps) {
  const { id, token, role: sessionRole } = useSession();

  if (!id || !token || sessionRole !== role) {
    return <Navigate to="/" replace />;
  }

  return <>{children}</>;
}
