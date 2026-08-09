export const ROLES = ['buyer', 'seller', 'shipper', 'dashboard'] as const;
export type Role = typeof ROLES[number];

export interface Session {
  id: string;
  token: string;
  name: string | null;
  role: Role;
}

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

function isRole(value: string | null): value is Role {
  return value !== null && (ROLES as readonly string[]).includes(value);
}

export function readSession(storage: StorageLike = sessionStorage): Session | null {
  const id = storage.getItem('id');
  const token = storage.getItem('token');
  const name = storage.getItem('name');
  const role = storage.getItem('role');
  if (!id || !token || !isRole(role)) return null;
  return { id, token, name, role };
}

export function writeSession(session: Session, storage: StorageLike = sessionStorage): void {
  storage.setItem('id', session.id);
  storage.setItem('token', session.token);
  if (session.name === null) {
    storage.removeItem('name');
  } else {
    storage.setItem('name', session.name);
  }
  storage.setItem('role', session.role);
}

export function clearSession(storage: StorageLike = sessionStorage): void {
  storage.removeItem('id');
  storage.removeItem('token');
  storage.removeItem('name');
  storage.removeItem('role');
}

export function hasRequiredRole(requiredRole: Role, storage: StorageLike = sessionStorage): boolean {
  return readSession(storage)?.role === requiredRole;
}
