import type { ReactNode } from 'react';

export type StatusTone = 'neutral' | 'warning' | 'info' | 'success' | 'error';

export interface StatusBadgeProps {
  tone: StatusTone;
  children: ReactNode;
}

export function StatusBadge({ tone, children }: StatusBadgeProps) {
  return <span className="status-badge" data-tone={tone}>{children}</span>;
}
