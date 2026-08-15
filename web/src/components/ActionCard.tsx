import type { ReactNode } from 'react';

export interface ActionCardProps {
  title: string;
  description?: string;
  children: ReactNode;
  className?: string;
}

export function ActionCard({ title, description, children, className = '' }: ActionCardProps) {
  const classes = ['action-card', className].filter(Boolean).join(' ');

  return (
    <section className={classes}>
      <header className="action-card__header">
        <h2>{title}</h2>
        {description && <p>{description}</p>}
      </header>
      {children}
    </section>
  );
}
