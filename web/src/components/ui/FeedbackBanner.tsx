import type { ReactNode } from 'react';

export interface FeedbackBannerProps {
  tone: 'success' | 'error';
  children: ReactNode;
}

export function FeedbackBanner({ tone, children }: FeedbackBannerProps) {
  return (
    <div className="feedback-banner" data-tone={tone} role={tone === 'error' ? 'alert' : 'status'}>
      {children}
    </div>
  );
}
