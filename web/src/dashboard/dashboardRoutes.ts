export interface DashboardStep {
  number: '01' | '02' | '03';
  path: string;
  shortLabel: string;
  title: string;
  eyebrow: string;
}

export const DASHBOARD_STEPS: readonly DashboardStep[] = [
  {
    number: '01',
    path: '/dashboard/live',
    shortLabel: 'Live events',
    title: 'Live event feed',
    eyebrow: 'Level 1 — What enters the stream',
  },
  {
    number: '02',
    path: '/dashboard/windows',
    shortLabel: 'Window metrics',
    title: 'Window metrics',
    eyebrow: 'Level 2 — What the stream remembers',
  },
  {
    number: '03',
    path: '/dashboard/patterns',
    shortLabel: 'Pattern signals',
    title: 'Pattern signals',
    eyebrow: 'Level 3 — What the stream recognizes',
  },
];

export function dashboardAdjacentPath(pathname: string, direction: -1 | 1): string | null {
  const index = DASHBOARD_STEPS.findIndex((step) => step.path === pathname);
  if (index === -1) return null;
  const adjacent = DASHBOARD_STEPS[index + direction];
  return adjacent?.path ?? null;
}
