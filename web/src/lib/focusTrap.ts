export const FOCUSABLE_SELECTOR = 'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

export function nextFocusIndex(current: number, length: number, backwards: boolean): number {
  if (length <= 0) return -1;
  return (current + (backwards ? -1 : 1) + length) % length;
}
