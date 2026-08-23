export const FOCUSABLE_SELECTOR = 'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

export function isolateModalBackground(foregroundElement: HTMLElement): () => void {
  const changedElements: HTMLElement[] = [];
  let foreground: HTMLElement | null = foregroundElement;

  while (foreground?.parentElement) {
    const parent: HTMLElement = foreground.parentElement;
    Array.from(parent.children).forEach((sibling) => {
      if (sibling === foreground || !(sibling instanceof HTMLElement) || sibling.hasAttribute('inert')) return;
      sibling.setAttribute('inert', '');
      changedElements.push(sibling);
    });
    foreground = parent;
    if (foreground === document.body) break;
  }

  return () => {
    changedElements.forEach((element) => {
      element.removeAttribute('inert');
    });
  };
}

export function nextFocusIndex(current: number, length: number, backwards: boolean): number {
  if (length <= 0) return -1;
  return (current + (backwards ? -1 : 1) + length) % length;
}
