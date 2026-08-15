export interface ActionFeedback {
  id: string;
  tone: 'success' | 'error';
  message: string;
}

export function createFeedback(
  tone: ActionFeedback['tone'],
  message: string,
): ActionFeedback {
  return { id: crypto.randomUUID(), tone, message };
}

export function expireFeedback(
  current: ActionFeedback | null,
  id: string,
): ActionFeedback | null {
  return current?.id === id ? null : current;
}
