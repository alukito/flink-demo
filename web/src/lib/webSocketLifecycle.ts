export interface ReconnectDecision {
  disposed: boolean;
  isCurrentSocket: boolean;
  hasToken: boolean;
}

export function shouldReconnect({
  disposed,
  isCurrentSocket,
  hasToken,
}: ReconnectDecision): boolean {
  return !disposed && isCurrentSocket && hasToken;
}
