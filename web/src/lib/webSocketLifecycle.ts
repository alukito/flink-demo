export interface ReconnectDecision {
  disposed: boolean;
  isCurrentSocket: boolean;
  hasToken: boolean;
}

export function resolveWebSocketToken(
  overrideToken: string | null | undefined,
  sessionToken: string | null,
): string | null {
  return overrideToken === undefined ? sessionToken : overrideToken;
}

export function shouldReconnect({
  disposed,
  isCurrentSocket,
  hasToken,
}: ReconnectDecision): boolean {
  return !disposed && isCurrentSocket && hasToken;
}
