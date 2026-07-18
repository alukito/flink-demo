import { useEffect, useRef, useCallback, useState } from 'react';
import { useSession } from '../context/SessionContext';
import type { EventEnvelope } from '../context/EventContext';

/**
 * useWebSocket connects to the server's WebSocket endpoint and calls
 * onEvent for every incoming event. Auto-reconnects with 1s backoff.
 *
 * If `overrideToken` is provided, it is used instead of the session
 * token. This lets the Dashboard connect with its own auto-created
 * dashboard token without polluting the shared SessionContext.
 */
export function useWebSocket(onEvent: (event: EventEnvelope) => void, overrideToken?: string | null) {
  const { token: sessionToken } = useSession();
  const token = overrideToken ?? sessionToken;
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const [connected, setConnected] = useState(false);
  const onEventRef = useRef(onEvent);
  onEventRef.current = onEvent;

  const connect = useCallback(() => {
    if (!token) return;

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws?token=${token}`;

    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      console.log('[ws] connected');
    };

    ws.onmessage = (e) => {
      try {
        const event: EventEnvelope = JSON.parse(e.data);
        onEventRef.current(event);
      } catch (err) {
        console.error('[ws] failed to parse message', err);
      }
    };

    ws.onclose = () => {
      setConnected(false);
      console.log('[ws] disconnected, reconnecting in 1s...');
      reconnectTimerRef.current = window.setTimeout(connect, 1000);
    };

    ws.onerror = (err) => {
      console.error('[ws] error', err);
      ws.close();
    };
  }, [token]);

  useEffect(() => {
    connect();
    return () => {
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
      }
      wsRef.current?.close();
    };
  }, [connect]);

  return { connected };
}
