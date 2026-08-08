import { useEffect, useRef, useState } from 'react';
import { useSession } from '../context/SessionContext';
import type { EventEnvelope } from '../context/EventContext';
import { shouldReconnect } from '../lib/webSocketLifecycle';

/**
 * useWebSocket connects to the server's WebSocket endpoint and calls
 * onEvent for every incoming event. Auto-reconnects with 1s backoff.
 *
 * If `overrideToken` is provided, it is used instead of the session
 * token. This lets the Dashboard connect with its own auto-created
 * dashboard token without polluting the shared SessionContext.
 */
export function useWebSocket<T = EventEnvelope>(onEvent: (event: T) => void, overrideToken?: string | null) {
  const { token: sessionToken } = useSession();
  const token = overrideToken ?? sessionToken;
  const [connected, setConnected] = useState(false);
  const onEventRef = useRef(onEvent);
  onEventRef.current = onEvent;

  useEffect(() => {
    let disposed = false;
    let socket: WebSocket | null = null;
    let reconnectTimer: number | null = null;

    const connect = () => {
      if (disposed || !token) return;

      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = `${protocol}//${window.location.host}/ws?token=${token}`;
      const ws = new WebSocket(wsUrl);
      socket = ws;

      ws.onopen = () => {
        if (disposed || socket !== ws) return;
        setConnected(true);
        console.log('[ws] connected');
      };

      ws.onmessage = (e) => {
        if (disposed || socket !== ws) return;
        try {
          const event: T = JSON.parse(e.data) as T;
          onEventRef.current(event);
        } catch (err) {
          console.error('[ws] failed to parse message', err);
        }
      };

      ws.onclose = () => {
        if (!shouldReconnect({
          disposed,
          isCurrentSocket: socket === ws,
          hasToken: Boolean(token),
        })) return;

        socket = null;
        setConnected(false);
        console.log('[ws] disconnected, reconnecting in 1s...');
        reconnectTimer = window.setTimeout(() => {
          reconnectTimer = null;
          connect();
        }, 1000);
      };

      ws.onerror = (err) => {
        console.error('[ws] error', err);
        if (!disposed && socket === ws) ws.close();
      };
    };

    setConnected(false);
    connect();

    return () => {
      disposed = true;
      if (reconnectTimer !== null) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      socket?.close();
      socket = null;
    };
  }, [token]);

  return { connected };
}
