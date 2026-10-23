import { apiBaseUrl } from '@/services/api';

export type BotEvent =
  | { type: 'connection_ready' | 'health' | 'bot_state_changed' | 'bot_paused' | 'token_detected' | 'token_approved' | 'token_rejected' | 'entry_submitted' | 'entry_rejected' | 'position_updated' | 'position_mark_updated' | 'buy_sent' | 'buy_confirmed' | 'buy_failed' | 'sell_sent' | 'sell_confirmed' | 'sell_failed' | 'exit_triggered' | 'exit_retry_scheduled' | 'rpc_changed' | 'risk_alert' | 'risk.alert' | 'operational_error'; data?: unknown; timestamp?: string; at?: string; [key: string]: unknown }
  | { type: string; data?: unknown; timestamp?: string; at?: string; [key: string]: unknown };

export type EventState = 'connecting' | 'connected' | 'disconnected';

type EventHandlers = {
  onEvent: (event: BotEvent) => void;
  onState: (state: EventState) => void;
  getAccessToken?: () => Promise<string | null>;
};

export function createEventSocket(handlers: EventHandlers) {
  let socket: WebSocket | null = null;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let authTimer: ReturnType<typeof setTimeout> | undefined;
  let closed = false;
  let delay = 800;

  const apiOrigin = new URL(apiBaseUrl, location.origin);
  const defaultEndpoint = `${apiOrigin.protocol === 'https:' ? 'wss:' : 'ws:'}//${apiOrigin.host}/ws/events`;
  const endpoint = import.meta.env.VITE_WS_URL || defaultEndpoint;

  const connect = () => {
    if (closed) return;
    handlers.onState('connecting');
    try {
      const current = new WebSocket(endpoint);
      socket = current;
      let openedAt: number | undefined;
      let authenticated = !handlers.getAccessToken;
      let authPending = false;
      current.onopen = async () => {
        openedAt = Date.now();
        if (!handlers.getAccessToken) {
          handlers.onState('connected');
          return;
        }
        authPending = true;
        authTimer = setTimeout(() => current.close(), 4500);
        try {
          const token = await handlers.getAccessToken();
          if (closed || socket !== current || current.readyState !== WebSocket.OPEN) return;
          if (!token) {
            current.close();
            return;
          }
          current.send(JSON.stringify({ type: 'auth', token }));
        } catch {
          current.close();
        }
      };
      current.onmessage = (message) => {
        try {
          const parsed: unknown = JSON.parse(String(message.data));
          if (!parsed || typeof parsed !== 'object' || !('type' in parsed)) return;
          const type = String(parsed.type);
          if (authPending) {
            if (type === 'auth.accepted' || type === 'connection_ready') {
              authPending = false;
              authenticated = true;
              if (authTimer) clearTimeout(authTimer);
              authTimer = undefined;
              delay = 800;
              handlers.onState('connected');
              if (type === 'connection_ready') handlers.onEvent(parsed as BotEvent);
            } else if (type === 'auth.rejected') {
              current.close();
            }
            return;
          }
          if (authenticated) handlers.onEvent(parsed as BotEvent);
        } catch { /* Ignore malformed event frames; the API remains the source of truth. */ }
      };
      current.onerror = () => current.close();
      current.onclose = () => {
        if (authTimer) clearTimeout(authTimer);
        authTimer = undefined;
        handlers.onState('disconnected');
        if (!closed) {
          if (openedAt != null && Date.now() - openedAt >= 10000) delay = 800;
          else delay = Math.min(delay * 1.7, 15000);
          timer = setTimeout(connect, delay);
        }
      };
    } catch {
      handlers.onState('disconnected');
      timer = setTimeout(connect, delay);
      delay = Math.min(delay * 1.7, 15000);
    }
  };

  connect();
  return () => {
    closed = true;
    if (timer) clearTimeout(timer);
    if (authTimer) clearTimeout(authTimer);
    socket?.close();
  };
}
