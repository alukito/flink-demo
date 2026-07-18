package ws

import (
	"log/slog"
	"net/http"

	"github.com/coder/websocket"
	"github.com/kuang/flink-demo/internal/auth"
)

// WSHandler handles WebSocket connection upgrades.
type WSHandler struct {
	jwtMgr *auth.JWTManager
	hub    *Hub
}

// NewHandler creates a WebSocket handler.
func NewHandler(jwtMgr *auth.JWTManager, hub *Hub) *WSHandler {
	return &WSHandler{jwtMgr: jwtMgr, hub: hub}
}

// ServeWS handles GET /ws — upgrades to WebSocket and registers the client.
func (h *WSHandler) ServeWS(w http.ResponseWriter, r *http.Request) {
	// Authenticate via query parameter token
	token := r.URL.Query().Get("token")
	if token == "" {
		http.Error(w, "missing token", http.StatusUnauthorized)
		return
	}

	claims, err := h.jwtMgr.Verify(token)
	if err != nil {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		slog.Error("websocket accept failed", "error", err)
		return
	}

	client := &Client{
		Name: claims.Name,
		Role: claims.Role,
		conn: conn,
		send: make(chan []byte, 50),
	}

	h.hub.Register <- client

	// Read loop (we don't expect messages from clients, but keep it alive)
	go func() {
		defer func() {
			h.hub.Unregister <- client
			conn.Close(websocket.StatusNormalClosure, "")
		}()
		for {
			_, _, err := conn.Read(r.Context())
			if err != nil {
				return
			}
		}
	}()

	// Write loop
	go func() {
		for data := range client.send {
			err := conn.Write(r.Context(), websocket.MessageText, data)
			if err != nil {
				return
			}
			slog.Debug("websocket event pushed", "client", client.Name, "data_len", len(data))
		}
	}()
}
