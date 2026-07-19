package ws

import (
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/coder/websocket"
	"github.com/kuang/flink-demo/internal/event"
)

// Client represents a connected WebSocket client.
type Client struct {
	Name string
	Role string
	conn *websocket.Conn
	send chan []byte
}

// Hub manages connected WebSocket clients and broadcasts events to them.
type Hub struct {
	mu         sync.RWMutex
	clients    map[*Client]bool
	Register   chan *Client
	Unregister chan *Client
	broadcast  chan event.EventEnvelope
	raw        chan []byte
	done       chan struct{}
}

// NewHub creates a new WebSocket hub.
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		broadcast:  make(chan event.EventEnvelope, 100),
		raw:        make(chan []byte, 100),
		done:       make(chan struct{}),
	}
}

// Run starts the hub's event loop. Blocks until Close() is called.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			slog.Info("websocket client connected", "name", client.Name, "role", client.Role)

		case client := <-h.Unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			slog.Info("websocket client disconnected", "name", client.Name, "role", client.Role)

		case ev := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				if h.shouldSendToClient(client, ev) {
					data, err := json.Marshal(ev)
					if err != nil {
						slog.Error("failed to marshal event for websocket", "error", err)
						continue
					}
					select {
					case client.send <- data:
					default:
						// Client buffer full, skip
					}
				}
			}
			h.mu.RUnlock()

		case data := <-h.raw:
			h.mu.RLock()
			for client := range h.clients {
				if client.Role != "dashboard" {
					continue
				}
				message := append([]byte(nil), data...)
				select {
				case client.send <- message:
				default:
				}
			}
			h.mu.RUnlock()

		case <-h.done:
			return
		}
	}
}

// Broadcast sends an event to all eligible connected clients.
func (h *Hub) Broadcast(ev event.EventEnvelope) {
	select {
	case h.broadcast <- ev:
	default:
		slog.Warn("broadcast channel full, dropping event")
	}
}

// BroadcastRaw sends a raw Flink output message to dashboard clients.
func (h *Hub) BroadcastRaw(data []byte) {
	message := append([]byte(nil), data...)
	select {
	case h.raw <- message:
	default:
		slog.Warn("raw broadcast channel full, dropping event")
	}
}

// Close stops the hub's event loop.
func (h *Hub) Close() {
	close(h.done)
}

// shouldSendToClient determines whether an event should be sent to a specific client.
func (h *Hub) shouldSendToClient(client *Client, ev event.EventEnvelope) bool {
	if client.Role == "dashboard" {
		return true
	}

	// Extract filter fields from payload
	buyerID, _ := ev.Payload["buyer_id"].(string)
	sellerID, _ := ev.Payload["seller_id"].(string)

	switch ev.EventType {
	case "product.listed":
		return client.Role == "buyer"

	case "cart.item.added":
		return false // dashboard only

	case "cart.checkout":
		if client.Role == "buyer" {
			return ev.ActorID == client.Name
		}
		if client.Role == "seller" {
			return sellerID == client.Name
		}
		return false

	case "order.confirmed":
		if client.Role == "buyer" {
			return buyerID == client.Name
		}
		if client.Role == "shipper" {
			return true
		}
		if client.Role == "seller" {
			return ev.ActorID == client.Name
		}
		return false

	case "shipment.picked":
		if client.Role == "buyer" {
			return buyerID == client.Name
		}
		return false

	case "shipment.delivered":
		if client.Role == "buyer" {
			return buyerID == client.Name
		}
		return false
	}
	return false
}
