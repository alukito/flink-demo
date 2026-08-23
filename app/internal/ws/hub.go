package ws

import (
	"encoding/json"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/kuang/flink-demo/internal/event"
)

var allowedMetricScopes = map[string]map[string]bool{
	"listings_count":   {"window": true},
	"cart_adds_count":  {"window": true},
	"tx_count":         {"window": true, "daily": true},
	"confirmed_orders": {"window": true},
	"delivered_orders": {"window": true, "daily": true},
	"top_product":      {"window": true},
	"revenue":          {"daily": true},
}

type metricIdentity struct {
	Metric string `json:"metric"`
	Scope  string `json:"scope"`
}

type alertIdentity struct {
	AlertID    string    `json:"alert_id"`
	DetectedAt time.Time `json:"detected_at"`
}

type cachedAlert struct {
	detectedAt time.Time
	data       []byte
}

func alertCacheEntry(data []byte) (string, cachedAlert, bool) {
	var identity alertIdentity
	if err := json.Unmarshal(data, &identity); err != nil || identity.AlertID == "" || identity.DetectedAt.IsZero() {
		return "", cachedAlert{}, false
	}
	return identity.AlertID, cachedAlert{
		detectedAt: identity.DetectedAt.UTC(),
		data:       append([]byte(nil), data...),
	}, true
}

func metricCacheKey(data []byte) (string, bool) {
	var identity metricIdentity
	if err := json.Unmarshal(data, &identity); err != nil {
		return "", false
	}
	scopes, ok := allowedMetricScopes[identity.Metric]
	if !ok || !scopes[identity.Scope] {
		return "", false
	}
	return identity.Metric + "\x00" + identity.Scope, true
}

func dashboardReplayMessage(data []byte) ([]byte, error) {
	var message map[string]json.RawMessage
	if err := json.Unmarshal(data, &message); err != nil {
		return nil, err
	}
	message["replay"] = json.RawMessage("true")
	return json.Marshal(message)
}

// Client represents a connected WebSocket client.
type Client struct {
	ID   string
	Name string
	Role string
	conn *websocket.Conn
	send chan []byte
}

// Hub manages connected WebSocket clients and broadcasts events to them.
type Hub struct {
	mu          sync.RWMutex
	clients     map[*Client]bool
	metricCache map[string][]byte
	alertCache  map[string]cachedAlert
	Register    chan *Client
	Unregister  chan *Client
	broadcast   chan event.EventEnvelope
	raw         chan []byte
	alerts      chan []byte
	done        chan struct{}
	now         func() time.Time
}

// NewHub creates a new WebSocket hub.
func NewHub() *Hub {
	return &Hub{
		clients:     make(map[*Client]bool),
		metricCache: make(map[string][]byte),
		alertCache:  make(map[string]cachedAlert),
		Register:    make(chan *Client),
		Unregister:  make(chan *Client),
		broadcast:   make(chan event.EventEnvelope, 100),
		raw:         make(chan []byte, 100),
		alerts:      make(chan []byte, 100),
		done:        make(chan struct{}),
		now:         time.Now,
	}
}

// Run starts the hub's event loop. Blocks until Close() is called.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.mu.Lock()
			h.clients[client] = true
			if client.Role == "dashboard" {
				keys := make([]string, 0, len(h.metricCache))
				for key := range h.metricCache {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				for _, key := range keys {
					message, err := dashboardReplayMessage(h.metricCache[key])
					if err != nil {
						slog.Warn("failed to mark dashboard metric replay", "error", err)
						continue
					}
					select {
					case client.send <- message:
					default:
						slog.Warn("dashboard replay buffer full", "name", client.Name)
					}
				}
				h.pruneAlerts(h.now().UTC())
				alerts := make([]struct {
					alertID string
					cachedAlert
				}, 0, len(h.alertCache))
				for alertID, alert := range h.alertCache {
					alerts = append(alerts, struct {
						alertID string
						cachedAlert
					}{alertID: alertID, cachedAlert: alert})
				}
				sort.Slice(alerts, func(i, j int) bool {
					if alerts[i].detectedAt.Equal(alerts[j].detectedAt) {
						return alerts[i].alertID < alerts[j].alertID
					}
					return alerts[i].detectedAt.Before(alerts[j].detectedAt)
				})
				for _, alert := range alerts {
					message, err := dashboardReplayMessage(alert.data)
					if err != nil {
						slog.Warn("failed to mark dashboard alert replay", "error", err)
						continue
					}
					select {
					case client.send <- message:
					default:
						slog.Warn("dashboard alert replay buffer full", "name", client.Name)
					}
				}
			}
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
			h.mu.Lock()
			if key, ok := metricCacheKey(data); ok {
				h.metricCache[key] = append([]byte(nil), data...)
			} else {
				slog.Warn("raw Level 2 message is not cacheable")
			}
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
			h.mu.Unlock()

		case data := <-h.alerts:
			h.mu.Lock()
			now := h.now().UTC()
			h.pruneAlerts(now)
			if alertID, alert, ok := alertCacheEntry(data); ok {
				if !alert.detectedAt.Before(now.Add(-8 * time.Hour)) {
					h.alertCache[alertID] = alert
				}
			} else {
				slog.Warn("raw CEP alert is not cacheable")
			}
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
			h.mu.Unlock()

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

// BroadcastCEPAlertRaw sends a raw CEP alert to dashboard clients and retains it
// for recent-dashboard replay.
func (h *Hub) BroadcastCEPAlertRaw(data []byte) {
	message := append([]byte(nil), data...)
	select {
	case h.alerts <- message:
	default:
		slog.Warn("CEP alert broadcast channel full, dropping alert")
	}
}

func (h *Hub) pruneAlerts(now time.Time) {
	cutoff := now.Add(-8 * time.Hour)
	for alertID, alert := range h.alertCache {
		if alert.detectedAt.Before(cutoff) {
			delete(h.alertCache, alertID)
		}
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
	shipperID, _ := ev.Payload["shipper_id"].(string)

	switch ev.EventType {
	case "product.listed":
		return client.Role == "buyer"

	case "cart.item.added":
		return false // dashboard only

	case "cart.checkout":
		if client.Role == "buyer" {
			return buyerID == client.ID
		}
		if client.Role == "seller" {
			return sellerID == client.ID
		}
		return false

	case "order.confirmed":
		if client.Role == "buyer" {
			return buyerID == client.ID
		}
		if client.Role == "shipper" {
			return true
		}
		if client.Role == "seller" {
			return sellerID == client.ID
		}
		return false

	case "shipment.picked":
		if client.Role == "buyer" {
			return buyerID == client.ID
		}
		if client.Role == "seller" {
			return sellerID == client.ID
		}
		return client.Role == "shipper"

	case "shipment.delivered":
		if client.Role == "buyer" {
			return buyerID == client.ID
		}
		if client.Role == "seller" {
			return sellerID == client.ID
		}
		return client.Role == "shipper" && shipperID == client.ID
	}
	return false
}
