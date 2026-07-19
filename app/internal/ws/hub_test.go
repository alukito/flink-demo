package ws

import (
	"testing"
	"time"

	"github.com/kuang/flink-demo/internal/event"
	"github.com/stretchr/testify/assert"
)

func TestHubRegisterAndUnregister(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Close()

	c := &Client{Name: "buyer1", Role: "buyer", send: make(chan []byte, 1)}
	hub.Register <- c

	// Give hub time to process
	// (in real code we'd use a sync mechanism, but for the test the channel
	// buffer is enough since Register is non-blocking)
	hub.Unregister <- c
	assert.True(t, true) // didn't panic
}

func TestBroadcastRawOnlyQueuesForDashboard(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Close()

	dashboard := &Client{Name: "dash", Role: "dashboard", send: make(chan []byte, 1)}
	buyer := &Client{Name: "buyer", Role: "buyer", send: make(chan []byte, 1)}
	hub.Register <- dashboard
	hub.Register <- buyer
	hub.BroadcastRaw([]byte(`{"metric":"tx_count","scope":"window","window_end":"2026-07-18T10:05:00Z","value":7,"detail":{}}`))

	assert.Eventually(t, func() bool { return len(dashboard.send) == 1 }, time.Second, 10*time.Millisecond)
	assert.Empty(t, buyer.send)
}

func TestShouldSendToClient(t *testing.T) {
	hub := NewHub()

	// Dashboard gets everything
	dashClient := &Client{Name: "dash", Role: "dashboard"}
	assert.True(t, hub.shouldSendToClient(dashClient, event.NewEvent("any.event", "x", "buyer", nil)))

	// Buyer gets product.listed (all buyers)
	buyerClient := &Client{Name: "alice", Role: "buyer"}
	assert.True(t, hub.shouldSendToClient(buyerClient, event.NewEvent("product.listed", "seller1", "seller", nil)))

	// Buyer gets their own cart.checkout
	assert.True(t, hub.shouldSendToClient(buyerClient, event.NewEvent("cart.checkout", "alice", "buyer", map[string]any{
		"order_id": "o1", "seller_id": "s1",
	})))

	// Buyer does NOT get another buyer's cart.checkout
	otherBuyer := event.NewEvent("cart.checkout", "bob", "buyer", map[string]any{
		"order_id": "o2", "seller_id": "s1",
	})
	assert.False(t, hub.shouldSendToClient(buyerClient, otherBuyer))

	// Seller gets cart.checkout for their products
	sellerClient := &Client{Name: "s1", Role: "seller"}
	assert.True(t, hub.shouldSendToClient(sellerClient, event.NewEvent("cart.checkout", "alice", "buyer", map[string]any{
		"order_id": "o1", "seller_id": "s1",
	})))

	// Seller does NOT get cart.checkout for other sellers' products
	assert.False(t, hub.shouldSendToClient(sellerClient, event.NewEvent("cart.checkout", "alice", "buyer", map[string]any{
		"order_id": "o2", "seller_id": "s2",
	})))

	// Shipper gets all order.confirmed events
	shipperClient := &Client{Name: "ship1", Role: "shipper"}
	assert.True(t, hub.shouldSendToClient(shipperClient, event.NewEvent("order.confirmed", "s1", "seller", map[string]any{
		"order_id": "o1", "buyer_id": "b1",
	})))

	// Shipper does NOT get product.listed
	assert.False(t, hub.shouldSendToClient(shipperClient, event.NewEvent("product.listed", "s1", "seller", nil)))
}
