package ws

import (
	"bytes"
	"testing"
	"time"

	"github.com/kuang/flink-demo/internal/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func waitForRawDrain(t *testing.T, hub *Hub) {
	t.Helper()
	require.Eventually(t, func() bool {
		return len(hub.raw) == 0
	}, time.Second, 10*time.Millisecond)

	// Seeing an empty channel only proves that Run received the message. A
	// registration rendezvous is an ordered barrier: Run cannot receive it until
	// it has completed processing the raw-message case that drained the channel.
	barrier := &Client{Name: "raw-drain-barrier", Role: "barrier", send: make(chan []byte, 1)}
	hub.Register <- barrier
	hub.Unregister <- barrier
}

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

func TestDashboardRegistrationReplaysLatestMetricPerScope(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Close()

	first := []byte(`{"metric":"tx_count","scope":"daily","window_end":"2026-07-25T17:00:00Z","value":1,"detail":{}}`)
	latest := []byte("{\n  \"metric\":\"tx_count\",\"scope\":\"daily\",\"window_end\":\"2026-07-25T17:00:00Z\",\"value\":2,\"detail\":{}\n}")
	hub.BroadcastRaw(first)
	waitForRawDrain(t, hub)
	hub.BroadcastRaw(latest)
	waitForRawDrain(t, hub)

	dashboard := &Client{Name: "dash", Role: "dashboard", send: make(chan []byte, 10)}
	hub.Register <- dashboard

	require.Eventually(t, func() bool { return len(dashboard.send) == 1 }, time.Second, 10*time.Millisecond)
	assert.True(t, bytes.Equal(latest, <-dashboard.send))
}

func TestDashboardMetricReplayUsesSortedMetricScopeOrder(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Close()

	second := []byte(`{"metric":"tx_count","scope":"daily","window_end":"2026-07-25T17:00:00Z","value":2,"detail":{}}`)
	first := []byte(`{"metric":"cart_adds_count","scope":"window","window_end":"2026-07-25T17:00:00Z","value":1,"detail":{}}`)
	hub.BroadcastRaw(second)
	waitForRawDrain(t, hub)
	hub.BroadcastRaw(first)
	waitForRawDrain(t, hub)

	dashboard := &Client{Name: "dash", Role: "dashboard", send: make(chan []byte, 10)}
	hub.Register <- dashboard
	require.Eventually(t, func() bool { return len(dashboard.send) == 2 }, time.Second, 10*time.Millisecond)
	assert.True(t, bytes.Equal(first, <-dashboard.send))
	assert.True(t, bytes.Equal(second, <-dashboard.send))
}

func TestMetricReplayIsDashboardOnlyAndOwnsEveryBuffer(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Close()

	source := []byte(`{"metric":"revenue","scope":"daily","window_end":"2026-07-25T17:00:00Z","value":489000,"detail":{}}`)
	expected := append([]byte(nil), source...)
	hub.BroadcastRaw(source)
	source[0] = '!'
	waitForRawDrain(t, hub)

	first := &Client{Name: "dash-1", Role: "dashboard", send: make(chan []byte, 10)}
	second := &Client{Name: "dash-2", Role: "dashboard", send: make(chan []byte, 10)}
	buyer := &Client{Name: "buyer", Role: "buyer", send: make(chan []byte, 10)}
	hub.Register <- first
	hub.Register <- second
	hub.Register <- buyer

	require.Eventually(t, func() bool {
		return len(first.send) == 1 && len(second.send) == 1
	}, time.Second, 10*time.Millisecond)
	firstMessage := <-first.send
	secondMessage := <-second.send
	assert.True(t, bytes.Equal(expected, firstMessage))
	assert.True(t, bytes.Equal(expected, secondMessage))
	firstMessage[0] = '!'
	assert.True(t, bytes.Equal(expected, secondMessage))
	assert.Never(t, func() bool { return len(buyer.send) != 0 }, 100*time.Millisecond, 10*time.Millisecond)
}

func TestMalformedRawMessageIsNotCached(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Close()

	hub.BroadcastRaw([]byte(`{"metric":`))
	waitForRawDrain(t, hub)

	dashboard := &Client{Name: "dash", Role: "dashboard", send: make(chan []byte, 10)}
	hub.Register <- dashboard
	assert.Never(t, func() bool { return len(dashboard.send) != 0 }, 100*time.Millisecond, 10*time.Millisecond)

	valid := []byte(`{"metric":"tx_count","scope":"window","window_end":"2026-07-25T10:00:00Z","value":3,"detail":{}}`)
	hub.BroadcastRaw(valid)
	require.Eventually(t, func() bool { return len(dashboard.send) == 1 }, time.Second, 10*time.Millisecond)
	assert.True(t, bytes.Equal(valid, <-dashboard.send))
}

func TestMetricCacheKeyAllowsOnlyPlannedMetricScopes(t *testing.T) {
	for _, test := range []struct {
		raw  []byte
		want string
	}{
		{[]byte(`{"metric":"listings_count","scope":"window"}`), "listings_count\x00window"},
		{[]byte(`{"metric":"cart_adds_count","scope":"window"}`), "cart_adds_count\x00window"},
		{[]byte(`{"metric":"tx_count","scope":"window"}`), "tx_count\x00window"},
		{[]byte(`{"metric":"tx_count","scope":"daily"}`), "tx_count\x00daily"},
		{[]byte(`{"metric":"confirmed_orders","scope":"window"}`), "confirmed_orders\x00window"},
		{[]byte(`{"metric":"delivered_orders","scope":"window"}`), "delivered_orders\x00window"},
		{[]byte(`{"metric":"delivered_orders","scope":"daily"}`), "delivered_orders\x00daily"},
		{[]byte(`{"metric":"top_product","scope":"window"}`), "top_product\x00window"},
		{[]byte(`{"metric":"revenue","scope":"daily"}`), "revenue\x00daily"},
	} {
		key, ok := metricCacheKey(test.raw)
		assert.True(t, ok, "raw=%s", test.raw)
		assert.Equal(t, test.want, key)
	}

	for _, raw := range [][]byte{
		[]byte(`{"metric":"revenue","scope":"window"}`),
		[]byte(`{"metric":"unknown","scope":"daily"}`),
		[]byte(`{"metric":`),
	} {
		_, ok := metricCacheKey(raw)
		assert.False(t, ok, "raw=%s", raw)
	}
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
