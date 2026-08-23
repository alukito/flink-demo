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

func waitForAlertCache(t *testing.T, hub *Hub, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		hub.mu.RLock()
		defer hub.mu.RUnlock()
		return len(hub.alertCache) == want
	}, time.Second, 10*time.Millisecond)
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

func TestBroadcastCEPAlertOnlyQueuesForDashboard(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Close()

	dashboard := &Client{Name: "dash", Role: "dashboard", send: make(chan []byte, 1)}
	buyer := &Client{Name: "buyer", Role: "buyer", send: make(chan []byte, 1)}
	hub.Register <- dashboard
	hub.Register <- buyer
	hub.BroadcastCEPAlertRaw([]byte(`{"alert_id":"slow_delivery:o1","detected_at":"2026-08-01T10:07:00Z"}`))

	require.Eventually(t, func() bool { return len(dashboard.send) == 1 }, time.Second, 10*time.Millisecond)
	assert.Empty(t, buyer.send)
}

func TestDashboardCacheReplayIsMarkedWithoutChangingLiveMessages(t *testing.T) {
	hub := NewHub()
	hub.now = func() time.Time { return time.Date(2026, 8, 1, 18, 0, 0, 0, time.UTC) }
	go hub.Run()
	defer hub.Close()

	dashboard := &Client{Name: "live", Role: "dashboard", send: make(chan []byte, 4)}
	hub.Register <- dashboard
	metric := []byte(`{"metric":"tx_count","scope":"daily","window_end":"2026-07-25T17:00:00Z","value":2,"detail":{}}`)
	alert := []byte(`{"alert_id":"slow_delivery:o1","detected_at":"2026-08-01T10:08:00Z","detail":{}}`)
	hub.BroadcastRaw(metric)
	hub.BroadcastCEPAlertRaw(alert)

	require.Eventually(t, func() bool { return len(dashboard.send) == 2 }, time.Second, 10*time.Millisecond)
	assert.ElementsMatch(t, []string{string(metric), string(alert)}, []string{
		string(<-dashboard.send),
		string(<-dashboard.send),
	})

	waitForRawDrain(t, hub)
	waitForAlertCache(t, hub, 1)
	reloaded := &Client{Name: "reloaded", Role: "dashboard", send: make(chan []byte, 4)}
	hub.Register <- reloaded
	require.Eventually(t, func() bool { return len(reloaded.send) == 2 }, time.Second, 10*time.Millisecond)
	assert.JSONEq(t, `{"metric":"tx_count","scope":"daily","window_end":"2026-07-25T17:00:00Z","value":2,"detail":{},"replay":true}`, string(<-reloaded.send))
	assert.JSONEq(t, `{"alert_id":"slow_delivery:o1","detected_at":"2026-08-01T10:08:00Z","detail":{},"replay":true}`, string(<-reloaded.send))
}

func TestCEPAlertReplayCopiesBytesAndReplacesDuplicateIDs(t *testing.T) {
	hub := NewHub()
	hub.now = func() time.Time { return time.Date(2026, 8, 1, 18, 0, 0, 0, time.UTC) }
	go hub.Run()
	defer hub.Close()

	first := []byte(`{"alert_id":"slow_delivery:o1","detected_at":"2026-08-01T10:07:00Z","detail":{"attempt":1}}`)
	latest := []byte(`{"alert_id":"slow_delivery:o1","detected_at":"2026-08-01T10:08:00Z","detail":{"attempt":2}}`)
	hub.BroadcastCEPAlertRaw(first)
	waitForAlertCache(t, hub, 1)
	first[0] = '!'
	hub.BroadcastCEPAlertRaw(latest)
	waitForAlertCache(t, hub, 1)
	latest[0] = '!'

	dashboard := &Client{Name: "dash", Role: "dashboard", send: make(chan []byte, 2)}
	hub.Register <- dashboard
	require.Eventually(t, func() bool { return len(dashboard.send) == 1 }, time.Second, 10*time.Millisecond)
	got := <-dashboard.send
	assert.JSONEq(t, `{"alert_id":"slow_delivery:o1","detected_at":"2026-08-01T10:08:00Z","detail":{"attempt":2},"replay":true}`, string(got))
	got[0] = '!'

	secondDashboard := &Client{Name: "dash-2", Role: "dashboard", send: make(chan []byte, 2)}
	hub.Register <- secondDashboard
	require.Eventually(t, func() bool { return len(secondDashboard.send) == 1 }, time.Second, 10*time.Millisecond)
	assert.JSONEq(t, `{"alert_id":"slow_delivery:o1","detected_at":"2026-08-01T10:08:00Z","detail":{"attempt":2},"replay":true}`, string(<-secondDashboard.send))
}

func TestCEPAlertReplayPrunesEntriesOlderThanEightHours(t *testing.T) {
	now := time.Date(2026, 8, 1, 18, 0, 0, 0, time.UTC)
	hub := NewHub()
	hub.now = func() time.Time { return now }
	go hub.Run()
	defer hub.Close()

	hub.BroadcastCEPAlertRaw([]byte(`{"alert_id":"expired","detected_at":"2026-08-01T09:59:59Z"}`))
	waitForAlertCache(t, hub, 0)
	hub.BroadcastCEPAlertRaw([]byte(`{"alert_id":"retained","detected_at":"2026-08-01T10:00:00Z"}`))
	waitForAlertCache(t, hub, 1)

	dashboard := &Client{Name: "dash", Role: "dashboard", send: make(chan []byte, 2)}
	hub.Register <- dashboard
	require.Eventually(t, func() bool { return len(dashboard.send) == 1 }, time.Second, 10*time.Millisecond)
	assert.JSONEq(t, `{"alert_id":"retained","detected_at":"2026-08-01T10:00:00Z","replay":true}`, string(<-dashboard.send))
}

func TestCEPAlertReplaySortsByDetectedAtThenAlertID(t *testing.T) {
	hub := NewHub()
	hub.now = func() time.Time { return time.Date(2026, 8, 1, 18, 0, 0, 0, time.UTC) }
	go hub.Run()
	defer hub.Close()

	hub.BroadcastCEPAlertRaw([]byte(`{"alert_id":"b","detected_at":"2026-08-01T10:01:00Z"}`))
	hub.BroadcastCEPAlertRaw([]byte(`{"alert_id":"z","detected_at":"2026-08-01T10:00:00Z"}`))
	hub.BroadcastCEPAlertRaw([]byte(`{"alert_id":"a","detected_at":"2026-08-01T10:01:00Z"}`))
	waitForAlertCache(t, hub, 3)

	dashboard := &Client{Name: "dash", Role: "dashboard", send: make(chan []byte, 4)}
	hub.Register <- dashboard
	require.Eventually(t, func() bool { return len(dashboard.send) == 3 }, time.Second, 10*time.Millisecond)
	assert.JSONEq(t, `{"alert_id":"z","detected_at":"2026-08-01T10:00:00Z","replay":true}`, string(<-dashboard.send))
	assert.JSONEq(t, `{"alert_id":"a","detected_at":"2026-08-01T10:01:00Z","replay":true}`, string(<-dashboard.send))
	assert.JSONEq(t, `{"alert_id":"b","detected_at":"2026-08-01T10:01:00Z","replay":true}`, string(<-dashboard.send))
}

func TestMalformedCEPAlertIsDeliveredLiveButNotReplayed(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Close()

	dashboard := &Client{Name: "dash", Role: "dashboard", send: make(chan []byte, 2)}
	hub.Register <- dashboard
	malformed := []byte(`{"alert_id":"missing-timestamp"}`)
	hub.BroadcastCEPAlertRaw(malformed)
	require.Eventually(t, func() bool { return len(dashboard.send) == 1 }, time.Second, 10*time.Millisecond)
	assert.True(t, bytes.Equal(malformed, <-dashboard.send))
	waitForAlertCache(t, hub, 0)

	reloaded := &Client{Name: "reload", Role: "dashboard", send: make(chan []byte, 1)}
	hub.Register <- reloaded
	assert.Never(t, func() bool { return len(reloaded.send) != 0 }, 100*time.Millisecond, 10*time.Millisecond)
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
	assert.JSONEq(t, `{"metric":"tx_count","scope":"daily","window_end":"2026-07-25T17:00:00Z","value":2,"detail":{},"replay":true}`, string(<-dashboard.send))
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
	assert.JSONEq(t, `{"metric":"cart_adds_count","scope":"window","window_end":"2026-07-25T17:00:00Z","value":1,"detail":{},"replay":true}`, string(<-dashboard.send))
	assert.JSONEq(t, `{"metric":"tx_count","scope":"daily","window_end":"2026-07-25T17:00:00Z","value":2,"detail":{},"replay":true}`, string(<-dashboard.send))
}

func TestMetricReplayIsDashboardOnlyAndOwnsEveryBuffer(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Close()

	source := []byte(`{"metric":"revenue","scope":"daily","window_end":"2026-07-25T17:00:00Z","value":489000,"detail":{}}`)
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
	assert.JSONEq(t, `{"metric":"revenue","scope":"daily","window_end":"2026-07-25T17:00:00Z","value":489000,"detail":{},"replay":true}`, string(firstMessage))
	assert.JSONEq(t, `{"metric":"revenue","scope":"daily","window_end":"2026-07-25T17:00:00Z","value":489000,"detail":{},"replay":true}`, string(secondMessage))
	firstMessage[0] = '!'
	assert.JSONEq(t, `{"metric":"revenue","scope":"daily","window_end":"2026-07-25T17:00:00Z","value":489000,"detail":{},"replay":true}`, string(secondMessage))
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
	dashClient := &Client{ID: "dash", Name: "dash", Role: "dashboard"}
	assert.True(t, hub.shouldSendToClient(dashClient, event.NewEvent("any.event", "x", "Buyer", "buyer", nil)))

	// Buyer gets product.listed (all buyers)
	buyerClient := &Client{ID: "alice", Name: "alice", Role: "buyer"}
	assert.True(t, hub.shouldSendToClient(buyerClient, event.NewEvent("product.listed", "seller1", "Seller", "seller", nil)))

	// Buyer gets their own cart.checkout
	assert.True(t, hub.shouldSendToClient(buyerClient, event.NewEvent("cart.checkout", "alice", "Alice", "buyer", map[string]any{
		"order_id": "o1", "buyer_id": "alice", "seller_id": "s1",
	})))

	// Buyer does NOT get another buyer's cart.checkout
	otherBuyer := event.NewEvent("cart.checkout", "bob", "Bob", "buyer", map[string]any{
		"order_id": "o2", "buyer_id": "bob", "seller_id": "s1",
	})
	assert.False(t, hub.shouldSendToClient(buyerClient, otherBuyer))

	// Seller gets cart.checkout for their products
	sellerClient := &Client{ID: "s1", Name: "s1", Role: "seller"}
	assert.True(t, hub.shouldSendToClient(sellerClient, event.NewEvent("cart.checkout", "alice", "Alice", "buyer", map[string]any{
		"order_id": "o1", "buyer_id": "alice", "seller_id": "s1",
	})))

	// Seller does NOT get cart.checkout for other sellers' products
	assert.False(t, hub.shouldSendToClient(sellerClient, event.NewEvent("cart.checkout", "alice", "Alice", "buyer", map[string]any{
		"order_id": "o2", "buyer_id": "alice", "seller_id": "s2",
	})))

	// Shipper gets all order.confirmed events
	shipperClient := &Client{ID: "ship1", Name: "ship1", Role: "shipper"}
	assert.True(t, hub.shouldSendToClient(shipperClient, event.NewEvent("order.confirmed", "s1", "Seller", "seller", map[string]any{
		"order_id": "o1", "buyer_id": "b1", "seller_id": "s1",
	})))

	// Shipper does NOT get product.listed
	assert.False(t, hub.shouldSendToClient(shipperClient, event.NewEvent("product.listed", "s1", "Seller", "seller", nil)))
}

func TestLifecycleRoutingUsesUUIDsForSameNameClients(t *testing.T) {
	hub := NewHub()
	sellerA := &Client{ID: "seller-a", Name: "alex", Role: "seller"}
	sellerB := &Client{ID: "seller-b", Name: "alex", Role: "seller"}
	buyer := &Client{ID: "buyer-a", Name: "alex", Role: "buyer"}
	owner := &Client{ID: "shipper-a", Name: "alex", Role: "shipper"}
	otherShipper := &Client{ID: "shipper-b", Name: "alex", Role: "shipper"}

	checkout := event.NewEvent("cart.checkout", "buyer-a", "alex", "buyer", map[string]any{"buyer_id": "buyer-a", "seller_id": "seller-a"})
	assert.True(t, hub.shouldSendToClient(sellerA, checkout))
	assert.False(t, hub.shouldSendToClient(sellerB, checkout))
	assert.True(t, hub.shouldSendToClient(buyer, checkout))

	confirmed := event.NewEvent("order.confirmed", "seller-a", "alex", "seller", map[string]any{"buyer_id": "buyer-a", "seller_id": "seller-a"})
	assert.True(t, hub.shouldSendToClient(sellerA, confirmed))
	assert.False(t, hub.shouldSendToClient(sellerB, confirmed))
	assert.True(t, hub.shouldSendToClient(buyer, confirmed))
	assert.True(t, hub.shouldSendToClient(otherShipper, confirmed))

	picked := event.NewEvent("shipment.picked", "shipper-a", "alex", "shipper", map[string]any{"buyer_id": "buyer-a", "seller_id": "seller-a", "shipper_id": "shipper-a"})
	assert.True(t, hub.shouldSendToClient(sellerA, picked))
	assert.False(t, hub.shouldSendToClient(sellerB, picked))
	assert.True(t, hub.shouldSendToClient(buyer, picked))
	assert.True(t, hub.shouldSendToClient(otherShipper, picked))

	delivered := event.NewEvent("shipment.delivered", "shipper-a", "alex", "shipper", map[string]any{"buyer_id": "buyer-a", "seller_id": "seller-a", "shipper_id": "shipper-a"})
	assert.True(t, hub.shouldSendToClient(sellerA, delivered))
	assert.False(t, hub.shouldSendToClient(sellerB, delivered))
	assert.True(t, hub.shouldSendToClient(buyer, delivered))
	assert.True(t, hub.shouldSendToClient(owner, delivered))
	assert.False(t, hub.shouldSendToClient(otherShipper, delivered))
}
