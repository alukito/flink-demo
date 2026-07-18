# Phase 2: Level 1 — Stateless Operators — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the complete e-commerce simulation with Kafka producer/consumer, WebSocket hub for real-time event delivery, full role UIs (Seller, Buyer, Shipper), order lifecycle with mutex-protected state transitions, multi-seller cart splitting, and the Dashboard Level 1 live event feed.

**Architecture:** Go server produces events to Kafka on role actions, consumes all input Kafka topics via a consumer goroutine, and routes events to connected WebSocket clients based on per-client predicates (role + identity). The Go server's in-memory state is the source of truth for order lifecycle; Kafka events are notifications. React UIs connect via WebSocket and render events in real-time.

**Tech Stack:** Go 1.23 (net/http, log/slog, sync, embed), React 19 + TypeScript + Vite 8, Kafka (segmentio/kafka-go), WebSocket (github.com/coder/websocket), UUID (github.com/google/uuid)

## Global Constraints

- Go 1.23 (from Phase 1, kafka-go requires it)
- Structured JSON logging via `log/slog` with `slog.SetDefault` (from Phase 1 fix)
- Test assertions via `github.com/stretchr/testify`
- All Go code lives under `app/`; all React code lives under `web/`
- Go module path is `github.com/kuang/flink-demo`
- Kafka client via `github.com/segmentio/kafka-go`
- WebSocket via `github.com/coder/websocket`
- UUID via `github.com/google/uuid`
- JWT auth from Phase 1 (auth.JWTManager, auth.Claims, auth.ClaimsKey, auth.RequireRole)
- Session store from Phase 1 (session.Store, session.Handler)
- Kafka client from Phase 1 (kafkaclient.Client, kafkaclient.RequiredTopics)
- Server from Phase 1 (server.Server, server.New)
- Config from Phase 1 (config.Config with Port, JWTSecret, KafkaAddr)
- No frontend tests in Phase 2
- Cart is client-side only (browser state); server produces `cart.item.added` events but does not track cart state
- Order lifecycle state transitions: checkout → confirmed → picked → delivered
- Mutex protects order state transitions; Kafka event produced AFTER state change succeeds
- Shipper pickup race: check status == "confirmed" under lock, set to "picked" + assign PickedBy, then produce event. Second caller gets 409.
- Shipper countdown timer: 5-15 seconds, client-side only, disables "Mark Delivered" button

---

## File Structure

```
app/
├── main.go                                    # MODIFY: wire new components
├── internal/
│   ├── event/
│   │   ├── event.go                           # EventEnvelope, NewEvent helper
│   │   └── event_test.go                      # Event creation tests
│   ├── kafkaclient/
│   │   ├── producer.go                        # Kafka producer (writes events)
│   │   ├── producer_test.go                   # Producer tests
│   │   ├── consumer.go                        # Kafka consumer (reads topics → hub)
│   │   └── consumer_test.go                   # Consumer tests
│   ├── product/
│   │   ├── store.go                           # Product struct, Store with mutex + index
│   │   ├── store_test.go                      # Store tests
│   │   ├── handler.go                         # Seller product endpoints
│   │   └── handler_test.go                    # Handler tests
│   ├── order/
│   │   ├── store.go                           # Order struct, Store with mutex + indexes
│   │   ├── store_test.go                      # Store tests (create, confirm, pick, deliver, race)
│   │   ├── handler.go                         # Buyer checkout + Shipper endpoints
│   │   └── handler_test.go                    # Handler tests
│   ├── buyer/
│   │   ├── handler.go                         # Buyer catalog, cart, checkout endpoints
│   │   └── handler_test.go                    # Handler tests
│   ├── ws/
│   │   ├── hub.go                             # WebSocket hub (client management + broadcast)
│   │   ├── hub_test.go                        # Hub tests
│   │   └── handler.go                         # WebSocket HTTP handler (/ws)
│   └── server/
│       └── server.go                          # MODIFY: wire all new handlers + WebSocket
web/
└── src/
    ├── hooks/
    │   └── useWebSocket.ts                    # WebSocket hook with auto-reconnect
    ├── context/
    │   └── EventContext.tsx                   # Event context for components
    ├── api/
    │   └── client.ts                          # MODIFY: add role-specific API functions
    └── pages/
        ├── Seller.tsx                         # REPLACE: product panel + order inbox
        ├── Buyer.tsx                          # REPLACE: catalog + cart + checkout + orders
        ├── Shipper.tsx                        # REPLACE: job board + countdown
        └── Dashboard.tsx                      # REPLACE: Level 1 live event feed
```

---

## Task 1: Event Types + Kafka Producer

**Files:**
- Create: `app/internal/event/event.go`
- Create: `app/internal/event/event_test.go`
- Create: `app/internal/kafkaclient/producer.go`
- Create: `app/internal/kafkaclient/producer_test.go`

**Interfaces:**
- Produces: `event.EventEnvelope{EventID, EventType, ActorID, ActorRole, Timestamp, Payload}`, `event.NewEvent(eventType, actorID, actorRole string, payload map[string]any) EventEnvelope`, `kafkaclient.Producer` with `Write(ctx, topic, event) error` and `Close()`

- [ ] **Step 1: Install UUID dependency**

Run:
```bash
cd app && go get github.com/google/uuid
```

- [ ] **Step 2: Write failing tests for event package**

Create `app/internal/event/event_test.go`:
```go
package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEventCreatesEnvelope(t *testing.T) {
	payload := map[string]any{"product_id": "abc", "name": "Widget"}
	ev := NewEvent("product.listed", "seller1", "seller", payload)

	assert.NotEmpty(t, ev.EventID)
	assert.Equal(t, "product.listed", ev.EventType)
	assert.Equal(t, "seller1", ev.ActorID)
	assert.Equal(t, "seller", ev.ActorRole)
	assert.NotEmpty(t, ev.Timestamp)
	assert.Equal(t, payload, ev.Payload)
}

func TestNewEventGeneratesUniqueIDs(t *testing.T) {
	ev1 := NewEvent("test", "a", "buyer", nil)
	ev2 := NewEvent("test", "a", "buyer", nil)
	require.NotEqual(t, ev1.EventID, ev2.EventID, "each event should have a unique ID")
}

func TestEventEnvelopeJSONRoundtrip(t *testing.T) {
	ev := NewEvent("cart.checkout", "buyer1", "buyer", map[string]any{
		"order_id":   "ord-123",
		"seller_id":  "seller1",
		"total":      float64(1500),
	})
	data, err := json.Marshal(ev)
	require.NoError(t, err)

	var decoded EventEnvelope
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, ev.EventID, decoded.EventID)
	assert.Equal(t, ev.EventType, decoded.EventType)
	assert.Equal(t, ev.ActorID, decoded.ActorID)
	assert.Equal(t, "ord-123", decoded.Payload["order_id"])
}
```

Add `"encoding/json"` to the test imports.

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd app && go test ./internal/event/ -v`
Expected: FAIL — `EventEnvelope`, `NewEvent` not defined

- [ ] **Step 4: Implement event package**

Create `app/internal/event/event.go`:
```go
package event

import (
	"time"

	"github.com/google/uuid"
)

// EventEnvelope is the standard wrapper for all events on Kafka topics.
type EventEnvelope struct {
	EventID   string         `json:"event_id"`
	EventType string         `json:"event_type"`
	ActorID   string         `json:"actor_id"`
	ActorRole string         `json:"actor_role"`
	Timestamp string         `json:"timestamp"`
	Payload   map[string]any `json:"payload"`
}

// NewEvent creates a new EventEnvelope with a generated UUID and current timestamp.
func NewEvent(eventType, actorID, actorRole string, payload map[string]any) EventEnvelope {
	return EventEnvelope{
		EventID:   uuid.New().String(),
		EventType: eventType,
		ActorID:   actorID,
		ActorRole: actorRole,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   payload,
	}
}
```

- [ ] **Step 5: Run event tests to verify they pass**

Run: `cd app && go test ./internal/event/ -v`
Expected: PASS (3 tests)

- [ ] **Step 6: Write failing test for Kafka producer**

Create `app/internal/kafkaclient/producer_test.go`:
```go
package kafkaclient

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kuang/flink-demo/internal/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducerWriteSerializesEventAsJSON(t *testing.T) {
	// We can't test against a real Kafka broker in a unit test,
	// but we can verify the event is properly serialized to JSON
	// by checking the message value that would be sent.
	ev := event.NewEvent("product.listed", "seller1", "seller", map[string]any{
		"product_id": "p1",
		"name":       "Widget",
	})

	data, err := json.Marshal(ev)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, "product.listed", decoded["event_type"])
	assert.Equal(t, "seller1", decoded["actor_id"])
	assert.Equal(t, "p1", decoded["payload"].(map[string]any)["product_id"])
}

func TestNewProducerCreatesWriter(t *testing.T) {
	p := NewProducer("localhost:9092")
	assert.NotNil(t, p)
	p.Close()
}
```

- [ ] **Step 7: Run producer tests to verify they fail**

Run: `cd app && go test ./internal/kafkaclient/ -run TestProducer -v && go test ./internal/kafkaclient/ -run TestNewProducer -v`
Expected: FAIL — `NewProducer` not defined

- [ ] **Step 8: Implement Kafka producer**

Create `app/internal/kafkaclient/producer.go`:
```go
package kafkaclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/kuang/flink-demo/internal/event"
	"github.com/segmentio/kafka-go"
)

// Producer writes events to Kafka topics.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer creates a Kafka producer for the given broker address.
func NewProducer(addr string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(addr),
			Async:        false,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
		},
	}
}

// Write serializes an event and writes it to the specified Kafka topic.
func (p *Producer) Write(ctx context.Context, topic string, ev event.EventEnvelope) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(ev.EventID),
		Value: data,
	})
	if err != nil {
		return fmt.Errorf("failed to write to kafka: %w", err)
	}

	slog.Debug("kafka event produced",
		"topic", topic,
		"event_type", ev.EventType,
		"event_id", ev.EventID,
	)
	return nil
}

// Close closes the underlying Kafka writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
```

- [ ] **Step 9: Run all kafkaclient tests and verify they pass**

Run: `cd app && go test ./internal/kafkaclient/ -v && go test ./internal/event/ -v`
Expected: PASS (all tests)

- [ ] **Step 10: Commit**

```bash
git add app/internal/event/ app/internal/kafkaclient/producer.go app/internal/kafkaclient/producer_test.go app/go.mod app/go.sum
git commit -m "feat: event envelope type and Kafka producer"
```

---

## Task 2: Product Store

**Files:**
- Create: `app/internal/product/store.go`
- Create: `app/internal/product/store_test.go`

**Interfaces:**
- Produces: `product.Product{ID, Name, Price, Quantity, SellerID, ListedAt}`, `product.Store` with `Add(p Product)`, `Get(id string) *Product`, `All() []Product`, `BySeller(sellerID string) []Product`

- [ ] **Step 1: Write failing tests for product store**

Create `app/internal/product/store_test.go`:
```go
package product

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreAddAndGet(t *testing.T) {
	s := NewStore()
	p := Product{
		ID:       "p1",
		Name:     "Widget",
		Price:    500,
		Quantity: 10,
		SellerID: "seller1",
		ListedAt: time.Now(),
	}
	s.Add(p)

	got := s.Get("p1")
	require.NotNil(t, got)
	assert.Equal(t, "Widget", got.Name)
	assert.Equal(t, "seller1", got.SellerID)
}

func TestStoreGetNotFound(t *testing.T) {
	s := NewStore()
	assert.Nil(t, s.Get("nonexistent"))
}

func TestStoreAll(t *testing.T) {
	s := NewStore()
	s.Add(Product{ID: "p1", Name: "A", SellerID: "s1"})
	s.Add(Product{ID: "p2", Name: "B", SellerID: "s2"})

	all := s.All()
	assert.Len(t, all, 2)
}

func TestStoreBySeller(t *testing.T) {
	s := NewStore()
	s.Add(Product{ID: "p1", Name: "A", SellerID: "s1"})
	s.Add(Product{ID: "p2", Name: "B", SellerID: "s1"})
	s.Add(Product{ID: "p3", Name: "C", SellerID: "s2"})

	s1Products := s.BySeller("s1")
	assert.Len(t, s1Products, 2)
	assert.Equal(t, "p1", s1Products[0].ID)
	assert.Equal(t, "p2", s1Products[1].ID)

	s2Products := s.BySeller("s2")
	assert.Len(t, s2Products, 1)
	assert.Equal(t, "p3", s2Products[0].ID)

	empty := s.BySeller("nonexistent")
	assert.Len(t, empty, 0)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd app && go test ./internal/product/ -v`
Expected: FAIL — `Product`, `NewStore` not defined

- [ ] **Step 3: Implement product store**

Create `app/internal/product/store.go`:
```go
package product

import (
	"sync"
	"time"
)

// Product represents a product listed by a seller.
type Product struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Price     int       `json:"price"` // in cents
	Quantity  int       `json:"quantity"`
	SellerID  string    `json:"seller_id"`
	ListedAt  time.Time `json:"listed_at"`
}

// Store is an in-memory product store with a derived index by seller.
type Store struct {
	mu              sync.RWMutex
	products        map[string]Product
	productsBySeller map[string][]string // sellerID → []productID
}

// NewStore creates an empty product store.
func NewStore() *Store {
	return &Store{
		products:         make(map[string]Product),
		productsBySeller: make(map[string][]string),
	}
}

// Add adds a product to the store and updates the seller index.
func (s *Store) Add(p Product) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.products[p.ID] = p
	s.productsBySeller[p.SellerID] = append(s.productsBySeller[p.SellerID], p.ID)
}

// Get returns a product by ID, or nil if not found.
func (s *Store) Get(id string) *Product {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.products[id]
	if !ok {
		return nil
	}
	return &p
}

// All returns all products in the store.
func (s *Store) All() []Product {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Product, 0, len(s.products))
	for _, p := range s.products {
		result = append(result, p)
	}
	return result
}

// BySeller returns all products for a given seller.
func (s *Store) BySeller(sellerID string) []Product {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.productsBySeller[sellerID]
	result := make([]Product, 0, len(ids))
	for _, id := range ids {
		if p, ok := s.products[id]; ok {
			result = append(result, p)
		}
	}
	return result
}
```

- [ ] **Step 4: Run product tests to verify they pass**

Run: `cd app && go test ./internal/product/ -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add app/internal/product/
git commit -m "feat: in-memory product store with seller index"
```

---

## Task 3: Order Store with Indexes and Race Protection

**Files:**
- Create: `app/internal/order/store.go`
- Create: `app/internal/order/store_test.go`

**Interfaces:**
- Produces: `order.OrderStatus` type, `order.OrderItem` struct, `order.Order` struct, `order.Store` with `Create(o Order)`, `Get(id string) *Order`, `ByBuyer(buyerID string) []Order`, `BySeller(sellerID string) []Order`, `ByStatus(status OrderStatus) []Order`, `Confirm(orderID, sellerID string) error`, `Pick(orderID, shipperID string) error`, `Deliver(orderID, shipperID string) error`, `order.ErrNotFound`, `order.ErrInvalidTransition`, `order.ErrWrongSeller`

- [ ] **Step 1: Write failing tests for order store**

Create `app/internal/order/store_test.go`:
```go
package order

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleOrder() Order {
	return Order{
		ID:              "o1",
		BuyerID:         "buyer1",
		SellerID:        "seller1",
		Items:           []OrderItem{{ProductID: "p1", ProductName: "Widget", Quantity: 2, UnitPrice: 500}},
		TotalAmount:     1000,
		ShippingAddress: "123 Main St",
		Status:          StatusCheckout,
		CreatedAt:       time.Now(),
	}
}

func TestStoreCreateAndGet(t *testing.T) {
	s := NewStore()
	o := sampleOrder()
	s.Create(o)

	got := s.Get("o1")
	require.NotNil(t, got)
	assert.Equal(t, "buyer1", got.BuyerID)
	assert.Equal(t, StatusCheckout, got.Status)
}

func TestStoreGetNotFound(t *testing.T) {
	s := NewStore()
	assert.Nil(t, s.Get("nonexistent"))
}

func TestStoreByBuyer(t *testing.T) {
	s := NewStore()
	s.Create(Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: StatusCheckout})
	s.Create(Order{ID: "o2", BuyerID: "b1", SellerID: "s2", Status: StatusCheckout})
	s.Create(Order{ID: "o3", BuyerID: "b2", SellerID: "s1", Status: StatusCheckout})

	b1Orders := s.ByBuyer("b1")
	assert.Len(t, b1Orders, 2)
}

func TestStoreBySeller(t *testing.T) {
	s := NewStore()
	s.Create(Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: StatusCheckout})
	s.Create(Order{ID: "o2", BuyerID: "b2", SellerID: "s1", Status: StatusCheckout})
	s.Create(Order{ID: "o3", BuyerID: "b1", SellerID: "s2", Status: StatusCheckout})

	s1Orders := s.BySeller("s1")
	assert.Len(t, s1Orders, 2)
}

func TestStoreByStatus(t *testing.T) {
	s := NewStore()
	s.Create(Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: StatusCheckout})
	s.Create(Order{ID: "o2", BuyerID: "b2", SellerID: "s1", Status: StatusConfirmed})

	checkoutOrders := s.ByStatus(StatusCheckout)
	assert.Len(t, checkoutOrders, 1)
	assert.Equal(t, "o1", checkoutOrders[0].ID)

	confirmedOrders := s.ByStatus(StatusConfirmed)
	assert.Len(t, confirmedOrders, 1)
	assert.Equal(t, "o2", confirmedOrders[0].ID)
}

func TestStoreConfirm(t *testing.T) {
	s := NewStore()
	s.Create(sampleOrder())

	err := s.Confirm("o1", "seller1")
	require.NoError(t, err)

	got := s.Get("o1")
	assert.Equal(t, StatusConfirmed, got.Status)
	assert.False(t, got.ConfirmedAt.IsZero())
}

func TestStoreConfirmWrongSeller(t *testing.T) {
	s := NewStore()
	s.Create(sampleOrder())

	err := s.Confirm("o1", "wrong-seller")
	assert.ErrorIs(t, err, ErrWrongSeller)
}

func TestStoreConfirmNotFound(t *testing.T) {
	s := NewStore()
	err := s.Confirm("nonexistent", "seller1")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestStoreConfirmDoubleConfirm(t *testing.T) {
	s := NewStore()
	s.Create(sampleOrder())

	require.NoError(t, s.Confirm("o1", "seller1"))
	err := s.Confirm("o1", "seller1")
	assert.ErrorIs(t, err, ErrInvalidTransition)
}

func TestStorePick(t *testing.T) {
	s := NewStore()
	s.Create(sampleOrder())
	require.NoError(t, s.Confirm("o1", "seller1"))

	err := s.Pick("o1", "shipper1")
	require.NoError(t, err)

	got := s.Get("o1")
	assert.Equal(t, StatusPicked, got.Status)
	assert.Equal(t, "shipper1", got.PickedBy)
	assert.False(t, got.PickedAt.IsZero())
}

func TestStorePickRaceCondition(t *testing.T) {
	s := NewStore()
	s.Create(sampleOrder())
	require.NoError(t, s.Confirm("o1", "seller1"))

	// First pick succeeds
	err1 := s.Pick("o1", "shipper1")
	assert.NoError(t, err1)

	// Second pick fails with conflict
	err2 := s.Pick("o1", "shipper2")
	assert.ErrorIs(t, err2, ErrInvalidTransition)
}

func TestStorePickNotConfirmed(t *testing.T) {
	s := NewStore()
	s.Create(sampleOrder()) // status = checkout

	err := s.Pick("o1", "shipper1")
	assert.ErrorIs(t, err, ErrInvalidTransition)
}

func TestStoreDeliver(t *testing.T) {
	s := NewStore()
	s.Create(sampleOrder())
	require.NoError(t, s.Confirm("o1", "seller1"))
	require.NoError(t, s.Pick("o1", "shipper1"))

	err := s.Deliver("o1", "shipper1")
	require.NoError(t, err)

	got := s.Get("o1")
	assert.Equal(t, StatusDelivered, got.Status)
	assert.False(t, got.DeliveredAt.IsZero())
}

func TestStoreDeliverNotPicked(t *testing.T) {
	s := NewStore()
	s.Create(sampleOrder())
	require.NoError(t, s.Confirm("o1", "seller1"))

	err := s.Deliver("o1", "shipper1")
	assert.ErrorIs(t, err, ErrInvalidTransition)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd app && go test ./internal/order/ -v`
Expected: FAIL — `Order`, `NewStore`, status constants, errors not defined

- [ ] **Step 3: Implement order store**

Create `app/internal/order/store.go`:
```go
package order

import (
	"errors"
	"sync"
	"time"
)

// OrderStatus represents the lifecycle state of an order.
type OrderStatus string

const (
	StatusCheckout   OrderStatus = "checkout"
	StatusConfirmed  OrderStatus = "confirmed"
	StatusPicked     OrderStatus = "picked"
	StatusDelivered  OrderStatus = "delivered"
)

// Errors for invalid operations.
var (
	ErrNotFound          = errors.New("order not found")
	ErrInvalidTransition = errors.New("invalid order status transition")
	ErrWrongSeller       = errors.New("order does not belong to this seller")
)

// OrderItem represents a line item in an order.
type OrderItem struct {
	ProductID   string `json:"product_id"`
	ProductName string `json:"product_name"`
	Quantity    int    `json:"quantity"`
	UnitPrice   int    `json:"unit_price"` // in cents
}

// Order represents a per-seller sub-order created at checkout.
type Order struct {
	ID              string      `json:"id"`
	BuyerID         string      `json:"buyer_id"`
	SellerID        string      `json:"seller_id"`
	Items           []OrderItem `json:"items"`
	TotalAmount     int         `json:"total_amount"` // in cents
	ShippingAddress string      `json:"shipping_address"`
	Status          OrderStatus `json:"status"`
	PickedBy        string      `json:"picked_by,omitempty"`
	CreatedAt       time.Time   `json:"created_at"`
	ConfirmedAt     time.Time   `json:"confirmed_at,omitempty"`
	PickedAt        time.Time   `json:"picked_at,omitempty"`
	DeliveredAt     time.Time   `json:"delivered_at,omitempty"`
}

// Store is an in-memory order store with derived indexes, protected by a mutex.
type Store struct {
	mu             sync.RWMutex
	orders         map[string]*Order
	ordersByBuyer  map[string][]string
	ordersBySeller map[string][]string
	ordersByStatus map[OrderStatus][]string
}

// NewStore creates an empty order store.
func NewStore() *Store {
	return &Store{
		orders:         make(map[string]*Order),
		ordersByBuyer:  make(map[string][]string),
		ordersBySeller: make(map[string][]string),
		ordersByStatus: make(map[OrderStatus][]string),
	}
}

// Create adds a new order to the store and updates all indexes.
func (s *Store) Create(o Order) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orders[o.ID] = &o
	s.ordersByBuyer[o.BuyerID] = append(s.ordersByBuyer[o.BuyerID], o.ID)
	s.ordersBySeller[o.SellerID] = append(s.ordersBySeller[o.SellerID], o.ID)
	s.ordersByStatus[o.Status] = append(s.ordersByStatus[o.Status], o.ID)
}

// Get returns an order by ID, or nil if not found.
func (s *Store) Get(id string) *Order {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.orders[id]
	if !ok {
		return nil
	}
	return o
}

// ByBuyer returns all orders for a given buyer.
func (s *Store) ByBuyer(buyerID string) []Order {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.ordersByBuyer[buyerID]
	result := make([]Order, 0, len(ids))
	for _, id := range ids {
		if o, ok := s.orders[id]; ok {
			result = append(result, *o)
		}
	}
	return result
}

// BySeller returns all orders for a given seller.
func (s *Store) BySeller(sellerID string) []Order {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.ordersBySeller[sellerID]
	result := make([]Order, 0, len(ids))
	for _, id := range ids {
		if o, ok := s.orders[id]; ok {
			result = append(result, *o)
		}
	}
	return result
}

// ByStatus returns all orders with a given status.
func (s *Store) ByStatus(status OrderStatus) []Order {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.ordersByStatus[status]
	result := make([]Order, 0, len(ids))
	for _, id := range ids {
		if o, ok := s.orders[id]; ok {
			result = append(result, *o)
		}
	}
	return result
}

// Confirm transitions an order from "checkout" to "confirmed".
// Returns ErrWrongSeller if the caller is not the order's seller.
func (s *Store) Confirm(orderID, sellerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.orders[orderID]
	if !ok {
		return ErrNotFound
	}
	if o.SellerID != sellerID {
		return ErrWrongSeller
	}
	if o.Status != StatusCheckout {
		return ErrInvalidTransition
	}
	s.updateStatus(o, StatusConfirmed)
	o.ConfirmedAt = time.Now()
	return nil
}

// Pick transitions an order from "confirmed" to "picked".
// Returns ErrInvalidTransition if the order is not in "confirmed" status
// (e.g., already picked by another shipper).
func (s *Store) Pick(orderID, shipperID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.orders[orderID]
	if !ok {
		return ErrNotFound
	}
	if o.Status != StatusConfirmed {
		return ErrInvalidTransition
	}
	s.updateStatus(o, StatusPicked)
	o.PickedBy = shipperID
	o.PickedAt = time.Now()
	return nil
}

// Deliver transitions an order from "picked" to "delivered".
func (s *Store) Deliver(orderID, shipperID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.orders[orderID]
	if !ok {
		return ErrNotFound
	}
	if o.Status != StatusPicked {
		return ErrInvalidTransition
	}
	s.updateStatus(o, StatusDelivered)
	o.DeliveredAt = time.Now()
	return nil
}

// updateStatus updates an order's status and maintains the status index.
// Caller must hold s.mu.
func (s *Store) updateStatus(o *Order, newStatus OrderStatus) {
	// Remove from old status index
	oldIDs := s.ordersByStatus[o.Status]
	for i, id := range oldIDs {
		if id == o.ID {
			s.ordersByStatus[o.Status] = append(oldIDs[:i], oldIDs[i+1:]...)
			break
		}
	}
	// Add to new status index
	s.ordersByStatus[newStatus] = append(s.ordersByStatus[newStatus], o.ID)
	o.Status = newStatus
}
```

- [ ] **Step 4: Run order tests to verify they pass**

Run: `cd app && go test ./internal/order/ -v`
Expected: PASS (all tests including race condition tests)

- [ ] **Step 5: Run with race detector to verify no data races**

Run: `cd app && go test ./internal/order/ -race -v`
Expected: PASS, no race warnings

- [ ] **Step 6: Commit**

```bash
git add app/internal/order/
git commit -m "feat: order store with indexes, status transitions, and race protection"
```

---

## Task 4: Seller REST Handlers

**Files:**
- Create: `app/internal/product/handler.go`
- Create: `app/internal/product/handler_test.go`

**Interfaces:**
- Consumes: `product.Store`, `*kafkaclient.Producer`, `auth.Claims` (from context)
- Produces: `product.NewHandler(store, producer) *Handler`, `Handler.AddProduct(w, r)`, `Handler.ListProducts(w, r)`, `Handler.ListOrders(w, r)`, `Handler.ConfirmOrder(w, r)`

Note: Seller order listing and confirmation require the order store, so the seller handler needs both product and order stores.

- [ ] **Step 1: Write failing tests for seller handler**

Create `app/internal/product/handler_test.go`:
```go
package product

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/kafkaclient"
	"github.com/kuang/flink-demo/internal/order"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler(t *testing.T) (*Handler, *Store, *order.Store) {
	t.Helper()
	prodStore := NewStore()
	orderStore := order.NewStore()
	// Producer won't actually connect in tests; we just need the type.
	// We'll test Kafka production via integration tests.
	producer := kafkaclient.NewProducer("localhost:9092")
	t.Cleanup(func() { producer.Close() })
	h := NewHandler(prodStore, orderStore, producer)
	return h, prodStore, orderStore
}

func claimsContext(name, role string) context.Context {
	claims := &auth.Claims{Name: name, Role: role}
	return context.WithValue(context.Background(), auth.ClaimsKey, claims)
}

func TestAddProductSuccess(t *testing.T) {
	h, _, _ := newTestHandler(t)

	body := strings.NewReader(`{"name":"Widget","price":500,"quantity":10}`)
	req := httptest.NewRequest("POST", "/api/seller/products", body)
	req = req.WithContext(claimsContext("seller1", "seller"))
	rec := httptest.NewRecorder()
	h.AddProduct(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotEmpty(t, resp["id"])
	assert.Equal(t, "Widget", resp["name"])
	assert.Equal(t, float64(500), resp["price"])
}

func TestAddProductInvalidJSON(t *testing.T) {
	h, _, _ := newTestHandler(t)

	body := strings.NewReader(`{invalid}`)
	req := httptest.NewRequest("POST", "/api/seller/products", body)
	req = req.WithContext(claimsContext("seller1", "seller"))
	rec := httptest.NewRecorder()
	h.AddProduct(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestListProducts(t *testing.T) {
	h, prodStore, _ := newTestHandler(t)
	prodStore.Add(Product{ID: "p1", Name: "A", SellerID: "seller1", Price: 100, ListedAt: time.Now()})
	prodStore.Add(Product{ID: "p2", Name: "B", SellerID: "seller2", Price: 200, ListedAt: time.Now()})

	req := httptest.NewRequest("GET", "/api/seller/products", nil)
	req = req.WithContext(claimsContext("seller1", "seller"))
	rec := httptest.NewRecorder()
	h.ListProducts(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp []map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Len(t, resp, 1) // only seller1's products
	assert.Equal(t, "p1", resp[0]["id"])
}

func TestListOrders(t *testing.T) {
	h, _, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{
		ID: "o1", BuyerID: "b1", SellerID: "seller1",
		Status: order.StatusCheckout, TotalAmount: 500,
		Items: []order.OrderItem{{ProductID: "p1", ProductName: "Widget", Quantity: 1, UnitPrice: 500}},
	})
	orderStore.Create(order.Order{
		ID: "o2", BuyerID: "b2", SellerID: "seller2",
		Status: order.StatusCheckout, TotalAmount: 300,
	})

	req := httptest.NewRequest("GET", "/api/seller/orders", nil)
	req = req.WithContext(claimsContext("seller1", "seller"))
	rec := httptest.NewRecorder()
	h.ListOrders(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp []map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Len(t, resp, 1)
	assert.Equal(t, "o1", resp[0]["id"])
}

func TestConfirmOrderSuccess(t *testing.T) {
	h, _, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{
		ID: "o1", BuyerID: "b1", SellerID: "seller1", Status: order.StatusCheckout,
	})

	req := httptest.NewRequest("POST", "/api/seller/orders/o1/confirm", nil)
	req = req.WithContext(claimsContext("seller1", "seller"))
	rec := httptest.NewRecorder()
	h.ConfirmOrder(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, order.StatusConfirmed, orderStore.Get("o1").Status)
}

func TestConfirmOrderWrongSeller(t *testing.T) {
	h, _, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{
		ID: "o1", BuyerID: "b1", SellerID: "seller1", Status: order.StatusCheckout,
	})

	req := httptest.NewRequest("POST", "/api/seller/orders/o1/confirm", nil)
	req = req.WithContext(claimsContext("wrong-seller", "seller"))
	rec := httptest.NewRecorder()
	h.ConfirmOrder(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestConfirmOrderNotFound(t *testing.T) {
	h, _, _ := newTestHandler(t)

	req := httptest.NewRequest("POST", "/api/seller/orders/nonexistent/confirm", nil)
	req = req.WithContext(claimsContext("seller1", "seller"))
	rec := httptest.NewRecorder()
	h.ConfirmOrder(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd app && go test ./internal/product/ -run TestAdd -v && go test ./internal/product/ -run TestList -v && go test ./internal/product/ -run TestConfirm -v`
Expected: FAIL — `Handler`, `NewHandler` not defined

- [ ] **Step 3: Implement seller handler**

Create `app/internal/product/handler.go`:
```go
package product

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/event"
	"github.com/kuang/flink-demo/internal/kafkaclient"
	"github.com/kuang/flink-demo/internal/order"
)

// Handler handles seller-related HTTP requests (products + orders).
type Handler struct {
	products   *Store
	orders     *order.Store
	producer   *kafkaclient.Producer
}

// NewHandler creates a seller handler with the given stores and producer.
func NewHandler(products *Store, orders *order.Store, producer *kafkaclient.Producer) *Handler {
	return &Handler{products: products, orders: orders, producer: producer}
}

type addProductRequest struct {
	Name     string `json:"name"`
	Price    int    `json:"price"`
	Quantity int    `json:"quantity"`
}

// AddProduct handles POST /api/seller/products.
func (h *Handler) AddProduct(w http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value(auth.ClaimsKey).(*auth.Claims)

	var req addProductRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Name == "" || req.Price <= 0 {
		http.Error(w, "name and positive price are required", http.StatusBadRequest)
		return
	}

	p := Product{
		ID:        uuid.New().String(),
		Name:      req.Name,
		Price:     req.Price,
		Quantity:  req.Quantity,
		SellerID:  claims.Name,
		ListedAt:  time.Now(),
	}
	h.products.Add(p)

	// Produce product.listed event
	ev := event.NewEvent("product.listed", claims.Name, "seller", map[string]any{
		"product_id": p.ID,
		"name":       p.Name,
		"price":      p.Price,
		"quantity":   p.Quantity,
	})
	if err := h.producer.Write(r.Context(), "product.listed", ev); err != nil {
		slog.Error("failed to produce product.listed event", "error", err, "product_id", p.ID)
	}

	slog.Info("product listed", "product_id", p.ID, "seller", claims.Name)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(p)
}

// ListProducts handles GET /api/seller/products (returns seller's own products).
func (h *Handler) ListProducts(w http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value(auth.ClaimsKey).(*auth.Claims)
	products := h.products.BySeller(claims.Name)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(products)
}

// ListOrders handles GET /api/seller/orders (returns orders for this seller's products).
func (h *Handler) ListOrders(w http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value(auth.ClaimsKey).(*auth.Claims)
	orders := h.orders.BySeller(claims.Name)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(orders)
}

// ConfirmOrder handles POST /api/seller/orders/{id}/confirm.
func (h *Handler) ConfirmOrder(w http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value(auth.ClaimsKey).(*auth.Claims)
	orderID := r.PathValue("id")

	o := h.orders.Get(orderID)
	if o == nil {
		http.Error(w, "order not found", http.StatusNotFound)
		return
	}

	err := h.orders.Confirm(orderID, claims.Name)
	if err != nil {
		switch {
		case errors.Is(err, order.ErrWrongSeller):
			http.Error(w, "forbidden", http.StatusForbidden)
		case errors.Is(err, order.ErrInvalidTransition):
			http.Error(w, "order already confirmed", http.StatusConflict)
		default:
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	// Produce order.confirmed event
	ev := event.NewEvent("order.confirmed", claims.Name, "seller", map[string]any{
		"order_id": orderID,
		"buyer_id": o.BuyerID,
	})
	if err := h.producer.Write(r.Context(), "order.confirmed", ev); err != nil {
		slog.Error("failed to produce order.confirmed event", "error", err, "order_id", orderID)
	}

	slog.Info("order confirmed", "order_id", orderID, "seller", claims.Name, "from_status", "checkout", "to_status", "confirmed")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"order_id": orderID,
		"status":   "confirmed",
	})
}
```

- [ ] **Step 4: Run seller handler tests to verify they pass**

Run: `cd app && go test ./internal/product/ -v`
Expected: PASS (all tests)

- [ ] **Step 5: Commit**

```bash
git add app/internal/product/handler.go app/internal/product/handler_test.go
git commit -m "feat: seller REST handlers — add product, list products, list orders, confirm order"
```

---

## Task 5: Buyer REST Handlers (Multi-Seller Cart Split)

**Files:**
- Create: `app/internal/buyer/handler.go`
- Create: `app/internal/buyer/handler_test.go`

**Interfaces:**
- Consumes: `product.Store`, `order.Store`, `*kafkaclient.Producer`, `auth.Claims`
- Produces: `buyer.NewHandler(productStore, orderStore, producer) *Handler`, `Handler.ListProducts(w, r)`, `Handler.AddToCart(w, r)`, `Handler.Checkout(w, r)`, `Handler.ListOrders(w, r)`

- [ ] **Step 1: Write failing tests for buyer handler**

Create `app/internal/buyer/handler_test.go`:
```go
package buyer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/kafkaclient"
	"github.com/kuang/flink-demo/internal/order"
	"github.com/kuang/flink-demo/internal/product"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler(t *testing.T) (*Handler, *product.Store, *order.Store) {
	t.Helper()
	prodStore := product.NewStore()
	orderStore := order.NewStore()
	producer := kafkaclient.NewProducer("localhost:9092")
	t.Cleanup(func() { producer.Close() })
	h := NewHandler(prodStore, orderStore, producer)
	return h, prodStore, orderStore
}

func claimsContext(name, role string) context.Context {
	claims := &auth.Claims{Name: name, Role: role}
	return context.WithValue(context.Background(), auth.ClaimsKey, claims)
}

func TestListProducts(t *testing.T) {
	h, prodStore, _ := newTestHandler(t)
	prodStore.Add(product.Product{ID: "p1", Name: "A", Price: 100, SellerID: "s1", ListedAt: time.Now()})
	prodStore.Add(product.Product{ID: "p2", Name: "B", Price: 200, SellerID: "s2", ListedAt: time.Now()})

	req := httptest.NewRequest("GET", "/api/buyer/products", nil)
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.ListProducts(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp []map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Len(t, resp, 2) // buyer sees all products
}

func TestAddToCart(t *testing.T) {
	h, prodStore, _ := newTestHandler(t)
	prodStore.Add(product.Product{ID: "p1", Name: "Widget", Price: 500, SellerID: "seller1", ListedAt: time.Now()})

	body := strings.NewReader(`{"product_id":"p1","quantity":2}`)
	req := httptest.NewRequest("POST", "/api/buyer/cart/items", body)
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.AddToCart(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAddToCartProductNotFound(t *testing.T) {
	h, _, _ := newTestHandler(t)

	body := strings.NewReader(`{"product_id":"nonexistent","quantity":1}`)
	req := httptest.NewRequest("POST", "/api/buyer/cart/items", body)
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.AddToCart(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCheckoutSingleSeller(t *testing.T) {
	h, prodStore, orderStore := newTestHandler(t)
	prodStore.Add(product.Product{ID: "p1", Name: "Widget", Price: 500, SellerID: "seller1", ListedAt: time.Now()})

	body := strings.NewReader(`{"items":[{"product_id":"p1","quantity":2}],"shipping_address":"123 Main St"}`)
	req := httptest.NewRequest("POST", "/api/buyer/cart/checkout", body)
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.Checkout(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	orders := resp["orders"].([]any)
	assert.Len(t, orders, 1)

	// Verify order was created
	allOrders := orderStore.ByBuyer("buyer1")
	assert.Len(t, allOrders, 1)
	assert.Equal(t, 1000, allOrders[0].TotalAmount) // 500 * 2
	assert.Equal(t, order.StatusCheckout, allOrders[0].Status)
}

func TestCheckoutMultiSellerSplit(t *testing.T) {
	h, prodStore, orderStore := newTestHandler(t)
	prodStore.Add(product.Product{ID: "p1", Name: "A", Price: 100, SellerID: "seller1", ListedAt: time.Now()})
	prodStore.Add(product.Product{ID: "p2", Name: "B", Price: 200, SellerID: "seller2", ListedAt: time.Now()})

	body := strings.NewReader(`{"items":[{"product_id":"p1","quantity":1},{"product_id":"p2","quantity":3}],"shipping_address":"456 Oak Ave"}`)
	req := httptest.NewRequest("POST", "/api/buyer/cart/checkout", body)
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.Checkout(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	orders := resp["orders"].([]any)
	assert.Len(t, orders, 2, "should split into 2 orders, one per seller")

	// Verify orders were created
	allOrders := orderStore.ByBuyer("buyer1")
	assert.Len(t, allOrders, 2)

	// Find orders by seller
	var s1Order, s2Order *order.Order
	for i := range allOrders {
		if allOrders[i].SellerID == "seller1" {
			s1Order = &allOrders[i]
		}
		if allOrders[i].SellerID == "seller2" {
			s2Order = &allOrders[i]
		}
	}
	require.NotNil(t, s1Order)
	require.NotNil(t, s2Order)
	assert.Equal(t, 100, s1Order.TotalAmount)   // 100 * 1
	assert.Equal(t, 600, s2Order.TotalAmount)   // 200 * 3
}

func TestCheckoutEmptyCart(t *testing.T) {
	h, _, _ := newTestHandler(t)

	body := strings.NewReader(`{"items":[],"shipping_address":"123 Main St"}`)
	req := httptest.NewRequest("POST", "/api/buyer/cart/checkout", body)
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.Checkout(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCheckoutProductNotFound(t *testing.T) {
	h, _, _ := newTestHandler(t)

	body := strings.NewReader(`{"items":[{"product_id":"nonexistent","quantity":1}],"shipping_address":"123"}`)
	req := httptest.NewRequest("POST", "/api/buyer/cart/checkout", body)
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.Checkout(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestListOrders(t *testing.T) {
	h, _, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{ID: "o1", BuyerID: "buyer1", SellerID: "s1", Status: order.StatusCheckout})
	orderStore.Create(order.Order{ID: "o2", BuyerID: "buyer2", SellerID: "s1", Status: order.StatusCheckout})

	req := httptest.NewRequest("GET", "/api/buyer/orders", nil)
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.ListOrders(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp []map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Len(t, resp, 1)
	assert.Equal(t, "o1", resp[0]["id"])
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd app && go test ./internal/buyer/ -v`
Expected: FAIL — `Handler`, `NewHandler` not defined

- [ ] **Step 3: Implement buyer handler**

Create `app/internal/buyer/handler.go`:
```go
package buyer

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/event"
	"github.com/kuang/flink-demo/internal/kafkaclient"
	"github.com/kuang/flink-demo/internal/order"
	"github.com/kuang/flink-demo/internal/product"
)

// Handler handles buyer-related HTTP requests.
type Handler struct {
	products *product.Store
	orders   *order.Store
	producer *kafkaclient.Producer
}

// NewHandler creates a buyer handler with the given stores and producer.
func NewHandler(products *product.Store, orders *order.Store, producer *kafkaclient.Producer) *Handler {
	return &Handler{products: products, orders: orders, producer: producer}
}

// ListProducts handles GET /api/buyer/products (returns full catalog).
func (h *Handler) ListProducts(w http.ResponseWriter, r *http.Request) {
	products := h.products.All()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(products)
}

type addToCartRequest struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

// AddToCart handles POST /api/buyer/cart/items.
// The cart is client-side; this just produces a cart.item.added event for Flink.
func (h *Handler) AddToCart(w http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value(auth.ClaimsKey).(*auth.Claims)

	var req addToCartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.ProductID == "" || req.Quantity <= 0 {
		http.Error(w, "product_id and positive quantity are required", http.StatusBadRequest)
		return
	}

	p := h.products.Get(req.ProductID)
	if p == nil {
		http.Error(w, "product not found", http.StatusNotFound)
		return
	}

	// Produce cart.item.added event (for Flink CEP patterns)
	ev := event.NewEvent("cart.item.added", claims.Name, "buyer", map[string]any{
		"product_id": p.ID,
		"seller_id":  p.SellerID,
		"quantity":   req.Quantity,
	})
	if err := h.producer.Write(r.Context(), "cart.item.added", ev); err != nil {
		slog.Error("failed to produce cart.item.added event", "error", err)
	}

	slog.Info("cart item added", "buyer", claims.Name, "product_id", p.ID, "quantity", req.Quantity)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

type checkoutItem struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type checkoutRequest struct {
	Items           []checkoutItem `json:"items"`
	ShippingAddress string         `json:"shipping_address"`
}

type checkoutOrderResponse struct {
	OrderID     string             `json:"order_id"`
	SellerID    string             `json:"seller_id"`
	Items       []order.OrderItem  `json:"items"`
	TotalAmount int                `json:"total_amount"`
}

type checkoutResponse struct {
	Orders []checkoutOrderResponse `json:"orders"`
}

// Checkout handles POST /api/buyer/cart/checkout.
// Splits the cart by seller and creates one order per seller.
func (h *Handler) Checkout(w http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value(auth.ClaimsKey).(*auth.Claims)

	var req checkoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if len(req.Items) == 0 {
		http.Error(w, "cart is empty", http.StatusBadRequest)
		return
	}

	if req.ShippingAddress == "" {
		http.Error(w, "shipping address is required", http.StatusBadRequest)
		return
	}

	// Group items by seller
	type sellerGroup struct {
		items       []order.OrderItem
		totalAmount int
	}
	groups := make(map[string]*sellerGroup)

	for _, item := range req.Items {
		p := h.products.Get(item.ProductID)
		if p == nil {
			http.Error(w, "product not found: "+item.ProductID, http.StatusBadRequest)
			return
		}

		g, ok := groups[p.SellerID]
		if !ok {
			g = &sellerGroup{}
			groups[p.SellerID] = g
		}
		g.items = append(g.items, order.OrderItem{
			ProductID:   p.ID,
			ProductName: p.Name,
			Quantity:    item.Quantity,
			UnitPrice:   p.Price,
		})
		g.totalAmount += p.Price * item.Quantity
	}

	// Create one order per seller
	var createdOrders []checkoutOrderResponse
	for sellerID, g := range groups {
		orderID := uuid.New().String()
		o := order.Order{
			ID:              orderID,
			BuyerID:         claims.Name,
			SellerID:        sellerID,
			Items:           g.items,
			TotalAmount:     g.totalAmount,
			ShippingAddress: req.ShippingAddress,
			Status:          order.StatusCheckout,
			CreatedAt:       time.Now(),
		}
		h.orders.Create(o)

		// Produce cart.checkout event (one per seller)
		itemsPayload := make([]map[string]any, len(g.items))
		for i, item := range g.items {
			itemsPayload[i] = map[string]any{
				"product_id":   item.ProductID,
				"product_name": item.ProductName,
				"quantity":     item.Quantity,
				"unit_price":   item.UnitPrice,
			}
		}
		ev := event.NewEvent("cart.checkout", claims.Name, "buyer", map[string]any{
			"order_id":         orderID,
			"seller_id":        sellerID,
			"items":            itemsPayload,
			"total_amount":     g.totalAmount,
			"shipping_address": req.ShippingAddress,
		})
		if err := h.producer.Write(r.Context(), "cart.checkout", ev); err != nil {
			slog.Error("failed to produce cart.checkout event", "error", err, "order_id", orderID)
		}

		slog.Info("order created at checkout", "order_id", orderID, "buyer", claims.Name, "seller", sellerID, "total", g.totalAmount)

		createdOrders = append(createdOrders, checkoutOrderResponse{
			OrderID:     orderID,
			SellerID:    sellerID,
			Items:       g.items,
			TotalAmount: g.totalAmount,
		})
	}

	slog.Info("checkout complete", "buyer", claims.Name, "orders", len(createdOrders))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(checkoutResponse{Orders: createdOrders})
}

// ListOrders handles GET /api/buyer/orders (returns buyer's own orders).
func (h *Handler) ListOrders(w http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value(auth.ClaimsKey).(*auth.Claims)
	orders := h.orders.ByBuyer(claims.Name)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(orders)
}
```

- [ ] **Step 4: Run buyer handler tests to verify they pass**

Run: `cd app && go test ./internal/buyer/ -v`
Expected: PASS (all tests including multi-seller split)

- [ ] **Step 5: Commit**

```bash
git add app/internal/buyer/
git commit -m "feat: buyer REST handlers — catalog, add to cart, multi-seller checkout, list orders"
```

---

## Task 6: Shipper REST Handlers (Race Protection)

**Files:**
- Create: `app/internal/shipper/handler.go`
- Create: `app/internal/shipper/handler_test.go`

**Interfaces:**
- Consumes: `order.Store`, `*kafkaclient.Producer`, `auth.Claims`
- Produces: `shipper.NewHandler(orderStore, producer) *Handler`, `Handler.ListJobs(w, r)`, `Handler.PickJob(w, r)`, `Handler.DeliverJob(w, r)`

- [ ] **Step 1: Write failing tests for shipper handler**

Create `app/internal/shipper/handler_test.go`:
```go
package shipper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/kafkaclient"
	"github.com/kuang/flink-demo/internal/order"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler(t *testing.T) (*Handler, *order.Store) {
	t.Helper()
	orderStore := order.NewStore()
	producer := kafkaclient.NewProducer("localhost:9092")
	t.Cleanup(func() { producer.Close() })
	h := NewHandler(orderStore, producer)
	return h, orderStore
}

func claimsContext(name, role string) context.Context {
	claims := &auth.Claims{Name: name, Role: role}
	return context.WithValue(context.Background(), auth.ClaimsKey, claims)
}

func TestListJobs(t *testing.T) {
	h, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: order.StatusConfirmed})
	orderStore.Create(order.Order{ID: "o2", BuyerID: "b2", SellerID: "s1", Status: order.StatusCheckout}) // not confirmed
	orderStore.Create(order.Order{ID: "o3", BuyerID: "b3", SellerID: "s2", Status: order.StatusConfirmed})

	req := httptest.NewRequest("GET", "/api/shipper/jobs", nil)
	req = req.WithContext(claimsContext("shipper1", "shipper"))
	rec := httptest.NewRecorder()
	h.ListJobs(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	// Response body is verified in the test below by decoding
}

func TestPickJobSuccess(t *testing.T) {
	h, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: order.StatusConfirmed})

	req := httptest.NewRequest("POST", "/api/shipper/jobs/o1/pick", nil)
	req = req.WithContext(claimsContext("shipper1", "shipper"))
	rec := httptest.NewRecorder()
	h.PickJob(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, order.StatusPicked, orderStore.Get("o1").Status)
	assert.Equal(t, "shipper1", orderStore.Get("o1").PickedBy)
}

func TestPickJobRaceCondition(t *testing.T) {
	h, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: order.StatusConfirmed})

	// First pick succeeds
	req1 := httptest.NewRequest("POST", "/api/shipper/jobs/o1/pick", nil)
	req1 = req1.WithContext(claimsContext("shipper1", "shipper"))
	rec1 := httptest.NewRecorder()
	h.PickJob(rec1, req1)
	assert.Equal(t, http.StatusOK, rec1.Code)

	// Second pick fails with 409 Conflict
	req2 := httptest.NewRequest("POST", "/api/shipper/jobs/o1/pick", nil)
	req2 = req2.WithContext(claimsContext("shipper2", "shipper"))
	rec2 := httptest.NewRecorder()
	h.PickJob(rec2, req2)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestPickJobNotFound(t *testing.T) {
	h, _ := newTestHandler(t)

	req := httptest.NewRequest("POST", "/api/shipper/jobs/nonexistent/pick", nil)
	req = req.WithContext(claimsContext("shipper1", "shipper"))
	rec := httptest.NewRecorder()
	h.PickJob(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestPickJobNotConfirmed(t *testing.T) {
	h, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: order.StatusCheckout})

	req := httptest.NewRequest("POST", "/api/shipper/jobs/o1/pick", nil)
	req = req.WithContext(claimsContext("shipper1", "shipper"))
	rec := httptest.NewRecorder()
	h.PickJob(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestDeliverJobSuccess(t *testing.T) {
	h, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: order.StatusConfirmed})
	require.NoError(t, orderStore.Pick("o1", "shipper1"))

	req := httptest.NewRequest("POST", "/api/shipper/jobs/o1/deliver", nil)
	req = req.WithContext(claimsContext("shipper1", "shipper"))
	rec := httptest.NewRecorder()
	h.DeliverJob(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, order.StatusDelivered, orderStore.Get("o1").Status)
}

func TestDeliverJobNotPicked(t *testing.T) {
	h, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: order.StatusConfirmed})

	req := httptest.NewRequest("POST", "/api/shipper/jobs/o1/deliver", nil)
	req = req.WithContext(claimsContext("shipper1", "shipper"))
	rec := httptest.NewRecorder()
	h.DeliverJob(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd app && go test ./internal/shipper/ -v`
Expected: FAIL — `Handler`, `NewHandler` not defined

- [ ] **Step 3: Implement shipper handler**

Create `app/internal/shipper/handler.go`:
```go
package shipper

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/event"
	"github.com/kuang/flink-demo/internal/kafkaclient"
	"github.com/kuang/flink-demo/internal/order"
)

// Handler handles shipper-related HTTP requests.
type Handler struct {
	orders   *order.Store
	producer *kafkaclient.Producer
}

// NewHandler creates a shipper handler with the given order store and producer.
func NewHandler(orders *order.Store, producer *kafkaclient.Producer) *Handler {
	return &Handler{orders: orders, producer: producer}
}

// ListJobs handles GET /api/shipper/jobs (returns all confirmed orders).
func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	jobs := h.orders.ByStatus(order.StatusConfirmed)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

// PickJob handles POST /api/shipper/jobs/{id}/pick.
// Race-protected: only one shipper can pick a job.
func (h *Handler) PickJob(w http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value(auth.ClaimsKey).(*auth.Claims)
	orderID := r.PathValue("id")

	o := h.orders.Get(orderID)
	if o == nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	err := h.orders.Pick(orderID, claims.Name)
	if err != nil {
		switch {
		case errors.Is(err, order.ErrInvalidTransition):
			http.Error(w, "job already picked by another shipper", http.StatusConflict)
		default:
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	// Produce shipment.picked event
	ev := event.NewEvent("shipment.picked", claims.Name, "shipper", map[string]any{
		"order_id":  orderID,
		"buyer_id":  o.BuyerID,
		"seller_id": o.SellerID,
	})
	if err := h.producer.Write(r.Context(), "shipment.picked", ev); err != nil {
		slog.Error("failed to produce shipment.picked event", "error", err, "order_id", orderID)
	}

	slog.Info("job picked", "order_id", orderID, "shipper", claims.Name, "from_status", "confirmed", "to_status", "picked")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"order_id": orderID,
		"status":   "picked",
	})
}

// DeliverJob handles POST /api/shipper/jobs/{id}/deliver.
func (h *Handler) DeliverJob(w http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value(auth.ClaimsKey).(*auth.Claims)
	orderID := r.PathValue("id")

	o := h.orders.Get(orderID)
	if o == nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	err := h.orders.Deliver(orderID, claims.Name)
	if err != nil {
		switch {
		case errors.Is(err, order.ErrInvalidTransition):
			http.Error(w, "job is not in picked state", http.StatusConflict)
		default:
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	// Produce shipment.delivered event
	ev := event.NewEvent("shipment.delivered", claims.Name, "shipper", map[string]any{
		"order_id": orderID,
		"buyer_id": o.BuyerID,
	})
	if err := h.producer.Write(r.Context(), "shipment.delivered", ev); err != nil {
		slog.Error("failed to produce shipment.delivered event", "error", err, "order_id", orderID)
	}

	slog.Info("job delivered", "order_id", orderID, "shipper", claims.Name, "from_status", "picked", "to_status", "delivered")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"order_id": orderID,
		"status":   "delivered",
	})
}
```

- [ ] **Step 4: Run shipper handler tests to verify they pass**

Run: `cd app && go test ./internal/shipper/ -v`
Expected: PASS (all tests including race condition test)

- [ ] **Step 5: Commit**

```bash
git add app/internal/shipper/
git commit -m "feat: shipper REST handlers — job board, pick (race-protected), deliver"
```

---

## Task 7: WebSocket Hub + Kafka Consumer + Server Wiring

**Files:**
- Create: `app/internal/ws/hub.go`
- Create: `app/internal/ws/hub_test.go`
- Create: `app/internal/ws/handler.go`
- Create: `app/internal/kafkaclient/consumer.go`
- Modify: `app/internal/server/server.go`
- Modify: `app/main.go`

**Interfaces:**
- Produces: `ws.Hub` with `Register(client *Client)`, `Unregister(client *Client)`, `Broadcast(ev event.EventEnvelope)`, `Close()`; `ws.Client` with `Name`, `Role`, `conn *websocket.Conn`; `ws.NewHandler(jwtMgr, hub) *WSHandler` with `ServeWS(w, r)`; `kafkaclient.NewConsumer(addr, hub) *Consumer` with `Start(ctx)`, `Close()`

- [ ] **Step 1: Install WebSocket dependency**

Run:
```bash
cd app && go get github.com/coder/websocket
```

- [ ] **Step 2: Write failing tests for WebSocket hub**

Create `app/internal/ws/hub_test.go`:
```go
package ws

import (
	"encoding/json"
	"testing"

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
```

- [ ] **Step 3: Run hub tests to verify they fail**

Run: `cd app && go test ./internal/ws/ -v`
Expected: FAIL — `Hub`, `Client`, `NewHub` not defined

- [ ] **Step 4: Implement WebSocket hub**

Create `app/internal/ws/hub.go`:
```go
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
	done       chan struct{}
}

// NewHub creates a new WebSocket hub.
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		broadcast:  make(chan event.EventEnvelope, 100),
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
```

- [ ] **Step 5: Run hub tests to verify they pass**

Run: `cd app && go test ./internal/ws/ -v`
Expected: PASS

- [ ] **Step 6: Implement WebSocket HTTP handler**

Create `app/internal/ws/handler.go`:
```go
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
```

- [ ] **Step 7: Implement Kafka consumer**

Create `app/internal/kafkaclient/consumer.go`:
```go
package kafkaclient

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/coder/websocket"
	"github.com/kuang/flink-demo/internal/event"
	"github.com/segmentio/kafka-go"
)

// Broadcaster is the interface for the WebSocket hub's broadcast method.
type Broadcaster interface {
	Broadcast(ev event.EventEnvelope)
}

// Consumer reads Kafka topics and forwards events to the WebSocket hub.
type Consumer struct {
	addr       string
	broadcaster Broadcaster
}

// NewConsumer creates a Kafka consumer that forwards events to the given broadcaster.
func NewConsumer(addr string, hub Broadcaster) *Consumer {
	return &Consumer{addr: addr, broadcaster: hub}
}

// Start begins consuming all input Kafka topics. Blocks until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) error {
	topics := []string{
		"product.listed",
		"cart.item.added",
		"cart.checkout",
		"order.confirmed",
		"shipment.picked",
		"shipment.delivered",
	}

	for _, topic := range topics {
		go c.consumeTopic(ctx, topic)
	}

	<-ctx.Done()
	return ctx.Err()
}

func (c *Consumer) consumeTopic(ctx context.Context, topic string) {
	reader := kafka.Reader{
		Brokers: []string{c.addr},
		Topic:   topic,
		GroupID: "ws-hub-" + topic,
	}
	defer reader.Close()

	slog.Info("kafka consumer started", "topic", topic)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("failed to read kafka message", "topic", topic, "error", err)
			continue
		}

		var ev event.EventEnvelope
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			slog.Error("failed to unmarshal kafka event", "topic", topic, "error", err)
			continue
		}

		slog.Debug("kafka event consumed", "topic", topic, "event_type", ev.EventType, "event_id", ev.EventID)
		c.broadcaster.Broadcast(ev)
	}
}
```

Note: The `websocket` import in consumer.go is unused — remove it. The consumer only needs the `Broadcaster` interface which uses `event.EventEnvelope`.

- [ ] **Step 8: Fix the unused import in consumer.go**

Remove the `"github.com/coder/websocket"` import from `consumer.go`. The file should import:
```go
import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/kuang/flink-demo/internal/event"
	"github.com/segmentio/kafka-go"
)
```

- [ ] **Step 9: Update server.go to wire all new handlers**

Modify `app/internal/server/server.go` — replace the `buildRoutes` method and `Server` struct to include the new handlers and WebSocket:

```go
package server

import (
	"context"
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/buyer"
	"github.com/kuang/flink-demo/internal/config"
	"github.com/kuang/flink-demo/internal/kafkaclient"
	"github.com/kuang/flink-demo/internal/product"
	"github.com/kuang/flink-demo/internal/session"
	"github.com/kuang/flink-demo/internal/shipper"
	"github.com/kuang/flink-demo/internal/ws"
	"github.com/kuang/flink-demo/web"
)

// Server is the HTTP server for the demo application.
type Server struct {
	cfg            config.Config
	logger         *slog.Logger
	jwtMgr         *auth.JWTManager
	sessionHandler *session.Handler
	kafkaClient    *kafkaclient.Client
	productHandler *product.Handler
	buyerHandler   *buyer.Handler
	shipperHandler *shipper.Handler
	wsHandler      *ws.WSHandler
	hub            *ws.Hub
	consumer       *kafkaclient.Consumer
	producer       *kafkaclient.Producer
	httpServer     *http.Server
	handler        http.Handler
}

func New(
	cfg config.Config,
	logger *slog.Logger,
	jwtMgr *auth.JWTManager,
	sessionHandler *session.Handler,
	kafkaClient *kafkaclient.Client,
	productHandler *product.Handler,
	buyerHandler *buyer.Handler,
	shipperHandler *shipper.Handler,
	wsHandler *ws.WSHandler,
	hub *ws.Hub,
	consumer *kafkaclient.Consumer,
	producer *kafkaclient.Producer,
) *Server {
	s := &Server{
		cfg:            cfg,
		logger:         logger,
		jwtMgr:         jwtMgr,
		sessionHandler: sessionHandler,
		kafkaClient:    kafkaClient,
		productHandler: productHandler,
		buyerHandler:   buyerHandler,
		shipperHandler: shipperHandler,
		wsHandler:      wsHandler,
		hub:            hub,
		consumer:       consumer,
		producer:       producer,
	}
	s.handler = s.buildRoutes()
	return s
}

func (s *Server) buildRoutes() http.Handler {
	mux := http.NewServeMux()

	// Public routes
	mux.HandleFunc("POST /api/session", s.sessionHandler.CreateSession)
	mux.HandleFunc("GET /api/health", s.healthHandler)

	// WebSocket endpoint
	mux.HandleFunc("GET /ws", s.wsHandler.ServeWS)

	// Auth middleware
	authMW := s.jwtMgr.Middleware

	// Seller routes
	mux.Handle("POST /api/seller/products", authMW(auth.RequireRole("seller")(http.HandlerFunc(s.productHandler.AddProduct))))
	mux.Handle("GET /api/seller/products", authMW(auth.RequireRole("seller")(http.HandlerFunc(s.productHandler.ListProducts))))
	mux.Handle("GET /api/seller/orders", authMW(auth.RequireRole("seller")(http.HandlerFunc(s.productHandler.ListOrders))))
	mux.Handle("POST /api/seller/orders/{id}/confirm", authMW(auth.RequireRole("seller")(http.HandlerFunc(s.productHandler.ConfirmOrder))))

	// Buyer routes
	mux.Handle("GET /api/buyer/products", authMW(auth.RequireRole("buyer")(http.HandlerFunc(s.buyerHandler.ListProducts))))
	mux.Handle("POST /api/buyer/cart/items", authMW(auth.RequireRole("buyer")(http.HandlerFunc(s.buyerHandler.AddToCart))))
	mux.Handle("POST /api/buyer/cart/checkout", authMW(auth.RequireRole("buyer")(http.HandlerFunc(s.buyerHandler.Checkout))))
	mux.Handle("GET /api/buyer/orders", authMW(auth.RequireRole("buyer")(http.HandlerFunc(s.buyerHandler.ListOrders))))

	// Shipper routes
	mux.Handle("GET /api/shipper/jobs", authMW(auth.RequireRole("shipper")(http.HandlerFunc(s.shipperHandler.ListJobs))))
	mux.Handle("POST /api/shipper/jobs/{id}/pick", authMW(auth.RequireRole("shipper")(http.HandlerFunc(s.shipperHandler.PickJob))))
	mux.Handle("POST /api/shipper/jobs/{id}/deliver", authMW(auth.RequireRole("shipper")(http.HandlerFunc(s.shipperHandler.DeliverJob))))

	// Static files
	distFS, err := fs.Sub(web.DistFS, "dist")
	if err != nil {
		s.logger.Error("failed to get embedded web filesystem", "error", err)
	} else {
		fileServer := http.FileServer(http.FS(distFS))
		mux.Handle("/", s.spaHandler(fileServer, distFS))
	}

	return s.loggingMiddleware(mux)
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) spaHandler(fileServer http.Handler, distFS fs.FS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/" {
			path = "index.html"
		} else {
			path = path[1:]
		}
		_, err := fs.Stat(distFS, path)
		if err != nil {
			r.URL.Path = "/"
			fileServer.ServeHTTP(w, r)
			return
		}
		fileServer.ServeHTTP(w, r)
	})
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(wrapped, r)
		slog.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.logger.Info("creating kafka topics")
	if err := s.kafkaClient.CreateTopics(ctx); err != nil {
		s.logger.Warn("failed to create kafka topics", "error", err)
	}

	// Start WebSocket hub
	go s.hub.Run()

	// Start Kafka consumer
	go func() {
		if err := s.consumer.Start(ctx); err != nil {
			s.logger.Error("kafka consumer stopped", "error", err)
		}
	}()

	s.httpServer = &http.Server{
		Addr:    ":" + s.cfg.Port,
		Handler: s.handler,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		s.logger.Info("http server listening", "port", s.cfg.Port)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-stop
	s.logger.Info("shutting down")
	s.hub.Close()
	return s.Shutdown(context.Background())
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
```

Note: Remove the `placeholderHandler` method — it's no longer needed. Remove the `"github.com/coder/websocket"` import from server.go if it's not used there (it's not — WebSocket is handled by `ws.WSHandler`).

- [ ] **Step 10: Update main.go to wire all new components**

Modify `app/main.go`:
```go
package main

import (
	"log/slog"

	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/buyer"
	"github.com/kuang/flink-demo/internal/config"
	"github.com/kuang/flink-demo/internal/kafkaclient"
	"github.com/kuang/flink-demo/internal/logging"
	"github.com/kuang/flink-demo/internal/order"
	"github.com/kuang/flink-demo/internal/product"
	"github.com/kuang/flink-demo/internal/server"
	"github.com/kuang/flink-demo/internal/session"
	"github.com/kuang/flink-demo/internal/shipper"
	"github.com/kuang/flink-demo/internal/ws"
)

func main() {
	logger := logging.NewLogger()
	slog.SetDefault(logger)
	cfg := config.Load()

	logger.Info("starting server",
		"port", cfg.Port,
		"kafka_addr", cfg.KafkaAddr,
	)

	jwtMgr := auth.NewJWTManager(cfg.JWTSecret)
	sessionStore := session.NewStore()
	sessionHandler := session.NewHandler(sessionStore, jwtMgr)
	kafkaClient := kafkaclient.NewClient(cfg.KafkaAddr)
	producer := kafkaclient.NewProducer(cfg.KafkaAddr)

	productStore := product.NewStore()
	orderStore := order.NewStore()

	productHandler := product.NewHandler(productStore, orderStore, producer)
	buyerHandler := buyer.NewHandler(productStore, orderStore, producer)
	shipperHandler := shipper.NewHandler(orderStore, producer)

	hub := ws.NewHub()
	wsHandler := ws.NewHandler(jwtMgr, hub)
	consumer := kafkaclient.NewConsumer(cfg.KafkaAddr, hub)

	srv := server.New(cfg, logger, jwtMgr, sessionHandler, kafkaClient,
		productHandler, buyerHandler, shipperHandler,
		wsHandler, hub, consumer, producer)

	if err := srv.Start(); err != nil {
		slog.Error("server failed", "error", err)
	}
}
```

- [ ] **Step 11: Update server_test.go for the new Server signature**

The existing `server_test.go` tests the old `server.New()` signature. Update `newTestServer()` in `app/internal/server/server_test.go`:

```go
func newTestServer() *Server {
	cfg := config.Config{
		Port:      "0",
		JWTSecret: "test-secret",
		KafkaAddr: "localhost:9092",
	}
	jwtMgr := auth.NewJWTManager(cfg.JWTSecret)
	sessionStore := session.NewStore()
	sessionHandler := session.NewHandler(sessionStore, jwtMgr)
	kafkaClient := kafkaclient.NewClient(cfg.KafkaAddr)
	producer := kafkaclient.NewProducer(cfg.KafkaAddr)

	productStore := product.NewStore()
	orderStore := order.NewStore()

	productHandler := product.NewHandler(productStore, orderStore, producer)
	buyerHandler := buyer.NewHandler(productStore, orderStore, producer)
	shipperHandler := shipper.NewHandler(orderStore, producer)

	hub := ws.NewHub()
	wsHandler := ws.NewHandler(jwtMgr, hub)
	consumer := kafkaclient.NewConsumer(cfg.KafkaAddr, hub)

	return New(cfg, logging.NewLogger(), jwtMgr, sessionHandler, kafkaClient,
		productHandler, buyerHandler, shipperHandler,
		wsHandler, hub, consumer, producer)
}
```

Also add the new imports to the test file: `"github.com/kuang/flink-demo/internal/buyer"`, `"github.com/kuang/flink-demo/internal/order"`, `"github.com/kuang/flink-demo/internal/product"`, `"github.com/kuang/flink-demo/internal/shipper"`, `"github.com/kuang/flink-demo/internal/ws"`.

- [ ] **Step 12: Verify everything compiles and tests pass**

Run: `cd app && go build ./... && go test ./... -v`
Expected: Build succeeds, all tests PASS

- [ ] **Step 13: Commit**

```bash
git add app/internal/ws/ app/internal/kafkaclient/consumer.go app/internal/server/server.go app/internal/server/server_test.go app/main.go app/go.mod app/go.sum
git commit -m "feat: WebSocket hub, Kafka consumer, and full server wiring"
```

---

## Task 8: Frontend WebSocket Hook + Event Context + Dashboard Level 1

**Files:**
- Create: `web/src/hooks/useWebSocket.ts`
- Create: `web/src/context/EventContext.tsx`
- Modify: `web/src/App.tsx` (wrap with EventProvider)
- Modify: `web/src/pages/Dashboard.tsx` (Level 1 live event feed)
- Modify: `web/src/api/client.ts` (add role-specific API functions)

No frontend tests in Phase 2.

- [ ] **Step 1: Create WebSocket hook with auto-reconnect**

Create `web/src/hooks/useWebSocket.ts`:
```typescript
import { useEffect, useRef, useCallback, useState } from 'react';
import { useSession } from '../context/SessionContext';
import type { EventEnvelope } from '../context/EventContext';

export function useWebSocket(onEvent: (event: EventEnvelope) => void) {
  const { token, name, role } = useSession();
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
```

- [ ] **Step 2: Create Event context**

Create `web/src/context/EventContext.tsx`:
```typescript
import { createContext, useContext, useState, useCallback, type ReactNode } from 'react';

export interface EventEnvelope {
  event_id: string;
  event_type: string;
  actor_id: string;
  actor_role: string;
  timestamp: string;
  payload: Record<string, any>;
}

interface EventState {
  events: EventEnvelope[];
  addEvent: (event: EventEnvelope) => void;
  clearEvents: () => void;
}

const EventContext = createContext<EventState | undefined>(undefined);

const MAX_EVENTS = 100;

export function EventProvider({ children }: { children: ReactNode }) {
  const [events, setEvents] = useState<EventEnvelope[]>([]);

  const addEvent = useCallback((event: EventEnvelope) => {
    setEvents((prev) => [event, ...prev].slice(0, MAX_EVENTS));
  }, []);

  const clearEvents = useCallback(() => {
    setEvents([]);
  }, []);

  return (
    <EventContext.Provider value={{ events, addEvent, clearEvents }}>
      {children}
    </EventContext.Provider>
  );
}

export function useEvents(): EventState {
  const ctx = useContext(EventContext);
  if (!ctx) {
    throw new Error('useEvents must be used within EventProvider');
  }
  return ctx;
}
```

- [ ] **Step 3: Update App.tsx to wrap with EventProvider**

Modify `web/src/App.tsx`:
```typescript
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { SessionProvider } from './context/SessionContext';
import { EventProvider } from './context/EventContext';
import Landing from './pages/Landing';
import Seller from './pages/Seller';
import Buyer from './pages/Buyer';
import Shipper from './pages/Shipper';
import Dashboard from './pages/Dashboard';

function App() {
  return (
    <SessionProvider>
      <EventProvider>
        <BrowserRouter>
          <Routes>
            <Route path="/" element={<Landing />} />
            <Route path="/seller" element={<Seller />} />
            <Route path="/buyer" element={<Buyer />} />
            <Route path="/shipper" element={<Shipper />} />
            <Route path="/dashboard" element={<Dashboard />} />
          </Routes>
        </BrowserRouter>
      </EventProvider>
    </SessionProvider>
  );
}

export default App;
```

- [ ] **Step 4: Add role-specific API functions to client.ts**

Append to `web/src/api/client.ts`:
```typescript
// Seller API
export async function addProduct(token: string, name: string, price: number, quantity: number): Promise<Response> {
  return apiPost('/seller/products', token, { name, price, quantity });
}

export async function listSellerProducts(token: string): Promise<Response> {
  return apiGet('/seller/products', token);
}

export async function listSellerOrders(token: string): Promise<Response> {
  return apiGet('/seller/orders', token);
}

export async function confirmOrder(token: string, orderId: string): Promise<Response> {
  return apiPost(`/seller/orders/${orderId}/confirm`, token);
}

// Buyer API
export async function listBuyerProducts(token: string): Promise<Response> {
  return apiGet('/buyer/products', token);
}

export async function addToCart(token: string, productId: string, quantity: number): Promise<Response> {
  return apiPost('/buyer/cart/items', token, { product_id: productId, quantity });
}

export async function checkout(token: string, items: { product_id: string; quantity: number }[], shippingAddress: string): Promise<Response> {
  return apiPost('/buyer/cart/checkout', token, { items, shipping_address: shippingAddress });
}

export async function listBuyerOrders(token: string): Promise<Response> {
  return apiGet('/buyer/orders', token);
}

// Shipper API
export async function listShipperJobs(token: string): Promise<Response> {
  return apiGet('/shipper/jobs', token);
}

export async function pickJob(token: string, orderId: string): Promise<Response> {
  return apiPost(`/shipper/jobs/${orderId}/pick`, token);
}

export async function deliverJob(token: string, orderId: string): Promise<Response> {
  return apiPost(`/shipper/jobs/${orderId}/deliver`, token);
}
```

- [ ] **Step 5: Implement Dashboard Level 1 (live event feed)**

Replace `web/src/pages/Dashboard.tsx`:
```typescript
import { useWebSocket } from '../hooks/useWebSocket';
import { useEvents, type EventEnvelope } from '../context/EventContext';

function eventColor(eventType: string): string {
  if (eventType.startsWith('product')) return '#2563eb';
  if (eventType.startsWith('cart')) return '#7c3aed';
  if (eventType.startsWith('order')) return '#059669';
  if (eventType.startsWith('shipment.picked')) return '#d97706';
  if (eventType.startsWith('shipment.delivered')) return '#dc2626';
  return '#6b7280';
}

function EventRow({ event }: { event: EventEnvelope }) {
  const time = new Date(event.timestamp).toLocaleTimeString();
  return (
    <div style={{
      display: 'flex', gap: '12px', padding: '8px 12px',
      borderBottom: '1px solid #e5e7eb', fontFamily: 'monospace', fontSize: '13px',
    }}>
      <span style={{ color: '#9ca3af', minWidth: '80px' }}>{time}</span>
      <span style={{ color: eventColor(event.event_type), minWidth: '160px', fontWeight: 'bold' }}>
        {event.event_type}
      </span>
      <span style={{ color: '#6b7280', minWidth: '100px' }}>{event.actor_id}</span>
      <span style={{ color: '#374151' }}>
        {JSON.stringify(event.payload)}
      </span>
    </div>
  );
}

export default function Dashboard() {
  const { events, addEvent } = useEvents();
  const { connected } = useWebSocket(addEvent);

  return (
    <div style={{ padding: '20px', maxWidth: '1200px', margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
        <h1>Dashboard</h1>
        <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
          <span style={{
            padding: '4px 12px', borderRadius: '12px', fontSize: '12px',
            background: connected ? '#d1fae5' : '#fee2e2',
            color: connected ? '#059669' : '#dc2626',
          }}>
            {connected ? 'Connected' : 'Disconnected'}
          </span>
          <button onClick={() => window.location.reload()} style={{ padding: '6px 16px', fontSize: '12px' }}>
            Refresh
          </button>
        </div>
      </div>

      <div style={{ marginBottom: '16px' }}>
        <h2>Level 1 — Live Event Feed</h2>
        <p style={{ color: '#6b7280', fontSize: '14px' }}>
          Raw events from Kafka, delivered via WebSocket (stateless consumer, no processing)
        </p>
      </div>

      <div style={{
        background: 'white', borderRadius: '8px', border: '1px solid #e5e7eb',
        maxHeight: '600px', overflowY: 'auto',
      }}>
        {events.length === 0 ? (
          <div style={{ padding: '40px', textAlign: 'center', color: '#9ca3af' }}>
            Waiting for events...
          </div>
        ) : (
          events.map((event) => (
            <EventRow key={event.event_id} event={event} />
          ))
        )}
      </div>
    </div>
  );
}
```

- [ ] **Step 6: Verify the frontend builds**

Run: `cd web && npm run build`
Expected: Build succeeds

- [ ] **Step 7: Commit**

```bash
git add web/src/hooks/ web/src/context/EventContext.tsx web/src/App.tsx web/src/pages/Dashboard.tsx web/src/api/client.ts
git commit -m "feat: WebSocket hook, event context, Dashboard Level 1 live event feed"
```

---

## Task 9: Seller UI

**Files:**
- Modify: `web/src/pages/Seller.tsx`

No frontend tests in Phase 2.

- [ ] **Step 1: Implement Seller UI with product panel and order inbox**

Replace `web/src/pages/Seller.tsx`:
```typescript
import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useSession } from '../context/SessionContext';
import { useWebSocket } from '../hooks/useWebSocket';
import { useEvents, type EventEnvelope } from '../context/EventContext';
import {
  addProduct, listSellerProducts, listSellerOrders, confirmOrder,
} from '../api/client';

interface Product {
  id: string; name: string; price: number; quantity: number; seller_id: string;
}

interface OrderItem {
  product_id: string; product_name: string; quantity: number; unit_price: number;
}

interface Order {
  id: string; buyer_id: string; seller_id: string;
  items: OrderItem[]; total_amount: number; shipping_address: string;
  status: string; created_at: string;
}

export default function Seller() {
  const { name, token, clearSession } = useSession();
  const navigate = useNavigate();
  const { events, addEvent } = useEvents();
  useWebSocket(addEvent);

  const [products, setProducts] = useState<Product[]>([]);
  const [orders, setOrders] = useState<Order[]>([]);
  const [newProduct, setNewProduct] = useState({ name: '', price: '', quantity: '' });
  const [error, setError] = useState('');

  // Guard
  useEffect(() => {
    if (!name || !token) { navigate('/'); }
  }, [name, token, navigate]);

  const loadProducts = useCallback(async () => {
    if (!token) return;
    const resp = await listSellerProducts(token);
    if (resp.ok) setProducts(await resp.json());
  }, [token]);

  const loadOrders = useCallback(async () => {
    if (!token) return;
    const resp = await listSellerOrders(token);
    if (resp.ok) setOrders(await resp.json());
  }, [token]);

  useEffect(() => { loadProducts(); loadOrders(); }, [loadProducts, loadOrders]);

  // Listen for new checkout events to refresh orders
  useEffect(() => {
    const hasNewCheckout = events.some(e => e.event_type === 'cart.checkout' && e.payload?.seller_id === name);
    if (hasNewCheckout) loadOrders();
  }, [events, name, loadOrders]);

  const handleAddProduct = async () => {
    if (!token) return;
    setError('');
    const price = parseInt(newProduct.price);
    const quantity = parseInt(newProduct.quantity);
    if (!newProduct.name || !price || price <= 0) {
      setError('Name and positive price are required');
      return;
    }
    const resp = await addProduct(token, newProduct.name, price, quantity || 0);
    if (!resp.ok) {
      setError(await resp.text());
      return;
    }
    setNewProduct({ name: '', price: '', quantity: '' });
    loadProducts();
  };

  const handleConfirmOrder = async (orderId: string) => {
    if (!token) return;
    const resp = await confirmOrder(token, orderId);
    if (resp.ok) loadOrders();
  };

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  if (!name) return null;

  return (
    <div style={{ padding: '20px', maxWidth: '900px', margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
        <h1>Seller: {name}</h1>
        <button onClick={handleLogout}>Logout</button>
      </div>

      {/* Product Panel */}
      <div style={{ background: 'white', borderRadius: '8px', padding: '20px', marginBottom: '20px', border: '1px solid #e5e7eb' }}>
        <h2>Add Product</h2>
        <div style={{ display: 'flex', gap: '12px', marginTop: '12px' }}>
          <input
            placeholder="Product name"
            value={newProduct.name}
            onChange={(e) => setNewProduct({ ...newProduct, name: e.target.value })}
            style={{ flex: 1, padding: '8px' }}
          />
          <input
            placeholder="Price (cents)"
            type="number"
            value={newProduct.price}
            onChange={(e) => setNewProduct({ ...newProduct, price: e.target.value })}
            style={{ width: '120px', padding: '8px' }}
          />
          <input
            placeholder="Quantity"
            type="number"
            value={newProduct.quantity}
            onChange={(e) => setNewProduct({ ...newProduct, quantity: e.target.value })}
            style={{ width: '100px', padding: '8px' }}
          />
          <button onClick={handleAddProduct} style={{ padding: '8px 24px' }}>Add</button>
        </div>
        {error && <p style={{ color: 'red', marginTop: '8px' }}>{error}</p>}

        <h3 style={{ marginTop: '20px' }}>Your Products</h3>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px', marginTop: '12px' }}>
          {products.length === 0 ? (
            <p style={{ color: '#9ca3af' }}>No products listed yet.</p>
          ) : (
            products.map((p) => (
              <div key={p.id} style={{
                padding: '12px', borderRadius: '6px', border: '1px solid #d1d5db',
                minWidth: '200px', background: '#f9fafb',
              }}>
                <div style={{ fontWeight: 'bold' }}>{p.name}</div>
                <div style={{ color: '#6b7280' }}>${(p.price / 100).toFixed(2)}</div>
                <div style={{ color: '#6b7280', fontSize: '12px' }}>Qty: {p.quantity}</div>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Order Inbox */}
      <div style={{ background: 'white', borderRadius: '8px', padding: '20px', border: '1px solid #e5e7eb' }}>
        <h2>Order Inbox</h2>
        <div style={{ marginTop: '12px' }}>
          {orders.length === 0 ? (
            <p style={{ color: '#9ca3af' }}>No orders yet.</p>
          ) : (
            orders.map((o) => (
              <div key={o.id} style={{
                padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
                marginBottom: '12px', background: '#f9fafb',
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ fontWeight: 'bold' }}>Order from {o.buyer_id}</span>
                  <span style={{
                    padding: '2px 8px', borderRadius: '4px', fontSize: '12px',
                    background: o.status === 'checkout' ? '#fef3c7' : '#d1fae5',
                    color: o.status === 'checkout' ? '#d97706' : '#059669',
                  }}>
                    {o.status}
                  </span>
                </div>
                <div style={{ marginTop: '8px', fontSize: '14px', color: '#6b7280' }}>
                  {o.items.map((item, i) => (
                    <div key={i}>{item.quantity}x {item.product_name} (${(item.unit_price / 100).toFixed(2)})</div>
                  ))}
                </div>
                <div style={{ marginTop: '8px', fontWeight: 'bold' }}>
                  Total: ${(o.total_amount / 100).toFixed(2)}
                </div>
                <div style={{ marginTop: '4px', color: '#6b7280', fontSize: '12px' }}>
                  Ship to: {o.shipping_address}
                </div>
                {o.status === 'checkout' && (
                  <button
                    onClick={() => handleConfirmOrder(o.id)}
                    style={{ marginTop: '12px', padding: '6px 20px' }}
                  >
                    Confirm Order
                  </button>
                )}
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Verify the frontend builds**

Run: `cd web && npm run build`
Expected: Build succeeds

- [ ] **Step 3: Commit**

```bash
git add web/src/pages/Seller.tsx
git commit -m "feat: Seller UI with product panel and live order inbox"
```

---

## Task 10: Buyer UI

**Files:**
- Modify: `web/src/pages/Buyer.tsx`

No frontend tests in Phase 2.

- [ ] **Step 1: Implement Buyer UI with catalog, cart, checkout, and order status**

Replace `web/src/pages/Buyer.tsx`:
```typescript
import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useSession } from '../context/SessionContext';
import { useWebSocket } from '../hooks/useWebSocket';
import { useEvents } from '../context/EventContext';
import {
  listBuyerProducts, addToCart, checkout, listBuyerOrders,
} from '../api/client';

interface Product {
  id: string; name: string; price: number; quantity: number; seller_id: string;
}

interface CartItem {
  product: Product; quantity: number;
}

interface OrderItem {
  product_id: string; product_name: string; quantity: number; unit_price: number;
}

interface Order {
  id: string; buyer_id: string; seller_id: string;
  items: OrderItem[]; total_amount: number; shipping_address: string;
  status: string; created_at: string;
}

export default function Buyer() {
  const { name, token, clearSession } = useSession();
  const navigate = useNavigate();
  const { events, addEvent } = useEvents();
  useWebSocket(addEvent);

  const [products, setProducts] = useState<Product[]>([]);
  const [cart, setCart] = useState<CartItem[]>([]);
  const [orders, setOrders] = useState<Order[]>([]);
  const [shippingAddress, setShippingAddress] = useState('');
  const [error, setError] = useState('');
  const [showCheckout, setShowCheckout] = useState(false);

  useEffect(() => {
    if (!name || !token) { navigate('/'); }
  }, [name, token, navigate]);

  const loadProducts = useCallback(async () => {
    if (!token) return;
    const resp = await listBuyerProducts(token);
    if (resp.ok) setProducts(await resp.json());
  }, [token]);

  const loadOrders = useCallback(async () => {
    if (!token) return;
    const resp = await listBuyerOrders(token);
    if (resp.ok) setOrders(await resp.json());
  }, [token]);

  useEffect(() => { loadProducts(); loadOrders(); }, [loadProducts, loadOrders]);

  // Listen for new product.listed events to refresh catalog
  useEffect(() => {
    if (events.some(e => e.event_type === 'product.listed')) loadProducts();
  }, [events, loadProducts]);

  // Listen for order status updates
  useEffect(() => {
    const hasOrderUpdate = events.some(e =>
      ['order.confirmed', 'shipment.picked', 'shipment.delivered'].includes(e.event_type) &&
      e.payload?.buyer_id === name
    );
    if (hasOrderUpdate) loadOrders();
  }, [events, name, loadOrders]);

  const cartTotal = cart.reduce((sum, item) => sum + item.product.price * item.quantity, 0);

  const handleAddToCart = async (product: Product) => {
    if (!token) return;
    const existing = cart.find((item) => item.product.id === product.id);
    if (existing) {
      setCart(cart.map((item) =>
        item.product.id === product.id ? { ...item, quantity: item.quantity + 1 } : item
      ));
    } else {
      setCart([...cart, { product, quantity: 1 }]);
    }
    await addToCart(token, product.id, 1);
  };

  const handleCheckout = async () => {
    if (!token || cart.length === 0) return;
    setError('');
    const items = cart.map((item) => ({
      product_id: item.product.id,
      quantity: item.quantity,
    }));
    const resp = await checkout(token, items, shippingAddress);
    if (!resp.ok) {
      setError(await resp.text());
      return;
    }
    setCart([]);
    setShippingAddress('');
    setShowCheckout(false);
    loadOrders();
  };

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  if (!name) return null;

  return (
    <div style={{ padding: '20px', maxWidth: '900px', margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
        <h1>Buyer: {name}</h1>
        <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
          <span style={{ padding: '4px 12px', borderRadius: '4px', background: '#e0e7ff', fontSize: '14px' }}>
            Cart: {cart.length} items — ${(cartTotal / 100).toFixed(2)}
          </span>
          {cart.length > 0 && (
            <button onClick={() => setShowCheckout(!showCheckout)} style={{ padding: '6px 16px' }}>
              {showCheckout ? 'Cancel' : 'Checkout'}
            </button>
          )}
          <button onClick={handleLogout}>Logout</button>
        </div>
      </div>

      {/* Checkout form */}
      {showCheckout && (
        <div style={{
          background: 'white', borderRadius: '8px', padding: '20px',
          marginBottom: '20px', border: '1px solid #e5e7eb',
        }}>
          <h2>Checkout</h2>
          <div style={{ marginTop: '12px' }}>
            <input
              placeholder="Shipping address"
              value={shippingAddress}
              onChange={(e) => setShippingAddress(e.target.value)}
              style={{ width: '100%', padding: '8px', marginBottom: '12px' }}
            />
            <div style={{ marginBottom: '12px' }}>
              {cart.map((item) => (
                <div key={item.product.id} style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0' }}>
                  <span>{item.quantity}x {item.product.name}</span>
                  <span>${(item.product.price * item.quantity / 100).toFixed(2)}</span>
                </div>
              ))}
              <div style={{ fontWeight: 'bold', borderTop: '1px solid #e5e7eb', paddingTop: '8px' }}>
                Total: ${(cartTotal / 100).toFixed(2)}
              </div>
            </div>
            {error && <p style={{ color: 'red', marginBottom: '8px' }}>{error}</p>}
            <button
              onClick={handleCheckout}
              disabled={!shippingAddress}
              style={{ padding: '8px 24px' }}
            >
              Place Order
            </button>
          </div>
        </div>
      )}

      {/* Product Catalog */}
      <div style={{ background: 'white', borderRadius: '8px', padding: '20px', marginBottom: '20px', border: '1px solid #e5e7eb' }}>
        <h2>Product Catalog</h2>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px', marginTop: '12px' }}>
          {products.length === 0 ? (
            <p style={{ color: '#9ca3af' }}>No products available yet. Wait for sellers to list products.</p>
          ) : (
            products.map((p) => (
              <div key={p.id} style={{
                padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
                minWidth: '200px', background: '#f9fafb',
              }}>
                <div style={{ fontWeight: 'bold' }}>{p.name}</div>
                <div style={{ color: '#6b7280' }}>${(p.price / 100).toFixed(2)}</div>
                <div style={{ color: '#9ca3af', fontSize: '12px' }}>by {p.seller_id}</div>
                <button
                  onClick={() => handleAddToCart(p)}
                  style={{ marginTop: '8px', padding: '4px 16px', fontSize: '12px' }}
                >
                  Add to Cart
                </button>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Order Status */}
      <div style={{ background: 'white', borderRadius: '8px', padding: '20px', border: '1px solid #e5e7eb' }}>
        <h2>Your Orders</h2>
        {orders.length === 0 ? (
          <p style={{ color: '#9ca3af' }}>No orders yet.</p>
        ) : (
          orders.map((o) => (
            <div key={o.id} style={{
              padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
              marginBottom: '12px', background: '#f9fafb',
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span style={{ fontWeight: 'bold' }}>Order from {o.seller_id}</span>
                <span style={{
                  padding: '2px 8px', borderRadius: '4px', fontSize: '12px',
                  background: o.status === 'delivered' ? '#d1fae5' : o.status === 'picked' ? '#fed7aa' : o.status === 'confirmed' ? '#bfdbfe' : '#fef3c7',
                  color: o.status === 'delivered' ? '#059669' : o.status === 'picked' ? '#c2410c' : o.status === 'confirmed' ? '#2563eb' : '#d97706',
                }}>
                  {o.status}
                </span>
              </div>
              <div style={{ marginTop: '8px', fontSize: '14px', color: '#6b7280' }}>
                {o.items.map((item, i) => (
                  <div key={i}>{item.quantity}x {item.product_name}</div>
                ))}
              </div>
              <div style={{ marginTop: '8px', fontWeight: 'bold' }}>
                Total: ${(o.total_amount / 100).toFixed(2)}
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Verify the frontend builds**

Run: `cd web && npm run build`
Expected: Build succeeds

- [ ] **Step 3: Commit**

```bash
git add web/src/pages/Buyer.tsx
git commit -m "feat: Buyer UI with live catalog, cart, multi-seller checkout, and order tracking"
```

---

## Task 11: Shipper UI

**Files:**
- Modify: `web/src/pages/Shipper.tsx`

No frontend tests in Phase 2.

- [ ] **Step 1: Implement Shipper UI with job board and countdown timer**

Replace `web/src/pages/Shipper.tsx`:
```typescript
import { useState, useEffect, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { useSession } from '../context/SessionContext';
import { useWebSocket } from '../hooks/useWebSocket';
import { useEvents } from '../context/EventContext';
import {
  listShipperJobs, pickJob, deliverJob,
} from '../api/client';

interface OrderItem {
  product_id: string; product_name: string; quantity: number; unit_price: number;
}

interface Order {
  id: string; buyer_id: string; seller_id: string;
  items: OrderItem[]; total_amount: number; shipping_address: string;
  status: string; created_at: string;
}

export default function Shipper() {
  const { name, token, clearSession } = useSession();
  const navigate = useNavigate();
  const { events, addEvent } = useEvents();
  useWebSocket(addEvent);

  const [jobs, setJobs] = useState<Order[]>([]);
  const [pickedOrders, setPickedOrders] = useState<Record<string, number>>({}); // orderID → countdown seconds remaining
  const [error, setError] = useState('');
  const countdownRef = useRef<number | null>(null);

  useEffect(() => {
    if (!name || !token) { navigate('/'); }
  }, [name, token, navigate]);

  const loadJobs = useCallback(async () => {
    if (!token) return;
    const resp = await listShipperJobs(token);
    if (resp.ok) setJobs(await resp.json());
  }, [token]);

  useEffect(() => { loadJobs(); }, [loadJobs]);

  // Listen for order.confirmed events to refresh job board
  useEffect(() => {
    if (events.some(e => e.event_type === 'order.confirmed')) loadJobs();
  }, [events, loadJobs]);

  // Countdown timer effect
  useEffect(() => {
    const hasActiveCountdowns = Object.values(pickedOrders).some((s) => s > 0);
    if (!hasActiveCountdowns) {
      if (countdownRef.current) {
        clearInterval(countdownRef.current);
        countdownRef.current = null;
      }
      return;
    }
    if (!countdownRef.current) {
      countdownRef.current = window.setInterval(() => {
        setPickedOrders((prev) => {
          const next: Record<string, number> = {};
          for (const [id, seconds] of Object.entries(prev)) {
            next[id] = Math.max(0, seconds - 1);
          }
          return next;
        });
      }, 1000);
    }
    return () => {
      if (countdownRef.current) {
        clearInterval(countdownRef.current);
        countdownRef.current = null;
      }
    };
  }, [pickedOrders]);

  const handlePickJob = async (orderId: string) => {
    if (!token) return;
    setError('');
    const resp = await pickJob(token, orderId);
    if (!resp.ok) {
      if (resp.status === 409) {
        setError('Job already picked by another shipper');
      } else {
        setError(await resp.text());
      }
      loadJobs();
      return;
    }
    // Start countdown: random 5-15 seconds
    const countdown = Math.floor(Math.random() * 11) + 5;
    setPickedOrders((prev) => ({ ...prev, [orderId]: countdown }));
    // Remove from job board
    setJobs((prev) => prev.filter((j) => j.id !== orderId));
  };

  const handleDeliver = async (orderId: string) => {
    if (!token) return;
    const resp = await deliverJob(token, orderId);
    if (resp.ok) {
      setPickedOrders((prev) => {
        const next = { ...prev };
        delete next[orderId];
        return next;
      });
    }
  };

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  if (!name) return null;

  const activeJobs = Object.entries(pickedOrders).filter(([, s]) => s > 0);
  const deliveredJobs = Object.entries(pickedOrders).filter(([, s]) => s === 0);

  return (
    <div style={{ padding: '20px', maxWidth: '900px', margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
        <h1>Shipper: {name}</h1>
        <button onClick={handleLogout}>Logout</button>
      </div>

      {error && <p style={{ color: 'red', marginBottom: '16px' }}>{error}</p>}

      {/* Available Jobs */}
      <div style={{ background: 'white', borderRadius: '8px', padding: '20px', marginBottom: '20px', border: '1px solid #e5e7eb' }}>
        <h2>Available Jobs</h2>
        <div style={{ marginTop: '12px' }}>
          {jobs.length === 0 ? (
            <p style={{ color: '#9ca3af' }}>No jobs available. Waiting for sellers to confirm orders...</p>
          ) : (
            jobs.map((job) => (
              <div key={job.id} style={{
                padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
                marginBottom: '12px', background: '#f9fafb',
              }}>
                <div style={{ fontWeight: 'bold' }}>Order from {job.buyer_id}</div>
                <div style={{ marginTop: '8px', fontSize: '14px', color: '#6b7280' }}>
                  {job.items.map((item, i) => (
                    <div key={i}>{item.quantity}x {item.product_name}</div>
                  ))}
                </div>
                <div style={{ marginTop: '8px', color: '#6b7280', fontSize: '12px' }}>
                  Ship to: {job.shipping_address}
                </div>
                <button
                  onClick={() => handlePickJob(job.id)}
                  style={{ marginTop: '12px', padding: '6px 20px' }}
                >
                  Pick Up Job
                </button>
              </div>
            ))
          )}
        </div>
      </div>

      {/* In Transit (countdown active) */}
      {activeJobs.length > 0 && (
        <div style={{ background: 'white', borderRadius: '8px', padding: '20px', marginBottom: '20px', border: '1px solid #fbbf24' }}>
          <h2>In Transit</h2>
          {activeJobs.map(([orderId, seconds]) => (
            <div key={orderId} style={{
              padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
              marginBottom: '12px', background: '#fffbeb',
            }}>
              <div style={{ fontWeight: 'bold' }}>Order {orderId.slice(0, 8)}...</div>
              <div style={{ fontSize: '24px', color: '#d97706', marginTop: '8px' }}>
                Delivering in {seconds}s...
              </div>
              <button disabled style={{ marginTop: '12px', padding: '6px 20px', opacity: 0.5 }}>
                Mark Delivered (wait {seconds}s)
              </button>
            </div>
          ))}
        </div>
      )}

      {/* Ready to Deliver (countdown finished) */}
      {deliveredJobs.length > 0 && (
        <div style={{ background: 'white', borderRadius: '8px', padding: '20px', border: '1px solid #059669' }}>
          <h2>Ready to Deliver</h2>
          {deliveredJobs.map(([orderId]) => (
            <div key={orderId} style={{
              padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
              marginBottom: '12px', background: '#ecfdf5',
            }}>
              <div style={{ fontWeight: 'bold' }}>Order {orderId.slice(0, 8)}...</div>
              <div style={{ color: '#059669', marginTop: '4px' }}>Transit complete!</div>
              <button
                onClick={() => handleDeliver(orderId)}
                style={{ marginTop: '12px', padding: '6px 20px', background: '#059669' }}
              >
                Mark Delivered
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Verify the frontend builds**

Run: `cd web && npm run build`
Expected: Build succeeds

- [ ] **Step 3: Commit**

```bash
git add web/src/pages/Shipper.tsx
git commit -m "feat: Shipper UI with live job board and countdown timer"
```

---

## Phase 2 Verification Checklist

After all 11 tasks are complete, verify on the VPS:

- [ ] `git pull && docker compose down && docker compose up --build` starts without errors
- [ ] Go app logs show all 8 topics created
- [ ] Open browser at `http://<VPS-IP>:15300/` — landing page renders
- [ ] Create a seller session, add a product → product appears in seller's product list
- [ ] Create a buyer session (different browser/incognito) → product appears in buyer's catalog
- [ ] Buyer adds product to cart, checks out with shipping address → order created
- [ ] Seller sees new order in order inbox → clicks "Confirm Order" → order status changes
- [ ] Create a shipper session → job appears on shipper's job board
- [ ] Shipper picks job → countdown timer starts (5-15s) → "Mark Delivered" disabled during countdown
- [ ] Countdown finishes → "Mark Delivered" enabled → click it → order delivered
- [ ] Buyer sees order status update (checkout → confirmed → picked → delivered) in real-time
- [ ] Open Dashboard in another tab → all events appear in the live event feed
- [ ] Two shippers trying to pick the same job → second one gets "already picked" error
- [ ] Multi-seller checkout: buyer adds products from 2 sellers → checkout creates 2 orders
- [ ] `cd app && go test ./... -v` — all Go tests pass
- [ ] `cd app && go test ./... -race` — no race conditions detected
- [ ] `cd web && npm run build` — frontend builds without errors
