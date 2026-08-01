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
	"github.com/kuang/flink-demo/internal/event"
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

type publishedEvent struct {
	topic string
	event event.EventEnvelope
}

type capturingProducer struct {
	published []publishedEvent
}

func (p *capturingProducer) Write(_ context.Context, topic string, ev event.EventEnvelope) error {
	p.published = append(p.published, publishedEvent{topic: topic, event: ev})
	return nil
}

func TestAddToCartPreservesCartIDInEvent(t *testing.T) {
	prodStore := product.NewStore()
	prodStore.Add(product.Product{ID: "p1", Name: "Widget", Price: 500, SellerID: "seller1", ListedAt: time.Now()})
	producer := &capturingProducer{}
	h := NewHandler(prodStore, order.NewStore(), producer)

	req := httptest.NewRequest("POST", "/api/buyer/cart/items", strings.NewReader(`{"cart_id":"cart-123","product_id":"p1","quantity":2}`))
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.AddToCart(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Len(t, producer.published, 1)
	assert.Equal(t, "cart.item.added", producer.published[0].topic)
	assert.Equal(t, "cart-123", producer.published[0].event.Payload["cart_id"])
}

func TestCheckoutPreservesCartIDInEvent(t *testing.T) {
	prodStore := product.NewStore()
	prodStore.Add(product.Product{ID: "p1", Name: "Widget", Price: 500, Quantity: 10, SellerID: "seller1", ListedAt: time.Now()})
	producer := &capturingProducer{}
	h := NewHandler(prodStore, order.NewStore(), producer)

	req := httptest.NewRequest("POST", "/api/buyer/cart/checkout", strings.NewReader(`{"cart_id":"cart-123","items":[{"product_id":"p1","quantity":2}],"shipping_address":"123 Main St"}`))
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.Checkout(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Len(t, producer.published, 1)
	assert.Equal(t, "cart.checkout", producer.published[0].topic)
	assert.Equal(t, "cart-123", producer.published[0].event.Payload["cart_id"])
}

func TestCartIDIsRequired(t *testing.T) {
	tests := []struct {
		name    string
		handler func(http.ResponseWriter, *http.Request)
		path    string
		body    string
	}{
		{
			name:    "add to cart",
			handler: newTestHandlerForCartID(t).AddToCart,
			path:    "/api/buyer/cart/items",
			body:    `{"cart_id":"","product_id":"p1","quantity":1}`,
		},
		{
			name:    "checkout",
			handler: newTestHandlerForCartID(t).Checkout,
			path:    "/api/buyer/cart/checkout",
			body:    `{"cart_id":"","items":[{"product_id":"p1","quantity":1}],"shipping_address":"123 Main St"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", tt.path, strings.NewReader(tt.body))
			req = req.WithContext(claimsContext("buyer1", "buyer"))
			rec := httptest.NewRecorder()
			tt.handler(rec, req)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func newTestHandlerForCartID(t *testing.T) *Handler {
	t.Helper()
	prodStore := product.NewStore()
	prodStore.Add(product.Product{ID: "p1", Name: "Widget", Price: 500, Quantity: 10, SellerID: "seller1", ListedAt: time.Now()})
	return NewHandler(prodStore, order.NewStore(), &capturingProducer{})
}

func TestCartItemPayloadIncludesProductIdentity(t *testing.T) {
	p := &product.Product{ID: "p1", Name: "Widget", SellerID: "seller1"}
	payload := cartItemPayload("cart-123", p, 2)
	assert.Equal(t, "cart-123", payload["cart_id"])
	assert.Equal(t, "p1", payload["product_id"])
	assert.Equal(t, "Widget", payload["product_name"])
	assert.Equal(t, "seller1", payload["seller_id"])
	assert.Equal(t, 2, payload["quantity"])
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

	body := strings.NewReader(`{"cart_id":"cart-123","product_id":"p1","quantity":2}`)
	req := httptest.NewRequest("POST", "/api/buyer/cart/items", body)
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.AddToCart(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAddToCartProductNotFound(t *testing.T) {
	h, _, _ := newTestHandler(t)

	body := strings.NewReader(`{"cart_id":"cart-123","product_id":"nonexistent","quantity":1}`)
	req := httptest.NewRequest("POST", "/api/buyer/cart/items", body)
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.AddToCart(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCheckoutSingleSeller(t *testing.T) {
	h, prodStore, orderStore := newTestHandler(t)
	prodStore.Add(product.Product{ID: "p1", Name: "Widget", Price: 500, Quantity: 10, SellerID: "seller1", ListedAt: time.Now()})

	body := strings.NewReader(`{"cart_id":"cart-123","items":[{"product_id":"p1","quantity":2}],"shipping_address":"123 Main St"}`)
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
	prodStore.Add(product.Product{ID: "p1", Name: "A", Price: 100, Quantity: 10, SellerID: "seller1", ListedAt: time.Now()})
	prodStore.Add(product.Product{ID: "p2", Name: "B", Price: 200, Quantity: 10, SellerID: "seller2", ListedAt: time.Now()})

	body := strings.NewReader(`{"cart_id":"cart-123","items":[{"product_id":"p1","quantity":1},{"product_id":"p2","quantity":3}],"shipping_address":"456 Oak Ave"}`)
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
	assert.Equal(t, 100, s1Order.TotalAmount) // first seller's one-item order
	assert.Equal(t, 600, s2Order.TotalAmount) // 200 * 3
}

func TestCheckoutEmptyCart(t *testing.T) {
	h, _, _ := newTestHandler(t)

	body := strings.NewReader(`{"cart_id":"cart-123","items":[],"shipping_address":"123 Main St"}`)
	req := httptest.NewRequest("POST", "/api/buyer/cart/checkout", body)
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.Checkout(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCheckoutProductNotFound(t *testing.T) {
	h, _, _ := newTestHandler(t)

	body := strings.NewReader(`{"cart_id":"cart-123","items":[{"product_id":"nonexistent","quantity":1}],"shipping_address":"123"}`)
	req := httptest.NewRequest("POST", "/api/buyer/cart/checkout", body)
	req = req.WithContext(claimsContext("buyer1", "buyer"))
	rec := httptest.NewRecorder()
	h.Checkout(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCheckoutInsufficientStock(t *testing.T) {
	h, prodStore, _ := newTestHandler(t)
	prodStore.Add(product.Product{ID: "p1", Name: "Rare", Price: 100, Quantity: 2, SellerID: "seller1", ListedAt: time.Now()})

	body := strings.NewReader(`{"cart_id":"cart-123","items":[{"product_id":"p1","quantity":5}],"shipping_address":"123"}`)
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
