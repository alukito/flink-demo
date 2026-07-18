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
	req.SetPathValue("id", "o1")
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
	req.SetPathValue("id", "o1")
	req = req.WithContext(claimsContext("wrong-seller", "seller"))
	rec := httptest.NewRecorder()
	h.ConfirmOrder(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestConfirmOrderNotFound(t *testing.T) {
	h, _, _ := newTestHandler(t)

	req := httptest.NewRequest("POST", "/api/seller/orders/nonexistent/confirm", nil)
	req.SetPathValue("id", "nonexistent")
	req = req.WithContext(claimsContext("seller1", "seller"))
	rec := httptest.NewRecorder()
	h.ConfirmOrder(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}
