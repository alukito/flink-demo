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
	products *Store
	orders   *order.Store
	producer *kafkaclient.Producer
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
		ID:         uuid.New().String(),
		Name:       req.Name,
		Price:      req.Price,
		Quantity:   req.Quantity,
		SellerID:   claims.ID,
		SellerName: claims.Name,
		ListedAt:   time.Now(),
	}
	h.products.Add(p)

	// Produce product.listed event
	ev := event.NewEvent("product.listed", claims.ID, claims.Name, claims.Role, map[string]any{
		"product_id":  p.ID,
		"name":        p.Name,
		"price":       p.Price,
		"quantity":    p.Quantity,
		"seller_id":   p.SellerID,
		"seller_name": p.SellerName,
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
	products := h.products.BySeller(claims.ID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(products)
}

// ListOrders handles GET /api/seller/orders (returns orders for this seller's products).
func (h *Handler) ListOrders(w http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value(auth.ClaimsKey).(*auth.Claims)
	orders := h.orders.BySeller(claims.ID)

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

	err := h.orders.Confirm(orderID, claims.ID)
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
	ev := event.NewEvent("order.confirmed", claims.ID, claims.Name, claims.Role, map[string]any{
		"order_id":    orderID,
		"buyer_id":    o.BuyerID,
		"buyer_name":  o.BuyerName,
		"seller_id":   o.SellerID,
		"seller_name": o.SellerName,
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
