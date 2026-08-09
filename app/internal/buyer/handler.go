package buyer

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/event"
	"github.com/kuang/flink-demo/internal/order"
	"github.com/kuang/flink-demo/internal/product"
)

// Handler handles buyer-related HTTP requests.
type Handler struct {
	products *product.Store
	orders   *order.Store
	producer eventProducer
}

type eventProducer interface {
	Write(context.Context, string, event.EventEnvelope) error
}

// NewHandler creates a buyer handler with the given stores and producer.
func NewHandler(products *product.Store, orders *order.Store, producer eventProducer) *Handler {
	return &Handler{products: products, orders: orders, producer: producer}
}

// ListProducts handles GET /api/buyer/products (returns full catalog).
func (h *Handler) ListProducts(w http.ResponseWriter, r *http.Request) {
	products := h.products.All()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(products)
}

type addToCartRequest struct {
	CartID    string `json:"cart_id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

func cartItemPayload(cartID string, p *product.Product, quantity int) map[string]any {
	return map[string]any{
		"cart_id":      cartID,
		"product_id":   p.ID,
		"product_name": p.Name,
		"seller_id":    p.SellerID,
		"seller_name":  p.SellerName,
		"quantity":     quantity,
	}
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

	if req.CartID == "" {
		http.Error(w, "cart_id is required", http.StatusBadRequest)
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
	ev := event.NewEvent("cart.item.added", claims.ID, claims.Name, claims.Role, cartItemPayload(req.CartID, p, req.Quantity))
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
	CartID          string         `json:"cart_id"`
	Items           []checkoutItem `json:"items"`
	ShippingAddress string         `json:"shipping_address"`
}

type checkoutOrderResponse struct {
	OrderID     string            `json:"order_id"`
	SellerID    string            `json:"seller_id"`
	Items       []order.OrderItem `json:"items"`
	TotalAmount int               `json:"total_amount"`
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

	if req.CartID == "" {
		http.Error(w, "cart_id is required", http.StatusBadRequest)
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
		sellerName  string
	}
	groups := make(map[string]*sellerGroup)

	for _, item := range req.Items {
		p := h.products.Get(item.ProductID)
		if p == nil {
			http.Error(w, "product not found: "+item.ProductID, http.StatusBadRequest)
			return
		}

		if item.Quantity <= 0 {
			http.Error(w, "quantity must be positive for product: "+p.Name, http.StatusBadRequest)
			return
		}

		if item.Quantity > p.Quantity {
			http.Error(w, "insufficient stock for product: "+p.Name+" (available: "+strconv.Itoa(p.Quantity)+", requested: "+strconv.Itoa(item.Quantity)+")", http.StatusBadRequest)
			return
		}

		g, ok := groups[p.SellerID]
		if !ok {
			g = &sellerGroup{sellerName: p.SellerName}
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
			BuyerID:         claims.ID,
			BuyerName:       claims.Name,
			SellerID:        sellerID,
			SellerName:      g.sellerName,
			Items:           g.items,
			TotalAmount:     g.totalAmount,
			ShippingAddress: req.ShippingAddress,
			Status:          order.StatusCheckout,
			CreatedAt:       time.Now(),
		}
		h.orders.Create(o)

		// Decrement product quantities
		for _, item := range g.items {
			h.products.DecrementQuantity(item.ProductID, item.Quantity)
		}

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
		ev := event.NewEvent("cart.checkout", claims.ID, claims.Name, claims.Role, map[string]any{
			"cart_id":          req.CartID,
			"order_id":         orderID,
			"buyer_id":         claims.ID,
			"buyer_name":       claims.Name,
			"seller_id":        sellerID,
			"seller_name":      g.sellerName,
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
	orders := h.orders.ByBuyer(claims.ID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(orders)
}
