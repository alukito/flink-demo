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

	err := h.orders.Pick(orderID, claims.ID, claims.Name)
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
	ev := event.NewEvent("shipment.picked", claims.ID, claims.Name, claims.Role, map[string]any{
		"order_id":     orderID,
		"buyer_id":     o.BuyerID,
		"buyer_name":   o.BuyerName,
		"seller_id":    o.SellerID,
		"seller_name":  o.SellerName,
		"shipper_id":   claims.ID,
		"shipper_name": claims.Name,
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

	err := h.orders.Deliver(orderID, claims.ID)
	if err != nil {
		switch {
		case errors.Is(err, order.ErrInvalidTransition):
			http.Error(w, "job is not in picked state", http.StatusConflict)
		case errors.Is(err, order.ErrWrongShipper):
			http.Error(w, "forbidden", http.StatusForbidden)
		case errors.Is(err, order.ErrNotReady):
			http.Error(w, "job is not ready for delivery", http.StatusConflict)
		default:
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	// Produce shipment.delivered event
	ev := event.NewEvent("shipment.delivered", claims.ID, claims.Name, claims.Role, map[string]any{
		"order_id":     orderID,
		"buyer_id":     o.BuyerID,
		"buyer_name":   o.BuyerName,
		"seller_id":    o.SellerID,
		"seller_name":  o.SellerName,
		"shipper_id":   o.PickedBy,
		"shipper_name": o.PickedByName,
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
