package shipper

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/event"
	"github.com/kuang/flink-demo/internal/order"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler(t *testing.T, storeOptions ...order.StoreOption) (*Handler, *order.Store) {
	t.Helper()
	if len(storeOptions) == 0 {
		storeOptions = []order.StoreOption{order.WithReadyDelay(func() time.Duration { return 0 })}
	}
	orderStore := order.NewStore(storeOptions...)
	h := NewHandler(orderStore, &capturingProducer{})
	return h, orderStore
}

func claimsContext(name, role string) context.Context {
	claims := &auth.Claims{ID: name, Name: name, Role: role}
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

func TestListDeliveriesReturnsOnlyAuthenticatedShipperRecords(t *testing.T) {
	now := time.Date(2026, time.August, 9, 10, 0, 0, 0, time.UTC)
	h, orderStore := newTestHandler(t,
		order.WithClock(func() time.Time { return now }),
		order.WithReadyDelay(func() time.Duration { return 0 }),
	)

	for _, id := range []string{"active", "history", "other"} {
		orderStore.Create(order.Order{
			ID: id, BuyerID: "buyer-id", BuyerName: "Buyer", SellerID: "seller-id", SellerName: "Seller", Status: order.StatusConfirmed,
		})
	}
	require.NoError(t, orderStore.Pick("active", "shipper-id", "Assigned Shipper"))
	require.NoError(t, orderStore.Pick("history", "shipper-id", "Assigned Shipper"))
	require.NoError(t, orderStore.Deliver("history", "shipper-id"))
	require.NoError(t, orderStore.Pick("other", "other-shipper-id", "Other Shipper"))

	req := httptest.NewRequest("GET", "/api/shipper/deliveries", nil)
	req = req.WithContext(context.WithValue(context.Background(), auth.ClaimsKey, &auth.Claims{ID: "shipper-id", Name: "Current Shipper", Role: "shipper"}))
	rec := httptest.NewRecorder()
	h.ListDeliveries(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var response struct {
		Active  []order.Order `json:"active"`
		History []order.Order `json:"history"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
	require.Len(t, response.Active, 1)
	assert.Equal(t, "active", response.Active[0].ID)
	assert.Equal(t, "shipper-id", response.Active[0].PickedBy)
	require.Len(t, response.History, 1)
	assert.Equal(t, "history", response.History[0].ID)
	assert.Equal(t, order.StatusDelivered, response.History[0].Status)
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

	var jobs []map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&jobs))
	assert.Len(t, jobs, 2, "only confirmed orders should appear as available jobs")
	ids := map[string]bool{}
	for _, j := range jobs {
		ids[j["id"].(string)] = true
	}
	assert.True(t, ids["o1"])
	assert.True(t, ids["o3"])
	assert.False(t, ids["o2"], "non-confirmed order must not be listed as a job")
}

func TestPickJobSuccess(t *testing.T) {
	h, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: order.StatusConfirmed})

	req := httptest.NewRequest("POST", "/api/shipper/jobs/o1/pick", nil)
	req.SetPathValue("id", "o1")
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
	req1.SetPathValue("id", "o1")
	req1 = req1.WithContext(claimsContext("shipper1", "shipper"))
	rec1 := httptest.NewRecorder()
	h.PickJob(rec1, req1)
	assert.Equal(t, http.StatusOK, rec1.Code)

	// Second pick fails with 409 Conflict
	req2 := httptest.NewRequest("POST", "/api/shipper/jobs/o1/pick", nil)
	req2.SetPathValue("id", "o1")
	req2 = req2.WithContext(claimsContext("shipper2", "shipper"))
	rec2 := httptest.NewRecorder()
	h.PickJob(rec2, req2)
	assert.Equal(t, http.StatusConflict, rec2.Code)

	// Verify the first shipper still owns the job
	got := orderStore.Get("o1")
	assert.Equal(t, order.StatusPicked, got.Status)
	assert.Equal(t, "shipper1", got.PickedBy, "second pick must not overwrite PickedBy")
}

func TestPickJobNotFound(t *testing.T) {
	h, _ := newTestHandler(t)

	req := httptest.NewRequest("POST", "/api/shipper/jobs/nonexistent/pick", nil)
	req.SetPathValue("id", "nonexistent")
	req = req.WithContext(claimsContext("shipper1", "shipper"))
	rec := httptest.NewRecorder()
	h.PickJob(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestPickJobNotConfirmed(t *testing.T) {
	h, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: order.StatusCheckout})

	req := httptest.NewRequest("POST", "/api/shipper/jobs/o1/pick", nil)
	req.SetPathValue("id", "o1")
	req = req.WithContext(claimsContext("shipper1", "shipper"))
	rec := httptest.NewRecorder()
	h.PickJob(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestDeliverJobSuccess(t *testing.T) {
	h, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: order.StatusConfirmed})
	require.NoError(t, orderStore.Pick("o1", "shipper1", "shipper1"))

	req := httptest.NewRequest("POST", "/api/shipper/jobs/o1/deliver", nil)
	req.SetPathValue("id", "o1")
	req = req.WithContext(claimsContext("shipper1", "shipper"))
	rec := httptest.NewRecorder()
	h.DeliverJob(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, order.StatusDelivered, orderStore.Get("o1").Status)
}

func TestDeliverJobUsesShipperUUIDOwnership(t *testing.T) {
	h, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: order.StatusConfirmed})
	require.NoError(t, orderStore.Pick("o1", "shipper-a", "alex"))

	req := httptest.NewRequest("POST", "/api/shipper/jobs/o1/deliver", nil)
	req.SetPathValue("id", "o1")
	req = req.WithContext(context.WithValue(context.Background(), auth.ClaimsKey, &auth.Claims{ID: "shipper-a", Name: "alex", Role: "shipper"}))
	rec := httptest.NewRecorder()
	h.DeliverJob(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, order.StatusDelivered, orderStore.Get("o1").Status)
}

func TestDeliverJobNotPicked(t *testing.T) {
	h, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: order.StatusConfirmed})

	req := httptest.NewRequest("POST", "/api/shipper/jobs/o1/deliver", nil)
	req.SetPathValue("id", "o1")
	req = req.WithContext(claimsContext("shipper1", "shipper"))
	rec := httptest.NewRecorder()
	h.DeliverJob(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestDeliverJobNotReady(t *testing.T) {
	h, orderStore := newTestHandler(t, order.WithReadyDelay(func() time.Duration { return 10 * time.Second }))
	orderStore.Create(order.Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: order.StatusConfirmed})
	require.NoError(t, orderStore.Pick("o1", "shipper1", "shipper1"))

	req := httptest.NewRequest("POST", "/api/shipper/jobs/o1/deliver", nil)
	req.SetPathValue("id", "o1")
	req = req.WithContext(claimsContext("shipper1", "shipper"))
	rec := httptest.NewRecorder()
	h.DeliverJob(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Equal(t, order.StatusPicked, orderStore.Get("o1").Status)
}

func TestDeliverJobRejectsAnotherShipper(t *testing.T) {
	h, orderStore := newTestHandler(t)
	orderStore.Create(order.Order{ID: "o1", BuyerID: "b1", SellerID: "s1", Status: order.StatusConfirmed})
	require.NoError(t, orderStore.Pick("o1", "shipper-owner", "Owner"))

	req := httptest.NewRequest("POST", "/api/shipper/jobs/o1/deliver", nil)
	req.SetPathValue("id", "o1")
	req = req.WithContext(context.WithValue(context.Background(), auth.ClaimsKey, &auth.Claims{ID: "shipper-other", Name: "Other", Role: "shipper"}))
	rec := httptest.NewRecorder()
	h.DeliverJob(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Equal(t, order.StatusPicked, orderStore.Get("o1").Status)
}

func TestShipmentEventsUsePostTransitionOrderRelationships(t *testing.T) {
	now := time.Date(2026, time.August, 9, 10, 0, 0, 0, time.UTC)
	orderStore := order.NewStore(
		order.WithClock(func() time.Time { return now }),
		order.WithReadyDelay(func() time.Duration { return 10 * time.Second }),
	)
	producer := &capturingProducer{}
	h := NewHandler(orderStore, producer)
	orderStore.Create(order.Order{
		ID: "o1", BuyerID: "buyer-id", BuyerName: "Buyer", SellerID: "seller-id", SellerName: "Seller", Status: order.StatusConfirmed,
	})

	pickReq := httptest.NewRequest("POST", "/api/shipper/jobs/o1/pick", nil)
	pickReq.SetPathValue("id", "o1")
	pickReq = pickReq.WithContext(context.WithValue(context.Background(), auth.ClaimsKey, &auth.Claims{ID: "shipper-id", Name: "Assigned Shipper", Role: "shipper"}))
	pickRec := httptest.NewRecorder()
	h.PickJob(pickRec, pickReq)
	require.Equal(t, http.StatusOK, pickRec.Code)
	require.Len(t, producer.published, 1)
	assert.Equal(t, "shipment.picked", producer.published[0].topic)
	assert.Equal(t, map[string]any{
		"order_id": "o1", "buyer_id": "buyer-id", "buyer_name": "Buyer", "seller_id": "seller-id", "seller_name": "Seller",
		"shipper_id": "shipper-id", "shipper_name": "Assigned Shipper", "ready_at": now.Add(10 * time.Second),
	}, producer.published[0].event.Payload)

	now = now.Add(10 * time.Second)
	deliverReq := httptest.NewRequest("POST", "/api/shipper/jobs/o1/deliver", nil)
	deliverReq.SetPathValue("id", "o1")
	deliverReq = deliverReq.WithContext(context.WithValue(context.Background(), auth.ClaimsKey, &auth.Claims{ID: "shipper-id", Name: "Renamed Shipper", Role: "shipper"}))
	deliverRec := httptest.NewRecorder()
	h.DeliverJob(deliverRec, deliverReq)
	require.Equal(t, http.StatusOK, deliverRec.Code)
	require.Len(t, producer.published, 2)
	assert.Equal(t, "shipment.delivered", producer.published[1].topic)
	assert.Equal(t, map[string]any{
		"order_id": "o1", "buyer_id": "buyer-id", "buyer_name": "Buyer", "seller_id": "seller-id", "seller_name": "Seller",
		"shipper_id": "shipper-id", "shipper_name": "Assigned Shipper", "ready_at": now,
	}, producer.published[1].event.Payload)
}
