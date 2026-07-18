package shipper

import (
	"context"
	"encoding/json"
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
	require.NoError(t, orderStore.Pick("o1", "shipper1"))

	req := httptest.NewRequest("POST", "/api/shipper/jobs/o1/deliver", nil)
	req.SetPathValue("id", "o1")
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
	req.SetPathValue("id", "o1")
	req = req.WithContext(claimsContext("shipper1", "shipper"))
	rec := httptest.NewRecorder()
	h.DeliverJob(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
}
