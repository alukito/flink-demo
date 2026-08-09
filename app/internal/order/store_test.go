package order

import (
	"strconv"
	"sync"
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
	o.BuyerName = "Alex"
	o.SellerName = "Merchant"
	s.Create(o)

	got := s.Get("o1")
	require.NotNil(t, got)
	assert.Equal(t, "buyer1", got.BuyerID)
	assert.Equal(t, "Alex", got.BuyerName)
	assert.Equal(t, "Merchant", got.SellerName)
	assert.Equal(t, StatusCheckout, got.Status)
}

func TestStoreGetNotFound(t *testing.T) {
	s := NewStore()
	assert.Nil(t, s.Get("nonexistent"))
}

func TestStoreGetReturnsAnIndependentCopy(t *testing.T) {
	s := NewStore()
	o := sampleOrder()
	s.Create(o)

	got := s.Get("o1")
	require.NotNil(t, got)
	got.Status = StatusDelivered
	got.Items[0].ProductName = "Changed outside the store"

	stored := s.Get("o1")
	require.NotNil(t, stored)
	assert.Equal(t, StatusCheckout, stored.Status)
	assert.Equal(t, "Widget", stored.Items[0].ProductName)
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

	err := s.Pick("o1", "shipper1", "shipper1")
	require.NoError(t, err)

	got := s.Get("o1")
	assert.Equal(t, StatusPicked, got.Status)
	assert.Equal(t, "shipper1", got.PickedBy)
	assert.False(t, got.PickedAt.IsZero())
}

func TestStorePickRecordsServerOwnedReadiness(t *testing.T) {
	pickedAt := time.Date(2026, time.August, 9, 10, 0, 0, 0, time.UTC)
	s := NewStore(
		WithClock(func() time.Time { return pickedAt }),
		WithReadyDelay(func() time.Duration { return 10 * time.Second }),
	)
	s.Create(sampleOrder())
	require.NoError(t, s.Confirm("o1", "seller1"))

	require.NoError(t, s.Pick("o1", "shipper-uuid", "Alex"))

	got := s.Get("o1")
	require.NotNil(t, got)
	assert.Equal(t, pickedAt, got.PickedAt)
	assert.Equal(t, pickedAt.Add(10*time.Second), got.ReadyAt)
}

func TestStorePickRaceCondition(t *testing.T) {
	s := NewStore()
	s.Create(sampleOrder())
	require.NoError(t, s.Confirm("o1", "seller1"))

	// First pick succeeds
	err1 := s.Pick("o1", "shipper1", "shipper1")
	assert.NoError(t, err1)

	// Second pick fails with conflict
	err2 := s.Pick("o1", "shipper2", "shipper2")
	assert.ErrorIs(t, err2, ErrInvalidTransition)
}

func TestStorePickNotConfirmed(t *testing.T) {
	s := NewStore()
	s.Create(sampleOrder()) // status = checkout

	err := s.Pick("o1", "shipper1", "shipper1")
	assert.ErrorIs(t, err, ErrInvalidTransition)
}

func TestStoreDeliver(t *testing.T) {
	now := time.Date(2026, time.August, 9, 10, 0, 0, 0, time.UTC)
	s := NewStore(
		WithClock(func() time.Time { return now }),
		WithReadyDelay(func() time.Duration { return 10 * time.Second }),
	)
	s.Create(sampleOrder())
	require.NoError(t, s.Confirm("o1", "seller1"))
	require.NoError(t, s.Pick("o1", "shipper1", "Alex"))
	now = now.Add(10 * time.Second)

	err := s.Deliver("o1", "shipper1")
	require.NoError(t, err)

	got := s.Get("o1")
	assert.Equal(t, StatusDelivered, got.Status)
	assert.Equal(t, "Alex", got.PickedByName)
	assert.False(t, got.DeliveredAt.IsZero())
}

func TestStoreDeliverRequiresReadinessAndOwningShipper(t *testing.T) {
	now := time.Date(2026, time.August, 9, 10, 0, 0, 0, time.UTC)
	s := NewStore(
		WithClock(func() time.Time { return now }),
		WithReadyDelay(func() time.Duration { return 10 * time.Second }),
	)
	s.Create(sampleOrder())
	require.NoError(t, s.Confirm("o1", "seller1"))
	require.NoError(t, s.Pick("o1", "shipper-owner", "Alex"))

	assert.ErrorIs(t, s.Deliver("o1", "shipper-owner"), ErrNotReady)
	assert.ErrorIs(t, s.Deliver("o1", "shipper-other"), ErrWrongShipper)
	assert.Equal(t, StatusPicked, s.Get("o1").Status)

	now = now.Add(10 * time.Second)
	require.NoError(t, s.Deliver("o1", "shipper-owner"))
	assert.Equal(t, now, s.Get("o1").DeliveredAt)
}

func TestStoreRejectsDeliveryByAnotherShipper(t *testing.T) {
	s := NewStore()
	s.Create(sampleOrder())
	require.NoError(t, s.Confirm("o1", "seller1"))
	require.NoError(t, s.Pick("o1", "shipper-a", "alex"))

	err := s.Deliver("o1", "shipper-b")
	assert.ErrorIs(t, err, ErrWrongShipper)
	assert.Equal(t, StatusPicked, s.Get("o1").Status)
}

func TestStoreDeliverNotPicked(t *testing.T) {
	s := NewStore()
	s.Create(sampleOrder())
	require.NoError(t, s.Confirm("o1", "seller1"))

	err := s.Deliver("o1", "shipper1")
	assert.ErrorIs(t, err, ErrInvalidTransition)
}

func TestStoreByShipperReturnsActiveAndNewestFirstHistory(t *testing.T) {
	now := time.Date(2026, time.August, 9, 10, 0, 0, 0, time.UTC)
	s := NewStore(
		WithClock(func() time.Time { return now }),
		WithReadyDelay(func() time.Duration { return 0 }),
	)

	for _, id := range []string{"active", "older", "newer", "other"} {
		o := sampleOrder()
		o.ID = id
		s.Create(o)
		require.NoError(t, s.Confirm(id, "seller1"))
	}

	require.NoError(t, s.Pick("active", "shipper-owner", "Alex"))
	require.NoError(t, s.Pick("older", "shipper-owner", "Alex"))
	require.NoError(t, s.Deliver("older", "shipper-owner"))
	now = now.Add(time.Second)
	require.NoError(t, s.Pick("newer", "shipper-owner", "Alex"))
	require.NoError(t, s.Deliver("newer", "shipper-owner"))
	require.NoError(t, s.Pick("other", "shipper-other", "Alex"))

	active, history := s.ByShipper("shipper-owner")
	require.Len(t, active, 1)
	assert.Equal(t, "active", active[0].ID)
	require.Len(t, history, 2)
	assert.Equal(t, []string{"newer", "older"}, []string{history[0].ID, history[1].ID})

	history[0].PickedBy = "mutated"
	_, historyAgain := s.ByShipper("shipper-owner")
	assert.Equal(t, "shipper-owner", historyAgain[0].PickedBy)
}

// TestStorePickConcurrentRace exercises the mutex-protected status transition
// under real concurrency: many shippers race to pick the same confirmed order.
// Exactly one must succeed and record its shipperID; all others must receive
// ErrInvalidTransition. Run with `go test -race` to verify no data races.
func TestStorePickConcurrentRace(t *testing.T) {
	const pickers = 64
	s := NewStore()
	s.Create(sampleOrder())
	require.NoError(t, s.Confirm("o1", "seller1"))

	var wg sync.WaitGroup
	wg.Add(pickers)
	results := make([]error, pickers)

	for i := 0; i < pickers; i++ {
		i := i
		go func() {
			defer wg.Done()
			results[i] = s.Pick("o1", "shipper"+strconv.Itoa(i), "shipper")
		}()
	}
	wg.Wait()

	var successCount int32
	for _, err := range results {
		if err == nil {
			successCount++
		} else {
			assert.ErrorIs(t, err, ErrInvalidTransition)
		}
	}

	assert.Equal(t, int32(1), successCount, "exactly one picker must succeed")

	got := s.Get("o1")
	require.NotNil(t, got)
	assert.Equal(t, StatusPicked, got.Status)
	assert.False(t, got.PickedAt.IsZero())
	assert.True(t, len(got.PickedBy) > 0, "winning shipper ID must be recorded")
}
