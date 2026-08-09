package order

import (
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// OrderStatus represents the lifecycle state of an order.
type OrderStatus string

const (
	StatusCheckout  OrderStatus = "checkout"
	StatusConfirmed OrderStatus = "confirmed"
	StatusPicked    OrderStatus = "picked"
	StatusDelivered OrderStatus = "delivered"
)

// Errors for invalid operations.
var (
	ErrNotFound          = errors.New("order not found")
	ErrInvalidTransition = errors.New("invalid order status transition")
	ErrWrongSeller       = errors.New("order does not belong to this seller")
	ErrWrongShipper      = errors.New("order is not assigned to this shipper")
	ErrNotReady          = errors.New("order is not ready for delivery")
)

// OrderItem represents a line item in an order.
type OrderItem struct {
	ProductID   string `json:"product_id"`
	ProductName string `json:"product_name"`
	Quantity    int    `json:"quantity"`
	UnitPrice   int    `json:"unit_price"`
}

// Order represents a per-seller sub-order created at checkout.
type Order struct {
	ID              string      `json:"id"`
	BuyerID         string      `json:"buyer_id"`
	BuyerName       string      `json:"buyer_name"`
	SellerID        string      `json:"seller_id"`
	SellerName      string      `json:"seller_name"`
	Items           []OrderItem `json:"items"`
	TotalAmount     int         `json:"total_amount"`
	ShippingAddress string      `json:"shipping_address"`
	Status          OrderStatus `json:"status"`
	PickedBy        string      `json:"picked_by,omitempty"`
	PickedByName    string      `json:"picked_by_name,omitempty"`
	CreatedAt       time.Time   `json:"created_at"`
	ConfirmedAt     time.Time   `json:"confirmed_at,omitempty"`
	PickedAt        time.Time   `json:"picked_at,omitempty"`
	ReadyAt         time.Time   `json:"ready_at,omitempty"`
	DeliveredAt     time.Time   `json:"delivered_at,omitempty"`
}

// StoreOption configures a Store.
type StoreOption func(*Store)

// WithClock provides the server clock used for lifecycle timestamps.
func WithClock(clock func() time.Time) StoreOption {
	return func(s *Store) {
		if clock != nil {
			s.clock = clock
		}
	}
}

// WithReadyDelay provides the server-selected delay between pickup and delivery.
func WithReadyDelay(readyDelay func() time.Duration) StoreOption {
	return func(s *Store) {
		if readyDelay != nil {
			s.readyDelay = readyDelay
		}
	}
}

// Store is an in-memory order store with derived indexes, protected by a mutex.
type Store struct {
	mu              sync.RWMutex
	orders          map[string]*Order
	ordersByBuyer   map[string][]string
	ordersBySeller  map[string][]string
	ordersByShipper map[string][]string
	ordersByStatus  map[OrderStatus][]string
	clock           func() time.Time
	readyDelay      func() time.Duration
}

// NewStore creates an empty order store.
func NewStore(options ...StoreOption) *Store {
	s := &Store{
		orders:          make(map[string]*Order),
		ordersByBuyer:   make(map[string][]string),
		ordersBySeller:  make(map[string][]string),
		ordersByShipper: make(map[string][]string),
		ordersByStatus:  make(map[OrderStatus][]string),
		clock:           time.Now,
		readyDelay: func() time.Duration {
			return time.Duration(5+rand.Intn(11)) * time.Second
		},
	}
	for _, option := range options {
		if option != nil {
			option(s)
		}
	}
	return s
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

// Get returns an independent copy of an order by ID, or nil if not found.
func (s *Store) Get(id string) *Order {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.orders[id]
	if !ok {
		return nil
	}
	copy := copyOrder(*o)
	return &copy
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
	o.ConfirmedAt = s.clock()
	return nil
}

// Pick transitions an order from "confirmed" to "picked".
// Returns ErrInvalidTransition if the order is not in "confirmed" status
// (e.g., already picked by another shipper).
func (s *Store) Pick(orderID, shipperID, shipperName string) error {
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
	o.PickedByName = shipperName
	o.PickedAt = s.clock()
	o.ReadyAt = o.PickedAt.Add(s.readyDelay())
	s.ordersByShipper[shipperID] = append(s.ordersByShipper[shipperID], o.ID)
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
	if o.PickedBy != shipperID {
		return ErrWrongShipper
	}
	now := s.clock()
	if now.Before(o.ReadyAt) {
		return ErrNotReady
	}
	s.updateStatus(o, StatusDelivered)
	o.DeliveredAt = now
	return nil
}

// ByShipper returns the shipper's active pickups and delivered orders. History
// is ordered newest delivery first, and returned orders are independent copies.
func (s *Store) ByShipper(shipperID string) (active, history []Order) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, id := range s.ordersByShipper[shipperID] {
		o, ok := s.orders[id]
		if !ok {
			continue
		}
		switch o.Status {
		case StatusPicked:
			active = append(active, copyOrder(*o))
		case StatusDelivered:
			history = append(history, copyOrder(*o))
		}
	}

	sort.Slice(active, func(i, j int) bool {
		if active[i].PickedAt.Equal(active[j].PickedAt) {
			return active[i].ID < active[j].ID
		}
		return active[i].PickedAt.After(active[j].PickedAt)
	})
	sort.Slice(history, func(i, j int) bool {
		if history[i].DeliveredAt.Equal(history[j].DeliveredAt) {
			return history[i].ID < history[j].ID
		}
		return history[i].DeliveredAt.After(history[j].DeliveredAt)
	})
	return active, history
}

func copyOrder(o Order) Order {
	o.Items = append([]OrderItem(nil), o.Items...)
	return o
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
