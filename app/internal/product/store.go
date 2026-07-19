package product

import (
	"sync"
	"time"
)

// Product represents a product listed by a seller.
type Product struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Price    int       `json:"price"`
	Quantity int       `json:"quantity"`
	SellerID string    `json:"seller_id"`
	ListedAt time.Time `json:"listed_at"`
}

// Store is an in-memory product store with a derived index by seller.
type Store struct {
	mu               sync.RWMutex
	products         map[string]Product
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

// DecrementQuantity reduces the available quantity of a product.
// Returns false if the product doesn't exist or has insufficient stock.
func (s *Store) DecrementQuantity(productID string, qty int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.products[productID]
	if !ok || p.Quantity < qty {
		return false
	}
	p.Quantity -= qty
	s.products[productID] = p
	return true
}
