package product

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreAddAndGet(t *testing.T) {
	s := NewStore()
	p := Product{
		ID:       "p1",
		Name:     "Widget",
		Price:    500,
		Quantity: 10,
		SellerID: "seller1",
		ListedAt: time.Now(),
	}
	s.Add(p)

	got := s.Get("p1")
	require.NotNil(t, got)
	assert.Equal(t, "Widget", got.Name)
	assert.Equal(t, "seller1", got.SellerID)
}

func TestStoreGetNotFound(t *testing.T) {
	s := NewStore()
	assert.Nil(t, s.Get("nonexistent"))
}

func TestStoreAll(t *testing.T) {
	s := NewStore()
	s.Add(Product{ID: "p1", Name: "A", SellerID: "s1"})
	s.Add(Product{ID: "p2", Name: "B", SellerID: "s2"})

	all := s.All()
	assert.Len(t, all, 2)
}

func TestStoreBySeller(t *testing.T) {
	s := NewStore()
	s.Add(Product{ID: "p1", Name: "A", SellerID: "s1"})
	s.Add(Product{ID: "p2", Name: "B", SellerID: "s1"})
	s.Add(Product{ID: "p3", Name: "C", SellerID: "s2"})

	s1Products := s.BySeller("s1")
	assert.Len(t, s1Products, 2)
	assert.Equal(t, "p1", s1Products[0].ID)
	assert.Equal(t, "p2", s1Products[1].ID)

	s2Products := s.BySeller("s2")
	assert.Len(t, s2Products, 1)
	assert.Equal(t, "p3", s2Products[0].ID)

	empty := s.BySeller("nonexistent")
	assert.Len(t, empty, 0)
}
