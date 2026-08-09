package session

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreCreate(t *testing.T) {
	s := NewStore()
	err := s.Create(Session{ID: "session-1", Name: "alice", Role: "buyer"})
	require.NoError(t, err)
}

func TestStoreAllowsDuplicateNames(t *testing.T) {
	s := NewStore()
	require.NoError(t, s.Create(Session{ID: "session-1", Name: "alex", Role: "buyer"}))
	err := s.Create(Session{ID: "session-2", Name: "alex", Role: "seller"})
	assert.NoError(t, err)
}

func TestStoreRejectsDuplicateIDs(t *testing.T) {
	s := NewStore()
	require.NoError(t, s.Create(Session{ID: "session-1", Name: "alex", Role: "buyer"}))
	err := s.Create(Session{ID: "session-1", Name: "alex", Role: "seller"})
	assert.ErrorIs(t, err, ErrDuplicateID)
}

func TestStoreExists(t *testing.T) {
	s := NewStore()
	s.Create(Session{ID: "session-1", Name: "alice", Role: "buyer"})
	assert.True(t, s.Exists("session-1"))
	assert.False(t, s.Exists("session-2"))
}
