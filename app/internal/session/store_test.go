package session

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreCreate(t *testing.T) {
	s := NewStore()
	err := s.Create("alice", "buyer")
	require.NoError(t, err)
}

func TestStoreDuplicateName(t *testing.T) {
	s := NewStore()
	require.NoError(t, s.Create("alice", "buyer"))
	err := s.Create("alice", "seller")
	assert.ErrorIs(t, err, ErrDuplicateName)
}

func TestStoreExists(t *testing.T) {
	s := NewStore()
	s.Create("alice", "buyer")
	assert.True(t, s.Exists("alice"))
	assert.False(t, s.Exists("bob"))
}
