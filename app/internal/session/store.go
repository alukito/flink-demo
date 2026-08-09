package session

import (
	"errors"
	"sync"
)

var ErrDuplicateID = errors.New("session ID already exists")

var validRoles = map[string]bool{
	"buyer":     true,
	"seller":    true,
	"shipper":   true,
	"dashboard": true,
}

// Session holds a user's session state.
type Session struct {
	ID   string
	Name string
	Role string
}

// Store is an in-memory session store protected by a mutex.
type Store struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewStore creates an empty session store.
func NewStore() *Store {
	return &Store{sessions: make(map[string]*Session)}
}

// Create adds a new session. Returns ErrDuplicateID if the ID is already in use.
func (s *Store) Create(session Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sessions[session.ID]; exists {
		return ErrDuplicateID
	}
	s.sessions[session.ID] = &session
	return nil
}

// Exists checks whether a session with the given ID exists.
func (s *Store) Exists(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.sessions[id]
	return exists
}

// IsValidRole checks whether a role string is one of the allowed roles.
func IsValidRole(role string) bool {
	return validRoles[role]
}
