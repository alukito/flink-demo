package session

import (
	"errors"
	"sync"
)

var ErrDuplicateName = errors.New("session name already taken")

var validRoles = map[string]bool{
	"buyer":     true,
	"seller":    true,
	"shipper":   true,
	"dashboard": true,
}

// Session holds a user's session state.
type Session struct {
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

// Create adds a new session. Returns ErrDuplicateName if the name is taken.
func (s *Store) Create(name, role string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sessions[name]; exists {
		return ErrDuplicateName
	}
	s.sessions[name] = &Session{Name: name, Role: role}
	return nil
}

// Exists checks whether a session with the given name exists.
func (s *Store) Exists(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.sessions[name]
	return exists
}

// IsValidRole checks whether a role string is one of the allowed roles.
func IsValidRole(role string) bool {
	return validRoles[role]
}
