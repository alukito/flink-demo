# Phase 1: Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the foundational infrastructure — Docker Compose with Kafka, Go server with JWT auth and session management, React shell with landing page and role selection — so that a user can log in, pick a role, and see an empty role page.

**Architecture:** Go server serves a REST API (session, health, role-namespaced placeholder routes) and embedded React static files. Kafka runs as a single broker with topics auto-created on startup. React is built by Vite and embedded into the Go binary via `go:embed`.

**Tech Stack:** Go 1.22+ (net/http, log/slog, embed), React 18 + TypeScript + Vite 5, Kafka (confluentinc/cp-kafka), Docker Compose, golang-jwt/jwt v5, segmentio/kafka-go, stretchr/testify

## Global Constraints

- Go 1.22+ (uses `http.ServeMux` pattern routing introduced in 1.22)
- Node 20+ for frontend build
- React 18 + TypeScript strict mode
- Vite 5 for frontend build tooling
- Structured JSON logging via `log/slog`
- JWT via `github.com/golang-jwt/jwt/v5`
- Kafka client via `github.com/segmentio/kafka-go`
- WebSocket via `github.com/coder/websocket` (not used in Phase 1, installed later)
- Test assertions via `github.com/stretchr/testify`
- No frontend tests in Phase 1
- Kafka topics auto-created by Go server on startup (8 topics total)
- JWT secret passed via `JWT_SECRET` env var
- Kafka address passed via `KAFKA_ADDR` env var
- Server port passed via `PORT` env var (default 8080)
- All Go code lives under `app/`; all React code lives under `web/`
- React build output goes to `app/web/dist/` and is embedded via `go:embed`

---

## File Structure

```
flink-demo/
├── Makefile                          # Build/dev commands
├── docker-compose.yml                # Zookeeper + Kafka + Go app
├── app/
│   ├── Dockerfile                    # Multi-stage: build React, build Go, runtime
│   ├── go.mod                        # Go module definition
│   ├── go.sum                        # Dependency checksums (generated)
│   ├── main.go                       # Entry point — wires all components
│   ├── internal/
│   │   ├── config/
│   │   │   └── config.go             # Load config from env vars
│   │   ├── logging/
│   │   │   └── logging.go            # slog JSON logger factory
│   │   ├── auth/
│   │   │   ├── jwt.go                # JWT sign/verify with Claims struct
│   │   │   ├── jwt_test.go           # JWT roundtrip, invalid token, wrong secret
│   │   │   ├── middleware.go         # Auth middleware + RequireRole
│   │   │   └── middleware_test.go    # Middleware extracts claims, role allow/deny
│   │   ├── session/
│   │   │   ├── store.go              # In-memory session store with mutex
│   │   │   ├── store_test.go         # Create, duplicate name rejection
│   │   │   ├── handler.go            # POST /api/session HTTP handler
│   │   │   └── handler_test.go       # Success, duplicate, validation
│   │   ├── kafkaclient/
│   │   │   ├── topics.go             # Topic list constant
│   │   │   ├── topics_test.go        # Topic list completeness
│   │   │   └── client.go             # Kafka admin client, topic creation
│   │   └── server/
│   │       ├── server.go             # HTTP server, routes, static file serving
│   │       └── server_test.go        # Health check, route protection
│   └── web/
│       ├── embed.go                  # go:embed directive for dist/
│       └── dist/
│           └── index.html            # Placeholder (overwritten by Vite build)
└── web/
    ├── package.json                  # React + Vite + React Router deps
    ├── package-lock.json             # (generated)
    ├── tsconfig.json                 # TypeScript strict config
    ├── vite.config.ts                # Vite config with proxy + output dir
    ├── index.html                    # Vite entry HTML
    └── src/
        ├── main.tsx                  # React entry point
        ├── App.tsx                   # Router setup
        ├── api/
        │   └── client.ts             # Fetch wrapper for API calls
        ├── context/
        │   └── SessionContext.tsx    # Session state (token, name, role)
        └── pages/
            ├── Landing.tsx           # Name input + role selection
            ├── Seller.tsx            # Placeholder
            ├── Buyer.tsx             # Placeholder
            ├── Shipper.tsx           # Placeholder
            └── Dashboard.tsx         # Placeholder (no auth needed)
```

---

## Task 1: Go Project Scaffold + Config + Logging

**Files:**
- Create: `app/go.mod`
- Create: `app/main.go`
- Create: `app/internal/config/config.go`
- Create: `app/internal/logging/logging.go`
- Create: `app/internal/logging/logging_test.go`
- Create: `app/web/embed.go`
- Create: `app/web/dist/index.html`

**Interfaces:**
- Produces: `config.Config{Port, JWTSecret, KafkaAddr}`, `logging.NewLogger() *slog.Logger`, `web.DistFS` (embedded filesystem for React static files)

- [ ] **Step 1: Create Go module, embed package, and placeholder web dist**

Run:
```bash
cd app && go mod init github.com/kuang/flink-demo
```

Create `app/web/embed.go`:
```go
package web

import "embed"

// DistFS holds the embedded React build output.
// The dist directory is populated by `npm run build` (Vite output).
//go:embed dist
var DistFS embed.FS
```

Create `app/web/dist/index.html`:
```html
<!DOCTYPE html>
<html><body>Frontend not built. Run: cd web && npm run build</body></html>
```

- [ ] **Step 2: Write the failing test for logging**

Create `app/internal/logging/logging_test.go`:
```go
package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoggerOutputsJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	logger.Info("test message", "key", "value")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))

	assert.Equal(t, "test message", entry["msg"])
	assert.Equal(t, "value", entry["key"])
	assert.Equal(t, "INFO", entry["level"])
}

func TestNewLoggerReturnsJSONHandler(t *testing.T) {
	logger := NewLogger()
	assert.NotNil(t, logger)
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd app && go test ./internal/logging/ -v`
Expected: FAIL — `NewLogger` not defined (the first test passes because it creates its own logger; the second fails)

- [ ] **Step 4: Install testify dependency**

Run:
```bash
cd app && go get github.com/stretchr/testify
```

- [ ] **Step 5: Implement logging package**

Create `app/internal/logging/logging.go`:
```go
package logging

import (
	"log/slog"
	"os"
)

// NewLogger creates a structured JSON logger writing to stdout.
func NewLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil))
}
```

- [ ] **Step 6: Implement config package**

Create `app/internal/config/config.go`:
```go
package config

import "os"

// Config holds all application configuration loaded from environment.
type Config struct {
	Port       string
	JWTSecret  string
	KafkaAddr  string
}

// Load reads configuration from environment variables with defaults.
func Load() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		secret = "demo-secret-not-for-production"
	}
	kafkaAddr := os.Getenv("KAFKA_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "localhost:9092"
	}
	return Config{
		Port:      port,
		JWTSecret: secret,
		KafkaAddr: kafkaAddr,
	}
}
```

- [ ] **Step 7: Create placeholder main.go**

Create `app/main.go`:
```go
package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/kuang/flink-demo/internal/config"
	"github.com/kuang/flink-demo/internal/logging"
)

func main() {
	logger := logging.NewLogger()
	cfg := config.Load()

	logger.Info("starting server",
		"port", cfg.Port,
		"kafka_addr", cfg.KafkaAddr,
	)

	// Phase 1 placeholder — full server wiring added in Task 5
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	logger.Info("shutting down")
}
```

- [ ] **Step 8: Run all tests and verify they pass**

Run: `cd app && go test ./... -v`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add app/
git commit -m "feat: Go project scaffold with config and structured logging"
```

---

## Task 2: JWT Auth + Role Middleware

**Files:**
- Create: `app/internal/auth/jwt.go`
- Create: `app/internal/auth/jwt_test.go`
- Create: `app/internal/auth/middleware.go`
- Create: `app/internal/auth/middleware_test.go`

**Interfaces:**
- Consumes: nothing (foundational)
- Produces: `auth.JWTManager`, `auth.Claims{Name, Role}`, `auth.ClaimsKey` (context key), `auth.Middleware(next) http.Handler`, `auth.RequireRole(role) func(http.Handler) http.Handler`

- [ ] **Step 1: Install JWT dependency**

Run:
```bash
cd app && go get github.com/golang-jwt/jwt/v5
```

- [ ] **Step 2: Write failing tests for JWT manager**

Create `app/internal/auth/jwt_test.go`:
```go
package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJWTSignAndVerify(t *testing.T) {
	mgr := NewJWTManager("test-secret")
	token, err := mgr.Sign("alice", "buyer")
	require.NoError(t, err)
	assert.NotEmpty(t, token)

	claims, err := mgr.Verify(token)
	require.NoError(t, err)
	assert.Equal(t, "alice", claims.Name)
	assert.Equal(t, "buyer", claims.Role)
}

func TestJWTVerifyInvalidToken(t *testing.T) {
	mgr := NewJWTManager("test-secret")
	_, err := mgr.Verify("invalid-token-string")
	assert.Error(t, err)
}

func TestJWTVerifyWrongSecret(t *testing.T) {
	mgr1 := NewJWTManager("secret-one")
	mgr2 := NewJWTManager("secret-two")
	token, err := mgr1.Sign("alice", "buyer")
	require.NoError(t, err)

	_, err = mgr2.Verify(token)
	assert.Error(t, err)
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd app && go test ./internal/auth/ -v`
Expected: FAIL — `NewJWTManager` not defined

- [ ] **Step 4: Implement JWT manager**

Create `app/internal/auth/jwt.go`:
```go
package auth

import (
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Claims holds the JWT custom claims for a session.
type Claims struct {
	Name string `json:"name"`
	Role string `json:"role"`
	jwt.RegisteredClaims
}

// JWTManager signs and verifies JWT tokens.
type JWTManager struct {
	secret []byte
}

// NewJWTManager creates a JWTManager with the given signing secret.
func NewJWTManager(secret string) *JWTManager {
	return &JWTManager{secret: []byte(secret)}
}

// Sign creates a signed JWT token for the given name and role.
func (m *JWTManager) Sign(name, role string) (string, error) {
	claims := Claims{
		Name: name,
		Role: role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.secret)
}

// Verify validates a JWT token string and returns the claims.
func (m *JWTManager) Verify(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return m.secret, nil
	})
	if err != nil {
		return nil, err
	}
	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token")
	}
	return claims, nil
}
```

- [ ] **Step 5: Run JWT tests to verify they pass**

Run: `cd app && go test ./internal/auth/ -run TestJWT -v`
Expected: PASS

- [ ] **Step 6: Write failing tests for middleware**

Create `app/internal/auth/middleware_test.go`:
```go
package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMiddlewareValidToken(t *testing.T) {
	mgr := NewJWTManager("test-secret")
	token, _ := mgr.Sign("alice", "buyer")

	called := false
	handler := mgr.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		claims := r.Context().Value(ClaimsKey).(*Claims)
		assert.Equal(t, "alice", claims.Name)
		assert.Equal(t, "buyer", claims.Role)
		w.WriteHeader(200)
	}))

	req := httptest.NewRequest("GET", "/api/buyer/products", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.True(t, called)
	assert.Equal(t, 200, rec.Code)
}

func TestMiddlewareNoToken(t *testing.T) {
	mgr := NewJWTManager("test-secret")

	handler := mgr.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler without token")
	}))

	req := httptest.NewRequest("GET", "/api/buyer/products", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, 401, rec.Code)
}

func TestMiddlewareInvalidToken(t *testing.T) {
	mgr := NewJWTManager("test-secret")

	handler := mgr.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler with invalid token")
	}))

	req := httptest.NewRequest("GET", "/api/buyer/products", nil)
	req.Header.Set("Authorization", "Bearer garbage")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, 401, rec.Code)
}

func TestRequireRoleAllowed(t *testing.T) {
	handler := RequireRole("buyer")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	claims := &Claims{Name: "alice", Role: "buyer"}
	ctx := context.WithValue(context.Background(), ClaimsKey, claims)
	req := httptest.NewRequest("GET", "/api/buyer/products", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, 200, rec.Code)
}

func TestRequireRoleDenied(t *testing.T) {
	handler := RequireRole("seller")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler with wrong role")
	}))

	claims := &Claims{Name: "alice", Role: "buyer"}
	ctx := context.WithValue(context.Background(), ClaimsKey, claims)
	req := httptest.NewRequest("GET", "/api/seller/products", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, 403, rec.Code)
}

func TestRequireRoleNoClaims(t *testing.T) {
	handler := RequireRole("buyer")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler without claims")
	}))

	req := httptest.NewRequest("GET", "/api/buyer/products", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, 401, rec.Code)
}
```

- [ ] **Step 7: Run middleware tests to verify they fail**

Run: `cd app && go test ./internal/auth/ -run TestMiddleware -v && go test ./internal/auth/ -run TestRequireRole -v`
Expected: FAIL — `Middleware`, `RequireRole`, `ClaimsKey` not defined

- [ ] **Step 8: Implement middleware**

Create `app/internal/auth/middleware.go`:
```go
package auth

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
)

// ClaimsKey is the context key for storing JWT claims in request context.
type contextKey string

const ClaimsKey contextKey = "claims"

// Middleware verifies the JWT token from the Authorization header and
// injects the claims into the request context.
func (m *JWTManager) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "missing authorization header", http.StatusUnauthorized)
			return
		}
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || parts[0] != "Bearer" {
			http.Error(w, "invalid authorization header format", http.StatusUnauthorized)
			return
		}
		claims, err := m.Verify(parts[1])
		if err != nil {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), ClaimsKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireRole returns middleware that checks the authenticated user has
// the specified role. Must be used after AuthMiddleware.
func RequireRole(role string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := r.Context().Value(ClaimsKey).(*Claims)
			if !ok {
				http.Error(w, "no claims in context", http.StatusUnauthorized)
				return
			}
			if claims.Role != role {
				slog.Warn("role check failed",
					"path", r.URL.Path,
					"expected_role", role,
					"actual_role", claims.Role,
				)
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
```

- [ ] **Step 9: Run all auth tests and verify they pass**

Run: `cd app && go test ./internal/auth/ -v`
Expected: PASS (all 8 tests)

- [ ] **Step 10: Commit**

```bash
git add app/internal/auth/ app/go.mod app/go.sum
git commit -m "feat: JWT auth manager and role-based middleware"
```

---

## Task 3: Session Store + Handler

**Files:**
- Create: `app/internal/session/store.go`
- Create: `app/internal/session/store_test.go`
- Create: `app/internal/session/handler.go`
- Create: `app/internal/session/handler_test.go`

**Interfaces:**
- Consumes: `auth.JWTManager` (for signing tokens)
- Produces: `session.Store` (in-memory session store), `session.Handler` (HTTP handler for POST /api/session), `session.ErrDuplicateName`, `session.ErrInvalidRole`

- [ ] **Step 1: Write failing tests for session store**

Create `app/internal/session/store_test.go`:
```go
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
```

- [ ] **Step 2: Run store tests to verify they fail**

Run: `cd app && go test ./internal/session/ -v`
Expected: FAIL — `NewStore`, `ErrDuplicateName` not defined

- [ ] **Step 3: Implement session store**

Create `app/internal/session/store.go`:
```go
package session

import (
	"errors"
	"sync"
)

var ErrDuplicateName = errors.New("session name already taken")

var validRoles = map[string]bool{
	"buyer":    true,
	"seller":   true,
	"shipper":  true,
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
```

- [ ] **Step 4: Run store tests to verify they pass**

Run: `cd app && go test ./internal/session/ -run TestStore -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Write failing tests for session handler**

Create `app/internal/session/handler_test.go`:
```go
package session

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuang/flink-demo/internal/auth"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler() *Handler {
	store := NewStore()
	jwtMgr := auth.NewJWTManager("test-secret")
	return NewHandler(store, jwtMgr)
}

func TestCreateSessionSuccess(t *testing.T) {
	h := newTestHandler()

	body := strings.NewReader(`{"name":"alice","role":"buyer"}`)
	req := httptest.NewRequest("POST", "/api/session", body)
	rec := httptest.NewRecorder()
	h.CreateSession(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		Token string `json:"token"`
		Name  string `json:"name"`
		Role  string `json:"role"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotEmpty(t, resp.Token)
	assert.Equal(t, "alice", resp.Name)
	assert.Equal(t, "buyer", resp.Role)
}

func TestCreateSessionDuplicateName(t *testing.T) {
	h := newTestHandler()
	h.store.Create("alice", "buyer")

	body := strings.NewReader(`{"name":"alice","role":"seller"}`)
	req := httptest.NewRequest("POST", "/api/session", body)
	rec := httptest.NewRecorder()
	h.CreateSession(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestCreateSessionInvalidRole(t *testing.T) {
	h := newTestHandler()

	body := strings.NewReader(`{"name":"alice","role":"admin"}`)
	req := httptest.NewRequest("POST", "/api/session", body)
	rec := httptest.NewRecorder()
	h.CreateSession(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateSessionEmptyName(t *testing.T) {
	h := newTestHandler()

	body := strings.NewReader(`{"name":"","role":"buyer"}`)
	req := httptest.NewRequest("POST", "/api/session", body)
	rec := httptest.NewRecorder()
	h.CreateSession(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateSessionInvalidJSON(t *testing.T) {
	h := newTestHandler()

	body := strings.NewReader(`{invalid json}`)
	req := httptest.NewRequest("POST", "/api/session", body)
	rec := httptest.NewRecorder()
	h.CreateSession(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
```

- [ ] **Step 6: Run handler tests to verify they fail**

Run: `cd app && go test ./internal/session/ -run TestCreateSession -v`
Expected: FAIL — `Handler`, `NewHandler` not defined

- [ ] **Step 7: Implement session handler**

Create `app/internal/session/handler.go`:
```go
package session

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/kuang/flink-demo/internal/auth"
)

// Handler handles session-related HTTP requests.
type Handler struct {
	store  *Store
	jwtMgr *auth.JWTManager
}

// NewHandler creates a session handler with the given store and JWT manager.
func NewHandler(store *Store, jwtMgr *auth.JWTManager) *Handler {
	return &Handler{store: store, jwtMgr: jwtMgr}
}

type createSessionRequest struct {
	Name string `json:"name"`
	Role string `json:"role"`
}

type createSessionResponse struct {
	Token string `json:"token"`
	Name  string `json:"name"`
	Role  string `json:"role"`
}

// CreateSession handles POST /api/session.
func (h *Handler) CreateSession(w http.ResponseWriter, r *http.Request) {
	var req createSessionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	if !IsValidRole(req.Role) {
		http.Error(w, "invalid role", http.StatusBadRequest)
		return
	}

	if err := h.store.Create(req.Name, req.Role); err != nil {
		if errors.Is(err, ErrDuplicateName) {
			http.Error(w, "name already taken", http.StatusConflict)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	token, err := h.jwtMgr.Sign(req.Name, req.Role)
	if err != nil {
		slog.Error("failed to sign JWT", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	slog.Info("session created", "name", req.Name, "role", req.Role)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(createSessionResponse{
		Token: token,
		Name:  req.Name,
		Role:  req.Role,
	})
}
```

- [ ] **Step 8: Run all session tests and verify they pass**

Run: `cd app && go test ./internal/session/ -v`
Expected: PASS (all 8 tests)

- [ ] **Step 9: Commit**

```bash
git add app/internal/session/
git commit -m "feat: session store and POST /api/session handler"
```

---

## Task 4: Kafka Client + Topic Auto-Creation

**Files:**
- Create: `app/internal/kafkaclient/topics.go`
- Create: `app/internal/kafkaclient/topics_test.go`
- Create: `app/internal/kafkaclient/client.go`

**Interfaces:**
- Consumes: Kafka broker address (from config)
- Produces: `kafkaclient.RequiredTopics() []string`, `kafkaclient.Client` with `CreateTopics(ctx) error` and `Close()`

- [ ] **Step 1: Install Kafka dependency**

Run:
```bash
cd app && go get github.com/segmentio/kafka-go
```

- [ ] **Step 2: Write failing test for topic list**

Create `app/internal/kafkaclient/topics_test.go`:
```go
package kafkaclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequiredTopicsContainsAllExpected(t *testing.T) {
	topics := RequiredTopics()

	// Input topics
	assert.Contains(t, topics, "product.listed")
	assert.Contains(t, topics, "cart.item.added")
	assert.Contains(t, topics, "cart.checkout")
	assert.Contains(t, topics, "order.confirmed")
	assert.Contains(t, topics, "shipment.picked")
	assert.Contains(t, topics, "shipment.delivered")

	// Output topics
	assert.Contains(t, topics, "flink.window.stats")
	assert.Contains(t, topics, "flink.cep.alerts")

	assert.Len(t, topics, 8)
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd app && go test ./internal/kafkaclient/ -v`
Expected: FAIL — `RequiredTopics` not defined

- [ ] **Step 4: Implement topics list**

Create `app/internal/kafkaclient/topics.go`:
```go
package kafkaclient

// RequiredTopics returns all Kafka topics the demo needs.
// 6 input topics (produced by Go API) + 2 output topics (produced by Flink).
var requiredTopics = []string{
	// Input topics — produced by Go API from role actions
	"product.listed",
	"cart.item.added",
	"cart.checkout",
	"order.confirmed",
	"shipment.picked",
	"shipment.delivered",
	// Output topics — produced by Flink jobs
	"flink.window.stats",
	"flink.cep.alerts",
}

// RequiredTopics returns a copy of the required topic list.
func RequiredTopics() []string {
	result := make([]string, len(requiredTopics))
	copy(result, requiredTopics)
	return result
}
```

- [ ] **Step 5: Run topic test to verify it passes**

Run: `cd app && go test ./internal/kafkaclient/ -run TestRequiredTopics -v`
Expected: PASS

- [ ] **Step 6: Implement Kafka client with topic creation**

Create `app/internal/kafkaclient/client.go`:
```go
package kafkaclient

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

// Client wraps a Kafka admin connection for topic management.
type Client struct {
	addr string
}

// NewClient creates a Kafka client for the given broker address.
func NewClient(addr string) *Client {
	return &Client{addr: addr}
}

// CreateTopics creates all required Kafka topics if they don't exist.
// Retries up to 10 times with 2-second intervals until Kafka is reachable.
func (c *Client) CreateTopics(ctx context.Context) error {
	conn, err := c.dialWithRetry(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer conn.Close()

	topics := RequiredTopics()
	for _, topic := range topics {
		err := conn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
		if err != nil {
			slog.Warn("failed to create topic (may already exist)", "topic", topic, "error", err)
			continue
		}
		slog.Info("created kafka topic", "topic", topic)
	}

	return nil
}

func (c *Client) dialWithRetry(ctx context.Context) (*kafka.Conn, error) {
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		conn, err := kafka.DialContext(ctx, "tcp", c.addr)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		slog.Warn("kafka connection attempt failed", "attempt", attempt+1, "error", err)
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("kafka unreachable after retries: %w", lastErr)
}

// Ping checks if the Kafka broker is reachable.
func (c *Client) Ping(ctx context.Context) error {
	d := net.Dialer{Timeout: 3 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}
```

- [ ] **Step 7: Run all kafkaclient tests and verify they pass**

Run: `cd app && go test ./internal/kafkaclient/ -v`
Expected: PASS (1 test — topic list; client creation is tested via integration in Task 9)

- [ ] **Step 8: Commit**

```bash
git add app/internal/kafkaclient/ app/go.mod app/go.sum
git commit -m "feat: Kafka client with topic auto-creation"
```

---

## Task 5: HTTP Server + Routes + Health Check

**Files:**
- Create: `app/internal/server/server.go`
- Create: `app/internal/server/server_test.go`
- Modify: `app/main.go`

**Interfaces:**
- Consumes: `config.Config`, `*slog.Logger`, `*auth.JWTManager`, `*session.Handler`, `*kafkaclient.Client`
- Produces: `server.New(cfg, logger, jwtMgr, sessionHandler, kafkaClient) *Server`, `Server.Start() error`, `Server.Shutdown(ctx) error`

- [ ] **Step 1: Write failing tests for server**

Create `app/internal/server/server_test.go`:
```go
package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/config"
	"github.com/kuang/flink-demo/internal/kafkaclient"
	"github.com/kuang/flink-demo/internal/logging"
	"github.com/kuang/flink-demo/internal/session"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestServer() *Server {
	cfg := config.Config{
		Port:      "0",
		JWTSecret: "test-secret",
		KafkaAddr: "localhost:9092",
	}
	jwtMgr := auth.NewJWTManager(cfg.JWTSecret)
	sessionStore := session.NewStore()
	sessionHandler := session.NewHandler(sessionStore, jwtMgr)
	kafkaClient := kafkaclient.NewClient(cfg.KafkaAddr)
	return New(cfg, logging.NewLogger(), jwtMgr, sessionHandler, kafkaClient)
}

func TestHealthCheck(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/health", nil)
	rec := httptest.NewRecorder()
	srv.handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestSessionEndpointCreatesToken(t *testing.T) {
	srv := newTestServer()
	body := strings.NewReader(`{"name":"alice","role":"buyer"}`)
	req := httptest.NewRequest("POST", "/api/session", body)
	rec := httptest.NewRecorder()
	srv.handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestProtectedRouteWithoutToken(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/buyer/products", nil)
	rec := httptest.NewRecorder()
	srv.handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestProtectedRouteWithWrongRole(t *testing.T) {
	srv := newTestServer()
	token := createSessionAndGetToken(t, srv, "alice", "buyer")

	// Try to access seller route with buyer token
	req2 := httptest.NewRequest("GET", "/api/seller/products", nil)
	req2.Header.Set("Authorization", "Bearer "+token)
	rec2 := httptest.NewRecorder()
	srv.handler.ServeHTTP(rec2, req2)

	assert.Equal(t, http.StatusForbidden, rec2.Code)
}

func TestProtectedRouteWithCorrectRole(t *testing.T) {
	srv := newTestServer()
	token := createSessionAndGetToken(t, srv, "alice", "buyer")

	// Access buyer route with buyer token
	req2 := httptest.NewRequest("GET", "/api/buyer/products", nil)
	req2.Header.Set("Authorization", "Bearer "+token)
	rec2 := httptest.NewRecorder()
	srv.handler.ServeHTTP(rec2, req2)

	assert.Equal(t, http.StatusOK, rec2.Code)
}

// createSessionAndGetToken is a helper that creates a session and extracts
// the JWT token from the response.
func createSessionAndGetToken(t *testing.T, srv *Server, name, role string) string {
	t.Helper()
	body := strings.NewReader(`{"name":"` + name + `","role":"` + role + `"}`)
	req := httptest.NewRequest("POST", "/api/session", body)
	rec := httptest.NewRecorder()
	srv.handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		Token string `json:"token"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.NotEmpty(t, resp.Token)
	return resp.Token
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd app && go test ./internal/server/ -v`
Expected: FAIL — `Server`, `New` not defined

- [ ] **Step 3: Implement HTTP server**

Create `app/internal/server/server.go`:
```go
package server

import (
	"context"
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/config"
	"github.com/kuang/flink-demo/internal/kafkaclient"
	"github.com/kuang/flink-demo/internal/session"
	"github.com/kuang/flink-demo/web"
)

// Server is the HTTP server for the demo application.
type Server struct {
	cfg            config.Config
	logger         *slog.Logger
	jwtMgr         *auth.JWTManager
	sessionHandler *session.Handler
	kafkaClient    *kafkaclient.Client
	httpServer     *http.Server
	handler        http.Handler // exposed for testing
}

// New creates a new Server with all dependencies wired.
func New(
	cfg config.Config,
	logger *slog.Logger,
	jwtMgr *auth.JWTManager,
	sessionHandler *session.Handler,
	kafkaClient *kafkaclient.Client,
) *Server {
	s := &Server{
		cfg:            cfg,
		logger:         logger,
		jwtMgr:         jwtMgr,
		sessionHandler: sessionHandler,
		kafkaClient:    kafkaClient,
	}
	s.handler = s.buildRoutes()
	return s
}

func (s *Server) buildRoutes() http.Handler {
	mux := http.NewServeMux()

	// Public routes
	mux.HandleFunc("POST /api/session", s.sessionHandler.CreateSession)
	mux.HandleFunc("GET /api/health", s.healthHandler)

	// Auth middleware applied to all role-namespaced routes
	authMW := s.jwtMgr.Middleware

	// Seller routes (Phase 1: placeholder)
	mux.Handle("/api/seller/", authMW(auth.RequireRole("seller")(http.HandlerFunc(s.placeholderHandler))))

	// Buyer routes (Phase 1: placeholder)
	mux.Handle("/api/buyer/", authMW(auth.RequireRole("buyer")(http.HandlerFunc(s.placeholderHandler))))

	// Shipper routes (Phase 1: placeholder)
	mux.Handle("/api/shipper/", authMW(auth.RequireRole("shipper")(http.HandlerFunc(s.placeholderHandler))))

	// Static files — serve embedded React build from the web package
	distFS, err := fs.Sub(web.DistFS, "dist")
	if err != nil {
		s.logger.Error("failed to get embedded web filesystem", "error", err)
	} else {
		fileServer := http.FileServer(http.FS(distFS))
		mux.Handle("/", s.spaHandler(fileServer, distFS))
	}

	return s.loggingMiddleware(mux)
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) placeholderHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "not implemented",
		"path":   r.URL.Path,
	})
}

// spaHandler wraps a file server to serve index.html for any path that
// doesn't match a static file (SPA client-side routing).
func (s *Server) spaHandler(fileServer http.Handler, distFS fs.FS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/" {
			path = "index.html"
		} else {
			path = path[1:] // strip leading slash
		}
		_, err := fs.Stat(distFS, path)
		if err != nil {
			// File not found — serve index.html for SPA routing
			r.URL.Path = "/"
			fileServer.ServeHTTP(w, r)
			return
		}
		fileServer.ServeHTTP(w, r)
	})
}

// loggingMiddleware logs each HTTP request.
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(wrapped, r)
		slog.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Start creates Kafka topics, then starts the HTTP server.
func (s *Server) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.logger.Info("creating kafka topics")
	if err := s.kafkaClient.CreateTopics(ctx); err != nil {
		s.logger.Warn("failed to create kafka topics", "error", err)
	}

	s.httpServer = &http.Server{
		Addr:    ":" + s.cfg.Port,
		Handler: s.handler,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		s.logger.Info("http server listening", "port", s.cfg.Port)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-stop
	s.logger.Info("shutting down")
	return s.Shutdown(context.Background())
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
```

- [ ] **Step 4: Run server tests to verify they pass**

Run: `cd app && go test ./internal/server/ -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Update main.go to wire the full server**

Modify `app/main.go`:
```go
package main

import (
	"log/slog"

	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/config"
	"github.com/kuang/flink-demo/internal/kafkaclient"
	"github.com/kuang/flink-demo/internal/logging"
	"github.com/kuang/flink-demo/internal/server"
	"github.com/kuang/flink-demo/internal/session"
)

func main() {
	logger := logging.NewLogger()
	cfg := config.Load()

	logger.Info("starting server",
		"port", cfg.Port,
		"kafka_addr", cfg.KafkaAddr,
	)

	jwtMgr := auth.NewJWTManager(cfg.JWTSecret)
	sessionStore := session.NewStore()
	sessionHandler := session.NewHandler(sessionStore, jwtMgr)
	kafkaClient := kafkaclient.NewClient(cfg.KafkaAddr)

	srv := server.New(cfg, logger, jwtMgr, sessionHandler, kafkaClient)

	if err := srv.Start(); err != nil {
		slog.Error("server failed", "error", err)
	}
}
```

- [ ] **Step 6: Verify the full app compiles and tests pass**

Run: `cd app && go build ./... && go test ./... -v`
Expected: Build succeeds, all tests PASS

- [ ] **Step 7: Commit**

```bash
git add app/internal/server/ app/main.go
git commit -m "feat: HTTP server with routes, auth middleware, health check, embedded static files"
```

---

## Task 6: React Project Setup + API Client + Session Context

**Files:**
- Create: `web/package.json`
- Create: `web/tsconfig.json`
- Create: `web/vite.config.ts`
- Create: `web/index.html`
- Create: `web/src/main.tsx`
- Create: `web/src/App.tsx`
- Create: `web/src/api/client.ts`
- Create: `web/src/context/SessionContext.tsx`

**Interfaces:**
- Produces: `api.createSession(name, role)`, `SessionContext` provider with `{token, name, role, setSession, clearSession}`

No frontend tests in Phase 1.

- [ ] **Step 1: Create Vite React TypeScript project**

Run:
```bash
npm create vite@latest web -- --template react-ts
cd web && npm install
npm install react-router-dom
```

- [ ] **Step 2: Configure Vite with proxy and output path**

Overwrite `web/vite.config.ts`:
```typescript
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
      '/ws': {
        target: 'http://localhost:8080',
        ws: true,
      },
    },
  },
  build: {
    outDir: '../app/web/dist',
    emptyOutDir: true,
  },
})
```

- [ ] **Step 3: Configure TypeScript strict mode**

Overwrite `web/tsconfig.json`:
```json
{
  "compilerOptions": {
    "target": "ES2020",
    "useDefineForClassFields": true,
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "noEmit": true,
    "jsx": "react-jsx",
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true
  },
  "include": ["src"]
}
```

- [ ] **Step 4: Create API client**

Create `web/src/api/client.ts`:
```typescript
const API_BASE = '/api';

export interface SessionResponse {
  token: string;
  name: string;
  role: string;
}

export async function createSession(name: string, role: string): Promise<SessionResponse> {
  const resp = await fetch(`${API_BASE}/session`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, role }),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `Failed to create session (status ${resp.status})`);
  }
  return resp.json();
}

export async function apiGet(path: string, token: string): Promise<Response> {
  return fetch(`${API_BASE}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
}

export async function apiPost(path: string, token: string, body?: unknown): Promise<Response> {
  return fetch(`${API_BASE}${path}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
}
```

- [ ] **Step 5: Create session context**

Create `web/src/context/SessionContext.tsx`:
```typescript
import { createContext, useContext, useState, type ReactNode } from 'react';

export type Role = 'buyer' | 'seller' | 'shipper' | 'dashboard';

interface SessionState {
  token: string | null;
  name: string | null;
  role: Role | null;
  setSession: (token: string, name: string, role: Role) => void;
  clearSession: () => void;
}

const SessionContext = createContext<SessionState | undefined>(undefined);

export function SessionProvider({ children }: { children: ReactNode }) {
  const [token, setToken] = useState<string | null>(() => {
    return localStorage.getItem('token');
  });
  const [name, setName] = useState<string | null>(() => {
    return localStorage.getItem('name');
  });
  const [role, setRole] = useState<Role | null>(() => {
    return localStorage.getItem('role') as Role | null;
  });

  const setSession = (newToken: string, newName: string, newRole: Role) => {
    setToken(newToken);
    setName(newName);
    setRole(newRole);
    localStorage.setItem('token', newToken);
    localStorage.setItem('name', newName);
    localStorage.setItem('role', newRole);
  };

  const clearSession = () => {
    setToken(null);
    setName(null);
    setRole(null);
    localStorage.removeItem('token');
    localStorage.removeItem('name');
    localStorage.removeItem('role');
  };

  return (
    <SessionContext.Provider value={{ token, name, role, setSession, clearSession }}>
      {children}
    </SessionContext.Provider>
  );
}

export function useSession(): SessionState {
  const ctx = useContext(SessionContext);
  if (!ctx) {
    throw new Error('useSession must be used within SessionProvider');
  }
  return ctx;
}
```

- [ ] **Step 6: Create App.tsx with router (placeholder routes)**

Overwrite `web/src/App.tsx`:
```typescript
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { SessionProvider } from './context/SessionContext';
import Landing from './pages/Landing';
import Seller from './pages/Seller';
import Buyer from './pages/Buyer';
import Shipper from './pages/Shipper';
import Dashboard from './pages/Dashboard';

function App() {
  return (
    <SessionProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Landing />} />
          <Route path="/seller" element={<Seller />} />
          <Route path="/buyer" element={<Buyer />} />
          <Route path="/shipper" element={<Shipper />} />
          <Route path="/dashboard" element={<Dashboard />} />
        </Routes>
      </BrowserRouter>
    </SessionProvider>
  );
}

export default App;
```

- [ ] **Step 7: Create placeholder page components**

Create `web/src/pages/Landing.tsx`:
```typescript
export default function Landing() {
  return <div><h1>Stream Processing Demo</h1><p>Loading...</p></div>;
}
```

Create `web/src/pages/Seller.tsx`:
```typescript
export default function Seller() {
  return <div><h1>Seller</h1></div>;
}
```

Create `web/src/pages/Buyer.tsx`:
```typescript
export default function Buyer() {
  return <div><h1>Buyer</h1></div>;
}
```

Create `web/src/pages/Shipper.tsx`:
```typescript
export default function Shipper() {
  return <div><h1>Shipper</h1></div>;
}
```

Create `web/src/pages/Dashboard.tsx`:
```typescript
export default function Dashboard() {
  return <div><h1>Dashboard</h1></div>;
}
```

- [ ] **Step 8: Update main.tsx and index.html**

Overwrite `web/src/main.tsx`:
```typescript
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import './index.css';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
```

Overwrite `web/index.html`:
```html
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Stream Processing Demo</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

- [ ] **Step 9: Verify the frontend builds**

Run: `cd web && npm run build`
Expected: Build succeeds, output in `../app/web/dist/`

- [ ] **Step 10: Commit**

```bash
git add web/
git commit -m "feat: React project setup with Vite, API client, session context, routing"
```

---

## Task 7: Landing Page

**Files:**
- Modify: `web/src/pages/Landing.tsx`

No frontend tests in Phase 1.

- [ ] **Step 1: Implement the landing page with name input and role selection**

Overwrite `web/src/pages/Landing.tsx`:
```typescript
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { createSession, type SessionResponse } from '../api/client';
import { useSession, type Role } from '../context/SessionContext';

export default function Landing() {
  const [name, setName] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();
  const { setSession } = useSession();

  const handleRoleSelect = async (role: Role) => {
    if (!name.trim()) {
      setError('Please enter your name first');
      return;
    }
    setLoading(true);
    setError('');
    try {
      const resp: SessionResponse = await createSession(name.trim(), role);
      setSession(resp.token, resp.name, role);
      navigate(`/${role}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create session');
    } finally {
      setLoading(false);
    }
  };

  const handleDashboard = () => {
    navigate('/dashboard');
  };

  return (
    <div style={{ maxWidth: '600px', margin: '100px auto', textAlign: 'center' }}>
      <h1>Stream Processing Demo</h1>
      <p>E-commerce simulation with 3 levels of stream processing</p>

      <div style={{ margin: '40px 0' }}>
        <input
          type="text"
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="Enter your display name"
          style={{ padding: '12px', fontSize: '16px', width: '300px' }}
          disabled={loading}
        />
      </div>

      {error && <p style={{ color: 'red' }}>{error}</p>}

      <div style={{ display: 'flex', gap: '16px', justifyContent: 'center' }}>
        <button
          onClick={() => handleRoleSelect('buyer')}
          disabled={loading}
          style={{ padding: '16px 32px', fontSize: '16px', cursor: 'pointer' }}
        >
          Buyer
        </button>
        <button
          onClick={() => handleRoleSelect('seller')}
          disabled={loading}
          style={{ padding: '16px 32px', fontSize: '16px', cursor: 'pointer' }}
        >
          Seller
        </button>
        <button
          onClick={() => handleRoleSelect('shipper')}
          disabled={loading}
          style={{ padding: '16px 32px', fontSize: '16px', cursor: 'pointer' }}
        >
          Shipper
        </button>
      </div>

      <div style={{ marginTop: '40px' }}>
        <button
          onClick={handleDashboard}
          style={{ padding: '12px 24px', fontSize: '14px', cursor: 'pointer' }}
        >
          View Dashboard (no login needed)
        </button>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Verify the frontend builds**

Run: `cd web && npm run build`
Expected: Build succeeds

- [ ] **Step 3: Commit**

```bash
git add web/src/pages/Landing.tsx
git commit -m "feat: landing page with name input and role selection"
```

---

## Task 8: Role Placeholder Pages + Route Guards

**Files:**
- Modify: `web/src/pages/Seller.tsx`
- Modify: `web/src/pages/Buyer.tsx`
- Modify: `web/src/pages/Shipper.tsx`
- Modify: `web/src/pages/Dashboard.tsx`
- Modify: `web/src/App.tsx`

No frontend tests in Phase 1.

- [ ] **Step 1: Implement role placeholder pages with session display and logout**

Overwrite `web/src/pages/Seller.tsx`:
```typescript
import { useNavigate } from 'react-router-dom';
import { useSession } from '../context/SessionContext';

export default function Seller() {
  const { name, role, clearSession } = useSession();
  const navigate = useNavigate();

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  if (!name) {
    navigate('/');
    return null;
  }

  return (
    <div style={{ padding: '20px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h1>Seller: {name}</h1>
        <button onClick={handleLogout}>Logout</button>
      </div>
      <p>Product panel and order inbox will be implemented in Phase 2.</p>
    </div>
  );
}
```

Overwrite `web/src/pages/Buyer.tsx`:
```typescript
import { useNavigate } from 'react-router-dom';
import { useSession } from '../context/SessionContext';

export default function Buyer() {
  const { name, clearSession } = useSession();
  const navigate = useNavigate();

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  if (!name) {
    navigate('/');
    return null;
  }

  return (
    <div style={{ padding: '20px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h1>Buyer: {name}</h1>
        <button onClick={handleLogout}>Logout</button>
      </div>
      <p>Product catalog and cart will be implemented in Phase 2.</p>
    </div>
  );
}
```

Overwrite `web/src/pages/Shipper.tsx`:
```typescript
import { useNavigate } from 'react-router-dom';
import { useSession } from '../context/SessionContext';

export default function Shipper() {
  const { name, clearSession } = useSession();
  const navigate = useNavigate();

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  if (!name) {
    navigate('/');
    return null;
  }

  return (
    <div style={{ padding: '20px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h1>Shipper: {name}</h1>
        <button onClick={handleLogout}>Logout</button>
      </div>
      <p>Job board will be implemented in Phase 2.</p>
    </div>
  );
}
```

Overwrite `web/src/pages/Dashboard.tsx`:
```typescript
export default function Dashboard() {
  return (
    <div style={{ padding: '20px' }}>
      <h1>Dashboard</h1>
      <p>Live event feed, aggregations, and CEP alerts will be implemented in Phases 2–4.</p>
    </div>
  );
}
```

- [ ] **Step 2: Add basic CSS for a clean look**

Create `web/src/index.css`:
```css
* {
  box-sizing: border-box;
  margin: 0;
  padding: 0;
}

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, sans-serif;
  background: #f5f5f5;
  color: #333;
}

button {
  background: #2563eb;
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  transition: background 0.2s;
}

button:hover:not(:disabled) {
  background: #1d4ed8;
}

button:disabled {
  background: #9ca3af;
  cursor: not-allowed;
}

input {
  border: 1px solid #d1d5db;
  border-radius: 6px;
}
```

- [ ] **Step 3: Verify the frontend builds**

Run: `cd web && npm run build`
Expected: Build succeeds

- [ ] **Step 4: Commit**

```bash
git add web/src/pages/ web/src/index.css
git commit -m "feat: role placeholder pages with session display and logout"
```

---

## Task 9: Build Integration + Docker Compose

**Files:**
- Create: `app/Dockerfile`
- Create: `docker-compose.yml`
- Create: `Makefile`
- Create: `.dockerignore`

**Interfaces:**
- Produces: `docker compose up` command that starts the full stack

- [ ] **Step 1: Create Dockerfile for the Go app (multi-stage with React build)**

Create `app/Dockerfile`:
```dockerfile
# Stage 1: Build React frontend
FROM node:20-slim AS web-builder
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

# Stage 2: Build Go binary
FROM golang:1.22 AS go-builder
WORKDIR /app
COPY app/go.mod app/go.sum ./
RUN go mod download
COPY app/ ./
COPY --from=web-builder /app/web/dist ./web/dist/
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/app .

# Stage 3: Minimal runtime
FROM gcr.io/distroless/static-debian12
COPY --from=go-builder /bin/app /app
EXPOSE 8080
ENTRYPOINT ["/app"]
```

- [ ] **Step 2: Create .dockerignore**

Create `.dockerignore`:
```
.git
node_modules
app/bin
docs
*.md
```

- [ ] **Step 3: Create docker-compose.yml**

Create `docker-compose.yml`:
```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 5s
      timeout: 3s
      retries: 10

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      zookeeper:
        condition: service_healthy
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 5s
      timeout: 5s
      retries: 10

  app:
    build:
      context: .
      dockerfile: app/Dockerfile
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      KAFKA_ADDR: kafka:9092
      JWT_SECRET: demo-secret-not-for-production
      PORT: "8080"
    ports:
      - "8080:8080"
```

- [ ] **Step 4: Create Makefile**

Create `Makefile`:
```makefile
.PHONY: dev-web dev-app build test docker-up docker-down clean

# Run Vite dev server (port 5173, proxies /api to localhost:8080)
dev-web:
	cd web && npm run dev

# Run Go server (port 8080)
dev-app:
	cd app && go run .

# Build React then Go binary
build: build-web
	cd app && go build -o bin/app .

build-web:
	cd web && npm run build

# Run all Go tests
test:
	cd app && go test ./... -v

# Start full stack via Docker Compose
docker-up:
	docker compose up --build

# Stop full stack
docker-down:
	docker compose down

# Clean build artifacts
clean:
	rm -rf app/bin app/web/dist web/node_modules
```

- [ ] **Step 5: Test the full stack with Docker Compose**

Run: `docker compose up --build`
Expected:
- Zookeeper starts and becomes healthy
- Kafka starts and becomes healthy
- Go app starts, creates 8 Kafka topics, listens on port 8080

In another terminal, verify:
```bash
curl http://localhost:8080/api/health
```
Expected: `{"status":"ok"}`

```bash
curl -X POST http://localhost:8080/api/session \
  -H "Content-Type: application/json" \
  -d '{"name":"alice","role":"buyer"}'
```
Expected: `{"token":"eyJ...","name":"alice","role":"buyer"}`

```bash
# Test that the landing page is served
curl http://localhost:8080/
```
Expected: HTML with `<div id="root"></div>`

- [ ] **Step 6: Stop the stack**

Run: `docker compose down`

- [ ] **Step 7: Commit**

```bash
git add Makefile docker-compose.yml app/Dockerfile .dockerignore
git commit -m "feat: Docker Compose deployment with multi-stage build"
```

---

## Phase 1 Verification Checklist

After all 9 tasks are complete, verify:

- [ ] `docker compose up --build` starts Zookeeper, Kafka, and the Go app without errors
- [ ] Go app logs show "created kafka topic" for all 8 topics
- [ ] `GET /api/health` returns 200 `{"status":"ok"}`
- [ ] `POST /api/session` with valid name+role returns 201 with JWT token
- [ ] `POST /api/session` with duplicate name returns 409
- [ ] `POST /api/session` with invalid role returns 400
- [ ] Role-namespaced routes without token return 401
- [ ] Role-namespaced routes with wrong-role token return 403
- [ ] Role-namespaced routes with correct-role token return 200
- [ ] Browser at `http://localhost:8080/` shows the landing page
- [ ] Entering a name and clicking a role button navigates to the role page
- [ ] Role page shows the user's name and a logout button
- [ ] Logout returns to the landing page
- [ ] Dashboard is accessible without logging in
- [ ] `cd app && go test ./... -v` — all tests pass
- [ ] `cd web && npm run build` — frontend builds without errors
