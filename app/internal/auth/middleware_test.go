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
	token, _ := mgr.Sign("6f4b7fca-24f8-4233-bf7a-0f56737a847c", "alice", "buyer")

	called := false
	handler := mgr.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		claims := r.Context().Value(ClaimsKey).(*Claims)
		assert.Equal(t, "6f4b7fca-24f8-4233-bf7a-0f56737a847c", claims.ID)
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
