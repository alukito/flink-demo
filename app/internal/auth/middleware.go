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
