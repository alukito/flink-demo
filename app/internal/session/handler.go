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
