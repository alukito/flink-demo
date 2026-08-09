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
		ID    string `json:"id"`
		Token string `json:"token"`
		Name  string `json:"name"`
		Role  string `json:"role"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotEmpty(t, resp.ID)
	assert.NotEmpty(t, resp.Token)
	assert.Equal(t, "alice", resp.Name)
	assert.Equal(t, "buyer", resp.Role)

	claims, err := h.jwtMgr.Verify(resp.Token)
	require.NoError(t, err)
	assert.Equal(t, resp.ID, claims.ID)
}

func TestCreateSessionAllowsDuplicateNamesWithDistinctIDs(t *testing.T) {
	h := newTestHandler()
	create := func() struct {
		ID string `json:"id"`
	} {
		body := strings.NewReader(`{"name":"alex","role":"seller"}`)
		req := httptest.NewRequest("POST", "/api/session", body)
		rec := httptest.NewRecorder()
		h.CreateSession(rec, req)
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp struct {
			ID string `json:"id"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		return resp
	}

	first := create()
	second := create()
	assert.NotEmpty(t, first.ID)
	assert.NotEmpty(t, second.ID)
	assert.NotEqual(t, first.ID, second.ID)
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
