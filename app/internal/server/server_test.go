package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/buyer"
	"github.com/kuang/flink-demo/internal/config"
	"github.com/kuang/flink-demo/internal/kafkaclient"
	"github.com/kuang/flink-demo/internal/logging"
	"github.com/kuang/flink-demo/internal/order"
	"github.com/kuang/flink-demo/internal/product"
	"github.com/kuang/flink-demo/internal/session"
	"github.com/kuang/flink-demo/internal/shipper"
	"github.com/kuang/flink-demo/internal/ws"

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
	producer := kafkaclient.NewProducer(cfg.KafkaAddr)

	productStore := product.NewStore()
	orderStore := order.NewStore()

	productHandler := product.NewHandler(productStore, orderStore, producer)
	buyerHandler := buyer.NewHandler(productStore, orderStore, producer)
	shipperHandler := shipper.NewHandler(orderStore, producer)

	hub := ws.NewHub()
	wsHandler := ws.NewHandler(jwtMgr, hub)
	consumer := kafkaclient.NewConsumer(cfg.KafkaAddr, hub)

	return New(cfg, logging.NewLogger(), jwtMgr, sessionHandler, kafkaClient,
		productHandler, buyerHandler, shipperHandler,
		wsHandler, hub, consumer, producer)
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
	var resp struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotEmpty(t, resp.ID)
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

func TestShipperDeliveriesRouteReturnsEmptyCollections(t *testing.T) {
	srv := newTestServer()
	token := createSessionAndGetToken(t, srv, "shipper", "shipper")

	req := httptest.NewRequest("GET", "/api/shipper/deliveries", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	srv.handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var response struct {
		Active  []json.RawMessage `json:"active"`
		History []json.RawMessage `json:"history"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
	assert.Empty(t, response.Active)
	assert.Empty(t, response.History)
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
