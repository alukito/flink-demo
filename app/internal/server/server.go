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
