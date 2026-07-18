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
