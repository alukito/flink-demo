package main

import (
	"log/slog"

	"github.com/kuang/flink-demo/internal/auth"
	"github.com/kuang/flink-demo/internal/buyer"
	"github.com/kuang/flink-demo/internal/config"
	"github.com/kuang/flink-demo/internal/kafkaclient"
	"github.com/kuang/flink-demo/internal/logging"
	"github.com/kuang/flink-demo/internal/order"
	"github.com/kuang/flink-demo/internal/product"
	"github.com/kuang/flink-demo/internal/server"
	"github.com/kuang/flink-demo/internal/session"
	"github.com/kuang/flink-demo/internal/shipper"
	"github.com/kuang/flink-demo/internal/ws"
)

func main() {
	logger := logging.NewLogger()
	slog.SetDefault(logger)
	cfg := config.Load()

	logger.Info("starting server",
		"port", cfg.Port,
		"kafka_addr", cfg.KafkaAddr,
	)

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

	srv := server.New(cfg, logger, jwtMgr, sessionHandler, kafkaClient,
		productHandler, buyerHandler, shipperHandler,
		wsHandler, hub, consumer, producer)

	if err := srv.Start(); err != nil {
		slog.Error("server failed", "error", err)
	}
}
