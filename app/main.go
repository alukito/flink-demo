package main

import (
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
