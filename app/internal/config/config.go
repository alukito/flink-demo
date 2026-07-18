package config

import "os"

// Config holds all application configuration loaded from environment.
type Config struct {
	Port      string
	JWTSecret string
	KafkaAddr string
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
