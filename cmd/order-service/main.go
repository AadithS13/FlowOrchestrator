package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/FlowOrchestrator/floworch/internal/db"
	"github.com/FlowOrchestrator/floworch/internal/kafka"
	"github.com/FlowOrchestrator/floworch/internal/logger"
	"github.com/FlowOrchestrator/floworch/internal/workflow"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	log := logger.Init("order-service")
	log.Info().Msg("starting")

	port := envOr("ORDER_SERVICE_PORT", "8081")

	database := db.MustConnect(db.Config{
		Host:     envOr("POSTGRES_HOST", "localhost"),
		Port:     5432,
		User:     envOr("POSTGRES_USER", "postgres"),
		Password: envOr("POSTGRES_PASSWORD", "postgres"),
		DBName:   envOr("POSTGRES_DB", "floworch"),
		SSLMode:  "disable",
	})
	defer database.Close()
	log.Info().Msg("postgres connected")

	producer := kafka.NewProducer(strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ","))
	defer producer.Close()
	log.Info().Msg("kafka producer ready")

	handler := &OrderHandler{
		log:      log,
		db:       database,
		producer: producer,
		engine:   workflow.NewEngine(database),
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handler.CreateOrder(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/orders/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handler.GetOrder(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Info().Str("port", port).Msg("http listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("http server error")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info().Msg("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
