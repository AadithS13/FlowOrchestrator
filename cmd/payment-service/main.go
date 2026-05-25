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
	"github.com/FlowOrchestrator/floworch/internal/idempotency"
	"github.com/FlowOrchestrator/floworch/internal/kafka"
	"github.com/FlowOrchestrator/floworch/internal/logger"
	"github.com/FlowOrchestrator/floworch/internal/retry"
	"github.com/FlowOrchestrator/floworch/internal/workflow"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	log := logger.Init("payment-service")
	log.Info().Msg("starting")

	port    := envOr("PAYMENT_SERVICE_PORT", "8082")
	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")

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

	producer  := kafka.NewProducer(brokers)
	defer producer.Close()

	consumer  := kafka.NewConsumer(brokers, kafka.TopicOrderCreated, kafka.GroupPaymentService)
	defer consumer.Close()

	engine    := workflow.NewEngine(database)
	idemStore := idempotency.NewStore(database)
	scheduler := retry.NewScheduler(database, producer, "payment-service", retry.DefaultPolicy)

	handler := &PaymentHandler{
		log:       log,
		producer:  producer,
		engine:    engine,
		idem:      idemStore,
		scheduler: scheduler,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go scheduler.Run(ctx)
	log.Info().Msg("retry scheduler started")

	go func() {
		log.Info().Str("topic", kafka.TopicOrderCreated).Msg("consumer started")
		if err := consumer.Run(ctx, handler.Handle); err != nil {
			log.Error().Err(err).Msg("consumer stopped")
		}
	}()

	// ── HTTP: metrics + failure-rate config ──────────────────────────────
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/config/payment-failure-rate", failureRateHandler)

	srv := &http.Server{Addr: ":" + port, Handler: mux}
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
	cancel()

	sCtx, sCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer sCancel()
	_ = srv.Shutdown(sCtx)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
