package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/FlowOrchestrator/floworch/internal/db"
	"github.com/FlowOrchestrator/floworch/internal/idempotency"
	"github.com/FlowOrchestrator/floworch/internal/kafka"
	"github.com/FlowOrchestrator/floworch/internal/retry"
	"github.com/FlowOrchestrator/floworch/internal/workflow"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	log.SetPrefix("[inventory-service] ")
	log.Println("starting...")

	port    := envOr("INVENTORY_SERVICE_PORT", "8083")
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

	producer  := kafka.NewProducer(brokers)
	defer producer.Close()

	consumer  := kafka.NewConsumer(brokers, kafka.TopicPaymentEvents, kafka.GroupInventoryService)
	defer consumer.Close()

	handler := &InventoryHandler{
		producer:  producer,
		engine:    workflow.NewEngine(database),
		idem:      idempotency.NewStore(database),
		scheduler: retry.NewScheduler(database, producer, "inventory-service", retry.DefaultPolicy),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go handler.scheduler.Run(ctx)
	go func() {
		log.Println("consuming payment.events...")
		if err := consumer.Run(ctx, handler.Handle); err != nil {
			log.Printf("consumer error: %v", err)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	srv := &http.Server{Addr: ":" + port, Handler: mux}
	go func() {
		log.Printf("metrics on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")
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
