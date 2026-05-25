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
	"github.com/FlowOrchestrator/floworch/internal/workflow"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	log.SetPrefix("[notification-service] ")
	log.Println("starting...")

	port    := envOr("NOTIFICATION_SERVICE_PORT", "8084")
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

	producer := kafka.NewProducer(brokers)
	defer producer.Close()

	// Notification service subscribes to BOTH payment and inventory events.
	paymentConsumer  := kafka.NewConsumer(brokers, kafka.TopicPaymentEvents, kafka.GroupNotificationService)
	inventoryConsumer := kafka.NewConsumer(brokers, kafka.TopicInventoryEvents, kafka.GroupNotificationService)
	defer paymentConsumer.Close()
	defer inventoryConsumer.Close()

	handler := &NotificationHandler{
		producer: producer,
		engine:   workflow.NewEngine(database),
		idem:     idempotency.NewStore(database),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		log.Println("consuming payment.events...")
		if err := paymentConsumer.Run(ctx, handler.HandlePayment); err != nil {
			log.Printf("payment consumer error: %v", err)
		}
	}()
	go func() {
		log.Println("consuming inventory.events...")
		if err := inventoryConsumer.Run(ctx, handler.HandleInventory); err != nil {
			log.Printf("inventory consumer error: %v", err)
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
