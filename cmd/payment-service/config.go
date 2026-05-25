package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync/atomic"
)

// failureRate is the global, atomically-updated payment failure percentage.
// Default: 30 (30%). Range: 0–100.
// Stored as uint64 so it can be read/written without a mutex.
var failureRate atomic.Uint64

func init() {
	failureRate.Store(30) // 30% failure by default
}

// getFailureRate returns the current failure probability as a float64 (0.0–1.0).
func getFailureRate() float64 {
	return float64(failureRate.Load()) / 100.0
}

// ── POST /config/payment-failure-rate ────────────────────────────────────────
//
// Demo control panel: lets you crank the failure rate up/down live
// without restarting the service — perfect for showing retries and DLQ
// in an interview or demo.
//
// Examples:
//
//	curl -X POST http://localhost:8082/config/payment-failure-rate \
//	     -d '{"rate": 100}'   # force all payments to fail → watch DLQ fill up
//
//	curl -X POST http://localhost:8082/config/payment-failure-rate \
//	     -d '{"rate": 0}'     # force all payments to succeed
//
//	curl -X POST http://localhost:8082/config/payment-failure-rate \
//	     -d '{"rate": 30}'    # back to default

type failureRateRequest struct {
	Rate uint64 `json:"rate"` // 0–100
}

type failureRateResponse struct {
	Rate    uint64 `json:"failure_rate_pct"`
	Message string `json:"message"`
}

func failureRateHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {

	case http.MethodPost:
		var req failureRateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
			return
		}
		if req.Rate > 100 {
			http.Error(w, `{"error":"rate must be 0–100"}`, http.StatusBadRequest)
			return
		}
		failureRate.Store(req.Rate)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(failureRateResponse{
			Rate:    req.Rate,
			Message: "payment failure rate updated",
		})

	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(failureRateResponse{
			Rate:    failureRate.Load(),
			Message: "current payment failure rate",
		})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// parsePort is a small helper used by the main file.
func parsePort(s string) int {
	p, _ := strconv.Atoi(s)
	return p
}
