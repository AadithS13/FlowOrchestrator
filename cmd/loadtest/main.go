// loadtest fires N concurrent orders against the Order Service and
// prints a latency/throughput summary.
//
// Usage:
//
//	go run ./cmd/loadtest/...                          # 100 orders, 10 workers
//	go run ./cmd/loadtest/... -n 500 -c 50             # 500 orders, 50 workers
//	go run ./cmd/loadtest/... -url http://localhost:8081
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ── Request / Response types ──────────────────────────────────────────────────

type orderItem struct {
	ProductID string  `json:"product_id"`
	Name      string  `json:"name"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type createOrderReq struct {
	CustomerID string      `json:"customer_id"`
	Items      []orderItem `json:"items"`
}

type result struct {
	duration time.Duration
	status   int
	err      error
}

// ── Products pool — adds variety to the load ──────────────────────────────────

var products = []orderItem{
	{ProductID: "prod-1", Name: "Laptop",     Price: 999.99},
	{ProductID: "prod-2", Name: "Mouse",      Price: 29.99},
	{ProductID: "prod-3", Name: "Keyboard",   Price: 79.99},
	{ProductID: "prod-4", Name: "Monitor",    Price: 349.99},
	{ProductID: "prod-5", Name: "Headphones", Price: 149.99},
	{ProductID: "prod-6", Name: "Webcam",     Price: 89.99},
}

func randomOrder(i int) createOrderReq {
	numItems := 1 + rand.Intn(3)
	items := make([]orderItem, numItems)
	for j := range items {
		p := products[rand.Intn(len(products))]
		p.Quantity = 1 + rand.Intn(3)
		items[j] = p
	}
	return createOrderReq{
		CustomerID: fmt.Sprintf("load-cust-%04d", i%50), // 50 unique customers
		Items:      items,
	}
}

// ── Worker ────────────────────────────────────────────────────────────────────

func fireOrder(client *http.Client, url string, i int) result {
	body, _ := json.Marshal(randomOrder(i))

	start := time.Now()
	resp, err := client.Post(url+"/orders", "application/json", bytes.NewReader(body))
	dur := time.Since(start)

	if err != nil {
		return result{duration: dur, err: err}
	}
	defer resp.Body.Close()
	return result{duration: dur, status: resp.StatusCode}
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	n       := flag.Int("n", 100, "total number of orders to send")
	workers := flag.Int("c", 10,  "concurrent workers")
	url     := flag.String("url", "http://localhost:8081", "order service base URL")
	flag.Parse()

	fmt.Printf("\n🚀 FlowOrchestrator Load Test\n")
	fmt.Printf("   Orders:  %d\n", *n)
	fmt.Printf("   Workers: %d\n", *workers)
	fmt.Printf("   Target:  %s\n\n", *url)

	client := &http.Client{Timeout: 10 * time.Second}

	jobs    := make(chan int, *n)
	results := make(chan result, *n)

	// Spin up workers
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				results <- fireOrder(client, *url, i)
			}
		}()
	}

	// Enqueue jobs
	testStart := time.Now()
	for i := 0; i < *n; i++ {
		jobs <- i
	}
	close(jobs)

	// Wait for all workers then close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var (
		success   int64
		failed    int64
		errCount  int64
		durations []float64
	)

	for r := range results {
		if r.err != nil {
			atomic.AddInt64(&errCount, 1)
			continue
		}
		ms := float64(r.duration.Milliseconds())
		durations = append(durations, ms)
		if r.status == 201 || r.status == 202 {
			atomic.AddInt64(&success, 1)
		} else {
			atomic.AddInt64(&failed, 1)
		}
	}

	totalTime := time.Since(testStart)
	sort.Float64s(durations)

	// ── Print report ──────────────────────────────────────────────────────
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  Total time    : %s\n", totalTime.Round(time.Millisecond))
	fmt.Printf("  Orders/sec    : %.1f\n", float64(*n)/totalTime.Seconds())
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  ✅ Success (201): %d\n", success)
	fmt.Printf("  ❌ Failed  (4xx): %d\n", failed)
	fmt.Printf("  💥 Net errors  : %d\n", errCount)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	if len(durations) > 0 {
		fmt.Printf("  Latency (ms):\n")
		fmt.Printf("    min  : %.1f ms\n", durations[0])
		fmt.Printf("    p50  : %.1f ms\n", percentile(durations, 50))
		fmt.Printf("    p90  : %.1f ms\n", percentile(durations, 90))
		fmt.Printf("    p95  : %.1f ms\n", percentile(durations, 95))
		fmt.Printf("    p99  : %.1f ms\n", percentile(durations, 99))
		fmt.Printf("    max  : %.1f ms\n", durations[len(durations)-1])
	}
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p / 100.0)
	return sorted[idx]
}
