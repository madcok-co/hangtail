package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	counter atomic.Int64
	logFile *os.File
	rdb     *redis.Client
)

type orderRequest struct {
	Item   string  `json:"item"`
	Amount float64 `json:"amount"`
}

func writeLog(level, requestID, msg string, extra map[string]any) {
	entry := map[string]any{
		"ts":         time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00"),
		"level":      level,
		"service":    "api-plain",
		"request_id": requestID,
		"msg":        msg,
	}
	for k, v := range extra {
		entry[k] = v
	}
	b, _ := json.Marshal(entry)
	fmt.Fprintln(logFile, string(b))
}

func handleOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	var req orderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	requestID := fmt.Sprintf("req-%06d", counter.Add(1))

	writeLog("INFO", requestID, "order received", map[string]any{
		"item": req.Item, "amount": req.Amount,
	})

	payload, _ := json.Marshal(map[string]any{
		"request_id": requestID,
		"item":       req.Item,
		"amount":     req.Amount,
	})
	if err := rdb.LPush(context.Background(), "orders", payload).Err(); err != nil {
		writeLog("ERROR", requestID, "failed to queue order", map[string]any{"error": err.Error()})
		http.Error(w, "queue error", http.StatusInternalServerError)
		return
	}

	writeLog("INFO", requestID, "order queued", nil)
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"request_id": requestID})
}

func main() {
	logPath := os.Getenv("LOG_FILE")
	if logPath == "" {
		logPath = "/var/log/app/api.log"
	}
	if err := os.MkdirAll("/var/log/app", 0o755); err != nil {
		log.Fatal(err)
	}
	var err error
	logFile, err = os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	rdb = redis.NewClient(&redis.Options{Addr: redisAddr})

	http.HandleFunc("/order", handleOrder)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	log.Println("api-plain listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
