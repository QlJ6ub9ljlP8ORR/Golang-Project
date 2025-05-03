package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/internal/kv"
	"mini_etcd/internal/raft"
)

type apiResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Value   string `json:"value,omitempty"`
}

func main() {
	// Parse command-line arguments
	id := flag.String("id", "node1", "Node ID")
	addr := flag.String("addr", ":9001", "Listen address")
	peersStr := flag.String("peers", "", "Comma list id=addr")
	dbTimeout := flag.Duration("db-timeout", 5*time.Second, "Database open timeout")
	httpTimeout := flag.Duration("http-timeout", 30*time.Second, "HTTP server request timeout")
	flag.Parse()

	// Configure logging
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// ---------- cluster topology ----------
	peers := map[string]string{}
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			parts := strings.Split(p, "=")
			if len(parts) != 2 {
				log.Fatalf("[ERROR] Invalid peer format: %s (expected id=addr)", p)
			}
			peers[parts[0]] = parts[1]
		}
	}

	// ---------- raft + kv ----------
	// Create directory if it doesn't exist
	if err := os.MkdirAll(".temp", 0755); err != nil {
		log.Fatalf("[ERROR] Failed to create directory: %v", err)
	}

	dbPath := filepath.Join(".temp", "raft_"+*id+".bolt")
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: *dbTimeout})
	if err != nil {
		log.Fatalf("[ERROR] Failed to open database at %s: %v", dbPath, err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("[ERROR] Failed to close database: %v", err)
		}
	}()

	store := kv.New(db, 20)
	applyCh := make(chan raft.ApplyMsg, 64)
	node := raft.NewNode(*id, peers, applyCh, db)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	// Apply committed commands
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case msg, ok := <-applyCh:
				if !ok {
					log.Printf("[INFO] Apply channel closed, exiting applier")
					return
				}
				if msg.CommandValid {
					if err := store.Apply(msg.Command); err != nil {
						log.Printf("[ERROR] Failed to apply command: %v", err)
					}
				}
			case <-ctx.Done():
				log.Printf("[INFO] Context cancelled, exiting applier")
				return
			}
		}
	}()

	// Start the raft node
	if err := node.Start(); err != nil {
		cancel()
		log.Fatalf("[ERROR] Failed to start raft node: %v", err)
	}

	// ---------- http mux (single listener) ----------
	mux := http.NewServeMux()

	// Raft RPCs under /raft/*
	mux.Handle("/raft/", http.StripPrefix("/raft", node.Trans()))

	// PUT
	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid request format: "+err.Error())
			return
		}

		if body.Key == "" {
			respondWithError(w, http.StatusBadRequest, "Key cannot be empty")
			return
		}

		idx, ok := node.Propose(kv.SetCmd{Key: body.Key, Value: body.Value})
		if !ok {
			// Not leader, redirect would be better with actual leader address
			respondWithError(w, http.StatusTemporaryRedirect, "Not the leader node")
			return
		}

		// Wait for the commit with timeout
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		success := waitForCommit(ctx, node, idx)
		if !success {
			respondWithError(w, http.StatusRequestTimeout, "Operation timed out waiting for consensus")
			return
		}

		respondWithSuccess(w, http.StatusOK, "Key set successfully", "")
	})

	// GET
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			Key string `json:"key"`
		}

		if r.Method == http.MethodPost {
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				respondWithError(w, http.StatusBadRequest, "Invalid request format: "+err.Error())
				return
			}
		} else {
			// Handle GET with query parameters
			body.Key = r.URL.Query().Get("key")
		}

		if body.Key == "" {
			respondWithError(w, http.StatusBadRequest, "Key cannot be empty")
			return
		}

		v, err := store.Get(body.Key)
		if err != nil {
			if err == kv.ErrKeyNotFound {
				respondWithError(w, http.StatusNotFound, "Key not found")
				return
			}
			respondWithError(w, http.StatusInternalServerError, "Error retrieving key: "+err.Error())
			return
		}

		respondWithSuccess(w, http.StatusOK, "", v)
	})

	// DEL
	mux.HandleFunc("/del", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			Key string `json:"key"`
		}

		if r.Method == http.MethodPost {
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				respondWithError(w, http.StatusBadRequest, "Invalid request format: "+err.Error())
				return
			}
		} else {
			// Handle DELETE with query parameters
			body.Key = r.URL.Query().Get("key")
		}

		if body.Key == "" {
			respondWithError(w, http.StatusBadRequest, "Key cannot be empty")
			return
		}

		idx, ok := node.Propose(kv.DelCmd{Key: body.Key})
		if !ok {
			respondWithError(w, http.StatusTemporaryRedirect, "Not the leader node")
			return
		}

		// Wait for the commit with timeout
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		success := waitForCommit(ctx, node, idx)
		if !success {
			respondWithError(w, http.StatusRequestTimeout, "Operation timed out waiting for consensus")
			return
		}

		respondWithSuccess(w, http.StatusOK, "Key deleted successfully", "")
	})

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Setup server with proper timeout handling
	server := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  *httpTimeout,
		WriteTimeout: *httpTimeout,
		IdleTimeout:  2 * (*httpTimeout),
	}

	// Setup graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Start HTTP server in a goroutine
	go func() {
		log.Printf("[INFO] %s listening on %s", *id, *addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[ERROR] Failed to start HTTP server: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-stop
	log.Printf("[INFO] Shutting down server...")

	// Give outstanding requests a deadline for completion
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[ERROR] Server shutdown error: %v", err)
	}

	// Cancel the context to signal all goroutines to stop
	cancel()

	// Wait for all goroutines to exit
	wg.Wait()

	// Stop the raft node
	if err := node.Stop(); err != nil {
		log.Printf("[ERROR] Failed to stop raft node: %v", err)
	}

	log.Printf("[INFO] Server shutdown complete")
}

// waitForCommit waits for the raft node to apply the command at the given index
func waitForCommit(ctx context.Context, node *raft.Node, idx uint64) bool {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if node.LastApplied() >= idx {
				return true
			}
		case <-ctx.Done():
			return false
		}
	}
}

// respondWithError sends a JSON error response
func respondWithError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	resp := apiResponse{
		Success: false,
		Message: message,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("[ERROR] Failed to encode error response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// respondWithSuccess sends a JSON success response
func respondWithSuccess(w http.ResponseWriter, status int, message, value string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	resp := apiResponse{
		Success: true,
		Message: message,
		Value:   value,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("[ERROR] Failed to encode success response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}