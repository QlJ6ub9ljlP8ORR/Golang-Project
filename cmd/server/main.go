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

func main() {
	id := flag.String("id", "node1", "Node ID")
	addr := flag.String("addr", ":9001", "Listen address")
	peersStr := flag.String("peers", "", "Comma list id=addr")
	flag.Parse()

	// Set up structured logging
	logger := log.New(os.Stdout, "["+*id+"] ", log.LstdFlags)

	// ---------- cluster topology ----------
	peers := map[string]string{}
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			parts := strings.Split(p, "=")
			if len(parts) != 2 {
				logger.Fatalf("[ERROR] Invalid peer format: %s, expected id=addr", p)
			}
			peers[parts[0]] = parts[1]
		}
	}

	// Ensure directory exists
	if err := os.MkdirAll(".temp", 0755); err != nil {
		logger.Fatalf("[ERROR] Failed to create directory: %v", err)
	}

	// ---------- raft + kv ----------
	dbPath := filepath.Join(".temp", "raft_"+*id+".bolt")
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 3 * time.Second})
	if err != nil {
		logger.Fatalf("[ERROR] Failed to open database: %v", err)
	}
	defer db.Close()

	store := kv.New(db, 20)
	applyCh := make(chan raft.ApplyMsg, 64)
	node := raft.NewNode(*id, peers, applyCh, db)

	// Set up context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Apply committed commands
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-applyCh:
				if msg.CommandValid {
					store.Apply(msg.Command)
				}
			case <-ctx.Done():
				logger.Println("[INFO] Shutting down command processor")
				return
			}
		}
	}()

	node.Start() // ticker only
	defer node.Stop()

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

		defer r.Body.Close()

		var body struct{ Key, Value string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid request: "+err.Error(), http.StatusBadRequest)
			return
		}

		if body.Key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}

		idx, isLeader := node.Propose(kv.SetCmd{Key: body.Key, Value: body.Value})
		if !isLeader {
			// If not leader, return correct redirect status code
			http.Error(w, "Not leader", http.StatusTemporaryRedirect)
			return
		}

		// Wait for commit with timeout
		timeout := time.After(5 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if node.LastApplied() >= idx {
					w.WriteHeader(http.StatusNoContent)
					return
				}
			case <-timeout:
				http.Error(w, "Commit timeout", http.StatusRequestTimeout)
				return
			case <-r.Context().Done():
				// Client disconnected
				return
			}
		}
	})

	// GET
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		defer r.Body.Close()

		var body struct{ Key string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid request: "+err.Error(), http.StatusBadRequest)
			return
		}

		if body.Key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}

		v := store.Get(body.Key)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(struct{ Value string }{Value: v}); err != nil {
			logger.Printf("[ERROR] Failed to encode response: %v", err)
		}
	})

	// DEL
	mux.HandleFunc("/del", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		defer r.Body.Close()

		var body struct{ Key string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid request: "+err.Error(), http.StatusBadRequest)
			return
		}

		if body.Key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}

		idx, isLeader := node.Propose(kv.DelCmd{Key: body.Key})
		if !isLeader {
			// If not leader, return correct redirect status code
			http.Error(w, "Not leader", http.StatusTemporaryRedirect)
			return
		}

		// Wait for commit with timeout
		timeout := time.After(5 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if node.LastApplied() >= idx {
					w.WriteHeader(http.StatusNoContent)
					return
				}
			case <-timeout:
				http.Error(w, "Commit timeout", http.StatusRequestTimeout)
				return
			case <-r.Context().Done():
				// Client disconnected
				return
			}
		}
	})

	// Set up HTTP server with timeout configurations
	srv := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Handle graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// Start server in a goroutine
	go func() {
		logger.Printf("[INFO] %s listening on %s", *id, *addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("[ERROR] Server failed: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-shutdown
	logger.Println("[INFO] Shutting down server...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown the server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Fatalf("[ERROR] Server shutdown failed: %v", err)
	}

	// Cancel the application context to stop all goroutines
	cancel()

	// Wait for goroutines to finish
	wg.Wait()

	logger.Println("[INFO] Server gracefully stopped")
}