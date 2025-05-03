package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/internal/kv"
	"mini_etcd/internal/raft"
)

// validatePeers checks the format and validity of peer configuration
func validatePeers(peersStr string) (map[string]string, error) {
	peers := map[string]string{}
	if peersStr == "" {
		return peers, nil
	}

	for _, p := range strings.Split(peersStr, ",") {
		parts := strings.Split(p, "=")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid peer format: %s", p)
		}
		peers[parts[0]] = parts[1]
	}
	return peers, nil
}

// ensureTempDir creates the .temp directory if it doesn't exist
func ensureTempDir() error {
	return os.MkdirAll(".temp", 0755)
}

func main() {
	// Graceful shutdown setup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	id := flag.String("id", "node1", "Node ID")
	addr := flag.String("addr", ":9001", "Listen address")
	peersStr := flag.String("peers", "", "Comma list id=addr")
	flag.Parse()

	// Validate and parse peers
	peers, err := validatePeers(*peersStr)
	if err != nil {
		log.Fatalf("[ERROR] Invalid peer configuration: %v", err)
	}

	// Ensure temp directory exists
	if err := ensureTempDir(); err != nil {
		log.Fatalf("[ERROR] Failed to create temp directory: %v", err)
	}

	// ---------- raft + kv ----------
	dbPath := filepath.Join(".temp", "raft_"+*id+".bolt")
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatalf("[ERROR] Failed to open database: %s: %v", *id, err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("[WARN] Error closing database: %v", err)
		}
	}()
	store := kv.New(db, 20)
	applyCh := make(chan raft.ApplyMsg, 64)
	node := raft.NewNode(*id, peers, applyCh, db)

	go func() { // apply committed commands
		for msg := range applyCh {
			if msg.CommandValid {
				store.Apply(msg.Command)
			}
		}
	}()
	node.Start() // ticker only

	// ---------- http mux (single listener) ----------
	mux := http.NewServeMux()

	// Raft RPCs under /raft/*
	mux.Handle("/raft/", http.StripPrefix("/raft", node.Trans()))

	// Middleware for logging and error handling
	logMiddleware := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			log.Printf("[REQUEST] %s %s", r.Method, r.URL.Path)
			next.ServeHTTP(w, r)
		}
	}

	// PUT handler
	mux.HandleFunc("/put", logMiddleware(func(w http.ResponseWriter, r *http.Request) {
		// Validate request method
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Decode and validate request body
		var body struct{ Key, Value string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Input validation
		if body.Key == "" {
			http.Error(w, "Key cannot be empty", http.StatusBadRequest)
			return
		}

		// Propose command with timeout
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		idx, ok := node.Propose(kv.SetCmd{Key: body.Key, Value: body.Value})
		if !ok {
			http.Error(w, "Not the leader", http.StatusTemporaryRedirect)
			return
		}

		// Wait for commit with timeout
		for {
			select {
			case <-ctx.Done():
				http.Error(w, "Commit timeout", http.StatusRequestTimeout)
				return
			default:
				if node.LastApplied() >= idx {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}))

	// GET handler
	mux.HandleFunc("/get", logMiddleware(func(w http.ResponseWriter, r *http.Request) {
		// Validate request method
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Decode and validate request body
		var body struct{ Key string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Input validation
		if body.Key == "" {
			http.Error(w, "Key cannot be empty", http.StatusBadRequest)
			return
		}

		// Retrieve value
		v := store.Get(body.Key)
		if err := json.NewEncoder(w).Encode(struct{ Value string }{Value: v}); err != nil {
			log.Printf("[ERROR] Failed to encode response: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}))

	// DEL handler
	mux.HandleFunc("/del", logMiddleware(func(w http.ResponseWriter, r *http.Request) {
		// Validate request method
		if r.Method != http.MethodDelete {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Decode and validate request body
		var body struct{ Key string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Input validation
		if body.Key == "" {
			http.Error(w, "Key cannot be empty", http.StatusBadRequest)
			return
		}

		// Propose delete command with timeout
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		idx, ok := node.Propose(kv.DelCmd{Key: body.Key})
		if !ok {
			http.Error(w, "Not the leader", http.StatusTemporaryRedirect)
			return
		}

		// Wait for commit with timeout
		for {
			select {
			case <-ctx.Done():
				http.Error(w, "Commit timeout", http.StatusRequestTimeout)
				return
			default:
				if node.LastApplied() >= idx {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}))

	// Graceful shutdown setup
	server := &http.Server{
		Addr:    *addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("[ERROR] Server shutdown: %v", err)
		}
	}()

	log.Printf("[INFO] %s listening on %s", *id, *addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[ERROR] Server failed: %v", err)
	}
}
