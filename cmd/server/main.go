package main

import (
	"encoding/json"
	"flag"
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

// Config holds the server configuration
type Config struct {
	NodeID   string
	Address  string
	PeersStr string
}

// parsePeers converts the peers string into a map of peer IDs to addresses
func parsePeers(peersStr string) map[string]string {
	peers := map[string]string{}
	if peersStr == "" {
		return peers
	}

	for _, p := range strings.Split(peersStr, ",") {
		parts := strings.Split(p, "=")
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			peers[parts[0]] = parts[1]
		}
	}
	return peers
}

// setupDatabase initializes the BoltDB database
func setupDatabase(nodeID string) (*bolt.DB, error) {
	// Ensure .temp directory exists
	if err := os.MkdirAll(".temp", 0755); err != nil {
		return nil, err
	}

	dbPath := filepath.Join(".temp", "raft_"+nodeID+".bolt")
	return bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
}

// setupRaftNode initializes the Raft node and key-value store
func setupRaftNode(config Config, db *bolt.DB) (*kv.Store, *raft.Node, chan raft.ApplyMsg) {
	store := kv.New(db, 20)
	applyCh := make(chan raft.ApplyMsg, 64)
	peers := parsePeers(config.PeersStr)
	node := raft.NewNode(config.NodeID, peers, applyCh, db)

	// Background goroutine to apply committed commands
	go func() {
		for msg := range applyCh {
			if msg.CommandValid {
				store.Apply(msg.Command)
			}
		}
	}()

	node.Start() // Start ticker
	return store, node, applyCh
}

// setupHTTPHandlers configures the HTTP routes for key-value operations
func setupHTTPHandlers(store *kv.Store, node *raft.Node) *http.ServeMux {
	mux := http.NewServeMux()

	// Raft RPCs
	mux.Handle("/raft/", http.StripPrefix("/raft", node.Trans()))

	// PUT handler
	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Key, Value string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		idx, ok := node.Propose(kv.SetCmd{Key: body.Key, Value: body.Value})
		if !ok {
			http.Error(w, "Not the leader", http.StatusTemporaryRedirect)
			return
		}

		// Wait for commit with a timeout
		for i := 0; i < 100; i++ { // 1 second timeout
			if node.LastApplied() >= idx {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}

		http.Error(w, "Commit timeout", http.StatusRequestTimeout)
	})

	// GET handler
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Key string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		v := store.Get(body.Key)
		if err := json.NewEncoder(w).Encode(struct{ Value string }{Value: v}); err != nil {
			log.Printf("Error encoding response: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	})

	// DEL handler
	mux.HandleFunc("/del", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Key string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		idx, ok := node.Propose(kv.DelCmd{Key: body.Key})
		if !ok {
			http.Error(w, "Not the leader", http.StatusTemporaryRedirect)
			return
		}

		// Wait for commit with a timeout
		for i := 0; i < 100; i++ { // 1 second timeout
			if node.LastApplied() >= idx {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}

		http.Error(w, "Commit timeout", http.StatusRequestTimeout)
	})

	return mux
}

func main() {
	// Parse command-line flags
	config := Config{
		NodeID:   *flag.String("id", "node1", "Node ID"),
		Address:  *flag.String("addr", ":9001", "Listen address"),
		PeersStr: *flag.String("peers", "", "Comma list id=addr"),
	}
	flag.Parse()

	// Setup database
	db, err := setupDatabase(config.NodeID)
	if err != nil {
		log.Fatalf("Database setup failed: %v", err)
	}
	defer db.Close()

	// Setup Raft node and key-value store
	store, node, applyCh := setupRaftNode(config, db)
	defer close(applyCh)

	// Setup HTTP handlers
	mux := setupHTTPHandlers(store, node)

	// Start server
	log.Printf("[INFO] %s listening on %s", config.NodeID, config.Address)
	log.Fatal(http.ListenAndServe(config.Address, mux))
}