package server

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"edge-cloud-replication/pkg/kv"
)

// ClusterAdmin exposes a minimal HTTP API for inspecting and mutating the
// local Raft cluster membership. It is mounted under /cluster/* on the
// admin server and is intended for operator use, not end-user traffic.
type ClusterAdmin struct {
	repl   clusterOps
	logger *slog.Logger
}

// clusterOps is the subset of pkg/raft.Node the admin surface needs. Kept
// as an interface so tests don't need a real raft cluster.
type clusterOps interface {
	IsLeader() bool
	Leader() string
	AddVoter(id, addr string) error
	RemoveServer(id string) error
}

// NewClusterAdmin returns a ClusterAdmin bound to the given raft node.
func NewClusterAdmin(node clusterOps, logger *slog.Logger) *ClusterAdmin {
	return &ClusterAdmin{
		repl:   node,
		logger: logger.With(slog.String("component", "cluster_admin")),
	}
}

// routeRegistrar is the minimal surface ClusterAdmin needs from
// AdminServer.
type routeRegistrar interface {
	HandleFunc(pattern string, fn http.HandlerFunc)
}

// Register mounts the cluster admin endpoints on the given registrar.
//
//	GET  /cluster/status   current leadership + local role
//	POST /cluster/join     add a voter to the cluster
//	POST /cluster/leave    remove a server from the cluster
func (c *ClusterAdmin) Register(reg routeRegistrar) {
	reg.HandleFunc("GET /cluster/status", c.handleStatus)
	reg.HandleFunc("POST /cluster/join", c.handleJoin)
	reg.HandleFunc("POST /cluster/leave", c.handleLeave)
}

func (c *ClusterAdmin) handleStatus(w http.ResponseWriter, _ *http.Request) {
	body := map[string]any{
		"is_leader": c.repl.IsLeader(),
		"leader":    c.repl.Leader(),
	}
	writeJSON(w, http.StatusOK, body)
}

func (c *ClusterAdmin) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID   string `json:"id"`
		Addr string `json:"addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.ID == "" || req.Addr == "" {
		http.Error(w, "id and addr are required", http.StatusBadRequest)
		return
	}
	if !c.repl.IsLeader() {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":  kv.ErrNotLeader.Error(),
			"leader": c.repl.Leader(),
		})
		return
	}
	if err := c.repl.AddVoter(req.ID, req.Addr); err != nil {
		c.logger.Warn("add voter failed", slog.String("id", req.ID), slog.Any("err", err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "joined", "id": req.ID, "addr": req.Addr})
}

func (c *ClusterAdmin) handleLeave(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.ID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	if !c.repl.IsLeader() {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":  kv.ErrNotLeader.Error(),
			"leader": c.repl.Leader(),
		})
		return
	}
	if err := c.repl.RemoveServer(req.ID); err != nil {
		c.logger.Warn("remove server failed", slog.String("id", req.ID), slog.Any("err", err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "left", "id": req.ID})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
