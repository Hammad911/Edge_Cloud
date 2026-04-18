package raft

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/pkg/storage"
)

// Config configures a Raft Node.
type Config struct {
	// NodeID is the unique identifier for this replica within the cluster.
	// Must be stable across restarts; used by hashicorp/raft in membership
	// configuration.
	NodeID string

	// BindAddr is the TCP address the Raft transport listens on (e.g.
	// "0.0.0.0:7101"). Peers connect here.
	BindAddr string

	// AdvertiseAddr is the address peers should use to reach this node.
	// When empty, BindAddr is used. Set this when running behind NAT or
	// when BindAddr is "0.0.0.0:PORT".
	AdvertiseAddr string

	// DataDir is the base directory for persistent raft state (log,
	// stable, snapshots). Created if missing.
	DataDir string

	// Bootstrap, when true, initializes a new single-node cluster with
	// this node as the sole voter. Only set this on the very first node
	// of a brand-new cluster. Existing clusters must use AddVoter to grow.
	Bootstrap bool

	// SnapshotInterval is how often the Raft runtime considers taking a
	// snapshot. Defaults to 120s if zero.
	SnapshotInterval time.Duration

	// SnapshotThreshold is the number of applied log entries that must
	// accumulate before a snapshot is considered. Defaults to 8192.
	SnapshotThreshold uint64
}

// Node is a running Raft replica. It owns the transport, log, stable, and
// snapshot stores plus the hashicorp/raft instance itself. The FSM is
// supplied externally so the caller controls the storage it wraps.
type Node struct {
	cfg    Config
	raft   *hraft.Raft
	fsm    *FSM
	logger *slog.Logger

	transport  *hraft.NetworkTransport
	logStore   hraft.LogStore
	stableStr  hraft.StableStore
	snapStore  hraft.SnapshotStore
	closeables []closer
}

type closer interface {
	Close() error
}

// New constructs, starts, and returns a Node. On success the node is
// participating in the cluster (either as leader after bootstrap, or as
// follower catching up). On any error, any partially-created resources are
// cleaned up before returning.
func New(cfg Config, store storage.Store, clock *hlc.Clock, logger *slog.Logger) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("raft: NodeID required")
	}
	if cfg.BindAddr == "" {
		return nil, fmt.Errorf("raft: BindAddr required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("raft: DataDir required")
	}

	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return nil, fmt.Errorf("raft: mkdir data dir: %w", err)
	}

	n := &Node{
		cfg:    cfg,
		fsm:    NewFSM(store, clock, logger),
		logger: logger.With(slog.String("component", "raft"), slog.String("node_id", cfg.NodeID)),
	}

	rc := hraft.DefaultConfig()
	rc.LocalID = hraft.ServerID(cfg.NodeID)
	rc.Logger = newHclogAdapter(n.logger)
	if cfg.SnapshotInterval > 0 {
		rc.SnapshotInterval = cfg.SnapshotInterval
	}
	if cfg.SnapshotThreshold > 0 {
		rc.SnapshotThreshold = cfg.SnapshotThreshold
	}

	logPath := filepath.Join(cfg.DataDir, "raft-log.bolt")
	stablePath := filepath.Join(cfg.DataDir, "raft-stable.bolt")

	logDB, err := raftboltdb.NewBoltStore(logPath)
	if err != nil {
		n.cleanup()
		return nil, fmt.Errorf("raft: open log store: %w", err)
	}
	n.logStore = logDB
	n.closeables = append(n.closeables, logDB)

	stableDB, err := raftboltdb.NewBoltStore(stablePath)
	if err != nil {
		n.cleanup()
		return nil, fmt.Errorf("raft: open stable store: %w", err)
	}
	n.stableStr = stableDB
	n.closeables = append(n.closeables, stableDB)

	snapStore, err := hraft.NewFileSnapshotStore(cfg.DataDir, 3, os.Stderr)
	if err != nil {
		n.cleanup()
		return nil, fmt.Errorf("raft: open snapshot store: %w", err)
	}
	n.snapStore = snapStore

	advertise := cfg.AdvertiseAddr
	if advertise == "" {
		advertise = cfg.BindAddr
	}
	addr, err := net.ResolveTCPAddr("tcp", advertise)
	if err != nil {
		n.cleanup()
		return nil, fmt.Errorf("raft: resolve advertise addr: %w", err)
	}
	transport, err := hraft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		n.cleanup()
		return nil, fmt.Errorf("raft: create transport: %w", err)
	}
	n.transport = transport

	r, err := hraft.NewRaft(rc, n.fsm, n.logStore, n.stableStr, n.snapStore, n.transport)
	if err != nil {
		n.cleanup()
		return nil, fmt.Errorf("raft: bring up: %w", err)
	}
	n.raft = r

	if cfg.Bootstrap {
		hasState, err := hraft.HasExistingState(n.logStore, n.stableStr, n.snapStore)
		if err != nil {
			n.cleanup()
			return nil, fmt.Errorf("raft: check existing state: %w", err)
		}
		if !hasState {
			n.logger.Info("bootstrapping new cluster",
				slog.String("node_id", cfg.NodeID),
				slog.String("addr", string(transport.LocalAddr())),
			)
			cfgFuture := r.BootstrapCluster(hraft.Configuration{
				Servers: []hraft.Server{{
					ID:      hraft.ServerID(cfg.NodeID),
					Address: transport.LocalAddr(),
				}},
			})
			if err := cfgFuture.Error(); err != nil {
				n.cleanup()
				return nil, fmt.Errorf("raft: bootstrap: %w", err)
			}
		} else {
			n.logger.Info("existing raft state detected, skipping bootstrap")
		}
	}

	n.logger.Info("raft node started",
		slog.String("bind_addr", cfg.BindAddr),
		slog.String("advertise_addr", advertise),
		slog.String("data_dir", cfg.DataDir),
	)
	return n, nil
}

// Apply implements Replicator.
func (n *Node) Apply(ctx context.Context, cmd Command, timeout time.Duration) error {
	if n.raft == nil {
		return ErrReplicatorClosed
	}
	if n.raft.State() != hraft.Leader {
		return ErrNotLeader
	}

	data, err := cmd.MarshalBinary()
	if err != nil {
		return fmt.Errorf("raft apply: encode: %w", err)
	}

	future := n.raft.Apply(data, timeout)

	done := make(chan error, 1)
	go func() { done <- future.Error() }()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		if err != nil {
			return fmt.Errorf("raft apply: %w", err)
		}
	}

	if res, ok := future.Response().(ApplyResult); ok && res.Err != nil {
		return res.Err
	}
	return nil
}

// IsLeader implements Replicator.
func (n *Node) IsLeader() bool {
	if n.raft == nil {
		return false
	}
	return n.raft.State() == hraft.Leader
}

// Leader implements Replicator.
func (n *Node) Leader() string {
	if n.raft == nil {
		return ""
	}
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// AddVoter implements Replicator.
func (n *Node) AddVoter(id, addr string) error {
	if n.raft == nil {
		return ErrReplicatorClosed
	}
	f := n.raft.AddVoter(hraft.ServerID(id), hraft.ServerAddress(addr), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft add voter: %w", err)
	}
	return nil
}

// RemoveServer implements Replicator.
func (n *Node) RemoveServer(id string) error {
	if n.raft == nil {
		return ErrReplicatorClosed
	}
	f := n.raft.RemoveServer(hraft.ServerID(id), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft remove server: %w", err)
	}
	return nil
}

// Shutdown implements Replicator. Idempotent.
func (n *Node) Shutdown() error {
	if n == nil || n.raft == nil {
		return nil
	}
	f := n.raft.Shutdown()
	if err := f.Error(); err != nil {
		n.logger.Warn("raft shutdown error", slog.Any("err", err))
	}
	n.raft = nil
	n.cleanup()
	return nil
}

func (n *Node) cleanup() {
	for _, c := range n.closeables {
		_ = c.Close()
	}
	n.closeables = nil
	if n.transport != nil {
		_ = n.transport.Close()
		n.transport = nil
	}
}
