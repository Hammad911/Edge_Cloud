package raft

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/pkg/storage"
)

type testNode struct {
	id     string
	addr   string
	node   *Node
	store  *storage.MemStore
	clock  *hlc.Clock
	dir    string
	logger *slog.Logger
}

func pickPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick port: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

func newTestNode(t *testing.T, id string, bootstrap bool, dataDir string) *testNode {
	t.Helper()
	addr := pickPort(t)
	store := storage.NewMemStore()
	clock := hlc.New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	node, err := New(Config{
		NodeID:            id,
		BindAddr:          addr,
		AdvertiseAddr:     addr,
		DataDir:           dataDir,
		Bootstrap:         bootstrap,
		SnapshotInterval:  30 * time.Second,
		SnapshotThreshold: 1 << 16,
	}, store, clock, logger)
	if err != nil {
		t.Fatalf("new node %s: %v", id, err)
	}
	return &testNode{
		id:     id,
		addr:   addr,
		node:   node,
		store:  store,
		clock:  clock,
		dir:    dataDir,
		logger: logger,
	}
}

func waitForLeader(t *testing.T, n *testNode, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if n.node.IsLeader() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node %s did not become leader within %s", n.id, d)
}

func waitFor(t *testing.T, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func TestNode_ThreeNodeCluster_ReplicatesPuts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping raft integration test in -short mode")
	}
	base := t.TempDir()

	leader := newTestNode(t, "n1", true, filepath.Join(base, "n1"))
	t.Cleanup(func() { _ = leader.node.Shutdown() })
	waitForLeader(t, leader, 10*time.Second)

	follower2 := newTestNode(t, "n2", false, filepath.Join(base, "n2"))
	t.Cleanup(func() { _ = follower2.node.Shutdown() })
	follower3 := newTestNode(t, "n3", false, filepath.Join(base, "n3"))
	t.Cleanup(func() { _ = follower3.node.Shutdown() })

	if err := leader.node.AddVoter(follower2.id, follower2.addr); err != nil {
		t.Fatalf("add voter n2: %v", err)
	}
	if err := leader.node.AddVoter(follower3.id, follower3.addr); err != nil {
		t.Fatalf("add voter n3: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		ts := leader.clock.Now()
		cmd := Command{
			Op:        OpPut,
			Timestamp: ts,
			Key:       fmt.Sprintf("k%d", i),
			Value:     []byte(fmt.Sprintf("v%d", i)),
		}
		if err := leader.node.Apply(ctx, cmd, 3*time.Second); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}

	for _, n := range []*testNode{leader, follower2, follower3} {
		n := n
		waitFor(t, 5*time.Second, func() bool {
			return n.store.Size() == 5
		})
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("k%d", i)
			v, err := n.store.Get(key)
			if err != nil {
				t.Fatalf("%s get %s: %v", n.id, key, err)
			}
			if got, want := string(v.Value), fmt.Sprintf("v%d", i); got != want {
				t.Errorf("%s: key=%s got=%q want=%q", n.id, key, got, want)
			}
		}
	}
}

func TestNode_Apply_RejectsOnFollower(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping raft integration test in -short mode")
	}
	base := t.TempDir()

	leader := newTestNode(t, "n1", true, filepath.Join(base, "n1"))
	t.Cleanup(func() { _ = leader.node.Shutdown() })
	waitForLeader(t, leader, 10*time.Second)

	follower := newTestNode(t, "n2", false, filepath.Join(base, "n2"))
	t.Cleanup(func() { _ = follower.node.Shutdown() })

	if err := leader.node.AddVoter(follower.id, follower.addr); err != nil {
		t.Fatalf("add voter: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool { return follower.node.Leader() != "" })

	err := follower.node.Apply(context.Background(), Command{
		Op:        OpPut,
		Timestamp: follower.clock.Now(),
		Key:       "x",
		Value:     []byte("y"),
	}, time.Second)
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}
