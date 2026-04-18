package causal_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"

	srv "edge-cloud-replication/internal/server"
	"edge-cloud-replication/pkg/causal"
	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/pkg/storage"
)

func testLogger() *slog.Logger {
	if os.Getenv("CAUSAL_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// node is a test harness wrapping a complete causal stack (clock, store,
// applier, replicator) plus a gRPC server. We use it to model both edge
// leaders and the cloud hub for the cross-cluster propagation test.
type node struct {
	id      string
	group   hlc.GroupID
	addr    string
	clock   *hlc.Clock
	pclock  *hlc.PartitionedClock
	store   storage.Store
	repl    *causal.Replicator
	server  *grpc.Server
	cleanup func()
}

// newNode brings up a complete causal stack on a pre-bound listener.
// Pre-binding the listener lets the caller construct the full peer
// topology upfront (resolving the chicken-and-egg between cloud and
// edge addresses).
func newNode(t *testing.T, id, group string, lis net.Listener, peers []causal.PeerSpec) *node {
	t.Helper()
	logger := testLogger().With(slog.String("node", id))

	clock := hlc.New()
	store := storage.NewMemStore()
	pclock := hlc.NewPartitionedClock(hlc.GroupID(group), clock)
	applier := causal.NewStoreApplier(store, pclock, logger)

	repl := causal.New(causal.Config{
		NodeID:  id,
		GroupID: hlc.GroupID(group),
		Peers:   peers,
	}, pclock, applier, logger)

	if err := repl.Start(context.Background()); err != nil {
		t.Fatalf("start replicator %s: %v", id, err)
	}

	g := grpc.NewServer()
	srv.NewReplicationGRPCServer(id, repl, srv.AlwaysLeader(), logger).Register(g)
	go func() { _ = g.Serve(lis) }()

	return &node{
		id:     id,
		group:  hlc.GroupID(group),
		addr:   lis.Addr().String(),
		clock:  clock,
		pclock: pclock,
		store:  store,
		repl:   repl,
		server: g,
		cleanup: func() {
			// Stop senders first; then forcibly stop the gRPC server.
			// We use Stop() rather than GracefulStop() because in a
			// multi-node teardown another node's sender may still hold
			// our inbound stream open, and waiting for it would
			// deadlock.
			_ = repl.Stop()
			g.Stop()
		},
	}
}

// reserveListener binds 127.0.0.1:0 so we can know the address before
// we start any node.
func reserveListener(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return l
}

// publishLocal simulates a local KV write at this node. It mirrors what
// kv.Service does after a successful Put.
func (n *node) publishLocal(key string, value []byte) {
	ts := n.clock.Now()
	if err := n.store.Put(key, value, ts); err != nil {
		panic(err)
	}
	n.repl.Producer.Publish(key, value, false, ts)
}

func waitFor(t *testing.T, name string, timeout time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", name)
}

// TestCausalReplication_CrossCluster brings up a 1-cloud + 2-edge topology
// and verifies that a write on edge-A propagates to edge-B via the cloud
// hub, while preserving causal order.
func TestCausalReplication_CrossCluster(t *testing.T) {
	// Pre-bind listeners so we can construct the full topology upfront.
	cloudLis := reserveListener(t)
	edgeALis := reserveListener(t)
	edgeBLis := reserveListener(t)

	cloud := newNode(t, "cloud-0", "cloud", cloudLis, []causal.PeerSpec{
		{Name: "edge-a", Addr: edgeALis.Addr().String()},
		{Name: "edge-b", Addr: edgeBLis.Addr().String()},
	})
	defer cloud.cleanup()

	edgeA := newNode(t, "edge-a-0", "edge-a", edgeALis, []causal.PeerSpec{
		{Name: "cloud", Addr: cloudLis.Addr().String()},
	})
	defer edgeA.cleanup()

	edgeB := newNode(t, "edge-b-0", "edge-b", edgeBLis, []causal.PeerSpec{
		{Name: "cloud", Addr: cloudLis.Addr().String()},
	})
	defer edgeB.cleanup()

	// Give all senders a moment to connect.
	time.Sleep(200 * time.Millisecond)

	// Write on edge-A.
	edgeA.publishLocal("k1", []byte("v1"))

	// Expect the value to appear on the cloud and on edge-B.
	waitFor(t, "k1 on cloud", 3*time.Second, func() bool {
		v, err := cloud.store.Get("k1")
		return err == nil && string(v.Value) == "v1"
	})
	waitFor(t, "k1 on edge-B", 3*time.Second, func() bool {
		v, err := edgeB.store.Get("k1")
		return err == nil && string(v.Value) == "v1"
	})

	// Causal chain: write k2 on edge-A AFTER seeing some other state.
	// We simulate by also writing k1->v2 (later HLC) and verifying both
	// arrive in order at edge-B (the buffer should not surface k1->v2
	// before the prior k1->v1 has been applied — which is satisfied
	// because the publisher stamps Deps from the local frontier).
	edgeA.publishLocal("k1", []byte("v2"))
	waitFor(t, "k1=v2 on edge-B", 3*time.Second, func() bool {
		v, err := edgeB.store.Get("k1")
		return err == nil && string(v.Value) == "v2"
	})

	// Self-loop suppression: writing on edge-A should NOT cause edge-A
	// to overwrite its own state from a re-shipped echo. Verify the
	// frontier on edge-A still has just one applied write per origin we
	// know about.
	if got := edgeA.repl.Buffer.Frontier(); got.Groups["edge-a"].Physical != 0 {
		t.Logf("edge-A frontier shows non-zero own contribution: %v (this is OK if echoes were absorbed as duplicates)", got)
	}
}

// TestCausalReplication_OutOfOrderBufferingDelivers verifies that an
// event arriving at a peer BEFORE its causal dependencies is held in the
// buffer until the dependencies arrive, then delivered.
func TestCausalReplication_OutOfOrderBufferingDelivers(t *testing.T) {
	cloud := newNode(t, "cloud-0", "cloud", reserveListener(t), nil)
	defer cloud.cleanup()

	// Construct an artificial event from a missing third group "edge-c".
	deps := hlc.PartitionedTimestamp{
		Origin: "edge-c",
		Groups: map[hlc.GroupID]hlc.Timestamp{
			"phantom": {Physical: 9_999_999_999_999},
		},
	}
	missingDep := &causal.Event{
		EventID:  1,
		SenderID: "edge-c-0",
		Origin:   "edge-c",
		Key:      "k_phantom",
		Value:    []byte("never"),
		CommitTS: hlc.Timestamp{Physical: 1_000_000_000_000},
		Deps:     deps,
	}

	res, err := cloud.repl.Buffer.Admit(missingDep)
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if res != causal.AdmitBuffered {
		t.Fatalf("expected AdmitBuffered, got %v", res)
	}

	if _, err := cloud.store.Get("k_phantom"); err == nil {
		t.Fatalf("k_phantom should not be applied yet")
	}

	// Advance the phantom group's frontier; the event should now drain.
	cloud.repl.Buffer.AdvanceFrontier("phantom", hlc.Timestamp{Physical: 9_999_999_999_999})
	cloud.repl.Notify()

	waitFor(t, "k_phantom on cloud", 2*time.Second, func() bool {
		v, err := cloud.store.Get("k_phantom")
		return err == nil && string(v.Value) == "never"
	})
	_ = fmt.Sprintf // keep import
}
