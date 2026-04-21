package ycsb

import (
	"context"
	"io"
	"log/slog"
	mrand "math/rand"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	kvv1 "edge-cloud-replication/gen/proto/edgecloud/kv/v1"
	"edge-cloud-replication/evaluation/checker"
	"edge-cloud-replication/internal/server"
	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/pkg/kv"
	"edge-cloud-replication/pkg/storage"
)

func newRand(seed int64) *mrand.Rand { return mrand.New(mrand.NewSource(seed)) }

func TestGenerator_Uniform(t *testing.T) {
	gen := UniformGen{N: 10}
	rng := newRand(42)
	seen := map[int]int{}
	for i := 0; i < 10000; i++ {
		seen[gen.Next(rng)]++
	}
	for k, v := range seen {
		if k < 0 || k >= 10 {
			t.Fatalf("out of range key %d", k)
		}
		if v < 500 {
			t.Fatalf("bucket %d drastically underrepresented: %d", k, v)
		}
	}
}

func TestGenerator_Zipfian_Hot(t *testing.T) {
	gen := NewZipfian(1000, 0.99)
	rng := newRand(1)
	var hot, cold int
	for i := 0; i < 20000; i++ {
		idx := gen.Next(rng)
		if idx < 10 {
			hot++
		} else if idx >= 900 {
			cold++
		}
	}
	if hot <= cold {
		t.Fatalf("Zipfian should concentrate on hot keys; hot=%d cold=%d", hot, cold)
	}
}

func TestHistogram_Percentiles(t *testing.T) {
	h := NewHistogram()
	for i := 1; i <= 1000; i++ {
		h.Observe(time.Duration(i) * time.Microsecond)
	}
	s := h.Snapshot()
	if s.Count != 1000 {
		t.Fatalf("count=%d", s.Count)
	}
	if s.P50Us < 400 || s.P50Us > 600 {
		t.Fatalf("p50 out of range: %d", s.P50Us)
	}
	if s.P99Us < 950 {
		t.Fatalf("p99 too low: %d", s.P99Us)
	}
	if s.MaxUs < s.P99Us {
		t.Fatalf("max < p99: max=%d p99=%d", s.MaxUs, s.P99Us)
	}
}

func TestMix_Normalised(t *testing.T) {
	m := Mix{Read: 50, Update: 50}
	probs, total, err := m.Normalised()
	if err != nil {
		t.Fatal(err)
	}
	if total != 100 {
		t.Fatalf("total=%f", total)
	}
	if got := probs[OpRead]; got < 0.49 || got > 0.51 {
		t.Fatalf("p(read)=%f", got)
	}
}

func TestPreset_Workloads(t *testing.T) {
	for _, w := range []Workload{WorkloadA, WorkloadB, WorkloadC, WorkloadD, WorkloadF} {
		s, err := Preset(w)
		if err != nil {
			t.Fatalf("preset %q: %v", w, err)
		}
		if _, _, err := s.Mix.Normalised(); err != nil {
			t.Fatalf("preset %q mix: %v", w, err)
		}
	}
}

// --- closed-loop integration against a real KV gRPC service ------------

type bufServer struct {
	t      *testing.T
	gsrv   *grpc.Server
	lis    *bufconn.Listener
	addr   string
	svc    kv.Service
	closed atomic.Bool
}

func newBufServer(t *testing.T) *bufServer {
	t.Helper()
	clock := hlc.New()
	store := storage.NewMemStore()
	svc := kv.New(kv.DefaultConfig(), clock, store)
	srv := server.NewKVGRPCServer(svc, slog.New(slog.NewTextHandler(io.Discard, nil)))

	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	srv.Register(g)

	b := &bufServer{t: t, gsrv: g, lis: lis, addr: "bufnet", svc: svc}
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(b.Close)
	return b
}

func (b *bufServer) Close() {
	if b.closed.Swap(true) {
		return
	}
	b.gsrv.GracefulStop()
	_ = b.lis.Close()
}

func (b *bufServer) dial() (Client, error) {
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return b.lis.DialContext(context.Background())
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	return &grpcClient{conn: conn, cli: kvv1.NewKVClient(conn)}, nil
}

func TestRunner_LoadAndRun_ClosedLoop(t *testing.T) {
	srv := newBufServer(t)
	dir := t.TempDir()
	historyPath := filepath.Join(dir, "history.jsonl")

	cfg := Config{
		Targets:       []string{srv.addr},
		Spec:          Spec{Workload: WorkloadA, Mix: Mix{Read: 0.5, Update: 0.5}, Distribution: DistUniform, RecordCount: 50, ValueSize: 32},
		Concurrency:   4,
		Operations:    400,
		OpTimeout:     time.Second,
		Stickiness:    true,
		Seed:          1,
		ValueSize:     32,
		LoadBatch:     4,
		HistoryOut:    historyPath,
		ClientFactory: func(string) (Client, error) { return srv.dial() },
	}

	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := r.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := r.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}

	rep := r.Report()
	if rep.Operations == 0 {
		t.Fatalf("no operations recorded")
	}
	if rep.Errors > 0 {
		t.Fatalf("unexpected errors: %d", rep.Errors)
	}
	if rep.Ops[string(OpRead)] == nil || rep.Ops[string(OpRead)].Success == 0 {
		t.Fatalf("expected reads to succeed, got %+v", rep.Ops)
	}
	if rep.Ops[string(OpUpdate)] == nil || rep.Ops[string(OpUpdate)].Success == 0 {
		t.Fatalf("expected updates to succeed, got %+v", rep.Ops)
	}

	// Close the runner so the history file is flushed; then feed it to
	// the offline checker. The closed-loop driver with stickiness should
	// satisfy MR / RYW / origin out of the box.
	if err := r.Close(); err != nil {
		t.Fatalf("close runner: %v", err)
	}
	events, err := checker.LoadJSONL(historyPath)
	if err != nil {
		t.Fatalf("load history: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("history is empty")
	}
	rep2 := checker.Check(events)
	if !rep2.OK() {
		t.Fatalf("checker reported violations: %+v", rep2.Violations)
	}
}

func TestRunner_Duration_Stops(t *testing.T) {
	srv := newBufServer(t)
	cfg := Config{
		Targets:       []string{srv.addr},
		Spec:          Spec{Workload: WorkloadC, Mix: Mix{Read: 1.0}, RecordCount: 20, Distribution: DistUniform},
		Concurrency:   2,
		Duration:      200 * time.Millisecond,
		OpTimeout:     time.Second,
		Stickiness:    true,
		Seed:          7,
		ValueSize:     16,
		LoadBatch:     2,
		ClientFactory: func(string) (Client, error) { return srv.dial() },
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	defer r.Close()

	ctx := context.Background()
	if err := r.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}

	start := time.Now()
	if err := r.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	dur := time.Since(start)

	// 200ms workload + some slack for teardown; definitely shouldn't run
	// for multiple seconds.
	if dur > 3*time.Second {
		t.Fatalf("duration-bounded run did not stop: %s", dur)
	}
}

// --- helpers ------------------------------------------------------------

type mockClient struct {
	mu   sync.Mutex
	gets int
	puts int
}

func (m *mockClient) Put(context.Context, string, []byte, *kvv1.CausalToken) (*kvv1.CausalToken, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts++
	return &kvv1.CausalToken{Physical: int64(m.puts), Logical: 0}, nil
}

func (m *mockClient) Get(context.Context, string, *kvv1.CausalToken) ([]byte, bool, *kvv1.CausalToken, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets++
	return []byte("v"), true, &kvv1.CausalToken{Physical: int64(m.gets), Logical: 0}, nil
}

func (m *mockClient) Delete(context.Context, string, *kvv1.CausalToken) (*kvv1.CausalToken, error) {
	return &kvv1.CausalToken{}, nil
}

func (m *mockClient) Close() error { return nil }

func TestPool_StickyPicks(t *testing.T) {
	targets := []string{"a", "b", "c"}
	p, err := NewPool(targets, func(addr string) (Client, error) { return &mockClient{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	for i := 0; i < 12; i++ {
		addr, _, err := p.Pick(i, true)
		if err != nil {
			t.Fatal(err)
		}
		if addr != targets[i%len(targets)] {
			t.Fatalf("worker %d expected %s got %s", i, targets[i%len(targets)], addr)
		}
	}
}
