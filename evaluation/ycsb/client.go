package ycsb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvv1 "edge-cloud-replication/gen/proto/edgecloud/kv/v1"
)

// Client is the minimal subset of the KV gRPC surface used by the driver.
// It is exposed as an interface so the test suite can inject an in-memory
// implementation without standing up a real gRPC server.
type Client interface {
	Put(ctx context.Context, key string, value []byte, after *kvv1.CausalToken) (*kvv1.CausalToken, error)
	Get(ctx context.Context, key string, after *kvv1.CausalToken) ([]byte, bool, *kvv1.CausalToken, error)
	Delete(ctx context.Context, key string, after *kvv1.CausalToken) (*kvv1.CausalToken, error)
	Close() error
}

// Pool dispenses one Client per worker, optionally pinning workers to a
// fixed target address so session stickiness holds end-to-end.
type Pool struct {
	targets []string
	factory func(addr string) (Client, error)

	mu      sync.Mutex
	clients map[string]Client
}

// NewPool dials each target once (lazily) and returns a pool keyed by the
// target string. `factory` is the hook used by tests.
func NewPool(targets []string, factory func(addr string) (Client, error)) (*Pool, error) {
	if len(targets) == 0 {
		return nil, errors.New("ycsb: no targets provided")
	}
	if factory == nil {
		factory = DialGRPC
	}
	return &Pool{
		targets: append([]string(nil), targets...),
		factory: factory,
		clients: make(map[string]Client),
	}, nil
}

// Pick returns the client bound to a given worker id. When `stickiness`
// is true, workers are mapped round-robin to targets and always receive
// the same client; when false, workers hash onto targets uniformly so
// load is even but sessions can hop.
func (p *Pool) Pick(worker int, stickiness bool) (string, Client, error) {
	idx := worker % len(p.targets)
	if !stickiness {
		idx = worker % len(p.targets) // same for closed-loop; we only differ if future caller wants randomness per op
	}
	addr := p.targets[idx]

	p.mu.Lock()
	defer p.mu.Unlock()

	c, ok := p.clients[addr]
	if ok {
		return addr, c, nil
	}
	c, err := p.factory(addr)
	if err != nil {
		return addr, nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	p.clients[addr] = c
	return addr, c, nil
}

// Close tears down every client connection.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for addr, c := range p.clients {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close %s: %w", addr, err)
		}
	}
	p.clients = map[string]Client{}
	return firstErr
}

// Targets returns a copy of the configured target list.
func (p *Pool) Targets() []string {
	out := make([]string, len(p.targets))
	copy(out, p.targets)
	return out
}

// grpcClient is the default, production-path client backed by a real
// `google.golang.org/grpc` connection.
type grpcClient struct {
	conn *grpc.ClientConn
	cli  kvv1.KVClient
}

// DialGRPC is the default factory used by NewPool.
func DialGRPC(addr string) (Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &grpcClient{conn: conn, cli: kvv1.NewKVClient(conn)}, nil
}

func (g *grpcClient) Put(ctx context.Context, key string, value []byte, after *kvv1.CausalToken) (*kvv1.CausalToken, error) {
	resp, err := g.cli.Put(ctx, &kvv1.PutRequest{Key: key, Value: value, After: after})
	if err != nil {
		return nil, err
	}
	return resp.GetToken(), nil
}

func (g *grpcClient) Get(ctx context.Context, key string, after *kvv1.CausalToken) ([]byte, bool, *kvv1.CausalToken, error) {
	resp, err := g.cli.Get(ctx, &kvv1.GetRequest{Key: key, After: after})
	if err != nil {
		return nil, false, nil, err
	}
	return resp.GetValue(), resp.GetFound(), resp.GetToken(), nil
}

func (g *grpcClient) Delete(ctx context.Context, key string, after *kvv1.CausalToken) (*kvv1.CausalToken, error) {
	resp, err := g.cli.Delete(ctx, &kvv1.DeleteRequest{Key: key, After: after})
	if err != nil {
		return nil, err
	}
	return resp.GetToken(), nil
}

func (g *grpcClient) Close() error { return g.conn.Close() }

// mergeToken returns the "newer" of two causal tokens using the HLC
// ordering. A nil token is treated as the zero element.
func mergeToken(a, b *kvv1.CausalToken) *kvv1.CausalToken {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if b.GetPhysical() > a.GetPhysical() ||
		(b.GetPhysical() == a.GetPhysical() && b.GetLogical() > a.GetLogical()) {
		return b
	}
	return a
}
