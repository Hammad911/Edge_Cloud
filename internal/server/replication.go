package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"google.golang.org/grpc"

	replv1 "edge-cloud-replication/gen/proto/edgecloud/replication/v1"
	"edge-cloud-replication/pkg/causal"
)

// LeaderProbe lets the replication server report leadership in
// HealthResponse without taking a hard dependency on pkg/raft. The cloud
// passes a stub that always returns true; edge nodes pass their raft node.
type LeaderProbe interface {
	IsLeader() bool
}

// alwaysLeader is the LeaderProbe used by the cloud (no raft).
type alwaysLeader struct{}

func (alwaysLeader) IsLeader() bool { return true }

// AlwaysLeader returns a LeaderProbe that reports true. Use for the cloud
// hub or for single-node edge installations without raft.
func AlwaysLeader() LeaderProbe { return alwaysLeader{} }

// ReplicationGRPCServer implements replv1.ReplicationServer. It feeds
// every received event into the local Buffer (via Admit) and notifies the
// Replicator's delivery loop that there may be work to do.
type ReplicationGRPCServer struct {
	replv1.UnimplementedReplicationServer

	nodeID string
	repl   *causal.Replicator
	leader LeaderProbe
	logger *slog.Logger
}

// NewReplicationGRPCServer constructs the server-side adapter.
func NewReplicationGRPCServer(nodeID string, repl *causal.Replicator, leader LeaderProbe, logger *slog.Logger) *ReplicationGRPCServer {
	if leader == nil {
		leader = AlwaysLeader()
	}
	return &ReplicationGRPCServer{
		nodeID: nodeID,
		repl:   repl,
		leader: leader,
		logger: logger.With(slog.String("component", "replication_grpc")),
	}
}

// Register attaches this service to a grpc.Server.
func (s *ReplicationGRPCServer) Register(g *grpc.Server) {
	replv1.RegisterReplicationServer(g, s)
}

// Push handles an inbound bidi stream of replication frames.
func (s *ReplicationGRPCServer) Push(stream replv1.Replication_PushServer) error {
	first, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return fmt.Errorf("recv first frame: %w", err)
	}
	hello := first.GetHello()
	if hello == nil {
		return fmt.Errorf("expected Hello as first frame, got %T", first.GetPayload())
	}
	logger := s.logger.With(
		slog.String("peer_sender", hello.GetSenderId()),
		slog.String("peer_origin", hello.GetOrigin()),
	)
	logger.Info("replication stream opened")

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			logger.Info("replication stream closed by peer")
			return nil
		}
		if err != nil {
			return fmt.Errorf("recv: %w", err)
		}
		ev := msg.GetEvent()
		if ev == nil {
			continue
		}
		s.handleEvent(stream, ev, logger)
	}
}

func (s *ReplicationGRPCServer) handleEvent(stream replv1.Replication_PushServer, p *replv1.Event, logger *slog.Logger) {
	ack := &replv1.PushAck{EventId: p.GetEventId(), Accepted: true}

	defer func() {
		if err := stream.Send(ack); err != nil {
			logger.Warn("send ack failed", slog.Any("err", err))
		}
	}()

	in := causal.EventFromProto(p)
	if err := in.Validate(); err != nil {
		ack.Accepted = false
		ack.Error = err.Error()
		return
	}
	if string(in.Origin) == s.repl.NodeID || in.Origin == s.repl.GroupID {
		// Self-loop suppression: never apply our own events.
		// This catches cloud<->edge cycles where the hub re-ships an
		// edge's own writes back to it.
	}
	res, err := s.repl.Buffer.Admit(in)
	if err != nil {
		ack.Accepted = false
		ack.Error = err.Error()
		return
	}
	if logger.Enabled(stream.Context(), slog.LevelDebug) {
		logger.Debug("admitted event",
			slog.Uint64("id", in.EventID),
			slog.String("origin", string(in.Origin)),
			slog.String("key", in.Key),
			slog.Int("res", int(res)),
		)
	}
	switch res {
	case causal.AdmitDuplicate:
		// Already applied; ack with accepted=true so the sender advances.
	case causal.AdmitDelivered, causal.AdmitBuffered:
		s.repl.Notify()
	}
}

// Health is a simple liveness/leadership probe.
func (s *ReplicationGRPCServer) Health(_ context.Context, _ *replv1.HealthRequest) (*replv1.HealthResponse, error) {
	return &replv1.HealthResponse{
		Serving:  true,
		NodeId:   s.nodeID,
		IsLeader: s.leader.IsLeader(),
	}, nil
}
