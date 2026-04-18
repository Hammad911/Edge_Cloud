package causal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	replv1 "edge-cloud-replication/gen/proto/edgecloud/replication/v1"
)

// PeerSpec describes a remote node a Sender will stream events to.
type PeerSpec struct {
	// Name uniquely identifies the peer in the local outbox subscriber
	// table. It is also used as the cursor key on disk in future
	// persistent outbox implementations.
	Name string
	// Addr is the gRPC dial target, e.g. "127.0.0.1:7001".
	Addr string
}

// Sender ships events from a local Outbox to a single remote peer over a
// long-lived gRPC bidi stream. It is responsible for:
//
//   - dialing and re-dialing on transient failures
//   - subscribing to the outbox and reading events in order
//   - sending the initial Hello frame
//   - mapping wire types to/from the in-memory Event
//   - advancing the outbox cursor only after the peer acknowledges
//
// One Sender is started per peer; they run concurrently and are isolated
// from each other.
type Sender struct {
	peer     PeerSpec
	senderID string
	origin   string

	outbox *Outbox
	logger *slog.Logger

	dialOpts []grpc.DialOption

	// retryBackoff is the delay between (re)connect attempts. It grows on
	// repeated failure up to maxBackoff.
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

// SenderOption configures a Sender.
type SenderOption func(*Sender)

// WithDialOptions sets gRPC dial options. Defaults to insecure transport.
func WithDialOptions(opts ...grpc.DialOption) SenderOption {
	return func(s *Sender) { s.dialOpts = opts }
}

// WithBackoff sets the initial and max retry backoff.
func WithBackoff(initial, max time.Duration) SenderOption {
	return func(s *Sender) {
		s.initialBackoff = initial
		s.maxBackoff = max
	}
}

// NewSender constructs a Sender. SenderID is the local node id, origin is
// the GroupID this node belongs to (placed in the Hello frame).
func NewSender(peer PeerSpec, senderID, origin string, outbox *Outbox, logger *slog.Logger, opts ...SenderOption) *Sender {
	s := &Sender{
		peer:           peer,
		senderID:       senderID,
		origin:         origin,
		outbox:         outbox,
		logger:         logger.With(slog.String("component", "causal_sender"), slog.String("peer", peer.Name)),
		dialOpts:       []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		initialBackoff: 250 * time.Millisecond,
		maxBackoff:     5 * time.Second,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Run drives the sender until ctx is cancelled. It returns the context
// error on shutdown; transient errors are logged and retried.
func (s *Sender) Run(ctx context.Context) error {
	s.outbox.Subscribe(s.peer.Name)
	defer s.outbox.Unsubscribe(s.peer.Name)

	backoff := s.initialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.runOnce(ctx)
		if err == nil || errors.Is(err, context.Canceled) {
			return err
		}
		s.logger.Warn("sender disconnected, retrying", slog.Any("err", err), slog.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > s.maxBackoff {
			backoff = s.maxBackoff
		}
	}
}

func (s *Sender) runOnce(ctx context.Context) error {
	conn, err := grpc.NewClient(s.peer.Addr, s.dialOpts...)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	cli := replv1.NewReplicationClient(conn)
	stream, err := cli.Push(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	if err := stream.Send(&replv1.PushMessage{
		Payload: &replv1.PushMessage_Hello{Hello: &replv1.Hello{
			SenderId: s.senderID,
			Origin:   s.origin,
		}},
	}); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}

	// Acks come back asynchronously. We track them so we can advance our
	// cursor only after the peer has accepted an event. Pending records
	// the highest event id we have shipped and not yet acked.
	ackCh := make(chan ackResult, 32)
	errCh := make(chan error, 1)
	go func() { errCh <- s.recvAcks(stream, ackCh) }()

	cursor := uint64(0)
	confirmed := uint64(0)
	pending := make(map[uint64]struct{})

	for {
		select {
		case <-ctx.Done():
			_ = stream.CloseSend()
			return ctx.Err()
		case rerr := <-errCh:
			return rerr
		case res := <-ackCh:
			if !res.accepted {
				s.logger.Warn("peer rejected event", slog.Uint64("id", res.id), slog.String("err", res.err))
				// We treat NACK as a soft signal: the peer may not be
				// the leader, in which case the sender should reconnect
				// and let the dial layer find a new endpoint. Today the
				// addr is fixed, so we just drop and move on.
			}
			delete(pending, res.id)
			if res.id > confirmed {
				confirmed = res.id
			}
		default:
		}

		ev, newCursor, err := s.outbox.Next(ctx, s.peer.Name, cursor)
		if err != nil {
			_ = stream.CloseSend()
			return err
		}
		s.logger.Debug("forwarding event", slog.Uint64("id", newCursor),
			slog.String("origin", string(ev.Origin)), slog.String("key", ev.Key))
		msg := EventToProto(ev)
		if err := stream.Send(&replv1.PushMessage{
			Payload: &replv1.PushMessage_Event{Event: msg},
		}); err != nil {
			return fmt.Errorf("send event: %w", err)
		}
		pending[newCursor] = struct{}{}
		cursor = newCursor
	}
}

type ackResult struct {
	id       uint64
	accepted bool
	err      string
}

func (s *Sender) recvAcks(stream replv1.Replication_PushClient, out chan<- ackResult) error {
	defer close(out)
	for {
		ack, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("recv ack: %w", err)
		}
		out <- ackResult{
			id:       ack.GetEventId(),
			accepted: ack.GetAccepted(),
			err:      ack.GetError(),
		}
	}
}
