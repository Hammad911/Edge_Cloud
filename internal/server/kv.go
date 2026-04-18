package server

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvv1 "edge-cloud-replication/gen/proto/edgecloud/kv/v1"
	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/pkg/kv"
)

// KVGRPCServer adapts a pkg/kv.Service to the generated gRPC KV interface.
// It handles:
//
//   - translating protobuf CausalTokens to/from hlc.Timestamps
//   - mapping domain errors to appropriate gRPC status codes
//   - logging and metric hooks (via injected *slog.Logger)
type KVGRPCServer struct {
	kvv1.UnimplementedKVServer

	svc    kv.Service
	logger *slog.Logger
}

// NewKVGRPCServer constructs the adapter. It does not register itself on any
// grpc.Server; call Register to do that.
func NewKVGRPCServer(svc kv.Service, logger *slog.Logger) *KVGRPCServer {
	return &KVGRPCServer{
		svc:    svc,
		logger: logger.With(slog.String("component", "kv_grpc")),
	}
}

// Register attaches this service to the given grpc.Server. Typical usage:
//
//	app.GRPC.Register(func(s *grpc.Server) { kvSrv.Register(s) })
func (k *KVGRPCServer) Register(s *grpc.Server) {
	kvv1.RegisterKVServer(s, k)
}

// Get implements the KV Get RPC.
func (k *KVGRPCServer) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}
	after := fromProtoToken(req.GetAfter())

	val, found, tok, err := k.svc.Get(ctx, req.GetKey(), after)
	if err != nil {
		return nil, mapError(err)
	}
	return &kvv1.GetResponse{
		Value: val,
		Found: found,
		Token: toProtoToken(tok),
	}, nil
}

// Put implements the KV Put RPC.
func (k *KVGRPCServer) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}
	after := fromProtoToken(req.GetAfter())

	tok, err := k.svc.Put(ctx, req.GetKey(), req.GetValue(), after)
	if err != nil {
		return nil, mapError(err)
	}
	return &kvv1.PutResponse{Token: toProtoToken(tok)}, nil
}

// Delete implements the KV Delete RPC.
func (k *KVGRPCServer) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}
	after := fromProtoToken(req.GetAfter())

	tok, err := k.svc.Delete(ctx, req.GetKey(), after)
	if err != nil {
		return nil, mapError(err)
	}
	return &kvv1.DeleteResponse{Token: toProtoToken(tok)}, nil
}

func toProtoToken(t hlc.Timestamp) *kvv1.CausalToken {
	if t.Zero() {
		return nil
	}
	return &kvv1.CausalToken{
		Physical: t.Physical,
		Logical:  t.Logical,
	}
}

func fromProtoToken(t *kvv1.CausalToken) hlc.Timestamp {
	if t == nil {
		return hlc.Timestamp{}
	}
	return hlc.Timestamp{Physical: t.GetPhysical(), Logical: t.GetLogical()}
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, kv.ErrEmptyKey):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, kv.ErrValueLimit):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, kv.ErrNotLeader):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, hlc.ErrClockDrift):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
