// Package config loads runtime configuration from files, environment variables,
// and command-line flags. It is intentionally strict: unknown fields and
// malformed values are rejected at startup rather than silently ignored.
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Role identifies which role a node plays in the two-tier topology.
type Role string

const (
	RoleEdge  Role = "edge"
	RoleCloud Role = "cloud"
)

// Config is the fully-resolved runtime configuration for a node.
type Config struct {
	Node        NodeConfig        `mapstructure:"node"`
	Admin       AdminConfig       `mapstructure:"admin"`
	GRPC        GRPCConfig        `mapstructure:"grpc"`
	Raft        RaftConfig        `mapstructure:"raft"`
	Replication ReplicationConfig `mapstructure:"replication"`
	Logging     LoggingConfig     `mapstructure:"logging"`
}

// NodeConfig identifies the node.
type NodeConfig struct {
	ID         string `mapstructure:"id"`
	Role       Role   `mapstructure:"role"`
	Datacenter string `mapstructure:"datacenter"`
}

// AdminConfig controls the HTTP admin surface (health, metrics, pprof).
type AdminConfig struct {
	ListenAddr    string        `mapstructure:"listen_addr"`
	ReadTimeout   time.Duration `mapstructure:"read_timeout"`
	WriteTimeout  time.Duration `mapstructure:"write_timeout"`
	ShutdownGrace time.Duration `mapstructure:"shutdown_grace"`
	EnablePprof   bool          `mapstructure:"enable_pprof"`
	EnableMetrics bool          `mapstructure:"enable_metrics"`
}

// GRPCConfig controls the data-plane gRPC server.
type GRPCConfig struct {
	ListenAddr        string        `mapstructure:"listen_addr"`
	MaxRecvMsgBytes   int           `mapstructure:"max_recv_msg_bytes"`
	MaxSendMsgBytes   int           `mapstructure:"max_send_msg_bytes"`
	KeepaliveInterval time.Duration `mapstructure:"keepalive_interval"`
	KeepaliveTimeout  time.Duration `mapstructure:"keepalive_timeout"`
	EnableReflection  bool          `mapstructure:"enable_reflection"`
	ShutdownGrace     time.Duration `mapstructure:"shutdown_grace"`
}

// RaftConfig configures the within-cluster Raft layer (edge nodes only).
type RaftConfig struct {
	Enabled   bool     `mapstructure:"enabled"`
	ClusterID string   `mapstructure:"cluster_id"`
	Bind      string   `mapstructure:"bind"`
	Peers     []string `mapstructure:"peers"`
	DataDir   string   `mapstructure:"data_dir"`
}

// ReplicationConfig configures the inter-cluster causal replication layer.
type ReplicationConfig struct {
	Enabled   bool     `mapstructure:"enabled"`
	CloudAddr string   `mapstructure:"cloud_addr"`
	Peers     []string `mapstructure:"peers"`
	BufferDir string   `mapstructure:"buffer_dir"`
}

// LoggingConfig controls process logging output.
type LoggingConfig struct {
	Level  string `mapstructure:"level"`  // debug|info|warn|error
	Format string `mapstructure:"format"` // text|json
}

// Load resolves configuration from (in precedence order):
//  1. Explicit file at `path` (if non-empty)
//  2. Environment variables with prefix ECR_ (double underscore for nesting)
//  3. Built-in defaults
//
// Example env overrides:
//
//	ECR_NODE_ID=edge-7
//	ECR_ADMIN_LISTEN_ADDR=0.0.0.0:8081
//	ECR_GRPC_LISTEN_ADDR=0.0.0.0:7001
func Load(path string) (*Config, error) {
	v := viper.New()
	applyDefaults(v)

	v.SetEnvPrefix("ECR")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("read config %q: %w", path, err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}

func applyDefaults(v *viper.Viper) {
	v.SetDefault("node.id", "node-1")
	v.SetDefault("node.role", string(RoleEdge))
	v.SetDefault("node.datacenter", "dc-1")

	v.SetDefault("admin.listen_addr", "127.0.0.1:8081")
	v.SetDefault("admin.read_timeout", "5s")
	v.SetDefault("admin.write_timeout", "30s")
	v.SetDefault("admin.shutdown_grace", "10s")
	v.SetDefault("admin.enable_pprof", false)
	v.SetDefault("admin.enable_metrics", true)

	v.SetDefault("grpc.listen_addr", "127.0.0.1:7001")
	v.SetDefault("grpc.max_recv_msg_bytes", 16*1024*1024)
	v.SetDefault("grpc.max_send_msg_bytes", 16*1024*1024)
	v.SetDefault("grpc.keepalive_interval", "30s")
	v.SetDefault("grpc.keepalive_timeout", "10s")
	v.SetDefault("grpc.enable_reflection", true)
	v.SetDefault("grpc.shutdown_grace", "10s")

	v.SetDefault("raft.enabled", false)
	v.SetDefault("raft.cluster_id", "edge-cluster-1")
	v.SetDefault("raft.bind", "127.0.0.1:7101")
	v.SetDefault("raft.peers", []string{})
	v.SetDefault("raft.data_dir", "./data/raft")

	v.SetDefault("replication.enabled", false)
	v.SetDefault("replication.cloud_addr", "127.0.0.1:9001")
	v.SetDefault("replication.peers", []string{})
	v.SetDefault("replication.buffer_dir", "./data/buffer")

	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "text")
}

// Validate enforces invariants that must hold before startup.
func (c *Config) Validate() error {
	if c.Node.ID == "" {
		return fmt.Errorf("node.id must be set")
	}
	switch c.Node.Role {
	case RoleEdge, RoleCloud:
	default:
		return fmt.Errorf("node.role must be 'edge' or 'cloud', got %q", c.Node.Role)
	}
	if c.Admin.ListenAddr == "" {
		return fmt.Errorf("admin.listen_addr must be set")
	}
	if c.GRPC.ListenAddr == "" {
		return fmt.Errorf("grpc.listen_addr must be set")
	}
	switch strings.ToLower(c.Logging.Level) {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("logging.level must be one of debug|info|warn|error, got %q", c.Logging.Level)
	}
	switch strings.ToLower(c.Logging.Format) {
	case "text", "json":
	default:
		return fmt.Errorf("logging.format must be 'text' or 'json', got %q", c.Logging.Format)
	}
	return nil
}
