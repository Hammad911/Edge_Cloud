// Package raft binds the hashicorp/raft library to our versioned storage
// layer, exposing the subset of consensus we need through a small Replicator
// interface. Intra-cluster writes flow through this package; cross-cluster
// causal replication is handled elsewhere.
package raft

import (
	"encoding/binary"
	"fmt"

	"edge-cloud-replication/pkg/hlc"
)

// OpType identifies the mutation encoded in a Command.
type OpType uint8

const (
	// OpPut writes a new value for a key.
	OpPut OpType = 1
	// OpDelete writes a tombstone for a key.
	OpDelete OpType = 2
)

// Command is the on-the-wire shape of a single mutation committed through
// Raft. It is serialized with MarshalBinary and written into raft.Log.Data.
// Deliberately flat: all fields are length-prefixed, little-endian, and we
// never embed Go types that require reflection to decode.
type Command struct {
	Op        OpType
	Timestamp hlc.Timestamp
	Key       string
	Value     []byte // nil for OpDelete
}

const cmdVersion byte = 1

// MarshalBinary encodes the command as:
//
//	version(1) op(1) phys(8) logical(4) keyLen(4) key valueLen(4) value
//
// All integers are little-endian.
func (c Command) MarshalBinary() ([]byte, error) {
	keyLen := uint32(len(c.Key))
	valLen := uint32(len(c.Value))

	size := 1 + 1 + 8 + 4 + 4 + int(keyLen) + 4 + int(valLen)
	buf := make([]byte, size)

	off := 0
	buf[off] = cmdVersion
	off++
	buf[off] = byte(c.Op)
	off++
	binary.LittleEndian.PutUint64(buf[off:], uint64(c.Timestamp.Physical))
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], c.Timestamp.Logical)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], keyLen)
	off += 4
	copy(buf[off:], c.Key)
	off += int(keyLen)
	binary.LittleEndian.PutUint32(buf[off:], valLen)
	off += 4
	copy(buf[off:], c.Value)

	return buf, nil
}

// UnmarshalBinary decodes a Command from the given bytes. It rejects buffers
// that are too short or carry an unknown version byte, since applying such
// an entry would silently corrupt the FSM.
func (c *Command) UnmarshalBinary(data []byte) error {
	if len(data) < 1+1+8+4+4+4 {
		return fmt.Errorf("raft: command too short (%d bytes)", len(data))
	}
	if data[0] != cmdVersion {
		return fmt.Errorf("raft: unsupported command version %d", data[0])
	}

	off := 1
	c.Op = OpType(data[off])
	off++
	c.Timestamp.Physical = int64(binary.LittleEndian.Uint64(data[off:]))
	off += 8
	c.Timestamp.Logical = binary.LittleEndian.Uint32(data[off:])
	off += 4

	keyLen := binary.LittleEndian.Uint32(data[off:])
	off += 4
	if off+int(keyLen) > len(data) {
		return fmt.Errorf("raft: command truncated (key)")
	}
	c.Key = string(data[off : off+int(keyLen)])
	off += int(keyLen)

	if off+4 > len(data) {
		return fmt.Errorf("raft: command truncated (valLen)")
	}
	valLen := binary.LittleEndian.Uint32(data[off:])
	off += 4
	if off+int(valLen) > len(data) {
		return fmt.Errorf("raft: command truncated (value)")
	}
	if valLen > 0 {
		c.Value = make([]byte, valLen)
		copy(c.Value, data[off:off+int(valLen)])
	}
	return nil
}
