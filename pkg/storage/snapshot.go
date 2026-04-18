package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"edge-cloud-replication/pkg/hlc"
)

// Snapshotter is implemented by stores that can serialize their entire state
// for Raft log compaction and bring-up. MemStore implements this.
type Snapshotter interface {
	Snapshot(w io.Writer) error
	Restore(r io.Reader) error
}

// snapshotMagic tags the start of a snapshot stream so we can detect version
// mismatches rather than silently corrupting state.
const snapshotMagic uint32 = 0x45435253 // "ECRS"

// snapshotVersion is bumped whenever the on-disk snapshot format changes in
// an incompatible way. Older versions are refused at Restore time.
const snapshotVersion uint8 = 1

// Snapshot writes a full, self-contained serialization of the store to w.
// The format is little-endian and framed so that Restore can parse it
// without needing any out-of-band schema:
//
//	magic(4) version(1) numKeys(8) [ keyLen(4) key numVersions(4) [ version ]* ]*
//
// where each version is:
//
//	physical(8) logical(4) flags(1) valueLen(4) value
//
// flags bit 0 = Deleted (tombstone).
func (m *MemStore) Snapshot(w io.Writer) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	bw := newBufWriter(w)

	if err := bw.putUint32(snapshotMagic); err != nil {
		return err
	}
	if err := bw.putUint8(snapshotVersion); err != nil {
		return err
	}
	if err := bw.putUint64(uint64(len(m.data))); err != nil {
		return err
	}

	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		versions := m.data[k]
		if err := bw.putString(k); err != nil {
			return err
		}
		if err := bw.putUint32(uint32(len(versions))); err != nil {
			return err
		}
		for _, v := range versions {
			if err := bw.putUint64(uint64(v.Timestamp.Physical)); err != nil {
				return err
			}
			if err := bw.putUint32(v.Timestamp.Logical); err != nil {
				return err
			}
			var flags uint8
			if v.Deleted {
				flags |= 0x01
			}
			if err := bw.putUint8(flags); err != nil {
				return err
			}
			if err := bw.putBytes(v.Value); err != nil {
				return err
			}
		}
	}
	return bw.flush()
}

// Restore replaces the store's entire state with the snapshot at r. The
// caller is responsible for ensuring no concurrent writers exist; this is
// the Raft FSM contract at Restore time.
func (m *MemStore) Restore(r io.Reader) error {
	br := newBufReader(r)

	magic, err := br.getUint32()
	if err != nil {
		return fmt.Errorf("snapshot: read magic: %w", err)
	}
	if magic != snapshotMagic {
		return fmt.Errorf("snapshot: bad magic 0x%x", magic)
	}

	version, err := br.getUint8()
	if err != nil {
		return fmt.Errorf("snapshot: read version: %w", err)
	}
	if version != snapshotVersion {
		return fmt.Errorf("snapshot: unsupported version %d (want %d)", version, snapshotVersion)
	}

	numKeys, err := br.getUint64()
	if err != nil {
		return fmt.Errorf("snapshot: read numKeys: %w", err)
	}

	data := make(map[string][]Version, numKeys)
	for i := uint64(0); i < numKeys; i++ {
		key, err := br.getString()
		if err != nil {
			return fmt.Errorf("snapshot: read key %d: %w", i, err)
		}
		numVersions, err := br.getUint32()
		if err != nil {
			return fmt.Errorf("snapshot: read numVersions %q: %w", key, err)
		}
		versions := make([]Version, 0, numVersions)
		for j := uint32(0); j < numVersions; j++ {
			phys, err := br.getUint64()
			if err != nil {
				return fmt.Errorf("snapshot: read phys %q[%d]: %w", key, j, err)
			}
			logical, err := br.getUint32()
			if err != nil {
				return fmt.Errorf("snapshot: read logical %q[%d]: %w", key, j, err)
			}
			flags, err := br.getUint8()
			if err != nil {
				return fmt.Errorf("snapshot: read flags %q[%d]: %w", key, j, err)
			}
			val, err := br.getBytes()
			if err != nil {
				return fmt.Errorf("snapshot: read value %q[%d]: %w", key, j, err)
			}
			versions = append(versions, Version{
				Timestamp: hlc.Timestamp{Physical: int64(phys), Logical: logical},
				Value:     val,
				Deleted:   flags&0x01 != 0,
			})
		}
		data[key] = versions
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = data
	return nil
}

// bufWriter is a small helper around io.Writer that buffers by returning the
// first error and short-circuits subsequent writes.
type bufWriter struct {
	w   io.Writer
	err error
}

func newBufWriter(w io.Writer) *bufWriter { return &bufWriter{w: w} }

func (b *bufWriter) putUint8(v uint8) error {
	if b.err != nil {
		return b.err
	}
	_, b.err = b.w.Write([]byte{v})
	return b.err
}

func (b *bufWriter) putUint32(v uint32) error {
	if b.err != nil {
		return b.err
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	_, b.err = b.w.Write(buf[:])
	return b.err
}

func (b *bufWriter) putUint64(v uint64) error {
	if b.err != nil {
		return b.err
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	_, b.err = b.w.Write(buf[:])
	return b.err
}

func (b *bufWriter) putBytes(v []byte) error {
	if err := b.putUint32(uint32(len(v))); err != nil {
		return err
	}
	if len(v) == 0 {
		return nil
	}
	_, b.err = b.w.Write(v)
	return b.err
}

func (b *bufWriter) putString(v string) error {
	if err := b.putUint32(uint32(len(v))); err != nil {
		return err
	}
	if len(v) == 0 {
		return nil
	}
	_, b.err = b.w.Write([]byte(v))
	return b.err
}

func (b *bufWriter) flush() error { return b.err }

type bufReader struct {
	r   io.Reader
	err error
}

func newBufReader(r io.Reader) *bufReader { return &bufReader{r: r} }

func (b *bufReader) read(n int) ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}
	buf := make([]byte, n)
	_, b.err = io.ReadFull(b.r, buf)
	if b.err != nil {
		return nil, b.err
	}
	return buf, nil
}

func (b *bufReader) getUint8() (uint8, error) {
	buf, err := b.read(1)
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (b *bufReader) getUint32() (uint32, error) {
	buf, err := b.read(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func (b *bufReader) getUint64() (uint64, error) {
	buf, err := b.read(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf), nil
}

func (b *bufReader) getBytes() ([]byte, error) {
	n, err := b.getUint32()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	return b.read(int(n))
}

func (b *bufReader) getString() (string, error) {
	buf, err := b.getBytes()
	if err != nil {
		return "", err
	}
	return string(buf), nil
}
