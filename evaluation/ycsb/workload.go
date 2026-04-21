package ycsb

import (
	"errors"
	"fmt"
	"strings"
)

// OpKind enumerates the closed-loop operations the driver knows how to
// issue. They map one-to-one to the KV gRPC surface on the edge nodes.
type OpKind string

const (
	OpRead             OpKind = "read"
	OpUpdate           OpKind = "update"
	OpInsert           OpKind = "insert"
	OpReadModifyWrite  OpKind = "rmw"
	OpDelete           OpKind = "delete"
)

// Mix expresses an operation mix as normalised ratios. Fields that are
// zero contribute nothing; the driver normalises the totals so callers
// can use raw percentages (e.g. 50/50) without worrying about summation.
type Mix struct {
	Read            float64 `json:"read"`
	Update          float64 `json:"update"`
	Insert          float64 `json:"insert"`
	ReadModifyWrite float64 `json:"rmw"`
	Delete          float64 `json:"delete"`
}

// Normalised returns (probabilities, total); total is guaranteed > 0.
func (m Mix) Normalised() (map[OpKind]float64, float64, error) {
	raw := map[OpKind]float64{
		OpRead:            m.Read,
		OpUpdate:          m.Update,
		OpInsert:          m.Insert,
		OpReadModifyWrite: m.ReadModifyWrite,
		OpDelete:          m.Delete,
	}
	var total float64
	for _, v := range raw {
		if v < 0 {
			return nil, 0, fmt.Errorf("ycsb: negative mix ratio")
		}
		total += v
	}
	if total == 0 {
		return nil, 0, errors.New("ycsb: empty operation mix")
	}
	out := make(map[OpKind]float64, len(raw))
	for k, v := range raw {
		if v == 0 {
			continue
		}
		out[k] = v / total
	}
	return out, total, nil
}

// Workload names the canonical YCSB presets we support. Workload E
// requires scan support which the KV service does not expose, so it is
// intentionally omitted.
type Workload string

const (
	WorkloadA Workload = "A" // 50/50 read/update
	WorkloadB Workload = "B" // 95/5 read/update
	WorkloadC Workload = "C" // 100% read
	WorkloadD Workload = "D" // 95/5 read/insert (latest)
	WorkloadF Workload = "F" // 50/50 read/read-modify-write
)

// Spec is the fully-expanded description of a benchmark run.
type Spec struct {
	Workload     Workload
	Mix          Mix
	Distribution KeyDistribution
	RecordCount  int
	ValueSize    int
	FieldCount   int
}

// Preset returns the mix + key distribution associated with a canonical
// YCSB workload.
func Preset(w Workload) (Spec, error) {
	switch Workload(strings.ToUpper(string(w))) {
	case WorkloadA:
		return Spec{Workload: WorkloadA, Mix: Mix{Read: 0.5, Update: 0.5}, Distribution: DistZipfian}, nil
	case WorkloadB:
		return Spec{Workload: WorkloadB, Mix: Mix{Read: 0.95, Update: 0.05}, Distribution: DistZipfian}, nil
	case WorkloadC:
		return Spec{Workload: WorkloadC, Mix: Mix{Read: 1.0}, Distribution: DistZipfian}, nil
	case WorkloadD:
		return Spec{Workload: WorkloadD, Mix: Mix{Read: 0.95, Insert: 0.05}, Distribution: DistLatest}, nil
	case WorkloadF:
		return Spec{Workload: WorkloadF, Mix: Mix{Read: 0.5, ReadModifyWrite: 0.5}, Distribution: DistZipfian}, nil
	}
	return Spec{}, fmt.Errorf("ycsb: unknown workload %q", w)
}
