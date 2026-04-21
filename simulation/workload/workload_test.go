package workload

import (
	"testing"
)

func TestGenerator_UniformDistribution(t *testing.T) {
	g := NewGenerator(Spec{
		NumKeys:      100,
		WriteRatio:   0.5,
		DeleteRatio:  0.0,
		ValueSize:    8,
		Distribution: Uniform,
		Seed:         42,
	})
	const N = 10000
	puts, gets := 0, 0
	for i := 0; i < N; i++ {
		op := g.Next()
		switch op.Kind {
		case OpPut:
			puts++
			if len(op.Value) != 8 {
				t.Fatalf("value size: got %d want 8", len(op.Value))
			}
		case OpGet:
			gets++
		}
	}
	// 50/50 split, allow ±5%
	if puts < int(0.45*N) || puts > int(0.55*N) {
		t.Fatalf("write ratio off: puts=%d/%d", puts, N)
	}
	_ = gets
}

func TestGenerator_ZipfianHotKeys(t *testing.T) {
	g := NewGenerator(Spec{
		NumKeys:      1000,
		WriteRatio:   1,
		Distribution: Zipfian,
		Seed:         1,
		ValueSize:    1,
	})
	counts := make(map[string]int)
	const N = 50000
	for i := 0; i < N; i++ {
		op := g.Next()
		counts[op.Key]++
	}
	// Top-1 key should be hit a non-trivial number of times.
	max := 0
	for _, c := range counts {
		if c > max {
			max = c
		}
	}
	if max < 1000 {
		t.Fatalf("hottest key only hit %d/%d times; expected zipfian skew", max, N)
	}
}

func TestZipfHotKeyShare(t *testing.T) {
	share := ZipfHotKeyShare(1000, 10)
	if share <= 0.05 || share >= 0.95 {
		t.Fatalf("hot-key share suspicious: %f", share)
	}
}
