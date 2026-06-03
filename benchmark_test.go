package universe

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// benchmarkSink prevents the compiler from optimising away benchmark work.
var benchmarkSink uint64

// benchmarkValues returns a reproducible slice of random ints.
func benchmarkValues(size int) []int {
	const maxValue = int64(1 << 32)
	values := make([]int, size)
	rng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = int(rng.Int63n(maxValue))
	}
	return values
}

// seqRun runs work over every value sequentially, one goroutine.
func seqRun(b *testing.B, values []int, work func(int)) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range values {
			work(v)
		}
	}
	b.StopTimer()
}

// pipelineRun fans work out across workers goroutines via the pipeline.
func pipelineRun(b *testing.B, values []int, workers, buffer int, work func(int)) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := From(ctx, values...).
			Configure(WithFailFast()).
			Transform(func(v int) (int, error) {
				work(v)
				return v, nil
			}).
			Concurrent(workers).
			Buffer(buffer).
			Execute()

		if err := out.Await(ctx); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
	b.StopTimer()
}

// BenchmarkCPULight measures a cheap CPU workload (primality test, ~µs/item).
// Highlights pipeline overhead when per-item cost is low.
func BenchmarkCPULight(b *testing.B) {
	const batch = 1 << 14 // 16 384 items
	values := benchmarkValues(batch)
	work := func(v int) {
		if isPrime(v) {
			atomic.AddUint64(&benchmarkSink, 1)
		}
	}

	b.Run("sequential", func(b *testing.B) { seqRun(b, values, work) })
	b.Run("pipeline/workers_1", func(b *testing.B) { pipelineRun(b, values, 1, 1024, work) })
	b.Run("pipeline/workers_2", func(b *testing.B) { pipelineRun(b, values, 2, 1024, work) })
	b.Run("pipeline/workers_3", func(b *testing.B) { pipelineRun(b, values, 3, 1024, work) })
	b.Run("pipeline/workers_auto", func(b *testing.B) { pipelineRun(b, values, runtime.GOMAXPROCS(0), 1024, work) })
}

// BenchmarkIOBound measures a workload where each item incurs a 1 µs sleep,
// simulating a fast I/O call (e.g. cache lookup, local socket round-trip).
// This is where pipeline concurrency provides the most dramatic speedup:
// workers overlap wait time instead of serialising it.
func BenchmarkIOBound(b *testing.B) {
	const batch = 1 << 10 // 1 024 items — smaller to keep wall time manageable
	values := benchmarkValues(batch)
	work := func(_ int) { time.Sleep(time.Microsecond) }

	b.Run("sequential", func(b *testing.B) { seqRun(b, values, work) })
	b.Run("pipeline/workers_1", func(b *testing.B) { pipelineRun(b, values, 1, 256, work) })
	b.Run("pipeline/workers_8", func(b *testing.B) { pipelineRun(b, values, 8, 256, work) })
	b.Run("pipeline/workers_auto", func(b *testing.B) { pipelineRun(b, values, runtime.GOMAXPROCS(0), 256, work) })
}

// --- Real heavy workload helpers ---

// sha256Block runs SHA-256 over data. Used as a single-call, no-allocation
// crypto benchmark kernel (the sha256 Hash state is stack-escapable but tiny).
func sha256Block(data []byte) uint64 {
	h := sha256.Sum256(data)
	return binary.LittleEndian.Uint64(h[:])
}

// makeFIRTaps returns n windowed-sinc FIR low-pass coefficients (Hann window,
// normalised cutoff 0.1). Computed once per benchmark, not in the hot path.
func makeFIRTaps(n int) []float64 {
	taps := make([]float64, n)
	center := float64(n-1) / 2.0
	const cutoff = 0.1
	for i := range taps {
		x := float64(i) - center
		var sinc float64
		if x == 0 {
			sinc = 2 * math.Pi * cutoff
		} else {
			sinc = math.Sin(2*math.Pi*cutoff*x) / (math.Pi * x)
		}
		win := 0.5 * (1 - math.Cos(2*math.Pi*float64(i)/float64(n-1)))
		taps[i] = sinc * win
	}
	return taps
}

// firConvolve applies taps as a linear FIR filter to samples and returns the
// output buffer. Allocates one output slice per call, as a real DSP stage would.
func firConvolve(samples, taps []float64) []float64 {
	out := make([]float64, len(samples))
	ntaps := len(taps)
	for i := range samples {
		var acc float64
		for j := 0; j < ntaps && i-j >= 0; j++ {
			acc += taps[j] * samples[i-j]
		}
		out[i] = acc
	}
	return out
}

// boxBlur performs one pass of 3×3 box blur from src into dst (w×h uint8 grayscale).
func boxBlur(src, dst []uint8, w, h int) {
	for y := 1; y < h-1; y++ {
		for x := 1; x < w-1; x++ {
			sum := int(src[(y-1)*w+x-1]) + int(src[(y-1)*w+x]) + int(src[(y-1)*w+x+1]) +
				int(src[y*w+x-1]) + int(src[y*w+x]) + int(src[y*w+x+1]) +
				int(src[(y+1)*w+x-1]) + int(src[(y+1)*w+x]) + int(src[(y+1)*w+x+1])
			dst[y*w+x] = uint8(sum / 9)
		}
	}
}

// BenchmarkRealCrypto benchmarks SHA-256 of a 1 MB block per item, modelling
// a file-chunk hashing pipeline (e.g. content-addressed storage, dedup, signing).
// At ~1 ms/item this is the floor of what counts as "real" cryptographic work.
func BenchmarkRealCrypto(b *testing.B) {
	const batch = 128

	rng := rand.New(rand.NewSource(42))
	block := make([]byte, 1<<20) // 1 MB
	for i := range block {
		block[i] = byte(rng.Intn(256))
	}

	values := benchmarkValues(batch)
	work := func(_ int) {
		atomic.AddUint64(&benchmarkSink, sha256Block(block))
	}

	b.Run("sequential", func(b *testing.B) { seqRun(b, values, work) })
	b.Run("pipeline/workers_2", func(b *testing.B) { pipelineRun(b, values, 2, 32, work) })
	b.Run("pipeline/workers_4", func(b *testing.B) { pipelineRun(b, values, 4, 32, work) })
	b.Run("pipeline/workers_auto", func(b *testing.B) { pipelineRun(b, values, runtime.GOMAXPROCS(0), 32, work) })
}

// BenchmarkRealAudio benchmarks a 128-tap FIR low-pass filter over a 4 096-sample
// float64 audio buffer per item. Models one frame of DSP work (~93 ms of audio at
// 44.1 kHz). This is representative of real-time audio effect chains (EQ, reverb send,
// dynamics) where each stage processes a fixed-size buffer.
func BenchmarkRealAudio(b *testing.B) {
	const (
		batch   = 256
		samples = 4096
		taps    = 128
	)

	rng := rand.New(rand.NewSource(42))
	audioBuf := make([]float64, samples)
	for i := range audioBuf {
		audioBuf[i] = rng.Float64()*2 - 1
	}
	firTaps := makeFIRTaps(taps)

	values := benchmarkValues(batch)
	work := func(_ int) {
		out := firConvolve(audioBuf, firTaps)
		atomic.AddUint64(&benchmarkSink, math.Float64bits(out[len(out)-1]))
	}

	b.Run("sequential", func(b *testing.B) { seqRun(b, values, work) })
	b.Run("pipeline/workers_2", func(b *testing.B) { pipelineRun(b, values, 2, 64, work) })
	b.Run("pipeline/workers_4", func(b *testing.B) { pipelineRun(b, values, 4, 64, work) })
	b.Run("pipeline/workers_auto", func(b *testing.B) { pipelineRun(b, values, runtime.GOMAXPROCS(0), 64, work) })
}

// BenchmarkRealImageBlur benchmarks 4 passes of 3×3 box blur on a 256×256 uint8
// grayscale tile per item. Models one video-frame tile processing step — the kind
// of per-tile work a software renderer or video transcoder would distribute across
// a worker pool.
func BenchmarkRealImageBlur(b *testing.B) {
	const (
		batch  = 128
		w, h   = 256, 256
		passes = 4
	)

	rng := rand.New(rand.NewSource(42))
	tile := make([]uint8, w*h)
	for i := range tile {
		tile[i] = uint8(rng.Intn(256))
	}

	values := benchmarkValues(batch)
	work := func(_ int) {
		src := make([]uint8, len(tile))
		dst := make([]uint8, len(tile))
		copy(src, tile)
		for i := 0; i < passes; i++ {
			boxBlur(src, dst, w, h)
			src, dst = dst, src
		}
		atomic.AddUint64(&benchmarkSink, uint64(src[(h/2)*w+(w/2)]))
	}

	b.Run("sequential", func(b *testing.B) { seqRun(b, values, work) })
	b.Run("pipeline/workers_2", func(b *testing.B) { pipelineRun(b, values, 2, 32, work) })
	b.Run("pipeline/workers_4", func(b *testing.B) { pipelineRun(b, values, 4, 32, work) })
	b.Run("pipeline/workers_auto", func(b *testing.B) { pipelineRun(b, values, runtime.GOMAXPROCS(0), 32, work) })
}

// isPrime is the predicate used by BenchmarkCPULight.
func isPrime(n int) bool {
	if n < 2 {
		return false
	}
	if n == 2 {
		return true
	}
	if n%2 == 0 {
		return false
	}
	for d := 3; d*d <= n; d += 2 {
		if n%d == 0 {
			return false
		}
	}
	return true
}
