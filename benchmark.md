# Pipeline Benchmark Results

**CPU:** 11th Gen Intel Core i7-11800H @ 2.30GHz  
**Go GOMAXPROCS:** 16

**Run all benchmarks:**
```bash
go test -run '^$' -bench '^Benchmark(CPULight|IOBound|RealCrypto|RealAudio|RealImageBlur)' -benchmem -benchtime=3s .
```

**Run one at a time:**
```bash
go test -run '^$' -bench '^BenchmarkCPULight' -benchmem -benchtime=3s .
go test -run '^$' -bench '^BenchmarkIOBound' -benchmem -benchtime=3s .
go test -run '^$' -bench '^Benchmark(RealCrypto|RealAudio|RealImageBlur)' -benchmem -benchtime=3s .
```

---

## What Is Being Benchmarked

Each benchmark compares **plain sequential Go** (a single `for` loop, zero goroutines) against the same work run through the **pipeline** at different worker counts.

One benchmark *operation* (`b.N` loop iteration) processes a fixed-size batch from start to finish. The timer covers only the processing loop — all inputs are pre-generated outside the timer.

All pipeline runs use `Transform → Concurrent(n) → Buffer → Execute` and drain with `Await`.  
Sequential runs use a plain `for _, v := range values` loop.

---

## Benchmark 1 — `BenchmarkCPULight`

**Workload:** `isPrime(v)` — trial-division primality test  
**Batch:** 16 384 items · **Per-item cost:** ~1–3 µs  
**Models:** any cheap per-record filter or validation step

| Variant | ns/op | B/op | allocs/op | vs sequential |
|---|---:|---:|---:|---:|
| sequential | 48 882 563 | 0 | 0 | baseline |
| pipeline / workers\_1 | 84 052 864 | 28 936 | 28 | 1.72× slower |
| pipeline / workers\_2 | 63 764 979 | 29 504 | 33 | 1.30× slower |
| pipeline / workers\_3 | 60 578 327 | 29 675 | 37 | 1.24× slower |
| pipeline / workers\_auto | 119 521 105 | 40 002 | 116 | 2.44× slower |

**Reading:** Sequential wins at every worker count. Each `isPrime` call finishes in a few µs — faster than a goroutine context-switch — so adding workers only adds overhead. Workers\_auto (16) is the worst: the scheduler is thrashed by 16 goroutines fighting over trivially short tasks.

**Takeaway:** When per-item work is in the single-µs range, pipeline overhead exceeds the value of concurrency. Plain Go wins.

---

## Benchmark 2 — `BenchmarkIOBound`

**Workload:** `time.Sleep(1 µs)` per item — simulated fast I/O call  
**Batch:** 1 024 items · **Per-item cost:** ~30 µs actual (OS timer granularity)  
**Models:** cache lookup, local socket round-trip, fast database read

| Variant | ns/op | B/op | allocs/op | vs sequential |
|---|---:|---:|---:|---:|
| sequential | 31 730 820 | 0 | 0 | baseline |
| pipeline / workers\_1 | 10 686 839 | 8 085 | 28 | **2.97× faster** |
| pipeline / workers\_8 | 7 851 420 | 10 554 | 64 | **4.04× faster** |
| pipeline / workers\_auto | 16 015 735 | 13 417 | 106 | 1.98× faster |

**Reading:** Even a single pipeline worker is 3× faster than sequential — the async staging overlaps the producer filling the channel with the consumer draining it. Workers\_8 is the sweet spot at 4×. Workers\_auto (16) drops back to 2×: too many goroutines contend on the fan-in channel.

**Takeaway:** I/O-bound work is where the pipeline shines most, because workers overlap wait time rather than serialising it.

---

## Benchmark 3 — `BenchmarkRealCrypto`

**Workload:** SHA-256 of a 1 MB block per item  
**Batch:** 128 items · **Per-item cost:** ~690 µs  
**Models:** file-chunk hashing pipeline (content-addressed storage, dedup, signing)

| Variant | ns/op | B/op | allocs/op | vs sequential |
|---|---:|---:|---:|---:|
| sequential | 88 054 080 | 0 | 0 | baseline |
| pipeline / workers\_2 | 46 791 948 | 3 085 | 33 | **1.88× faster** |
| pipeline / workers\_4 | 25 379 380 | 3 300 | 39 | **3.47× faster** |
| pipeline / workers\_auto | 12 809 808 | 6 114 | 88 | **6.88× faster** |

**Reading:** Near-ideal linear scaling — workers\_4 gives 3.47× (ideal would be 4×). Workers\_auto (16) reaches 6.88× with no plateau, because SHA-256 has no shared mutable state and the 1 MB input is read-only (cache-friendly). This is the pipeline at its best for CPU-bound work.

**Takeaway:** For ms-range per-item CPU work with no shared state, the pipeline scales nearly linearly with worker count.

---

## Benchmark 4 — `BenchmarkRealAudio`

**Workload:** 128-tap FIR low-pass filter over a 4 096-sample `float64` audio buffer  
**Batch:** 256 items · **Per-item cost:** ~446 µs · **Alloc per item:** ~32 KB (output buffer)  
**Models:** one audio frame through a DSP effect chain (~93 ms of audio at 44.1 kHz)

| Variant | ns/op | B/op | allocs/op | vs sequential |
|---|---:|---:|---:|---:|
| sequential | 114 073 703 | 8 388 608 | 256 | baseline |
| pipeline / workers\_2 | 63 538 535 | 8 392 067 | 287 | **1.80× faster** |
| pipeline / workers\_4 | 36 414 208 | 8 392 678 | 296 | **3.13× faster** |
| pipeline / workers\_auto | 18 633 434 | 8 396 595 | 354 | **6.12× faster** |

**Reading:** Scales cleanly to workers\_auto (6.12×). The per-op B/op (~8 MB) is entirely from allocating one 32 KB output buffer per audio frame — present in both sequential and pipeline runs, so it does not affect the relative comparison. Workers\_auto still scales well because each worker allocates independently and GC pressure is modest at this batch size.

**Takeaway:** Real DSP workloads with moderate per-frame allocation scale nearly as well as zero-allocation crypto work.

---

## Benchmark 5 — `BenchmarkRealImageBlur`

**Workload:** 3×3 box blur × 4 passes on a 256×256 uint8 grayscale tile  
**Batch:** 128 items · **Per-item cost:** ~1 ms · **Alloc per item:** ~128 KB (src + dst ping-pong buffers)  
**Models:** one video-frame tile processing step in a software renderer or transcoder

| Variant | ns/op | B/op | allocs/op | vs sequential |
|---|---:|---:|---:|---:|
| sequential | 128 712 945 | 16 777 216 | 256 | baseline |
| pipeline / workers\_2 | 74 119 914 | 16 779 805 | 287 | **1.74× faster** |
| pipeline / workers\_4 | 41 544 195 | 16 780 391 | 296 | **3.10× faster** |
| pipeline / workers\_auto | 27 789 673 | 16 784 332 | 354 | **4.63× faster** |

**Reading:** workers\_4 gives 3.1× (close to ideal 4×). Workers\_auto (16) reaches 4.63× — slightly below linear because allocating and touching ~128 KB per item per worker adds GC pressure. The ~16 MB B/op comes from the ping-pong buffers; both sequential and pipeline allocate the same amount per item, so it is a fair comparison.

**Takeaway:** Even allocation-heavy image processing scales well. The GC pressure from large per-item allocations caps scaling somewhat at high worker counts.

---

## Metric Reference

| Metric | Meaning | Better |
|---|---|---|
| `ns/op` | Average wall time for one full batch operation | Lower |
| `B/op` | Heap bytes allocated per operation | Lower |
| `allocs/op` | Number of heap allocations per operation | Lower |

---

## Summary

| Benchmark | Workload | Cost/item | Pipeline vs sequential |
|---|---|---|---|
| CPULight | isPrime | ~1–3 µs | **Sequential wins** — overhead > work |
| IOBound | sleep/wait | ~30 µs | Pipeline wins at workers\_8 (4×) |
| RealCrypto | SHA-256 / 1 MB | ~690 µs | **Pipeline wins at workers\_auto (6.9×)** |
| RealAudio | FIR 128-tap / 4096 samples | ~446 µs | **Pipeline wins at workers\_auto (6.1×)** |
| RealImageBlur | box blur 256×256 × 4 | ~1 ms | **Pipeline wins at workers\_auto (4.6×)** |

The pipeline's value scales directly with per-item cost. Below ~10 µs, overhead dominates and sequential Go is faster. Above ~100 µs per item, the pipeline delivers near-linear speedup with worker count — 4 workers → ~3× faster, 16 workers → 5–7× faster.
