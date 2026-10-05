# Chronos-VFS

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE-MIT)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE-APACHE)
[![Rust 2024](https://img.shields.io/badge/Rust-2024-orange.svg)]()

Chronos-VFS is an in-memory I/O bridge. A producer thread writes a byte stream through a standard `std::io::Write` handle into a lock-free SPSC ring buffer; a consumer thread drains the ring, compresses it with zstd on the fly and writes the compressed stream to disk. The uncompressed stream never touches the disk, and the memory used is fixed by the size of the ring no matter how long the stream is.

It was built to absorb the VeriPB proof traces of a SAT engine, which run to terabytes, without the producer ever blocking on a disk write.

## Architecture

- **`nvm_core` — block-oriented SPSC ring.** A byte ring whose producer copies with `memcpy` straight into ring memory and publishes its position on 4 KiB boundaries: one atomic store per chunk instead of one per byte. Head and tail live on separate 64-byte cache lines. The buffer is allocated once, page-aligned and pre-faulted.
- **`writer::RingWriter` — the bridge.** Wraps the producer side and implements `std::io::Write`, so `write!`, `writeln!` and `write_all` work unchanged. A write that fits in the current chunk is a bounds check and a `memcpy`; it touches no atomics.
- **`consumer::ZstdSink` — the consumer thread.** Feeds the zstd encoder directly from ring memory (no intermediate copy), returns space to the producer every 256 KiB, and syncs the output file when the stream closes.
- **Backpressure, not growth.** When the ring is full the producer waits for the consumer. Both sides spin with `core::hint::spin_loop`, then yield, then sleep.
- **Failure propagation.** If the consumer dies (disk full, I/O error) the producer's next write fails with `BrokenPipe` instead of hanging, and `ZstdSink::join` returns the underlying error.

## Usage

```toml
[dependencies]
chronos-vfs = { git = "https://github.com/yemsy26/Chronos-VFS" }
```

```rust
use std::io::Write;

let (mut proof, sink) = chronos_vfs::zstd_bridge(
    "proof.pbp.zst",
    256 << 20, // ring capacity in bytes, a power of two
    chronos_vfs::ZstdConfig::default(),
)?;

writeln!(proof, "pseudo-Boolean proof version 3.0")?;
// ... hand `proof` to anything that takes a `Write` ...

proof.close();             // publish the partial chunk, signal end of stream
let stats = sink.join()?;  // wait for the .zst to be complete and synced
println!("{} -> {} bytes", stats.raw_bytes, stats.compressed_bytes);
```

`sink.join()` must be called after the writer is closed and before the process exits; a process that exits first leaves a truncated file.

The pieces can also be used separately: `RingBuffer::with_capacity` returns a `(Producer, Consumer)` pair, and `drain_to_zstd` compresses a `Consumer` into any `Write`.

## Memory

Resident memory is the ring plus a fixed overhead that does not depend on the length of the stream: the zstd compression context and a 1 MiB output buffer. `ZstdConfig::workers` adds zstd's per-worker buffers on top.

## Benchmark

`cargo run --release --example veripb_bench -- 4096 64 3 0` emits 4 GiB of synthetic VeriPB-shaped lines with `writeln!`. On an Intel Core i7-13620H (Windows 11), 64 MiB ring:

| Pass | Throughput | Peak working set |
| --- | --- | --- |
| `BufWriter` to nowhere (reference, no ring) | ~890 MB/s | |
| Ring, `writeln!`, discarding consumer | ~790 MB/s | |
| Ring, raw 4 KiB blocks, discarding consumer | ~15 GB/s | |
| Ring + zstd -3 to disk | ~250 MB/s | 72 MiB |
| Ring + zstd -1 to disk | ~375 MB/s | 70 MiB |
| Ring + zstd -3, 4 workers, to disk | ~650 MB/s | 143 MiB |

With a single compression thread zstd is the bottleneck and the producer is throttled to its speed. The synthetic lines carry random literals and compress 3.6x; real traces will differ.

## Stress test

`cargo run --release --bin stress_test -- [payload MiB] [backpressure seconds per ring]` (defaults: 500 and 10) runs three checks in sequence and exits with 0 if all pass, 1 if any fails and 2 if its watchdog sees no progress for 60 seconds:

1. **Integrity and wrap-around.** A position-dependent payload is written in pieces of 1, 17, 4099 and other prime sizes up to 65537 bytes, with a `flush` every 13 writes, through a 64 KiB ring and through the minimum 8 KiB ring, then the `.zst` is decompressed as a stream and compared against the original by length, FNV-1a hash and first differing offset.
2. **Backpressure.** zstd -19 on a single thread against a producer writing as fast as it can, with a 1 MiB ring and with the minimum ring. Passes if nothing is lost and the producer thread spends at most 10% of the wall time on CPU while it is being throttled.
3. **Partial flush.** Three bytes, then `close()` or `drop()`, then `join()`: the file must decompress to exactly those three bytes.

## Author & Citation

**Ramon Antonio Burgos Jerez**
- **Profession:** B.S. in Computer Science (Licenciado en Informática)
- **GitHub:** [@yemsy26](https://github.com/yemsy26)
