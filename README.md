# Chronos-VFS

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE-MIT)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE-APACHE)
[![Rust 2024](https://img.shields.io/badge/Rust-2024-orange.svg)]()

Chronos-VFS is an experimental user-space generative file system. It addresses the Von Neumann cache-wall and DRAM load-store latency constraints by mathematically computing requested data chunks in-situ rather than loading them from physical memory or disk storage.

## Design Philosophy

In modern data-intensive workloads, memory bandwidth and CPU cache invalidation frequently bottleneck execution pipelines. Data fetched from RAM stalls the arithmetic logic unit (ALU). Chronos-VFS implements Compute-In-Memory principles natively in software via procedural state expansion. 

Instead of storing multi-terabyte datasets in physical memory, Chronos-VFS maps logical offsets to a minimal base seed (8 bytes). Target payload structures (such as High-Frequency Trading ticks) are mathematically derived at the CPU register level strictly on demand. This approach yields an asymptotic memory footprint approaching O(1) for logically boundless, structured datasets.

## Architecture

- **Zero-Copy Generative Stream**: Operates via direct binary casting using `core::slice::from_raw_parts_mut`. The generative engine bypasses traditional serialization (such as JSON or Protobuf) by writing raw C-struct byte arrays directly into the outbound network buffer.
- **SPSC Lock-Free Command Buffer**: Employs a 64-byte aligned RingBuffer to eliminate false sharing between the producer thread and the asynchronous consumer loop.
- **Thread-Per-Core Asynchronous Networking**: Designed around `io_uring` (via `monoio`) to bypass `epoll` overhead and handle socket read/write operations by yielding buffers to the kernel. Currently running a Tokio-based implementation for widespread host compatibility.
- **Hybrid Polling Mechanism**: Reduces aggressive OS context switching by spinning with `core::hint::spin_loop` before cooperatively yielding to the asynchronous executor.

## Benchmark Validation

A native Python stress client using `asyncio` and `aiohttp` evaluated the data-sink extraction capabilities of the system.

- **Payload**: Structured HFT `MarketTick` (24 bytes).
- **Scale**: 2,000,000 sequential ticks requested across 1,000 concurrent network tasks.
- **Result**: Complete ingestion achieved with strict mathematical sequentiality (zero data loss). The Python client executed C-level binary deserialization (`struct.iter_unpack('<Qdd')`) of 2 million ticks into a Pandas DataFrame in **215 milliseconds**. Total network transmission time concluded in 1.52 seconds at ~30 MB/s.

## Quickstart

### Prerequisites
- Docker & Docker Compose (for the Linux `io_uring` environment)
- Python 3.10+ (for the stress client)
- Rust 1.94+ (for local host compilation)

### Setup & Execution

1. **Start the Generative Network Layer**:
   Launch the Rust server to bind the HTTP generator on port 8081.
   ```bash
   cargo run --release
   ```

2. **Prepare the Data Sink Environment**:
   Install the required asynchronous and data manipulation libraries for the Python client.
   ```bash
   pip install aiohttp pandas
   ```

3. **Execute the Concurrent Stress Test**:
   Launch the client to request, deserialize, and validate the 2M ticks.
   ```bash
   python stress_client.py
   ```
