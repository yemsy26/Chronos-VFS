//! Chronos-VFS v2: puente de I/O en RAM entre un productor de trazas y el disco.
//!
//! ```no_run
//! use std::io::Write;
//!
//! let (mut proof, sink) = chronos_vfs::zstd_bridge(
//!     "proof.pbp.zst",
//!     256 << 20,
//!     chronos_vfs::ZstdConfig::default(),
//! )?;
//! writeln!(proof, "pseudo-Boolean proof version 3.0")?;
//! proof.close();
//! let stats = sink.join()?;
//! println!("{} -> {} bytes", stats.raw_bytes, stats.compressed_bytes);
//! # Ok::<(), std::io::Error>(())
//! ```

pub mod consumer;
pub mod nvm_core;
pub mod writer;

pub use consumer::{SinkStats, ZstdConfig, ZstdSink, drain_to_zstd};
pub use nvm_core::{CHUNK_SIZE, Consumer, Disconnected, Producer, RingBuffer};
pub use writer::RingWriter;

use std::io;
use std::path::Path;

/// Monta el puente completo: un anillo de `ring_capacity` bytes (potencia de dos), su
/// [`RingWriter`] para el productor y el hilo consumidor comprimiendo hacia `path`.
pub fn zstd_bridge(
    path: impl AsRef<Path>,
    ring_capacity: usize,
    config: ZstdConfig,
) -> io::Result<(RingWriter, ZstdSink)> {
    let (producer, consumer) = RingBuffer::with_capacity(ring_capacity);
    let sink = ZstdSink::spawn(consumer, path, config)?;
    Ok((RingWriter::new(producer), sink))
}
