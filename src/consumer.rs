//! Hilo consumidor: drena el anillo, comprime con zstd al vuelo y vuelca a disco.

use crate::nvm_core::{CHUNK_SIZE, Consumer};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::thread::{self, JoinHandle};

/// Tope de bytes por pasada de compresión. Aunque el anillo esté lleno, el espacio se
/// devuelve al productor cada `DRAIN_BATCH` bytes y no al terminar de drenarlo entero.
const DRAIN_BATCH: usize = 64 * CHUNK_SIZE;
/// Buffer de salida hacia el archivo: agrupa los bloques comprimidos en escrituras grandes.
const OUT_BUFFER: usize = 1 << 20;

#[derive(Clone, Copy, Debug)]
pub struct ZstdConfig {
    /// Nivel de compresión zstd (1 = más rápido, 3 = por defecto, hasta 22).
    pub level: i32,
    /// Hilos de compresión adicionales. Con 0 se comprime en el propio hilo consumidor;
    /// cada worker añade sus propios buffers de trabajo a la memoria del compresor.
    pub workers: u32,
    /// Añade al frame un checksum XXH64 del contenido original.
    pub checksum: bool,
}

impl Default for ZstdConfig {
    fn default() -> Self {
        Self {
            level: zstd::DEFAULT_COMPRESSION_LEVEL,
            workers: 0,
            checksum: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SinkStats {
    /// Bytes drenados del anillo.
    pub raw_bytes: u64,
    /// Bytes entregados al destino tras comprimir.
    pub compressed_bytes: u64,
}

struct CountingWriter<W> {
    inner: W,
    written: u64,
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let count = self.inner.write(buf)?;
        self.written += count as u64;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Lazo del consumidor. Drena `consumer` hasta que el productor cierre, alimentando al
/// compresor directamente desde la memoria del anillo, y cierra el frame zstd sobre `out`.
///
/// No hay copia intermedia del flujo: la RAM es el anillo más el estado de tamaño fijo
/// del compresor. Si falla, el `Consumer` se suelta y el productor recibe `BrokenPipe`.
pub fn drain_to_zstd<W: Write>(
    mut consumer: Consumer,
    out: W,
    config: ZstdConfig,
) -> io::Result<(W, SinkStats)> {
    let out = CountingWriter { inner: out, written: 0 };
    let mut encoder = zstd::stream::write::Encoder::new(out, config.level)?;
    encoder.include_checksum(config.checksum)?;
    if config.workers > 0 {
        encoder.multithread(config.workers)?;
    }

    let mut raw_bytes = 0u64;
    while consumer.wait_for_data() {
        let (front, back) = consumer.readable();
        let front = &front[..front.len().min(DRAIN_BATCH)];
        let back = &back[..back.len().min(DRAIN_BATCH - front.len())];
        encoder.write_all(front)?;
        encoder.write_all(back)?;
        let count = front.len() + back.len();
        consumer.release(count);
        raw_bytes += count as u64;
    }

    let out = encoder.finish()?;
    let stats = SinkStats {
        raw_bytes,
        compressed_bytes: out.written,
    };
    Ok((out.inner, stats))
}

/// Hilo consumidor que comprime el anillo hacia un archivo `.zst`.
pub struct ZstdSink {
    thread: JoinHandle<io::Result<SinkStats>>,
}

impl ZstdSink {
    /// Crea (o trunca) `path` y lanza el hilo que drena `consumer` sobre él.
    pub fn spawn(
        consumer: Consumer,
        path: impl AsRef<Path>,
        config: ZstdConfig,
    ) -> io::Result<Self> {
        let file = File::create(path)?;
        let thread = thread::Builder::new()
            .name("chronos-zstd".into())
            .spawn(move || {
                let out = BufWriter::with_capacity(OUT_BUFFER, file);
                let (out, stats) = drain_to_zstd(consumer, out, config)?;
                let file = out.into_inner().map_err(io::IntoInnerError::into_error)?;
                file.sync_all()?;
                Ok(stats)
            })?;
        Ok(Self { thread })
    }

    /// Espera a que el productor cierre y el archivo quede completo y sincronizado.
    /// Debe llamarse después de soltar el `RingWriter`; hasta entonces bloquea.
    pub fn join(self) -> io::Result<SinkStats> {
        self.thread
            .join()
            .unwrap_or_else(|_| Err(io::Error::other("el hilo consumidor entró en pánico")))
    }
}
