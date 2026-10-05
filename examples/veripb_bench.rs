// Mide el puente con una traza sintética con forma de VeriPB emitida vía `writeln!`.
//
//   cargo run --release --example veripb_bench -- [MiB de traza] [MiB de anillo] [nivel zstd] [workers]
//
// Cinco pasadas: dos referencias sin anillo (el formateo solo y un `BufWriter` que no
// escribe a ningún sitio), el anillo con un consumidor que descarta (con `writeln!` y con
// bloques crudos de 4 KiB) y el puente completo a un .zst en el directorio temporal, que
// se borra al terminar.

use chronos_vfs::{CHUNK_SIZE, Consumer, RingBuffer, RingWriter, ZstdConfig, ZstdSink};
use std::io::{self, BufWriter, Write};
use std::thread;
use std::time::Instant;

const LINE_BYTES: usize = 39; // Longitud media de una línea sintética.

fn emit_trace(out: &mut impl Write, total_bytes: usize) -> io::Result<()> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    writeln!(out, "pseudo-Boolean proof version 3.0")?;
    for _ in 0..total_bytes / LINE_BYTES {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let (a, b, c) = (state % 100_000, (state >> 20) % 100_000, (state >> 40) % 100_000);
        writeln!(out, "rup 1 x{a} 1 ~x{b} 1 x{c} >= 1 ;")?;
    }
    Ok(())
}

fn emit_blocks(out: &mut impl Write, total_bytes: usize) -> io::Result<()> {
    let block = [0xABu8; CHUNK_SIZE];
    for _ in 0..total_bytes / CHUNK_SIZE {
        out.write_all(&block)?;
    }
    Ok(())
}

// Destino nulo que el optimizador no puede eliminar: mide solo el coste de formatear.
struct BlackHole(u64);

impl Write for BlackHole {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += std::hint::black_box(buf).len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn discard(mut consumer: Consumer) -> u64 {
    let mut total = 0u64;
    while consumer.wait_for_data() {
        let (front, back) = consumer.readable();
        let count = front.len() + back.len();
        consumer.release(count);
        total += count as u64;
    }
    total
}

fn megabytes(bytes: u64) -> f64 {
    bytes as f64 / 1e6
}

fn ring_only(
    label: &str,
    ring_bytes: usize,
    feed: impl FnOnce(&mut RingWriter) -> io::Result<()>,
) -> io::Result<()> {
    let (producer, consumer) = RingBuffer::with_capacity(ring_bytes);
    let drain = thread::spawn(move || discard(consumer));
    let mut proof = RingWriter::new(producer);
    let start = Instant::now();
    feed(&mut proof)?;
    let stalls = proof.stalls();
    proof.close();
    let raw = drain.join().expect("el consumidor no entra en pánico");
    let secs = start.elapsed().as_secs_f64();
    println!(
        "{label} {:>6.0} MB en {secs:>5.2} s = {:>6.0} MB/s, {stalls} esperas de contrapresión",
        megabytes(raw),
        megabytes(raw) / secs,
    );
    Ok(())
}

fn main() -> io::Result<()> {
    let mut args = std::env::args().skip(1).map(|arg| arg.parse::<usize>().expect("argumento numérico"));
    let trace_bytes = args.next().unwrap_or(1024) << 20;
    let ring_bytes = args.next().unwrap_or(64) << 20;
    let level = args.next().unwrap_or(3) as i32;
    let workers = args.next().unwrap_or(0) as u32;

    let mut hole = BlackHole(0);
    let start = Instant::now();
    emit_trace(&mut hole, trace_bytes)?;
    let secs = start.elapsed().as_secs_f64();
    println!(
        "formateo solo (sin copia):   {:>6.0} MB en {secs:>5.2} s = {:>6.0} MB/s",
        megabytes(hole.0),
        megabytes(hole.0) / secs,
    );

    let mut buffered = BufWriter::with_capacity(1 << 20, BlackHole(0));
    let start = Instant::now();
    emit_trace(&mut buffered, trace_bytes)?;
    let hole = buffered.into_inner().map_err(io::IntoInnerError::into_error)?;
    let secs = start.elapsed().as_secs_f64();
    println!(
        "BufWriter de 1 MiB a la nada:{:>6.0} MB en {secs:>5.2} s = {:>6.0} MB/s",
        megabytes(hole.0),
        megabytes(hole.0) / secs,
    );

    ring_only("anillo, writeln!:           ", ring_bytes, |proof| emit_trace(proof, trace_bytes))?;
    ring_only("anillo, bloques de 4 KiB:   ", ring_bytes, |proof| emit_blocks(proof, trace_bytes))?;

    let path = std::env::temp_dir().join("chronos_vfs_bench.pbp.zst");
    let (producer, consumer) = RingBuffer::with_capacity(ring_bytes);
    let sink = ZstdSink::spawn(consumer, &path, ZstdConfig { level, workers, checksum: true })?;
    let mut proof = RingWriter::new(producer);
    let start = Instant::now();
    emit_trace(&mut proof, trace_bytes)?;
    let stalls = proof.stalls();
    proof.close();
    let stats = sink.join()?;
    let secs = start.elapsed().as_secs_f64();
    println!(
        "anillo + zstd -{level} ({workers} workers): {:>6.0} MB en {secs:>5.2} s = {:>6.0} MB/s, \
         {stalls} esperas de contrapresión, {:.0} MB en disco ({:.1}x)",
        megabytes(stats.raw_bytes),
        megabytes(stats.raw_bytes) / secs,
        megabytes(stats.compressed_bytes),
        stats.raw_bytes as f64 / stats.compressed_bytes.max(1) as f64,
    );
    std::fs::remove_file(&path)
}
