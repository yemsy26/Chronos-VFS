use chronos_vfs::{CHUNK_SIZE, RingBuffer, RingWriter, ZstdConfig, drain_to_zstd, zstd_bridge};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::PathBuf;
use std::thread;

// Elimina el archivo temporal aunque el test falle.
struct TempFile(PathBuf);

impl TempFile {
    fn new(name: &str) -> Self {
        Self(std::env::temp_dir().join(format!("chronos_vfs_{}_{name}.zst", std::process::id())))
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn trace_roundtrips_through_a_small_ring() {
    let file = TempFile::new("roundtrip");
    let (mut proof, sink) = zstd_bridge(&file.0, 4 * CHUNK_SIZE, ZstdConfig::default()).unwrap();

    let mut expected = Vec::new();
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    writeln!(proof, "pseudo-Boolean proof version 3.0").unwrap();
    writeln!(expected, "pseudo-Boolean proof version 3.0").unwrap();
    for _ in 0..200_000 {
        let random = xorshift(&mut state);
        let (a, b, c) = (random % 5000, (random >> 20) % 5000, (random >> 40) % 5000);
        writeln!(proof, "rup 1 x{a} 1 ~x{b} 1 x{c} >= 1 ;").unwrap();
        writeln!(expected, "rup 1 x{a} 1 ~x{b} 1 x{c} >= 1 ;").unwrap();
    }
    // Una escritura mayor que el anillo entero también debe pasar íntegra.
    let blob: Vec<u8> = (0..10 * CHUNK_SIZE).map(|i| (i % 251) as u8).collect();
    proof.write_all(&blob).unwrap();
    expected.extend_from_slice(&blob);

    proof.close();
    let stats = sink.join().unwrap();

    assert_eq!(stats.raw_bytes, expected.len() as u64);
    assert_eq!(stats.compressed_bytes, fs::metadata(&file.0).unwrap().len());
    assert!(stats.compressed_bytes < stats.raw_bytes);
    let decoded = zstd::stream::decode_all(File::open(&file.0).unwrap()).unwrap();
    assert!(decoded == expected, "el contenido descomprimido difiere del emitido");
}

#[test]
fn multithreaded_compression_roundtrips() {
    let file = TempFile::new("workers");
    let config = ZstdConfig { level: 1, workers: 2, checksum: true };
    let (mut proof, sink) = zstd_bridge(&file.0, 64 * CHUNK_SIZE, config).unwrap();

    let mut expected = Vec::new();
    for line in 0..300_000u32 {
        writeln!(proof, "red 1 x{} 1 x{} >= 1 ; x{} -> 1", line % 977, line % 3301, line).unwrap();
        writeln!(expected, "red 1 x{} 1 x{} >= 1 ; x{} -> 1", line % 977, line % 3301, line).unwrap();
    }
    drop(proof);
    let stats = sink.join().unwrap();

    assert_eq!(stats.raw_bytes, expected.len() as u64);
    let decoded = zstd::stream::decode_all(File::open(&file.0).unwrap()).unwrap();
    assert!(decoded == expected, "el contenido descomprimido difiere del emitido");
}

#[test]
fn empty_stream_is_a_valid_frame() {
    let file = TempFile::new("empty");
    let (proof, sink) = zstd_bridge(&file.0, 2 * CHUNK_SIZE, ZstdConfig::default()).unwrap();
    proof.close();
    let stats = sink.join().unwrap();

    assert_eq!(stats.raw_bytes, 0);
    let decoded = zstd::stream::decode_all(File::open(&file.0).unwrap()).unwrap();
    assert!(decoded.is_empty());
}

#[test]
fn flush_exposes_the_partial_chunk() {
    let (producer, mut consumer) = RingBuffer::with_capacity(2 * CHUNK_SIZE);
    let mut writer = RingWriter::new(producer);

    write!(writer, "rup 1 x1 >= 1 ;").unwrap();
    assert!(consumer.readable().0.is_empty());
    writer.flush().unwrap();
    assert_eq!(consumer.readable().0, b"rup 1 x1 >= 1 ;");
    consumer.release(15);
    assert_eq!(writer.stalls(), 0);
}

// Destino que se queda sin espacio tras `budget` bytes, como un disco lleno.
struct FullDisk {
    budget: usize,
}

impl Write for FullDisk {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() > self.budget {
            return Err(io::Error::new(io::ErrorKind::StorageFull, "disco lleno"));
        }
        self.budget -= buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn sink_failure_reaches_the_producer_instead_of_hanging() {
    let (producer, consumer) = RingBuffer::with_capacity(4 * CHUNK_SIZE);
    let sink = thread::spawn(move || {
        drain_to_zstd(consumer, FullDisk { budget: 64 << 10 }, ZstdConfig::default()).map(|_| ())
    });

    // Datos incompresibles: la salida crece al ritmo de la entrada y agota el destino.
    let mut writer = RingWriter::new(producer);
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut block = [0u8; 1024];
    let error = loop {
        for chunk in block.chunks_exact_mut(8) {
            chunk.copy_from_slice(&xorshift(&mut state).to_le_bytes());
        }
        if let Err(error) = writer.write_all(&block) {
            break error;
        }
    };

    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(sink.join().unwrap().unwrap_err().kind(), io::ErrorKind::StorageFull);
}
