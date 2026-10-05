// Banco de estrés de Chronos-VFS: tres pruebas secuenciales contra el anillo, el
// `RingWriter` y el consumidor zstd. Sale con 0 si todas pasan, 1 si alguna falla y 2 si
// el vigilante detecta un bloqueo.
//
//   cargo run --release --bin stress_test -- [MiB de payload] [segundos de contrapresión]

use chronos_vfs::{CHUNK_SIZE, ZstdConfig, zstd_bridge};
use std::error::Error;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

type Outcome = Result<(), Box<dyn Error>>;

/// Tiempo máximo sin progreso antes de declarar un bloqueo.
const STALL_LIMIT: Duration = Duration::from_secs(60);

fn ensure(condition: bool, failure: impl FnOnce() -> String) -> Outcome {
    if condition { Ok(()) } else { Err(failure().into()) }
}

fn megabytes(bytes: usize) -> f64 {
    bytes as f64 / 1e6
}

// Texto pseudoaleatorio de 16 símbolos. Cada palabra de 8 bytes sale de splitmix64 sobre
// su índice: el contenido depende de la posición, así que un bloque perdido, repetido o
// desplazado cambia el hash.
fn build_payload(len: usize) -> Vec<u8> {
    let mut payload = vec![0u8; len];
    for (index, word) in payload.chunks_mut(8).enumerate() {
        let mut z = (index as u64).wrapping_add(1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let text = (z & 0x0F0F_0F0F_0F0F_0F0F) | 0x4040_4040_4040_4040;
        word.copy_from_slice(&text.to_le_bytes()[..word.len()]);
    }
    payload
}

const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;

fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    for &byte in bytes {
        hash = (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME);
    }
    hash
}

// Archivo temporal que se borra al salir de ámbito, pase o falle la prueba.
struct TempFile(PathBuf);

impl TempFile {
    fn new(name: &str) -> Self {
        Self(std::env::temp_dir().join(format!("chronos_stress_{}_{name}.zst", std::process::id())))
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

// Vigilante de bloqueos: si nadie llama a `beat` durante `STALL_LIMIT`, la prueba está
// colgada y no va a devolver el control; se informa y se aborta el proceso.
struct Watchdog {
    beats: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Watchdog {
    fn start(test: &'static str) -> Self {
        let beats = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let (seen_beats, seen_done) = (Arc::clone(&beats), Arc::clone(&done));
        let thread = thread::spawn(move || {
            let mut last = seen_beats.load(Ordering::Relaxed);
            let mut since = Instant::now();
            while !seen_done.load(Ordering::Relaxed) {
                thread::park_timeout(Duration::from_millis(250));
                let current = seen_beats.load(Ordering::Relaxed);
                if current != last {
                    last = current;
                    since = Instant::now();
                } else if since.elapsed() > STALL_LIMIT {
                    println!("      BLOQUEO en {test}: sin progreso durante {STALL_LIMIT:?}");
                    std::process::exit(2);
                }
            }
        });
        Self { beats, done, thread: Some(thread) }
    }

    #[inline]
    fn beat(&self) {
        self.beats.fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.done.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            let _ = thread.join();
        }
    }
}

// Tiempo de CPU (núcleo + usuario) consumido por el hilo que llama.
#[cfg(windows)]
fn thread_cpu_time() -> Option<Duration> {
    use std::ffi::c_void;

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct FileTime {
        low: u32,
        high: u32,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentThread() -> *mut c_void;
        fn GetThreadTimes(
            thread: *mut c_void,
            creation: *mut FileTime,
            exit: *mut FileTime,
            kernel: *mut FileTime,
            user: *mut FileTime,
        ) -> i32;
    }

    let mut times = [FileTime::default(); 4];
    let [creation, exit, kernel, user] = &mut times;
    // Safety: el pseudo-handle del hilo actual siempre es válido y los cuatro punteros
    // apuntan a estructuras FILETIME vivas.
    let ok = unsafe { GetThreadTimes(GetCurrentThread(), creation, exit, kernel, user) };
    if ok == 0 {
        return None;
    }
    // FILETIME cuenta intervalos de 100 ns.
    let ticks = |time: FileTime| (u64::from(time.high) << 32) | u64::from(time.low);
    Some(Duration::from_nanos((ticks(times[2]) + ticks(times[3])) * 100))
}

#[cfg(all(target_pointer_width = "64", any(target_os = "linux", target_os = "macos")))]
fn thread_cpu_time() -> Option<Duration> {
    use std::ffi::{c_int, c_long};

    #[repr(C)]
    struct Timespec {
        seconds: c_long,
        nanoseconds: c_long,
    }

    unsafe extern "C" {
        fn clock_gettime(clock: c_int, time: *mut Timespec) -> c_int;
    }

    const CLOCK_THREAD_CPUTIME_ID: c_int = if cfg!(target_os = "macos") { 16 } else { 3 };

    let mut time = Timespec { seconds: 0, nanoseconds: 0 };
    // Safety: `time` es un timespec vivo con el layout de la plataforma de 64 bits.
    if unsafe { clock_gettime(CLOCK_THREAD_CPUTIME_ID, &mut time) } != 0 {
        return None;
    }
    Some(Duration::new(time.seconds as u64, time.nanoseconds as u32))
}

#[cfg(not(any(
    windows,
    all(target_pointer_width = "64", any(target_os = "linux", target_os = "macos"))
)))]
fn thread_cpu_time() -> Option<Duration> {
    None
}

// Calibra la sonda de CPU con 200 ms de trabajo puro: si no los ve, una lectura de 0 %
// durante la contrapresión no probaría nada y el criterio de CPU no se aplica.
fn cpu_probe_is_live() -> bool {
    let Some(before) = thread_cpu_time() else {
        return false;
    };
    let start = Instant::now();
    let mut hash = FNV_OFFSET;
    while start.elapsed() < Duration::from_millis(200) {
        hash = std::hint::black_box(fnv1a(hash, &[0u8; 4096]));
    }
    thread_cpu_time().is_some_and(|after| after.saturating_sub(before) >= Duration::from_millis(100))
}

struct Decoded {
    bytes: usize,
    hash: u64,
    /// Primer desplazamiento en que el flujo descomprimido se aparta del esperado.
    first_difference: Option<usize>,
}

// Descomprime `path` al vuelo, sin materializarlo, y lo compara contra `expected`.
fn decode(path: &Path, expected: &[u8]) -> Result<Decoded, Box<dyn Error>> {
    let mut decoder = zstd::stream::read::Decoder::new(File::open(path)?)?;
    let mut buffer = vec![0u8; 1 << 20];
    let mut decoded = Decoded { bytes: 0, hash: FNV_OFFSET, first_difference: None };
    loop {
        let count = decoder.read(&mut buffer)?;
        if count == 0 {
            return Ok(decoded);
        }
        let got = &buffer[..count];
        decoded.hash = fnv1a(decoded.hash, got);
        if decoded.first_difference.is_none() {
            let reference = expected.get(decoded.bytes..).unwrap_or(&[]);
            let common = count.min(reference.len());
            decoded.first_difference = got[..common]
                .iter()
                .zip(reference)
                .position(|(a, b)| a != b)
                .or((common < count).then_some(common))
                .map(|position| decoded.bytes + position);
        }
        decoded.bytes += count;
    }
}

fn check_decoded(path: &Path, expected: &[u8]) -> Outcome {
    let expected_hash = fnv1a(FNV_OFFSET, expected);
    let decoded = decode(path, expected)?;
    println!(
        "      .zst descomprimido: {} bytes, fnv1a=0x{:016x} (esperado {} bytes, 0x{expected_hash:016x})",
        decoded.bytes,
        decoded.hash,
        expected.len(),
    );
    ensure(decoded.first_difference.is_none(), || {
        format!("el flujo difiere del original en el byte {}", decoded.first_difference.unwrap_or(0))
    })?;
    ensure(decoded.bytes == expected.len(), || {
        format!("se esperaban {} bytes y hay {}", expected.len(), decoded.bytes)
    })?;
    ensure(decoded.hash == expected_hash, || "el hash no coincide".into())
}

/// Tamaños de escritura: primos a ambos lados del bloque de 4 KiB, uno mayor que dos
/// bloques y uno mayor que el anillo entero.
const WRITE_SIZES: [usize; 10] = [1, 17, 4099, 3, 251, 8191, 7, 65537, 127, 4093];
/// Un `flush` cada tantas escrituras deja el `head` publicado fuera de frontera de bloque.
const FLUSH_EVERY: u64 = 13;

// 1. Integridad y envoltura: todo el payload en escrituras de tamaño primo y desalineado.
fn integrity(payload: &[u8], ring_capacity: usize) -> Outcome {
    let file = TempFile::new("integridad");
    let watchdog = Watchdog::start("integridad");
    let (mut writer, sink) = zstd_bridge(&file.0, ring_capacity, ZstdConfig::default())?;

    let start = Instant::now();
    let (mut offset, mut writes, mut wrapped, mut flushes) = (0usize, 0u64, 0u64, 0u64);
    while offset < payload.len() {
        let size = WRITE_SIZES[(writes % WRITE_SIZES.len() as u64) as usize];
        let end = (offset + size).min(payload.len());
        writer.write_all(&payload[offset..end])?;
        // La posición en el anillo es el desplazamiento del flujo módulo la capacidad.
        if offset / ring_capacity != (end - 1) / ring_capacity {
            wrapped += 1;
        }
        offset = end;
        writes += 1;
        if writes.is_multiple_of(FLUSH_EVERY) {
            writer.flush()?;
            flushes += 1;
        }
        watchdog.beat();
    }
    let stalls = writer.stalls();
    writer.close();
    watchdog.beat();
    let stats = sink.join()?;
    let secs = start.elapsed().as_secs_f64();
    drop(watchdog);

    println!(
        "      anillo de {} KiB: {writes} escrituras, {wrapped} partidas por la envoltura, \
         {} vueltas, {flushes} flush parciales, {stalls} esperas de contrapresión",
        ring_capacity >> 10,
        payload.len() / ring_capacity,
    );
    println!(
        "      {:.0} MB en {secs:.2} s = {:.0} MB/s, {:.0} MB en disco",
        megabytes(payload.len()),
        megabytes(payload.len()) / secs,
        megabytes(stats.compressed_bytes as usize),
    );
    ensure(stats.raw_bytes == payload.len() as u64, || {
        format!("el consumidor drenó {} bytes de {}", stats.raw_bytes, payload.len())
    })?;
    check_decoded(&file.0, payload)
}

/// Fracción máxima del tiempo de pared que el productor puede pasar en CPU estando frenado.
const MAX_PRODUCER_CPU: f64 = 0.10;

// 2. Contrapresión extrema: zstd -19 en un solo hilo contra un productor a toda velocidad.
// Con un anillo grande el productor duerme tramos largos; con el mínimo se frena y se
// reanuda cientos de veces por segundo.
fn backpressure(payload: &[u8], ring_capacity: usize, duration: Duration, probe_live: bool) -> Outcome {
    // Primo justo por debajo de 64 KiB: ninguna escritura cae alineada con el anillo.
    const BLOCK: usize = 65521;
    // `workers: 0` es un único hilo de compresión, el del consumidor, y además el caso más
    // duro: un worker de zstd aparte absorbería decenas de MiB antes de frenar al anillo.
    let config = ZstdConfig { level: 19, workers: 0, checksum: true };

    let file = TempFile::new("contrapresion");
    let watchdog = Watchdog::start("contrapresión");
    let (mut writer, sink) = zstd_bridge(&file.0, ring_capacity, config)?;

    let cpu_before = probe_live.then(thread_cpu_time).flatten();
    let start = Instant::now();
    let mut offset = 0usize;
    while start.elapsed() < duration && offset < payload.len() {
        let end = (offset + BLOCK).min(payload.len());
        writer.write_all(&payload[offset..end])?;
        offset = end;
        watchdog.beat();
    }
    let wall = start.elapsed();
    let cpu = cpu_before.zip(thread_cpu_time()).map(|(before, after)| after.saturating_sub(before));
    let stalls = writer.stalls();
    writer.close();
    watchdog.beat();
    let stats = sink.join()?;
    drop(watchdog);

    let sent = &payload[..offset];
    println!(
        "      zstd -19, 1 hilo, anillo de {} KiB: {:.1} MB en {:.2} s = {:.1} MB/s, \
         {stalls} esperas de contrapresión",
        ring_capacity >> 10,
        megabytes(sent.len()),
        wall.as_secs_f64(),
        megabytes(sent.len()) / wall.as_secs_f64(),
    );
    let cpu_share = cpu.map(|cpu| cpu.as_secs_f64() / wall.as_secs_f64());
    match (cpu, cpu_share) {
        (Some(cpu), Some(share)) => println!(
            "      CPU del hilo productor: {:.0} ms de {:.0} ms de pared ({:.2} %)",
            cpu.as_secs_f64() * 1e3,
            wall.as_secs_f64() * 1e3,
            share * 100.0,
        ),
        _ => println!("      CPU del hilo productor: no medible en esta plataforma, criterio omitido"),
    }

    ensure(stalls > 0, || "el productor nunca llegó a frenarse: la prueba no ejerció contrapresión".into())?;
    ensure(stats.raw_bytes == sent.len() as u64, || {
        format!("el consumidor drenó {} bytes de {}", stats.raw_bytes, sent.len())
    })?;
    if let Some(share) = cpu_share {
        ensure(share <= MAX_PRODUCER_CPU, || {
            format!("el productor quemó CPU mientras esperaba: {:.1} % del tiempo de pared", share * 100.0)
        })?;
    }
    check_decoded(&file.0, sent)
}

// 3. Flush parcial: 3 bytes, muy por debajo del bloque, deben llegar al disco al cerrar.
fn partial_flush() -> Outcome {
    const BYTES: [u8; 3] = [0xC0, 0xFF, 0xEE];
    for by_drop in [false, true] {
        let label = if by_drop { "drop()" } else { "close()" };
        let file = TempFile::new("parcial");
        let watchdog = Watchdog::start("flush parcial");
        let (mut writer, sink) = zstd_bridge(&file.0, 2 * CHUNK_SIZE, ZstdConfig::default())?;
        writer.write_all(&BYTES)?;
        if by_drop {
            drop(writer);
        } else {
            writer.close();
        }
        let stats = sink.join()?;
        drop(watchdog);

        let content = zstd::stream::decode_all(File::open(&file.0)?)?;
        println!(
            "      {label} + join(): {} bytes drenados, .zst de {} bytes, contenido {content:02x?}",
            stats.raw_bytes,
            fs::metadata(&file.0)?.len(),
        );
        ensure(stats.raw_bytes == 3, || format!("{label}: el consumidor drenó {} bytes", stats.raw_bytes))?;
        ensure(content == BYTES, || format!("{label}: el archivo contiene {content:02x?}"))?;
    }
    Ok(())
}

fn main() -> ExitCode {
    // Por debajo de esto el payload cabría en los buffers y no habría contrapresión.
    const MIN_PAYLOAD_MIB: usize = 8;
    const MINIMAL_RING: usize = 2 * CHUNK_SIZE;

    let mut args = std::env::args().skip(1).map(|arg| arg.parse::<usize>());
    let (payload_mib, seconds) = match (args.next().unwrap_or(Ok(500)), args.next().unwrap_or(Ok(10))) {
        (Ok(payload_mib), Ok(seconds)) if payload_mib >= MIN_PAYLOAD_MIB => (payload_mib, seconds as u64),
        _ => {
            eprintln!(
                "Uso: stress_test [MiB de payload, mínimo {MIN_PAYLOAD_MIB}] [segundos de contrapresión por anillo]"
            );
            return ExitCode::from(64);
        }
    };

    let start = Instant::now();
    let payload = build_payload(payload_mib << 20);
    println!(
        "payload: {payload_mib} MiB, fnv1a=0x{:016x}, generado en {:.2} s",
        fnv1a(FNV_OFFSET, &payload),
        start.elapsed().as_secs_f64(),
    );
    let probe_live = cpu_probe_is_live();
    println!("sonda de CPU por hilo: {}", if probe_live { "calibrada" } else { "no disponible" });

    // Se recorta un número primo de bytes para que el flujo no termine en frontera de
    // bloque: el cierre tiene que publicar un resto parcial también tras un flujo largo.
    // El anillo mínimo (dos bloques) es el caso más hostil para la envoltura; recibe una
    // fracción del payload porque cada vuelta cuesta un relevo entre hilos.
    let whole = &payload[..payload.len() - 1021];
    let minimal_share = &payload[..payload.len() / 8 - 4093];
    let duration = Duration::from_secs(seconds);
    let tests: [(&str, &dyn Fn() -> Outcome); 5] = [
        ("1. integridad y envoltura", &|| integrity(whole, 64 << 10)),
        ("1. integridad y envoltura, anillo mínimo", &|| integrity(minimal_share, MINIMAL_RING)),
        ("2. contrapresión extrema", &|| backpressure(&payload, 1 << 20, duration, probe_live)),
        ("2. contrapresión extrema, anillo mínimo", &|| {
            backpressure(&payload, MINIMAL_RING, duration, probe_live)
        }),
        ("3. flush parcial", &partial_flush),
    ];

    let mut failures = 0;
    for (name, test) in tests {
        println!("[{name}]");
        match test() {
            Ok(()) => println!("      OK"),
            Err(error) => {
                println!("      FALLO: {error}");
                failures += 1;
            }
        }
    }
    println!("{} de {} pruebas superadas", tests.len() - failures, tests.len());
    if failures == 0 { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}
