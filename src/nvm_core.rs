//! Anillo SPSC lock-free de bytes, orientado a bloques.
//!
//! El productor copia con `memcpy` directamente sobre la memoria del anillo y publica su
//! avance en fronteras de [`CHUNK_SIZE`]: una operación atómica por bloque de 4 KiB, no por
//! byte. El consumidor lee la región publicada in situ y devuelve el espacio por lotes.
//! La memoria es fija: `capacity` bytes reservados y residentes desde la construcción;
//! cuando el anillo se llena, el productor espera (contrapresión) en vez de crecer.

use core::hint::spin_loop;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::alloc::{self, Layout};
use std::fmt;
use std::io;
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread;
use std::time::Duration;

/// Granularidad de publicación del productor y alineación del buffer.
pub const CHUNK_SIZE: usize = 4096;

/// Sondeo híbrido: primero `spin_loop`, luego ceder el turno, y solo entonces dormir.
const SPIN_LIMIT: u32 = 128;
const YIELD_LIMIT: u32 = 32;
/// Siesta máxima del consumidor con el anillo vacío antes de volver a mirar `head`.
const POLL_INTERVAL: Duration = Duration::from_millis(1);

#[inline]
fn backoff(round: u32) {
    if round < SPIN_LIMIT {
        spin_loop();
    } else {
        thread::yield_now();
    }
}

/// Alineación a 64 bytes para corresponder a la línea de caché típica
/// y prevenir 'false sharing' mutuo entre el productor y el consumidor.
#[repr(align(64))]
struct CacheAligned<T>(T);

/// Estacionamiento del lado que se queda sin trabajo. Quien espera publica `waiting` y
/// revalida su condición; quien despierta publica su índice y después lee `waiting`.
/// Con ambos pares store/load en SeqCst al menos uno ve al otro: no se pierde el despertar.
struct Signal {
    waiting: AtomicBool,
    lock: Mutex<()>,
    condvar: Condvar,
}

impl Signal {
    fn new() -> Self {
        Self {
            waiting: AtomicBool::new(false),
            lock: Mutex::new(()),
            condvar: Condvar::new(),
        }
    }

    #[inline]
    fn notify(&self) {
        if self.waiting.load(Ordering::SeqCst) {
            self.wake();
        }
    }

    #[cold]
    fn wake(&self) {
        // Tomar el candado serializa con la revalidación de quien espera.
        let _guard = self.lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.condvar.notify_one();
    }

    /// Duerme hasta que `ready` sea cierto. Con `poll`, además revalida a ese intervalo.
    #[cold]
    fn wait_until(&self, poll: Option<Duration>, mut ready: impl FnMut() -> bool) {
        let mut guard = self.lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.waiting.store(true, Ordering::SeqCst);
        while !ready() {
            guard = match poll {
                Some(interval) => {
                    self.condvar
                        .wait_timeout(guard, interval)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0
                }
                None => self.condvar.wait(guard).unwrap_or_else(PoisonError::into_inner),
            };
        }
        self.waiting.store(false, Ordering::SeqCst);
    }
}

/// Banderas de cierre, en su propia línea: se leen en caliente y se escriben una sola vez.
struct Hangup {
    producer_closed: AtomicBool,
    consumer_closed: AtomicBool,
}

/// El otro extremo del anillo ya no existe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Disconnected;

impl fmt::Display for Disconnected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("el consumidor del anillo terminó")
    }
}

impl std::error::Error for Disconnected {}

impl From<Disconnected> for io::Error {
    fn from(error: Disconnected) -> Self {
        io::Error::new(io::ErrorKind::BrokenPipe, error)
    }
}

/// Buffer SPSC (Single Producer Single Consumer) lock-free de bytes. Solo se opera a través
/// de sus dos extremos, [`Producer`] y [`Consumer`], que garantizan el SPSC por tipos.
pub struct RingBuffer {
    buffer: NonNull<u8>,
    capacity: usize,
    /// Posición del productor, escrita por él, leída por el consumidor.
    head: CacheAligned<AtomicUsize>,
    /// Posición del consumidor, escrita por él, leída por el productor.
    tail: CacheAligned<AtomicUsize>,
    /// Donde duerme el consumidor con el anillo vacío.
    data_ready: CacheAligned<Signal>,
    /// Donde duerme el productor con el anillo lleno.
    space_ready: CacheAligned<Signal>,
    hangup: CacheAligned<Hangup>,
}

// Safety: `buffer` es propiedad exclusiva del anillo. Cada byte lo escribe solo el productor
// y lo lee solo el consumidor, y el traspaso se sincroniza mediante `head` y `tail`.
unsafe impl Send for RingBuffer {}
unsafe impl Sync for RingBuffer {}

impl RingBuffer {
    /// Reserva un anillo de `capacity` bytes y devuelve sus dos extremos.
    pub fn with_capacity(capacity: usize) -> (Producer, Consumer) {
        // Exigir potencias de 2 asegura un enmascaramiento ultrarrápido (módulo).
        assert!(capacity.is_power_of_two(), "Capacity must be a power of two");
        assert!(capacity >= 2 * CHUNK_SIZE, "Capacity must hold at least two chunks");
        let layout = Self::layout(capacity);
        // Safety: el layout tiene tamaño no nulo.
        let raw = unsafe { alloc::alloc(layout) };
        let Some(buffer) = NonNull::new(raw) else {
            alloc::handle_alloc_error(layout)
        };
        // Poner a cero inicializa la región y deja residentes todas sus páginas: el coste
        // de los fallos de página se paga aquí y no durante la primera vuelta del anillo.
        // Safety: `buffer` apunta a `capacity` bytes recién reservados.
        unsafe { ptr::write_bytes(buffer.as_ptr(), 0, capacity) };

        let ring = Arc::new(Self {
            buffer,
            capacity,
            head: CacheAligned(AtomicUsize::new(0)),
            tail: CacheAligned(AtomicUsize::new(0)),
            data_ready: CacheAligned(Signal::new()),
            space_ready: CacheAligned(Signal::new()),
            hangup: CacheAligned(Hangup {
                producer_closed: AtomicBool::new(false),
                consumer_closed: AtomicBool::new(false),
            }),
        });
        let producer = Producer {
            ring: Arc::clone(&ring),
            cursor: buffer.as_ptr(),
            room: CHUNK_SIZE,
            write_pos: 0,
            published: 0,
            cached_tail: 0,
        };
        (producer, Consumer { ring, tail: 0 })
    }

    fn layout(capacity: usize) -> Layout {
        Layout::from_size_align(capacity, CHUNK_SIZE).expect("Capacity overflows the address space")
    }
}

impl Drop for RingBuffer {
    fn drop(&mut self) {
        // Safety: `buffer` proviene de `alloc` con este mismo layout.
        unsafe { alloc::dealloc(self.buffer.as_ptr(), Self::layout(self.capacity)) }
    }
}

/// Extremo productor. Hay exactamente uno por anillo.
pub struct Producer {
    ring: Arc<RingBuffer>,
    /// Ventana de escritura: `room` bytes contiguos a partir de `cursor` que están libres
    /// y no pasan del bloque en curso. Mientras un push quepa en ella es un memcpy puro.
    cursor: *mut u8,
    room: usize,
    /// Posición de escritura local: incluye lo copiado que aún no se ha publicado.
    write_pos: usize,
    /// Último `head` publicado.
    published: usize,
    /// Copia local de `tail`: solo se refresca cuando el espacio aparente no alcanza.
    cached_tail: usize,
}

// Safety: `cursor` apunta dentro del buffer que `ring` mantiene vivo, y solo se usa con
// `&mut self`; el productor puede migrar de hilo, no compartirse.
unsafe impl Send for Producer {}

impl Producer {
    pub fn capacity(&self) -> usize {
        self.ring.capacity
    }

    /// Bytes copiados al anillo que el consumidor todavía no puede ver.
    #[inline]
    pub fn pending(&self) -> usize {
        self.write_pos.wrapping_sub(self.published)
    }

    #[inline]
    pub fn is_disconnected(&self) -> bool {
        self.ring.hangup.0.consumer_closed.load(Ordering::Relaxed)
    }

    /// Copia en el anillo tanto de `src` como quepa (memcpy, a lo sumo dos tramos si
    /// envuelve) y publica el avance al cruzar fronteras de [`CHUNK_SIZE`]. No bloquea.
    /// Retorna la cantidad de bytes copiados; el resto parcial queda a la espera del
    /// siguiente bloque completo o de [`flush`](Self::flush). Si el consumidor terminó
    /// deja de aceptar datos en la siguiente frontera de bloque.
    #[inline]
    pub fn push_slice(&mut self, src: &[u8]) -> usize {
        if src.len() < self.room {
            // Safety: la ventana garantiza `room` bytes libres y dentro del buffer.
            unsafe {
                ptr::copy_nonoverlapping(src.as_ptr(), self.cursor, src.len());
                self.cursor = self.cursor.add(src.len());
            }
            self.room -= src.len();
            self.write_pos = self.write_pos.wrapping_add(src.len());
            return src.len();
        }
        self.push_across_chunks(src)
    }

    /// Ruta general: el push agota la ventana. Copia lo que quepa en el espacio libre,
    /// publica los bloques completados y abre la ventana siguiente.
    #[inline(never)]
    fn push_across_chunks(&mut self, src: &[u8]) -> usize {
        let ring = &*self.ring;
        if ring.hangup.0.consumer_closed.load(Ordering::Relaxed) {
            return 0;
        }
        let capacity = ring.capacity;
        let mut free = capacity - self.write_pos.wrapping_sub(self.cached_tail);
        if free < src.len() {
            // Acquire para ver el progreso real del consumidor
            self.cached_tail = ring.tail.0.load(Ordering::Acquire);
            free = capacity - self.write_pos.wrapping_sub(self.cached_tail);
        }
        let count = src.len().min(free);
        let index = self.write_pos & (capacity - 1);
        let first = count.min(capacity - index);
        let base = ring.buffer.as_ptr();
        // Safety: `count <= free`, así que los bytes destino quedan fuera de la región
        // publicada [tail, head) y el consumidor no los toca; `index + first <= capacity`
        // y `count - first <= index`, de modo que ambos tramos caen dentro del buffer.
        unsafe {
            ptr::copy_nonoverlapping(src.as_ptr(), base.add(index), first);
            if count > first {
                ptr::copy_nonoverlapping(src.as_ptr().add(first), base, count - first);
            }
        }
        self.write_pos = self.write_pos.wrapping_add(count);

        let partial = self.write_pos & (CHUNK_SIZE - 1);
        if self.pending() > partial {
            self.publish_chunks(self.write_pos.wrapping_sub(partial));
        }

        // `capacity` es múltiplo de `CHUNK_SIZE` y el buffer está alineado a él: lo que
        // resta del bloque en curso es contiguo y no pasa del final del buffer.
        self.room = (CHUNK_SIZE - partial).min(free - count);
        // Safety: el índice enmascarado es menor que `capacity`.
        self.cursor = unsafe { base.add(self.write_pos & (capacity - 1)) };
        count
    }

    #[inline]
    fn publish_chunks(&mut self, head: usize) {
        let ring = &*self.ring;
        self.published = head;
        // Release para hacer visible el commit de la data al consumidor
        ring.head.0.store(head, Ordering::Release);
        // Sin syscalls en régimen: el consumidor sondea por su cuenta y solo se le
        // despierta si el atraso real ya ocupa medio anillo.
        if head.wrapping_sub(self.cached_tail) >= ring.capacity / 2 {
            self.cached_tail = ring.tail.0.load(Ordering::Acquire);
            if head.wrapping_sub(self.cached_tail) >= ring.capacity / 2 {
                ring.data_ready.0.notify();
            }
        }
    }

    /// Publica todo lo copiado, incluido el bloque parcial.
    #[inline]
    pub fn flush(&mut self) {
        if self.published != self.write_pos {
            self.published = self.write_pos;
            self.ring.head.0.store(self.write_pos, Ordering::Release);
        }
    }

    /// Contrapresión: bloquea hasta que haya al menos `want` bytes libres (acotado a lo
    /// que el consumidor puede llegar a liberar). Falla si el consumidor terminó.
    pub fn wait_for_space(&mut self, want: usize) -> Result<(), Disconnected> {
        let ring = &*self.ring;
        let write_pos = self.write_pos;
        let want = want.min(ring.capacity - self.pending());
        let free = |tail: usize| ring.capacity - write_pos.wrapping_sub(tail);

        for round in 0..SPIN_LIMIT + YIELD_LIMIT {
            if ring.hangup.0.consumer_closed.load(Ordering::Acquire) {
                return Err(Disconnected);
            }
            let tail = ring.tail.0.load(Ordering::Acquire);
            if free(tail) >= want {
                self.cached_tail = tail;
                return Ok(());
            }
            backoff(round);
        }

        let mut tail = self.cached_tail;
        let mut closed = false;
        ring.space_ready.0.wait_until(None, || {
            closed = ring.hangup.0.consumer_closed.load(Ordering::SeqCst);
            tail = ring.tail.0.load(Ordering::SeqCst);
            closed || free(tail) >= want
        });
        self.cached_tail = tail;
        if closed { Err(Disconnected) } else { Ok(()) }
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        let ring = &*self.ring;
        self.published = self.write_pos;
        ring.head.0.store(self.write_pos, Ordering::SeqCst);
        ring.hangup.0.producer_closed.store(true, Ordering::SeqCst);
        ring.data_ready.0.notify();
    }
}

/// Extremo consumidor. Hay exactamente uno por anillo.
pub struct Consumer {
    ring: Arc<RingBuffer>,
    tail: usize,
}

impl Consumer {
    pub fn capacity(&self) -> usize {
        self.ring.capacity
    }

    /// Región publicada, in situ, como dos tramos contiguos; el segundo solo tiene datos
    /// cuando la región envuelve el final del buffer. No bloquea.
    #[inline]
    pub fn readable(&self) -> (&[u8], &[u8]) {
        let ring = &*self.ring;
        // Acquire para ver el progreso real del productor
        let head = ring.head.0.load(Ordering::Acquire);
        let len = head.wrapping_sub(self.tail);
        let index = self.tail & (ring.capacity - 1);
        let first = len.min(ring.capacity - index);
        // Safety: [tail, head) ya fue escrita y publicada por el productor, que no vuelve
        // a tocarla hasta que `release` (que exige `&mut self`) avance `tail`.
        unsafe {
            let base = ring.buffer.as_ptr() as *const u8;
            (
                slice::from_raw_parts(base.add(index), first),
                slice::from_raw_parts(base, len - first),
            )
        }
    }

    /// Devuelve al productor los primeros `count` bytes de la región publicada.
    #[inline]
    pub fn release(&mut self, count: usize) {
        let ring = &*self.ring;
        let head = ring.head.0.load(Ordering::Acquire);
        // Pasar de `head` entregaría al productor memoria que aún no existe como libre.
        assert!(count <= head.wrapping_sub(self.tail), "Released more than was published");
        self.tail = self.tail.wrapping_add(count);
        ring.tail.0.store(self.tail, Ordering::SeqCst);
        ring.space_ready.0.notify();
    }

    /// Extrae hasta `dst.len()` bytes copiándolos fuera del anillo (memcpy). No bloquea.
    pub fn pop_slice(&mut self, dst: &mut [u8]) -> usize {
        let (front, back) = self.readable();
        let first = front.len().min(dst.len());
        dst[..first].copy_from_slice(&front[..first]);
        let second = back.len().min(dst.len() - first);
        dst[first..first + second].copy_from_slice(&back[..second]);
        self.release(first + second);
        first + second
    }

    /// Bloquea hasta que haya datos publicados. Retorna `false` cuando el productor cerró
    /// y el anillo quedó drenado: fin del flujo.
    pub fn wait_for_data(&self) -> bool {
        let ring = &*self.ring;
        let tail = self.tail;

        for round in 0..SPIN_LIMIT + YIELD_LIMIT {
            // El cierre se lee antes que `head`: si ya estaba cerrado, `head` es definitivo.
            let closed = ring.hangup.0.producer_closed.load(Ordering::Acquire);
            if ring.head.0.load(Ordering::Acquire) != tail {
                return true;
            }
            if closed {
                return false;
            }
            backoff(round);
        }

        let mut has_data = false;
        ring.data_ready.0.wait_until(Some(POLL_INTERVAL), || {
            let closed = ring.hangup.0.producer_closed.load(Ordering::SeqCst);
            has_data = ring.head.0.load(Ordering::SeqCst) != tail;
            has_data || closed
        });
        has_data
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        let ring = &*self.ring;
        ring.hangup.0.consumer_closed.store(true, Ordering::SeqCst);
        ring.space_ready.0.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Byte esperado en la posición `position` del flujo de prueba.
    fn pattern(position: usize) -> u8 {
        (position.wrapping_mul(31) ^ (position >> 8)) as u8
    }

    fn fill(buffer: &mut [u8], start: usize) {
        for (offset, byte) in buffer.iter_mut().enumerate() {
            *byte = pattern(start + offset);
        }
    }

    #[test]
    fn publishes_only_whole_chunks_until_flush() {
        let (mut producer, mut consumer) = RingBuffer::with_capacity(4 * CHUNK_SIZE);
        let data = vec![7u8; CHUNK_SIZE + 100];

        assert_eq!(producer.push_slice(&data[..100]), 100);
        assert_eq!(consumer.readable().0.len(), 0);

        assert_eq!(producer.push_slice(&data[100..]), CHUNK_SIZE);
        assert_eq!(consumer.readable().0.len(), CHUNK_SIZE);
        assert_eq!(producer.pending(), 100);

        producer.flush();
        let (front, back) = consumer.readable();
        assert_eq!(front.len() + back.len(), CHUNK_SIZE + 100);
        consumer.release(CHUNK_SIZE + 100);
        assert_eq!(consumer.readable().0.len(), 0);
    }

    #[test]
    fn push_slice_stops_at_capacity() {
        let (mut producer, mut consumer) = RingBuffer::with_capacity(2 * CHUNK_SIZE);
        let data = vec![1u8; 3 * CHUNK_SIZE];
        assert_eq!(producer.push_slice(&data), 2 * CHUNK_SIZE);
        assert_eq!(producer.push_slice(&data), 0);

        let mut sink = vec![0u8; CHUNK_SIZE];
        assert_eq!(consumer.pop_slice(&mut sink), CHUNK_SIZE);
        assert_eq!(producer.push_slice(&data), CHUNK_SIZE);
    }

    #[test]
    fn wraps_around_preserving_content() {
        let (mut producer, mut consumer) = RingBuffer::with_capacity(2 * CHUNK_SIZE);
        let mut block = vec![0u8; 3000];
        let mut out = vec![0u8; 3000];
        let (mut written, mut read) = (0usize, 0usize);
        for _ in 0..200 {
            fill(&mut block, written);
            let count = producer.push_slice(&block);
            producer.flush();
            written += count;

            let count = consumer.pop_slice(&mut out);
            for (offset, &byte) in out[..count].iter().enumerate() {
                assert_eq!(byte, pattern(read + offset));
            }
            read += count;
        }
        assert_eq!(written, read);
        assert!(written > 50 * CHUNK_SIZE);
    }

    #[test]
    fn stream_survives_two_threads_and_backpressure() {
        const TOTAL: usize = 16 << 20;
        let (mut producer, mut consumer) = RingBuffer::with_capacity(4 * CHUNK_SIZE);

        let reader = thread::spawn(move || {
            let mut position = 0usize;
            while consumer.wait_for_data() {
                let (front, back) = consumer.readable();
                for &byte in front.iter().chain(back) {
                    assert_eq!(byte, pattern(position));
                    position += 1;
                }
                let count = front.len() + back.len();
                consumer.release(count);
            }
            position
        });

        let mut block = vec![0u8; 10_000];
        let mut written = 0usize;
        let mut size = 1usize;
        while written < TOTAL {
            let len = size.min(TOTAL - written);
            fill(&mut block[..len], written);
            let mut rest = &block[..len];
            while !rest.is_empty() {
                let count = producer.push_slice(rest);
                rest = &rest[count..];
                if !rest.is_empty() {
                    producer.wait_for_space(rest.len().min(CHUNK_SIZE)).unwrap();
                }
            }
            written += len;
            size = size * 7 % 9973 + 1;
        }
        drop(producer);
        assert_eq!(reader.join().unwrap(), TOTAL);
    }

    #[test]
    fn sleeping_consumer_picks_up_a_trickle() {
        let (mut producer, mut consumer) = RingBuffer::with_capacity(2 * CHUNK_SIZE);
        let writer = thread::spawn(move || {
            for _ in 0..20 {
                assert_eq!(producer.push_slice(b"gota"), 4);
                producer.flush();
                thread::sleep(Duration::from_millis(5));
            }
        });

        let mut received = Vec::new();
        while consumer.wait_for_data() {
            let (front, back) = consumer.readable();
            received.extend_from_slice(front);
            received.extend_from_slice(back);
            let count = front.len() + back.len();
            consumer.release(count);
        }
        writer.join().unwrap();
        assert_eq!(received, b"gota".repeat(20));
    }

    #[test]
    fn close_ends_the_stream_after_draining() {
        let (mut producer, mut consumer) = RingBuffer::with_capacity(2 * CHUNK_SIZE);
        producer.push_slice(b"resto parcial");
        drop(producer);

        assert!(consumer.wait_for_data());
        let mut out = [0u8; 64];
        let count = consumer.pop_slice(&mut out);
        assert_eq!(&out[..count], b"resto parcial");
        assert!(!consumer.wait_for_data());
    }

    #[test]
    fn producer_unblocks_when_consumer_dies() {
        let (mut producer, consumer) = RingBuffer::with_capacity(2 * CHUNK_SIZE);
        let data = vec![0u8; 2 * CHUNK_SIZE];
        assert_eq!(producer.push_slice(&data), data.len());

        let killer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            drop(consumer);
        });
        assert_eq!(producer.wait_for_space(CHUNK_SIZE), Err(Disconnected));
        assert!(producer.is_disconnected());
        killer.join().unwrap();
    }

    #[test]
    #[should_panic(expected = "Released more than was published")]
    fn release_cannot_overtake_head() {
        let (_producer, mut consumer) = RingBuffer::with_capacity(2 * CHUNK_SIZE);
        consumer.release(1);
    }
}
