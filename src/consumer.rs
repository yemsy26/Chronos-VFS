use crate::nvm_core::CommandRingBuffer;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::future::Future;
use std::task::{Poll, Context};
use std::pin::Pin;

/// Estructura de mercado plana y estructurada
/// `#[repr(C)]` asegura que el layout binario es exacto y compatible
/// con las arquitecturas estándar (C, Python struct, etc).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MarketTick {
    pub timestamp: u64,
    pub price: f64,
    pub volume: f64,
}

pub struct StateEngine {
    base_seed: u64,
    pub processed_events: AtomicUsize,
}

impl StateEngine {
    pub fn new(seed: u64) -> Self {
        Self {
            base_seed: seed,
            processed_events: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    pub fn process_command(&self, cmd: u64) {
        let _virtual_state = self.calculate_virtual_node(cmd);
        self.processed_events.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn calculate_virtual_node(&self, id: u64) -> u64 {
        id.wrapping_mul(self.base_seed) ^ 0xDEADBEEFCAFEBABE
    }

    /// Genera determinísticamente un MarketTick basado en un índice lógico.
    /// Simula la extracción matemática in-situ de datos de un HFT en vez de leer de disco.
    #[inline(always)]
    pub fn generate_tick(&self, tick_index: u64) -> MarketTick {
        // Timestamp perfectamente secuencial para la validación (cada tick = 100ms lógicos)
        let timestamp = 1600000000000 + (tick_index * 100);
        
        // Entropía determinista
        let mut seed = tick_index.wrapping_mul(self.base_seed);
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;

        // Precio base 50000.0 con variación pseudoaleatoria predictible (-50.0 a 49.99)
        let price = 50000.0 + ((seed % 10000) as f64 - 5000.0) / 100.0;
        
        // Volumen pseudoaleatorio
        let volume = ((seed >> 16) % 500) as f64 + 1.5;

        MarketTick {
            timestamp,
            price,
            volume,
        }
    }
}

pub struct RingBufferPoller<'a, T> {
    ring: &'a CommandRingBuffer<T>,
    spin_limit: usize,
}

impl<'a, T> RingBufferPoller<'a, T> {
    pub fn new(ring: &'a CommandRingBuffer<T>, spin_limit: usize) -> Self {
        Self { ring, spin_limit }
    }
}

impl<'a, T: Unpin> Future for RingBufferPoller<'a, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut spins = 0;
        loop {
            if let Some(cmd) = self.ring.pop() {
                return Poll::Ready(cmd);
            }
            
            spins += 1;
            if spins >= self.spin_limit {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            
            core::hint::spin_loop();
        }
    }
}

pub async fn run_consumer_loop(ring: &CommandRingBuffer<u64>, engine: &StateEngine) {
    loop {
        let poller = RingBufferPoller::new(ring, 128);
        let cmd = poller.await;
        
        if cmd == 0 { break; }
        
        engine.process_command(cmd);
    }
}
