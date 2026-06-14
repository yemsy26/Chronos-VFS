use crate::consumer::{StateEngine, MarketTick};

pub struct VirtualNode {
    pub name: String,
    pub id: u64,
}

pub struct GenerativeFile<'a> {
    pub name: String,
    pub seed: u64,
    pub logical_size: u64,
    engine: &'a StateEngine, 
}

impl<'a> GenerativeFile<'a> {
    pub fn new(name: &str, seed: u64, logical_size: u64, engine: &'a StateEngine) -> Self {
        Self {
            name: name.to_string(),
            seed,
            logical_size,
            engine,
        }
    }

    /// Implementa el Stream Generativo con Casting Binario Crudo (Zero-Copy serialización).
    #[inline]
    pub fn read_chunk(&self, offset: u64, size: usize, out_buffer: &mut [u8]) -> usize {
        let tick_size = core::mem::size_of::<MarketTick>() as u64;
        
        // Calcular en qué tick exacto comenzamos según el offset solicitado
        let start_tick_index = offset / tick_size;
        let mut bytes_to_read = size as u64;
        
        if offset + bytes_to_read > self.logical_size {
            bytes_to_read = self.logical_size - offset; // Truncamos si sobrepasamos EOF
        }
        
        let num_ticks = (bytes_to_read / tick_size) as usize;
        let valid_bytes = num_ticks * (tick_size as usize);

        if valid_bytes == 0 {
            return 0; // O bien EOF o pidieron menos de 1 struct completo.
        }

        // --- MAGIA ZERO-COPY: Casting directo al buffer de red ---
        // Engañamos a Rust de forma segura para interpretar la memoria cruda
        // del buffer de red como si fuera un arreglo de la estructura MarketTick.
        let ticks_slice = unsafe {
            core::slice::from_raw_parts_mut(
                out_buffer.as_mut_ptr() as *mut MarketTick,
                num_ticks
            )
        };

        // Rellenamos las estructuras directamente sobre el buffer tcp
        for i in 0..num_ticks {
            let current_tick_index = start_tick_index + (i as u64);
            ticks_slice[i] = self.engine.generate_tick(current_tick_index);
        }

        valid_bytes
    }
}

pub struct VirtualFileSystem<'a> {
    engine: &'a StateEngine,
    root: VirtualNode,
}

impl<'a> VirtualFileSystem<'a> {
    pub fn new(engine: &'a StateEngine) -> Self {
        Self {
            engine,
            root: VirtualNode {
                name: "root".to_string(),
                id: 0xCAFE_BABE,
            },
        }
    }

    pub fn open_generative_file(&self, name: &str, logical_size: u64) -> GenerativeFile<'a> {
        let seed_basis = self.root.id ^ (name.len() as u64);
        let file_seed = self.engine.calculate_virtual_node(seed_basis);
        
        GenerativeFile::new(name, file_seed, logical_size, self.engine)
    }
}
