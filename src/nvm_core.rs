use core::sync::atomic::{AtomicUsize, Ordering};
use std::fs::OpenOptions;
use memmap2::MmapMut;

/// Wrapper 'Newtype' para acceso directo y seguro a memoria.
/// Encapsula el uso de punteros crudos para asegurar que la lectura
/// y escritura sigan la semántica volátil (evitando optimizaciones
/// del compilador que puedan omitir lecturas/escrituras en NVM).
#[repr(transparent)]
pub struct PPtr<T> {
    ptr: *mut T,
}

impl<T> PPtr<T> {
    pub fn new(ptr: *mut T) -> Self {
        Self { ptr }
    }

    /// Lee de forma volátil desde la memoria (ej. NVM).
    /// Safety: El llamador debe garantizar que el puntero es válido y está alineado.
    #[inline(always)]
    pub unsafe fn read_volatile(&self) -> T {
        unsafe { core::ptr::read_volatile(self.ptr) }
    }

    /// Escribe de forma volátil a la memoria (ej. Write-Combining buffer).
    /// Safety: El llamador debe garantizar que el puntero es válido.
    #[inline(always)]
    pub unsafe fn write_volatile(&self, val: T) {
        unsafe { core::ptr::write_volatile(self.ptr, val) }
    }
}

/// Alineación a 64 bytes para corresponder a la línea de caché típica
/// y prevenir 'false sharing' mutuo entre el productor y el consumidor.
#[repr(align(64))]
struct CacheAligned<T>(T);

/// Buffer SPSC (Single Producer Single Consumer) lock-free, zero-copy, zero-allocation.
pub struct CommandRingBuffer<T> {
    buffer: *mut T,
    capacity: usize,
    /// Posición del productor, escrita por él, leída por el consumidor.
    head: CacheAligned<AtomicUsize>, 
    /// Posición del consumidor, escrita por él, leída por el productor.
    tail: CacheAligned<AtomicUsize>, 
    _marker: core::marker::PhantomData<T>,
}

unsafe impl<T: Send> Send for CommandRingBuffer<T> {}
unsafe impl<T: Sync> Sync for CommandRingBuffer<T> {}

impl<T> CommandRingBuffer<T> {
    /// Inicializa el ring buffer en la región de memoria pre-alojada.
    /// Safety: `buffer` debe apuntar a una región contigua de memoria válida
    /// con un tamaño de al menos `capacity * size_of::<T>()`.
    pub unsafe fn new(buffer: *mut T, capacity: usize) -> Self {
        // Exigir potencias de 2 asegura un enmascaramiento ultrarrápido (módulo).
        assert!(capacity.is_power_of_two(), "Capacity must be a power of two");
        Self {
            buffer,
            capacity,
            head: CacheAligned(AtomicUsize::new(0)),
            tail: CacheAligned(AtomicUsize::new(0)),
            _marker: core::marker::PhantomData,
        }
    }

    /// Escribe un comando en el buffer (Zero-Copy push via volatile).
    #[inline]
    pub fn push(&self, item: T) -> Result<(), T> {
        let head = self.head.0.load(Ordering::Relaxed);
        // Acquire para ver el progreso real del consumidor
        let tail = self.tail.0.load(Ordering::Acquire); 

        if head.wrapping_sub(tail) >= self.capacity {
            return Err(item); // Buffer lleno
        }

        let idx = head & (self.capacity - 1);
        unsafe {
            let pptr = PPtr::new(self.buffer.add(idx));
            pptr.write_volatile(item);
        }

        // Release para hacer visible el commit de la data al consumidor
        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Extrae un comando del buffer (Zero-Copy pop via volatile).
    #[inline]
    pub fn pop(&self) -> Option<T> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        // Acquire para ver el progreso real del productor
        let head = self.head.0.load(Ordering::Acquire); 

        if head == tail {
            return None; // Buffer vacío
        }

        let idx = tail & (self.capacity - 1);
        let item = unsafe {
            let pptr = PPtr::new(self.buffer.add(idx));
            pptr.read_volatile()
        };

        // Release para indicar que la posición queda libre
        self.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        Some(item)
    }
}

/// Orquestador para consumir comandos por lotes, optimizado para latencia ultra-baja.
pub struct BatchExecutor<'a, T> {
    ring: &'a CommandRingBuffer<T>,
}

impl<'a, T> BatchExecutor<'a, T> {
    pub fn new(ring: &'a CommandRingBuffer<T>) -> Self {
        Self { ring }
    }

    /// Ejecuta hasta `batch_size` comandos del ring buffer de forma secuencial.
    /// Retorna la cantidad de comandos efectivamente ejecutados.
    pub fn execute_batch<F>(&self, batch_size: usize, mut f: F) -> usize 
    where 
        F: FnMut(T) 
    {
        let mut count = 0;
        for _ in 0..batch_size {
            if let Some(cmd) = self.ring.pop() {
                f(cmd);
                count += 1;
            } else {
                break;
            }
        }
        count
    }
}

/// Inicializa una región mock en disco para simular NVM/Write-Combining mediante mmap.
pub fn init_mock_nvm<T>(file_path: &str, capacity: usize) -> std::io::Result<(MmapMut, CommandRingBuffer<T>)> {
    let size = capacity * core::mem::size_of::<T>();
    
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(file_path)?;
    
    // Asegurar que el archivo tenga la longitud requerida
    file.set_len(size as u64)?;
    
    // Mapear memoria
    let mut mmap = unsafe { memmap2::MmapOptions::new().map_mut(&file)? };
    
    let buffer_ptr = mmap.as_mut_ptr() as *mut T;
    
    // Safety: Garantizamos que mmap tiene el size >= capacity * size_of::<T>()
    let ring = unsafe { CommandRingBuffer::new(buffer_ptr, capacity) };
    
    // Retornamos el mmap y el ring. El mmap *debe* sobrevivir mientras el ring exista.
    Ok((mmap, ring))
}
