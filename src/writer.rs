//! El puente: el lado productor del anillo expuesto como `std::io::Write`.

use crate::nvm_core::{CHUNK_SIZE, Disconnected, Producer};
use std::io::{self, Write};

/// Adaptador `io::Write` sobre el [`Producer`]. Cada `write` es un memcpy directo a la
/// memoria del anillo; el consumidor ve los datos por bloques de 4 KiB. Con el anillo lleno
/// la escritura bloquea hasta que el consumidor libere espacio, y si el consumidor murió
/// (p. ej. disco lleno) falla con `BrokenPipe` en lugar de colgarse.
///
/// Al soltarlo (`drop` o [`close`](Self::close)) publica el bloque parcial y cierra el flujo.
pub struct RingWriter {
    producer: Producer,
    stalls: u64,
}

impl RingWriter {
    pub fn new(producer: Producer) -> Self {
        Self { producer, stalls: 0 }
    }

    /// Veces que una escritura encontró el anillo lleno y tuvo que esperar al consumidor.
    pub fn stalls(&self) -> u64 {
        self.stalls
    }

    /// Publica lo pendiente y señala fin de flujo al consumidor.
    pub fn close(self) {}

    /// Contrapresión: el anillo no aceptó todo. Espera espacio y termina la copia.
    #[cold]
    fn write_blocking(&mut self, mut rest: &[u8]) -> Result<(), Disconnected> {
        while !rest.is_empty() {
            if self.producer.is_disconnected() {
                return Err(Disconnected);
            }
            self.stalls += 1;
            self.producer.wait_for_space(rest.len().min(CHUNK_SIZE))?;
            let count = self.producer.push_slice(rest);
            rest = &rest[count..];
        }
        Ok(())
    }
}

impl Write for RingWriter {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let count = self.producer.push_slice(buf);
        if count < buf.len() {
            self.write_blocking(&buf[count..])?;
        }
        Ok(buf.len())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.write(buf).map(drop)
    }

    /// Hace visible al consumidor el bloque parcial. No espera a que llegue a disco.
    fn flush(&mut self) -> io::Result<()> {
        self.producer.flush();
        if self.producer.is_disconnected() {
            return Err(Disconnected.into());
        }
        Ok(())
    }
}
