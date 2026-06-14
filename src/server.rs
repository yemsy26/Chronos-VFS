use crate::consumer::StateEngine;
use crate::vfs::VirtualFileSystem;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;

pub async fn start_generative_server(engine: Arc<StateEngine>, port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    println!("[SERVER] Servidor Generativo Tokio escuchando en 0.0.0.0:{}", port);

    loop {
        let (stream, _) = listener.accept().await?;
        let engine_clone = Arc::clone(&engine);
        tokio::spawn(async move {
            let _ = handle_client(stream, engine_clone).await;
        });
    }
}

async fn handle_client(mut stream: TcpStream, engine: Arc<StateEngine>) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    if n == 0 { return Ok(()); }
    
    let request = String::from_utf8_lossy(&buf[..n]);
    
    let mut offset: u64 = 0;
    let mut len: usize = 4096;
    
    if let Some(line) = request.lines().next() {
        if line.starts_with("GET /virtual_file?") {
            if let Some(query) = line.split(' ').nth(1) {
                for pair in query.trim_start_matches("/virtual_file?").split('&') {
                    let mut parts = pair.split('=');
                    match (parts.next(), parts.next()) {
                        (Some("offset"), Some(val)) => offset = val.parse().unwrap_or(0),
                        (Some("len"), Some(val)) => len = val.parse().unwrap_or(4096),
                        _ => {}
                    }
                }
            }
        } else {
            return stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n").await;
        }
    }
    
    let vfs = VirtualFileSystem::new(&engine);
    let giant_file = vfs.open_generative_file("simulacion_red.dat", 10_000_000_000_000); 
    
    let mut payload = vec![0u8; len];
    let generated_bytes = giant_file.read_chunk(offset, len, &mut payload);
    payload.truncate(generated_bytes);
    
    let header = format!(
        "HTTP/1.1 200 OK\r\n\
        Content-Type: application/octet-stream\r\n\
        Content-Length: {}\r\n\
        Connection: close\r\n\r\n", 
        generated_bytes
    );
    
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(&payload).await?;
    
    Ok(())
}
