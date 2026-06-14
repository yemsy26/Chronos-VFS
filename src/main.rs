pub mod nvm_core;
pub mod consumer;
pub mod vfs;
pub mod server;

use consumer::StateEngine;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    println!("=== NVM Prototype V7/9: Tokio Validation Network Layer ===");
    
    let engine = Arc::new(StateEngine::new(0x1337));
    let port = 8081;
    
    println!("Iniciando Motor y Servidor HTTP Generativo...");
    if let Err(e) = server::start_generative_server(engine, port).await {
        eprintln!("Fallo crítico del servidor: {:?}", e);
    }
}

