#![allow(unused_imports)]
#![allow(non_camel_case_types)]

mod environment;
mod cluster;
mod cache;
mod api;
mod whisper;

use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::time;
use log::debug;
use socket2;
use clap::Parser;

use environment::read_flux_toml;
use cache::ServerState;
use api::handle_client;
use whisper::WhisperServer;

// Command line arguments
#[derive(Parser, Debug)]
#[command(name = "flux-cache")]
#[command(author = "thebyteslayer")]
#[command(version = "1.0.0")]
#[command(about = "A scalable and extensible caching software written in Rust")]
struct Args {
    /// Server port (overrides port in flxc.toml)
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Parse command-line arguments
    let args = Args::parse();
    
    // Read bind IP and port from flxc.toml (create if missing)
    let conf = read_flux_toml();
    let port = args.port.unwrap_or(conf.port);
    let bind_addr_str = format!("{}:{}", conf.bind, port);
    
    // Create public address for cluster communication
    let public_port = if args.port.is_some() { 
        // If port was overridden via CLI, use the same override for public port
        args.port.unwrap() 
    } else { 
        conf.public_port 
    };
    let public_addr = format!("{}:{}", conf.public_ip, public_port);
    
    // Create server state with public address for cluster
    let state = Arc::new(RwLock::new(ServerState::new(public_addr.clone(), conf.cluster_enabled)));
    
    // Parse bind address
    let bind_addr = match bind_addr_str.parse::<SocketAddr>() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("Invalid address format - {}: {}", bind_addr_str, e);
            return Ok(());
        }
    };
    
    // Configure TCP socket for large buffers
    let socket_config = socket2::Socket::new(
        match bind_addr {
            SocketAddr::V4(_) => socket2::Domain::IPV4,
            SocketAddr::V6(_) => socket2::Domain::IPV6,
        },
        socket2::Type::STREAM,
        None,
    )?;
    
    // Set socket options for large buffers
    socket_config.set_recv_buffer_size(16 * 1024 * 1024)?; // 16MB buffer
    socket_config.set_send_buffer_size(16 * 1024 * 1024)?;
    
    // Allow address reuse to avoid "address already in use" errors
    socket_config.set_reuse_address(true)?;
    
    // Bind and convert to tokio listener
    socket_config.bind(&bind_addr.into())?;
    socket_config.listen(1024)?; // Allow up to 1024 connections in the queue
    
    let listener = TcpListener::from_std(socket_config.into())?;
    
    // Start whisper server for inter-node communication
    let whisper_server = WhisperServer::new(port, state.clone());
    let whisper_bind_ip = conf.bind.clone();
    tokio::spawn(async move {
        if let Err(e) = whisper_server.start(whisper_bind_ip).await {
            eprintln!("Whisper server error: {}", e);
        }
    });
    
    // Print startup message
    println!("Flux is running on {}", bind_addr);
    println!("Whisper protocol running on {}:{}", conf.bind, port + 10000);
    
    // Count of active connections
    let mut active_connections = 0;
    
    // Accept connections
    loop {
        match tokio::time::timeout(Duration::from_secs(5), listener.accept()).await {
            Ok(Ok((socket, addr))) => {
                active_connections += 1;
                debug!("Accepted connection from: {} (active: {})", addr, active_connections);
                // Set socket buffer sizes
                if let Ok(stream) = socket.into_std() {
                    match socket2::Socket::try_from(stream) {
                        Ok(sock) => {
                            // Set large buffer sizes for this connection
                            let _ = sock.set_recv_buffer_size(16 * 1024 * 1024);
                            let _ = sock.set_send_buffer_size(16 * 1024 * 1024);
                            // Convert back to tokio socket
                            if let Ok(socket) = TcpStream::from_std(sock.into()) {
                                // Clone state for the new task
                                let state = state.clone();
                                // Spawn a new task to handle the connection
                                tokio::spawn(async move {
                                    handle_client(socket, state).await;
                                    debug!("Client handler task completed for {}", addr);
                                });
                            } else {
                                eprintln!("Failed to convert socket back to TcpStream");
                            }
                        },
                        Err(e) => {
                            eprintln!("Failed to convert to socket2::Socket: {}", e);
                        }
                    }
                } else {
                    eprintln!("Failed to get standard socket from TcpStream");
                }
            }
            Ok(Err(e)) => {
                eprintln!("Failed to accept connection: {}", e);
            }
            Err(_) => {
                // Timeout occurred, just continue
                debug!("Accept timed out, checking system state");
            }
        }
    }
}
