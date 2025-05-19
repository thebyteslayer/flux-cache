#![allow(unused_imports)]
#![allow(non_camel_case_types)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::process;
use std::net::SocketAddr;
use std::fs;
use std::path::Path;

use bytes::{Bytes, BytesMut};
use log::{debug, error, info, LevelFilter};
use rand::Rng;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time;
use env_logger::Builder;
use clap::Parser;
use socket2;

// Command line arguments
#[derive(Parser, Debug)]
#[command(name = "pluto-server")]
#[command(author = "Pluto Team")]
#[command(version = "1.0.0")]
#[command(about = "In-memory cache server with compression", long_about = None)]
#[command(disable_version_flag = true)]
struct Args {
    /// Server bind address
    #[arg(short, long, default_value = "0.0.0.0:8080")]
    address: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,

    /// Just print version and exit
    #[arg(short, long)]
    version: bool,
}

// Define command types for our protocol
#[derive(Debug, Serialize, Deserialize)]
enum Command {
    SET { key: String, value: Vec<u8> },
    GET { key: String },
    DEL { keys: Vec<String> },
    EXISTS { key: String },
}

// Define response types for our protocol
#[derive(Debug, Serialize, Deserialize)]
enum Response {
    Success,
    Error(String),
    Data(Vec<u8>),
    Exists(bool),
}

// Custom error type
#[derive(Error, Debug)]
enum ServerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    #[error("Compression error: {0}")]
    Compression(String),
    
    #[error("Key not found: {0}")]
    KeyNotFound(String),
}

// Cache entry structure
struct CacheEntry {
    compressed_data: Bytes,
}

// Server state
struct ServerState {
    cache: HashMap<String, CacheEntry>,
}

impl ServerState {
    fn new() -> Self {
        ServerState {
            cache: HashMap::new(),
        }
    }

    // Compress data using zstd
    fn compress_data(&self, data: &[u8]) -> Result<Bytes, ServerError> {
        let compressed = zstd::encode_all(data, 3)
            .map_err(|e| ServerError::Compression(e.to_string()))?;
        Ok(Bytes::from(compressed))
    }

    // Decompress data using zstd
    fn decompress_data(&self, data: &[u8]) -> Result<Vec<u8>, ServerError> {
        let decompressed = zstd::decode_all(data)
            .map_err(|e| ServerError::Compression(e.to_string()))?;
        Ok(decompressed)
    }
}

// Process client commands
async fn process_command(
    cmd: Command, 
    state: &Arc<RwLock<ServerState>>
) -> Result<Response, ServerError> {
    match cmd {
        Command::SET { key, value } => {
            let mut state = state.write().unwrap();
            let compressed_data = state.compress_data(&value)?;
            let entry = CacheEntry {
                compressed_data,
            };
            state.cache.insert(key, entry);
            Ok(Response::Success)
        },
        Command::GET { key } => {
            let state = state.read().unwrap();
            if let Some(entry) = state.cache.get(&key) {
                let data = state.decompress_data(&entry.compressed_data)?;
                Ok(Response::Data(data))
            } else {
                Err(ServerError::KeyNotFound(key))
            }
        },
        Command::DEL { keys } => {
            let mut state = state.write().unwrap();
            let mut found = false;
            for key in keys {
                if state.cache.remove(&key).is_some() {
                    found = true;
                }
            }
            if found {
                Ok(Response::Success)
            } else {
                Err(ServerError::KeyNotFound("None of the keys found".to_string()))
            }
        },
        Command::EXISTS { key } => {
            let state = state.read().unwrap();
            Ok(Response::Exists(state.cache.contains_key(&key)))
        },
    }
}

// Handle a client connection
async fn handle_client(
    mut socket: TcpStream, 
    state: Arc<RwLock<ServerState>>,
) {
    let (mut reader, mut writer) = socket.split();
    let mut buf = BytesMut::with_capacity(1024 * 1024); // 1MB initial capacity
    loop {
        match reader.read_buf(&mut buf).await {
            Ok(0) => {
                // Connection was closed
                debug!("Client disconnected");
                break;
            }
            Ok(n) => {
                debug!("Read {n} bytes from client");
                // Parse the command
                match serde_json::from_slice::<Command>(&buf[..n]) {
                    Ok(cmd) => {
                        debug!("Received command: {:?}", cmd);
                        // Process the command
                        let response = match process_command(cmd, &state).await {
                            Ok(resp) => resp,
                            Err(e) => Response::Error(e.to_string()),
                        };
                        // Serialize and send the response
                        match serde_json::to_vec(&response) {
                            Ok(data) => {
                                if let Err(e) = writer.write_all(&data).await {
                                    error!("Failed to write response: {}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Failed to serialize response: {}", e);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse command: {}", e);
                        // Send error response
                        let response = Response::Error(format!("Invalid command: {}", e));
                        match serde_json::to_vec(&response) {
                            Ok(data) => {
                                if let Err(e) = writer.write_all(&data).await {
                                    error!("Failed to write error response: {}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Failed to serialize error response: {}", e);
                                break;
                            }
                        }
                    }
                }
                // Clear the buffer for the next command
                buf.clear();
            }
            Err(e) => {
                error!("Failed to read from socket: {}", e);
                break;
            }
        }
    }
}

// Initialize the logger with custom settings
fn setup_logger(log_level: &str) {
    let level = match log_level.to_lowercase().as_str() {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };
    
    let mut builder = Builder::new();
    
    // Set the base log level
    builder.filter_level(level);
    
    // Format each log line to include the file and line number
    builder.format(|buf, record| {
        use std::io::Write;
        let level_style = buf.default_level_style(record.level());
        
        writeln!(
            buf,
            "[{} {} {}:{}] {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            level_style.value(record.level()),
            record.file().unwrap_or("unknown"),
            record.line().unwrap_or(0),
            record.args()
        )
    });
    
    // Apply the configuration
    builder.init();
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Parse command-line arguments
    let args = Args::parse();
    
    // If version flag is set, just print version and exit
    if args.version {
        println!("Pluto Server v1.0.0");
        return Ok(());
    }
    
    // Initialize custom logger
    setup_logger(&args.log_level);
    
    // Create server state
    let state = Arc::new(RwLock::new(ServerState::new()));
    
    // Parse bind address
    let bind_addr = match args.address.parse::<SocketAddr>() {
        Ok(addr) => addr,
        Err(e) => {
            error!("Invalid address format - {}: {}", args.address, e);
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
    
    // Clean startup message - only showing server is running and address
    info!("Pluto running on {}", bind_addr);
    
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
                                error!("Failed to convert socket back to TcpStream");
                            }
                        },
                        Err(e) => {
                            error!("Failed to convert to socket2::Socket: {}", e);
                        }
                    }
                } else {
                    error!("Failed to get standard socket from TcpStream");
                }
            }
            Ok(Err(e)) => {
                error!("Failed to accept connection: {}", e);
            }
            Err(_) => {
                // Timeout occurred, just continue
                debug!("Accept timed out, checking system state");
            }
        }
    }
}
