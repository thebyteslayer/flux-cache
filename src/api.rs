use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use bytes::BytesMut;
use log::{debug, error, warn};
use crate::cache::{ServerState, ServerError, CacheEntry};
use crate::whisper::WhisperServer;

// Define command types for our protocol
#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    SET { key: String, value: Vec<u8> },
    GET { key: String },
    DEL { keys: Vec<String> },
    EXISTS { key: String },
    CLUSTER_JOIN { address: String },
    CLUSTER_REMOVE { address: String },
    CLUSTER_ISOLATE,
    CLUSTER_SLOTS,
    NODE_INFO,
}

// Define response types for our protocol
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Success,
    Error(String),
    Data(Vec<u8>),
    Exists(bool),
    Slots(String),
    NodeInfo { node_id: String, address: String },
}

// Helper function to get node info from a remote server
async fn get_node_info(address: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    
    let mut stream = TcpStream::connect(address).await?;
    let cmd_json = serde_json::json!("NODE_INFO");
    let data = serde_json::to_vec(&cmd_json)?;
    stream.write_all(&data).await?;
    
    let mut buf = vec![0u8; 64 * 1024];
    let n = stream.read(&mut buf).await?;
    let response: Response = serde_json::from_slice(&buf[..n])?;
    
    match response {
        Response::NodeInfo { node_id, address } => Ok((node_id, address)),
        Response::Error(e) => Err(format!("Node returned error: {}", e).into()),
        _ => Err("Unexpected response from node".into()),
    }
}

// Process client commands
pub async fn process_command(
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
        Command::CLUSTER_JOIN { address } => {
            // Check if clustering is enabled first
            {
                let state_read = state.read().unwrap();
                if !state_read.cluster_enabled {
                    return Err(ServerError::KeyNotFound("Clustering is disabled".to_string()));
                }
            } // Lock is dropped here
            
            // First, contact the new node to get its real node ID
            match get_node_info(&address).await {
                Ok((node_id, actual_address)) => {
                    let (whisper_port, nodes, our_address) = {
                        let mut state = state.write().unwrap();
                        // Set the real node ID before adding the node
                        state.cluster.node_ids.insert(actual_address.clone(), node_id);
                        state.cluster.add_node(actual_address.clone());
                        // Calculate whisper port
                        let base_port = state.cluster.nodes.first()
                            .and_then(|addr| addr.split(':').nth(1))
                            .and_then(|port_str| port_str.parse::<u16>().ok())
                            .unwrap_or(6124);
                        let our_addr = state.cluster.nodes.first().cloned().unwrap_or_default();
                        (base_port + 10000, state.cluster.nodes.clone(), our_addr)
                    };
                    
                    // Also tell the target node to add us to their cluster
                    let our_address_clone = our_address.clone();
                    let actual_address_clone = actual_address.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        
                        // Send CLUSTER_JOIN command to the target node to add us
                        let reverse_join_cmd = serde_json::json!({
                            "CLUSTER_JOIN": { "address": our_address_clone }
                        });
                        
                        match TcpStream::connect(&actual_address_clone).await {
                            Ok(mut stream) => {
                                let data = serde_json::to_vec(&reverse_join_cmd).unwrap();
                                if let Err(e) = stream.write_all(&data).await {
                                    warn!("Failed to send reverse join to {}: {}", actual_address_clone, e);
                                } else {
                                    debug!("Sent reverse join command to {}", actual_address_clone);
                                }
                            }
                            Err(e) => {
                                warn!("Failed to connect to {} for reverse join: {}", actual_address_clone, e);
                            }
                        }
                    });
                    
                    // Initiate immediate gossip with the new node to sync cluster state
                    let state_clone = state.clone();
                    tokio::spawn(async move {
                        // Give the new node a moment to start its whisper server
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                        
                        // Gossip with the newly added node multiple times to ensure sync
                        for _ in 0..3 {
                            WhisperServer::gossip_with_node(&state_clone, &actual_address, whisper_port).await;
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        }
                        
                        // Also gossip with existing nodes to spread the update
                        for node in nodes.iter().take(3) {
                            if node != &actual_address {
                                WhisperServer::gossip_with_node(&state_clone, node, whisper_port).await;
                            }
                        }
                    });
                    
                    Ok(Response::Success)
                }
                Err(e) => {
                    Err(ServerError::KeyNotFound(format!("Failed to contact new node {}: {}", address, e)))
                }
            }
        },
        Command::CLUSTER_REMOVE { address } => {
            // Check if clustering is enabled first
            {
                let state_read = state.read().unwrap();
                if !state_read.cluster_enabled {
                    return Err(ServerError::KeyNotFound("Clustering is disabled".to_string()));
                }
            } // Lock is dropped here
            
            // First, tell the target node to isolate itself
            let target_address_clone = address.clone();
            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                
                // Send CLUSTER_ISOLATE command to the target node
                let isolate_cmd = serde_json::json!("CLUSTER_ISOLATE");
                
                match TcpStream::connect(&target_address_clone).await {
                    Ok(mut stream) => {
                        let data = serde_json::to_vec(&isolate_cmd).unwrap();
                        if let Err(e) = stream.write_all(&data).await {
                            warn!("Failed to send isolate command to {}: {}", target_address_clone, e);
                        } else {
                            debug!("Sent isolate command to {}", target_address_clone);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to connect to {} for isolation: {}", target_address_clone, e);
                    }
                }
            });
            
            let (removed, whisper_port, nodes) = {
                let mut state = state.write().unwrap();
                let removed = state.cluster.remove_node(address.clone());
                // Calculate whisper port
                let base_port = state.cluster.nodes.first()
                    .and_then(|addr| addr.split(':').nth(1))
                    .and_then(|port_str| port_str.parse::<u16>().ok())
                    .unwrap_or(6124);
                (removed, base_port + 10000, state.cluster.nodes.clone())
            };
            
            if removed {
                // Initiate gossip with remaining nodes to spread the removal
                let state_clone = state.clone();
                tokio::spawn(async move {
                    for node in nodes.iter().take(3) {
                        WhisperServer::gossip_with_node(&state_clone, node, whisper_port).await;
                    }
                });
                Ok(Response::Success)
            } else {
                Err(ServerError::KeyNotFound("Node not found in cluster".to_string()))
            }
        },
        Command::CLUSTER_ISOLATE => {
            // Check if clustering is enabled first
            {
                let state_read = state.read().unwrap();
                if !state_read.cluster_enabled {
                    return Err(ServerError::KeyNotFound("Clustering is disabled".to_string()));
                }
            } // Lock is dropped here
            
            // Remove all other nodes from this node's cluster, keeping only itself
            let our_address = {
                let mut state = state.write().unwrap();
                let our_addr = state.cluster.nodes.first().cloned().unwrap_or_default();
                
                // Get all other nodes before clearing
                let other_nodes: Vec<String> = state.cluster.nodes.iter()
                    .filter(|&addr| addr != &our_addr)
                    .cloned()
                    .collect();
                
                // Remove all other nodes, keeping only ourselves
                for other_node in other_nodes {
                    state.cluster.remove_node(other_node);
                }
                
                our_addr
            };
            
            debug!("Node {} isolated from cluster", our_address);
            Ok(Response::Success)
        },
        Command::CLUSTER_SLOTS => {
            let state = state.read().unwrap();
            if !state.cluster_enabled {
                return Err(ServerError::KeyNotFound("Clustering is disabled".to_string()));
            }
            Ok(Response::Slots(state.cluster.get_cluster_json()))
        },
        Command::NODE_INFO => {
            let state = state.read().unwrap();
            let our_address = state.cluster.nodes.first().cloned().unwrap_or_default();
            let our_node_id = state.cluster.node_ids
                .get(&our_address)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            Ok(Response::NodeInfo { 
                node_id: our_node_id, 
                address: our_address 
            })
        },

    }
}

// Handle a client connection
pub async fn handle_client(
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