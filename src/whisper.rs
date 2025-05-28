use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time;
use serde::{Deserialize, Serialize};
use log::{debug, error, info, warn};
use rand::Rng;
use rand::prelude::SliceRandom;
use crate::cache::ServerState;
use crate::cluster::{NodeSlots, ClusterData};

// Whisper protocol messages for inter-node communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WhisperMessage {
    // Gossip cluster state with timestamp
    Gossip { cluster_data: ClusterData },
    // Request cluster state from another node
    ClusterRequest,
    // Heartbeat to check if node is alive
    Heartbeat { node_id: String, address: String },
    // Response to heartbeat
    HeartbeatAck { node_id: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WhisperResponse {
    pub success: bool,
    pub message: Option<String>,
    pub data: Option<WhisperMessage>,
}

pub struct WhisperServer {
    pub port: u16,
    pub state: Arc<RwLock<ServerState>>,
}

impl WhisperServer {
    pub fn new(base_port: u16, state: Arc<RwLock<ServerState>>) -> Self {
        WhisperServer {
            port: base_port + 10000,
            state,
        }
    }

    // Start the whisper server
    pub async fn start(&self, bind_ip: String) -> std::io::Result<()> {
        let whisper_addr = format!("{}:{}", bind_ip, self.port);
        let bind_addr = match whisper_addr.parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(e) => {
                error!("Invalid whisper address format - {}: {}", whisper_addr, e);
                return Ok(());
            }
        };

        let listener = TcpListener::bind(bind_addr).await?;
        info!("Whisper protocol listening on {}", whisper_addr);

        // Start periodic gossip task
        let state_clone = self.state.clone();
        tokio::spawn(async move {
            // Wait a bit for the server to fully start
            tokio::time::sleep(Duration::from_secs(2)).await;
            
            // Try to get cluster state from any existing nodes on startup
            WhisperServer::startup_cluster_sync(state_clone.clone()).await;
            
            // Start periodic gossip
            WhisperServer::periodic_gossip(state_clone).await;
        });

        // Accept whisper connections
        loop {
            match tokio::time::timeout(Duration::from_secs(5), listener.accept()).await {
                Ok(Ok((socket, addr))) => {
                    debug!("Whisper connection from: {}", addr);
                    let state = self.state.clone();
                    tokio::spawn(async move {
                        WhisperServer::handle_whisper_client(socket, state).await;
                    });
                }
                Ok(Err(e)) => {
                    error!("Failed to accept whisper connection: {}", e);
                }
                Err(_) => {
                    // Timeout occurred, continue
                    debug!("Whisper accept timed out");
                }
            }
        }
    }

    // Handle incoming whisper connections
    async fn handle_whisper_client(mut socket: TcpStream, state: Arc<RwLock<ServerState>>) {
        let mut buf = vec![0u8; 64 * 1024]; // 64KB buffer for whisper messages
        
        loop {
            match socket.read(&mut buf).await {
                Ok(0) => {
                    debug!("Whisper client disconnected");
                    break;
                }
                Ok(n) => {
                    match serde_json::from_slice::<WhisperMessage>(&buf[..n]) {
                        Ok(message) => {
                            debug!("Received whisper message: {:?}", message);
                            let response = WhisperServer::process_whisper_message(message, &state).await;
                            
                            match serde_json::to_vec(&response) {
                                Ok(data) => {
                                    if let Err(e) = socket.write_all(&data).await {
                                        error!("Failed to write whisper response: {}", e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to serialize whisper response: {}", e);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to parse whisper message: {}", e);
                            let error_response = WhisperResponse {
                                success: false,
                                message: Some(format!("Invalid message: {}", e)),
                                data: None,
                            };
                            if let Ok(data) = serde_json::to_vec(&error_response) {
                                let _ = socket.write_all(&data).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to read from whisper socket: {}", e);
                    break;
                }
            }
        }
    }

    // Process whisper protocol messages
    async fn process_whisper_message(
        message: WhisperMessage,
        state: &Arc<RwLock<ServerState>>
    ) -> WhisperResponse {
        match message {
            WhisperMessage::Gossip { cluster_data } => {
                let updated = {
                    let mut state_guard = state.write().unwrap();
                    state_guard.cluster.update_from_gossip(cluster_data)
                };
                
                if updated {
                    info!("Cluster state updated via gossip");
                }
                
                // Always respond with our current cluster data for gossip exchange
                let our_cluster_data = {
                    let state_guard = state.read().unwrap();
                    state_guard.cluster.get_cluster_data()
                };
                
                WhisperResponse {
                    success: true,
                    message: Some("Gossip processed".to_string()),
                    data: Some(WhisperMessage::Gossip { cluster_data: our_cluster_data }),
                }
            }
            WhisperMessage::ClusterRequest => {
                let state_guard = state.read().unwrap();
                let cluster_data = state_guard.cluster.get_cluster_data();
                
                WhisperResponse {
                    success: true,
                    message: Some("Cluster data sent".to_string()),
                    data: Some(WhisperMessage::Gossip { cluster_data }),
                }
            }
            WhisperMessage::Heartbeat { node_id, address } => {
                debug!("Received heartbeat from node {} at {}", node_id, address);
                
                let state_guard = state.read().unwrap();
                let our_node_id = state_guard.cluster.node_ids
                    .values()
                    .next()
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                
                WhisperResponse {
                    success: true,
                    message: Some("Heartbeat acknowledged".to_string()),
                    data: Some(WhisperMessage::HeartbeatAck { node_id: our_node_id }),
                }
            }
            WhisperMessage::HeartbeatAck { node_id } => {
                debug!("Received heartbeat ack from node {}", node_id);
                
                WhisperResponse {
                    success: true,
                    message: Some("Heartbeat ack received".to_string()),
                    data: None,
                }
            }
        }
    }

    // Send whisper message to another node
    pub async fn send_whisper_message(
        target_addr: &str,
        whisper_port: u16,
        message: WhisperMessage
    ) -> Result<WhisperResponse, Box<dyn std::error::Error>> {
        let whisper_addr = format!("{}:{}", 
            target_addr.split(':').next().unwrap_or(target_addr), 
            whisper_port
        );
        
        let mut stream = TcpStream::connect(whisper_addr).await?;
        let data = serde_json::to_vec(&message)?;
        stream.write_all(&data).await?;
        
        let mut buf = vec![0u8; 64 * 1024];
        let n = stream.read(&mut buf).await?;
        let response: WhisperResponse = serde_json::from_slice(&buf[..n])?;
        
        Ok(response)
    }

    // Periodic gossip with random nodes
    async fn periodic_gossip(state: Arc<RwLock<ServerState>>) {
        let mut interval = time::interval(Duration::from_secs(15)); // Gossip every 15 seconds
        
        loop {
            interval.tick().await;
            
            let (nodes, whisper_port, our_address, our_cluster_data) = {
                let state_guard = state.read().unwrap();
                let nodes = state_guard.cluster.nodes.clone();
                let our_address = state_guard.cluster.nodes.first().cloned().unwrap_or_default();
                let cluster_data = state_guard.cluster.get_cluster_data();
                
                // Calculate whisper port
                let base_port = state_guard.cluster.nodes.first()
                    .and_then(|addr| addr.split(':').nth(1))
                    .and_then(|port_str| port_str.parse::<u16>().ok())
                    .unwrap_or(6124);
                
                (nodes, base_port + 10000, our_address, cluster_data)
            };
            
            // Select random nodes to gossip with (gossip with up to 3 random nodes)
            let other_nodes: Vec<String> = nodes.iter()
                .filter(|&addr| addr != &our_address)
                .cloned()
                .collect();
            
            if !other_nodes.is_empty() {
                let gossip_count = std::cmp::min(3, other_nodes.len());
                
                // Create a shuffled copy of nodes for random selection
                let mut shuffled_nodes = other_nodes.clone();
                {
                    let mut rng = rand::thread_rng();
                    shuffled_nodes.shuffle(&mut rng);
                }
                
                for target_node in shuffled_nodes.iter().take(gossip_count) {
                    let gossip_message = WhisperMessage::Gossip {
                        cluster_data: our_cluster_data.clone(),
                    };
                    
                    match WhisperServer::send_whisper_message(target_node, whisper_port, gossip_message).await {
                        Ok(response) => {
                            debug!("Gossip to {} successful", target_node);
                            
                            // Process any gossip data we received back
                            if let Some(WhisperMessage::Gossip { cluster_data }) = response.data {
                                let mut state_guard = state.write().unwrap();
                                if state_guard.cluster.update_from_gossip(cluster_data) {
                                    info!("Cluster state updated from gossip response");
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to gossip with {}: {}", target_node, e);
                        }
                    }
                }
            }
            
            // Also send heartbeats to all nodes periodically (every 30 seconds)
            if interval.period().as_secs() % 30 == 0 {
                let our_node_id = {
                    let state_guard = state.read().unwrap();
                    state_guard.cluster.node_ids
                        .get(&our_address)
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_string())
                };
                
                for node_addr in &nodes {
                    if node_addr != &our_address {
                        let heartbeat = WhisperMessage::Heartbeat {
                            node_id: our_node_id.clone(),
                            address: our_address.clone(),
                        };
                        
                        match WhisperServer::send_whisper_message(node_addr, whisper_port, heartbeat).await {
                            Ok(_) => {
                                debug!("Heartbeat to {} successful", node_addr);
                            }
                            Err(e) => {
                                warn!("Failed to send heartbeat to {}: {}", node_addr, e);
                            }
                        }
                    }
                }
            }
        }
    }

    // Startup cluster synchronization - try to get cluster state from existing nodes
    async fn startup_cluster_sync(state: Arc<RwLock<ServerState>>) {
        let (our_address, whisper_port) = {
            let state_guard = state.read().unwrap();
            let our_address = state_guard.cluster.nodes.first().cloned().unwrap_or_default();
            let base_port = state_guard.cluster.nodes.first()
                .and_then(|addr| addr.split(':').nth(1))
                .and_then(|port_str| port_str.parse::<u16>().ok())
                .unwrap_or(6124);
            (our_address, base_port + 10000)
        };
        
        // Try to contact some common ports to find existing cluster nodes
        let common_ports = [6124, 6125, 6126, 6127, 6128];
        let our_ip = our_address.split(':').next().unwrap_or("127.0.0.1");
        
        for port in common_ports {
            if port != whisper_port - 10000 { // Don't try to contact ourselves
                let target_addr = format!("{}:{}", our_ip, port);
                let target_whisper_port = port + 10000;
                
                // Send a cluster request to see if there's a node there
                let request_message = WhisperMessage::ClusterRequest;
                
                match WhisperServer::send_whisper_message(&target_addr, target_whisper_port, request_message).await {
                    Ok(response) => {
                        if let Some(WhisperMessage::Gossip { cluster_data }) = response.data {
                            let mut state_guard = state.write().unwrap();
                            if state_guard.cluster.update_from_gossip(cluster_data) {
                                info!("Cluster state updated from startup sync with {}", target_addr);
                                break; // Found a node and got cluster state, we're done
                            }
                        }
                    }
                    Err(_) => {
                        // Node not found at this address, continue trying
                        debug!("No node found at {}", target_addr);
                    }
                }
            }
        }
    }

    // Initiate gossip with a specific node (used when cluster changes)
    pub async fn gossip_with_node(
        state: &Arc<RwLock<ServerState>>,
        target_addr: &str,
        whisper_port: u16
    ) {
        let cluster_data = {
            let state_guard = state.read().unwrap();
            state_guard.cluster.get_cluster_data()
        };
        
        let gossip_message = WhisperMessage::Gossip { cluster_data };
        
        match WhisperServer::send_whisper_message(target_addr, whisper_port, gossip_message).await {
            Ok(response) => {
                info!("Immediate gossip to {} successful", target_addr);
                
                // Process any gossip data we received back
                if let Some(WhisperMessage::Gossip { cluster_data }) = response.data {
                    let mut state_guard = state.write().unwrap();
                    if state_guard.cluster.update_from_gossip(cluster_data) {
                        info!("Cluster state updated from immediate gossip response");
                    }
                }
            }
            Err(e) => {
                warn!("Failed to gossip with {}: {}", target_addr, e);
            }
        }
    }
} 