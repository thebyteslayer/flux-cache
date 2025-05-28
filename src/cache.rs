use std::collections::HashMap;
use bytes::Bytes;
use thiserror::Error;
use crate::cluster::ClusterState;

// Custom error type
#[derive(Error, Debug)]
pub enum ServerError {
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
pub struct CacheEntry {
    pub compressed_data: Bytes,
}

// Server state
pub struct ServerState {
    pub cache: HashMap<String, CacheEntry>,
    pub cluster: ClusterState,
    pub cluster_enabled: bool,
}

impl ServerState {
    pub fn new(self_addr: String, cluster_enabled: bool) -> Self {
        ServerState {
            cache: HashMap::new(),
            cluster: ClusterState::new(self_addr, cluster_enabled),
            cluster_enabled,
        }
    }

    // Compress data using zstd
    pub fn compress_data(&self, data: &[u8]) -> Result<Bytes, ServerError> {
        let compressed = zstd::encode_all(data, 3)
            .map_err(|e| ServerError::Compression(e.to_string()))?;
        Ok(Bytes::from(compressed))
    }

    // Decompress data using zstd
    pub fn decompress_data(&self, data: &[u8]) -> Result<Vec<u8>, ServerError> {
        let decompressed = zstd::decode_all(data)
            .map_err(|e| ServerError::Compression(e.to_string()))?;
        Ok(decompressed)
    }
} 