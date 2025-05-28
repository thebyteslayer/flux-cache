use std::fs;
use serde::{Deserialize, Serialize};
use rand::Rng;
use chrono::{DateTime, Utc};

// Cluster state and slot management
pub const TOTAL_SLOTS: usize = 16384;
const CLUSTER_FILE: &str = "cluster.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSlots {
    pub node_id: String,
    pub address: String,
    pub slot_range: (usize, usize),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterData {
    pub timestamp: DateTime<Utc>,
    pub nodes: Vec<NodeSlots>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    pub nodes: Vec<String>, // list of node addresses
    pub node_ids: std::collections::HashMap<String, String>, // address -> node_id mapping
    pub slot_map: Vec<NodeSlots>, // slot assignments
    pub last_updated: DateTime<Utc>, // when cluster was last modified
    #[serde(skip)]
    pub cluster_enabled: bool, // whether clustering is enabled
}

impl ClusterState {
    pub fn new(self_addr: String, cluster_enabled: bool) -> Self {
        // Try to load existing cluster state first
        if cluster_enabled {
            if let Ok(mut existing_state) = Self::load_from_cluster_file() {
                existing_state.cluster_enabled = cluster_enabled;
                // Check if this node is already in the cluster
                if existing_state.nodes.contains(&self_addr) {
                    return existing_state;
                } else {
                    // Add this node to existing cluster
                    existing_state.add_node(self_addr);
                    return existing_state;
                }
            }
        }
        
        // No existing cluster state, create new one
        let node_id = Self::generate_node_id();
        let mut node_ids = std::collections::HashMap::new();
        node_ids.insert(self_addr.clone(), node_id);
        
        let mut state = ClusterState {
            nodes: vec![self_addr],
            node_ids,
            slot_map: vec![],
            last_updated: Utc::now(),
            cluster_enabled,
        };
        state.rebalance_slots();
        if cluster_enabled {
            state.write_cluster_file();
        }
        state
    }

    pub fn generate_node_id() -> String {
        let mut rng = rand::thread_rng();
        let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyz0123456789".chars().collect();
        (0..7)
            .map(|_| chars[rng.gen_range(0..chars.len())])
            .collect()
    }

    pub fn load_from_cluster_file() -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(CLUSTER_FILE)?;
        let cluster_data: ClusterData = serde_json::from_str(&content)?;
        
        // Extract unique node addresses and IDs from slot map
        let mut nodes: Vec<String> = Vec::new();
        let mut node_ids = std::collections::HashMap::new();
        
        for ns in &cluster_data.nodes {
            if !nodes.contains(&ns.address) {
                nodes.push(ns.address.clone());
                node_ids.insert(ns.address.clone(), ns.node_id.clone());
            }
        }
        nodes.sort();
        
        Ok(ClusterState {
            nodes,
            node_ids,
            slot_map: cluster_data.nodes,
            last_updated: cluster_data.timestamp,
            cluster_enabled: true,
        })
    }

    pub fn rebalance_slots(&mut self) {
        self.slot_map.clear();
        let n = self.nodes.len();
        if n == 0 { return; }
        let base = TOTAL_SLOTS / n;
        let extra = TOTAL_SLOTS % n;
        let mut slots = 0;
        // The first 'extra' nodes get (base+1) slots, the rest get 'base' slots
        for (i, addr) in self.nodes.iter().enumerate() {
            let count = if i < extra { base + 1 } else { base };
            let start = slots;
            let end = slots + count - 1;
            let node_id = self.node_ids.get(addr).cloned().unwrap_or_else(|| Self::generate_node_id());
            self.slot_map.push(NodeSlots {
                node_id,
                address: addr.clone(),
                slot_range: (start, end),
            });
            slots += count;
        }
        // Ensure all slots are covered
        assert_eq!(slots, TOTAL_SLOTS);
        self.last_updated = Utc::now();
    }

    pub fn write_cluster_file(&self) {
        if !self.cluster_enabled {
            return;
        }
        let cluster_data = ClusterData {
            timestamp: self.last_updated,
            nodes: self.slot_map.clone(),
        };
        let _ = fs::write(CLUSTER_FILE, serde_json::to_string_pretty(&cluster_data).unwrap());
    }

    pub fn add_node(&mut self, addr: String) {
        if !self.nodes.contains(&addr) {
            // Generate new node ID if not exists
            if !self.node_ids.contains_key(&addr) {
                self.node_ids.insert(addr.clone(), Self::generate_node_id());
            }
            self.nodes.push(addr);
            self.nodes.sort(); // keep order stable for slot assignment
            self.rebalance_slots();
            self.write_cluster_file();
        }
    }

    pub fn remove_node(&mut self, addr: String) -> bool {
        if let Some(pos) = self.nodes.iter().position(|x| *x == addr) {
            self.nodes.remove(pos);
            self.node_ids.remove(&addr); // Remove node ID mapping
            self.nodes.sort(); // keep order stable for slot assignment
            if !self.nodes.is_empty() {
                self.rebalance_slots();
            } else {
                self.slot_map.clear();
                self.last_updated = Utc::now();
            }
            self.write_cluster_file();
            true
        } else {
            false
        }
    }

    pub fn update_from_gossip(&mut self, gossip_data: ClusterData) -> bool {
        // Only update if the gossip data is newer
        if gossip_data.timestamp > self.last_updated {
            // Replace entire cluster state with the gossip data
            self.slot_map = gossip_data.nodes;
            self.last_updated = gossip_data.timestamp;
            
            // Rebuild nodes list from slot map
            let mut new_nodes: Vec<String> = Vec::new();
            let mut new_node_ids = std::collections::HashMap::new();
            
            for node_slot in &self.slot_map {
                if !new_nodes.contains(&node_slot.address) {
                    new_nodes.push(node_slot.address.clone());
                    new_node_ids.insert(node_slot.address.clone(), node_slot.node_id.clone());
                }
            }
            
            self.nodes = new_nodes;
            self.node_ids = new_node_ids;
            self.write_cluster_file();
            true
        } else {
            false
        }
    }

    pub fn get_cluster_json(&self) -> String {
        let cluster_data = ClusterData {
            timestamp: self.last_updated,
            nodes: self.slot_map.clone(),
        };
        serde_json::to_string_pretty(&cluster_data).unwrap_or_else(|_| "{}".to_string())
    }

    pub fn get_cluster_data(&self) -> ClusterData {
        ClusterData {
            timestamp: self.last_updated,
            nodes: self.slot_map.clone(),
        }
    }
} 