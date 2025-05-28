use std::fs;
use std::path::Path;
use serde::{Deserialize, Serialize};
use toml;

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct FluxConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_cluster_enabled")]
    pub cluster_enabled: bool,
    #[serde(default = "default_public_ip")]
    pub public_ip: String,
    #[serde(default = "default_public_port")]
    pub public_port: u16,
}

fn default_bind() -> String { 
    "127.0.0.1".to_string() 
}

fn default_port() -> u16 { 
    6124 
}

fn default_cluster_enabled() -> bool {
    false
}

fn default_public_ip() -> String {
    "127.0.0.1".to_string()
}

fn default_public_port() -> u16 {
    6124
}

fn write_complete_config(config: &FluxConfig) {
    let conf_path = "flxc.toml";
    let toml_str = format!(
        "bind = \"{}\"\nport = {}\ncluster_enabled = {}\npublic_ip = \"{}\"\npublic_port = {}\n", 
        config.bind, 
        config.port, 
        config.cluster_enabled, 
        config.public_ip, 
        config.public_port
    );
    let _ = fs::write(conf_path, toml_str);
}

pub fn read_flux_toml() -> FluxConfig {
    let conf_path = "flxc.toml";
    if !Path::new(conf_path).exists() {
        // Create default config if missing
        let default = FluxConfig {
            bind: default_bind(),
            port: default_port(),
            cluster_enabled: default_cluster_enabled(),
            public_ip: default_public_ip(),
            public_port: default_public_port(),
        };
        write_complete_config(&default);
        return default;
    }
    
    let content = fs::read_to_string(conf_path).unwrap_or_default();
    
    // Check if required fields are missing from the raw content
    let has_bind = content.contains("bind");
    let has_port = content.contains("port");
    let has_cluster_enabled = content.contains("cluster_enabled");
    let has_public_ip = content.contains("public_ip");
    let has_public_port = content.contains("public_port");
    
    let parsed_config = match toml::from_str::<FluxConfig>(&content) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("[WARN] Could not parse flxc.toml: {e}. Using defaults.");
            FluxConfig::default()
        }
    };
    
    // If any field is missing from the file, rewrite it with complete config
    if !has_bind || !has_port || !has_cluster_enabled || !has_public_ip || !has_public_port {
        let complete_config = FluxConfig {
            bind: if has_bind { parsed_config.bind } else { default_bind() },
            port: if has_port { parsed_config.port } else { default_port() },
            cluster_enabled: if has_cluster_enabled { parsed_config.cluster_enabled } else { default_cluster_enabled() },
            public_ip: if has_public_ip { parsed_config.public_ip } else { default_public_ip() },
            public_port: if has_public_port { parsed_config.public_port } else { default_public_port() },
        };
        write_complete_config(&complete_config);
        return complete_config;
    }
    
    parsed_config
} 