use std::{env, path::Path};

pub struct Options {

}

impl Options {
    pub fn platform_specific_root() -> String {
        if env::consts::OS == "windows" {
            Path::new(&env::var("LOCALAPPDATA").unwrap())
                .join("nexus-agent")
                .to_string_lossy()
                .to_string()
        }
        else {
            Path::new(&env::var("HOME").unwrap())
                .join(".local")
                .join(".share")
                .join(".nexus-agent")
                .to_string_lossy()
                .to_string()
        }
    }

    pub fn config_folder_path() -> String {
        env::var("NEXUSAGENT_PATHS__CONFIG").unwrap_or_else(|_| {
            Path::new(&Self::platform_specific_root())
                .join("config")
                .to_string_lossy()
                .to_string()
        })
    }

    pub fn packages_folder_path() -> String {
        env::var("NEXUSAGENT_PATHS__PACKAGES").unwrap_or_else(|_| {
            Path::new(&Self::platform_specific_root())
                .join("packages")
                .to_string_lossy()
                .to_string()
        })
    }

    pub fn json_rpc_listen_address() -> String {
        env::var("NEXUSAGENT_SYSTEM__JSONRPCLISTENADDRESS").unwrap_or("0.0.0.0".to_string())
    }

    pub fn json_rpc_listen_port() -> u16 {
        env::var("NEXUSAGENT_SYSTEM__JSONRPCLISTENPORT")
            .ok()
            .and_then(|port| port.parse::<u16>().ok())
            .unwrap_or(56145)
    }
}