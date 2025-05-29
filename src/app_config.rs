use config::{Config, ConfigError, File};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct Timeout(pub u64);
impl Default for Timeout {
    fn default() -> Self {
        Timeout(30)
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct TLSSettings {
    pub ca_cert: String,
    pub certfile: String,
    pub keyfile: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct MQTTSettings {
    pub host: String,
    pub port: u16,

    pub tls: Option<TLSSettings>,

    #[serde(default)]
    pub connection_timeout: Timeout,
}

#[derive(Deserialize, Clone, Debug)]
pub struct RelaySettings {
    pub device: String,
    pub unit_id: u8,
    pub baud_rate: u32,
    pub timeout: Timeout,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Settings {
    pub mqtt: MQTTSettings,
    pub relay: RelaySettings,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let settings = Config::builder()
            .add_source(File::with_name("config/local"))
            .add_source(config::Environment::with_prefix("app"))
            .build()?;

        settings.try_deserialize()
    }
}
