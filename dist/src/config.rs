use std::time::Duration;

#[derive(Debug)]
pub struct DistConfig {
    pub heartbeat_interval: Duration,
}

impl DistConfig {
    pub fn new() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(20),
        }
    }
}

impl Default for DistConfig {
    fn default() -> Self {
        Self::new()
    }
}
