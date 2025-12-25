use std::time::Duration;

#[derive(Debug)]
pub struct DistConfig {
    pub heartbeat_interval: Duration,
    pub stage0_task_poll_timeout: Duration,
}

impl DistConfig {
    pub fn new() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(20),
            stage0_task_poll_timeout: Duration::from_secs(10),
        }
    }
}

impl Default for DistConfig {
    fn default() -> Self {
        Self::new()
    }
}
