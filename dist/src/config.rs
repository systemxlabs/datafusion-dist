use std::time::Duration;

#[derive(Debug, derive_with::With)]
pub struct DistConfig {
    pub heartbeat_interval: Duration,
    pub stage0_task_poll_timeout: Duration,
    pub job_ttl: Duration,
    pub job_ttl_check_interval: Duration,
}

impl DistConfig {
    pub fn new() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(20),
            stage0_task_poll_timeout: Duration::from_secs(10),
            job_ttl: Duration::from_secs(12 * 60 * 60),
            job_ttl_check_interval: Duration::from_secs(10 * 60),
        }
    }
}

impl Default for DistConfig {
    fn default() -> Self {
        Self::new()
    }
}
