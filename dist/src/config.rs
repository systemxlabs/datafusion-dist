use std::time::Duration;

#[derive(Debug, derive_with::With)]
pub struct DistConfig {
    pub heartbeat_interval: Duration,
    pub stage0_task_poll_timeout: Duration,
    pub job_ttl: Duration,
    /// Interval of the background stage cleanup checks. Also controls how
    /// precisely `job_ttl` and `stage_inactivity_timeout` are enforced.
    pub job_ttl_check_interval: Duration,
    /// Cleans up a stage whose running tasks have made no progress for this
    /// long, even if the job itself is not expired yet.
    pub stage_inactivity_timeout: Duration,
    pub event_queue_size: usize,
}

impl DistConfig {
    pub fn new() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(20),
            stage0_task_poll_timeout: Duration::from_secs(10),
            job_ttl: Duration::from_secs(30 * 60),
            job_ttl_check_interval: Duration::from_secs(5 * 60),
            stage_inactivity_timeout: Duration::from_secs(10 * 60),
            event_queue_size: 8 * 1024,
        }
    }
}

impl Default for DistConfig {
    fn default() -> Self {
        Self::new()
    }
}
