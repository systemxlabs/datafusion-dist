use std::time::{SystemTime, UNIX_EPOCH};

pub fn timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as i64
}

pub fn get_local_ip() -> String {
    local_ip_address::local_ip()
        .expect("Failed to get local IP")
        .to_string()
}
