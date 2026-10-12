//! Wall-clock time as the Kafka protocol and our catalog tables carry it.

/// Milliseconds since the Unix epoch; zero if the system clock is set before it.
pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
