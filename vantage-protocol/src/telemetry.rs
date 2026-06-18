use serde::{Deserialize, Serialize};

/// A single temperature sensor reading.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TempReading {
    pub label: String,
    pub celsius: f32,
}

/// Host/device metrics sampled by the robot and shown beside the video.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub cpu_percent: f32,
    pub mem_used_mb: u64,
    pub mem_total_mb: u64,
    pub temps: Vec<TempReading>,
    pub uptime_s: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_info_json_round_trips() {
        let info = DeviceInfo {
            cpu_percent: 12.5,
            mem_used_mb: 2048,
            mem_total_mb: 8192,
            temps: vec![TempReading { label: "cpu".into(), celsius: 47.0 }],
            uptime_s: 3600,
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: DeviceInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info, back);
    }
}
