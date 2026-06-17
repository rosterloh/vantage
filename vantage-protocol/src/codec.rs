use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("encode failed: {0}")]
    Encode(String),
    #[error("decode failed: {0}")]
    Decode(String),
}

/// Encode a wire value. JSON during bring-up (readable in logs); swapping the two
/// bodies below to `bincode` is the entire codec change (see design.md §7).
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    serde_json::to_vec(value).map_err(|e| CodecError::Encode(e.to_string()))
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    serde_json::from_slice(bytes).map_err(|e| CodecError::Decode(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{DeviceInfo, TempReading};

    #[test]
    fn encode_decode_round_trips() {
        let info = DeviceInfo {
            cpu_percent: 3.0,
            mem_used_mb: 100,
            mem_total_mb: 200,
            temps: vec![TempReading { label: "soc".into(), celsius: 40.0 }],
            uptime_s: 10,
        };
        let bytes = encode(&info).unwrap();
        let back: DeviceInfo = decode(&bytes).unwrap();
        assert_eq!(info, back);
    }

    #[test]
    fn decode_garbage_is_an_error_not_a_panic() {
        let err = decode::<DeviceInfo>(b"not json").unwrap_err();
        assert!(matches!(err, CodecError::Decode(_)));
    }
}
