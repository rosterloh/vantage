//! Pure, ROS-free conversion of pre-encode tee frames into plain message-body
//! descriptions. `ros/mod.rs` maps these onto `sensor_msgs` types; this module
//! never references `rclrs`/`sensor_msgs`, so it compiles and tests in the
//! default (ROS-free) lane.

use vantage_signalling::peer::RawFrame;

/// Plain description of a `sensor_msgs/Image` body.
pub struct ImageParts {
    pub height: u32,
    pub width: u32,
    pub encoding: String, // "rgb8"
    pub is_bigendian: u8, // 0
    pub step: u32,        // width * 3
    pub data: Vec<u8>,    // moved from RawFrame.data
}

/// Minimal `sensor_msgs/CameraInfo` body. No real calibration in 4b — the
/// streamer owns the device; width/height + header must match the image.
pub struct CameraInfoParts {
    pub height: u32,
    pub width: u32,
    pub distortion_model: String, // "plumb_bob"
}

/// Convert a raw tee frame into Image body parts. Moves the pixel buffer (no copy).
/// `rgb8` is 3 bytes/pixel, tightly packed, so `step = width * 3`.
pub fn image_parts(frame: RawFrame) -> ImageParts {
    ImageParts {
        height: frame.height,
        width: frame.width,
        is_bigendian: 0,
        step: frame.width * 3,
        encoding: frame.encoding,
        data: frame.data,
    }
}

/// Derive a placeholder `CameraInfo` body from a frame. Borrows (does not consume)
/// so the same frame can be handed to `image_parts` afterwards.
pub fn camera_info_parts(frame: &RawFrame) -> CameraInfoParts {
    CameraInfoParts {
        height: frame.height,
        width: frame.width,
        distortion_model: "plumb_bob".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vantage_signalling::peer::RawFrame;

    fn frame(w: u32, h: u32) -> RawFrame {
        RawFrame {
            width: w,
            height: h,
            encoding: "rgb8".to_string(),
            data: vec![0u8; (w * h * 3) as usize],
        }
    }

    #[test]
    fn image_parts_sets_step_and_preserves_encoding_and_data() {
        let parts = image_parts(frame(4, 2));
        assert_eq!(parts.width, 4);
        assert_eq!(parts.height, 2);
        assert_eq!(parts.step, 12); // width * 3
        assert_eq!(parts.encoding, "rgb8");
        assert_eq!(parts.is_bigendian, 0);
        assert_eq!(parts.data.len(), 4 * 2 * 3);
    }

    #[test]
    fn camera_info_matches_image_dimensions() {
        let info = camera_info_parts(&frame(640, 480));
        assert_eq!(info.width, 640);
        assert_eq!(info.height, 480);
        assert_eq!(info.distortion_model, "plumb_bob");
    }
}
