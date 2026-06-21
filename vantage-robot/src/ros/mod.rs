//! Feature-gated ROS 2 camera bridge. The ONLY place `rclrs`/`sensor_msgs`
//! appear. Maps the pure parts from `crate::convert` onto ROS messages and
//! publishes `~/image_raw` + `~/camera_info`.

use rclrs::{Context, CreateBasicExecutor, Node, Publisher, RclrsError, SpinOptions};
use vantage_signalling::peer::RawFrame;

use crate::convert::{camera_info_parts, image_parts};

pub struct CameraBridge {
    _node: Node,
    image_pub: Publisher<sensor_msgs::msg::Image>,
    info_pub: Publisher<sensor_msgs::msg::CameraInfo>,
    frame_id: String,
}

impl CameraBridge {
    /// Create the ROS context/node/publishers and start servicing the node by
    /// spinning its executor on a dedicated OS thread (so the tokio runtime is
    /// never blocked). Publishing does not depend on the spin, but spinning
    /// keeps the node live for discovery.
    pub fn new() -> Result<Self, RclrsError> {
        let context = Context::default_from_env()?;
        let executor = context.create_basic_executor();
        let node_name =
            std::env::var("VANTAGE_CAMERA_NODE").unwrap_or_else(|_| "vantage_camera".to_string());
        let node = executor.create_node(node_name.as_str())?;
        // Relative (`~/`) topics so they remap/namespace. Default QoS; rmw-agnostic.
        let image_pub = node.create_publisher::<sensor_msgs::msg::Image>("~/image_raw")?;
        let info_pub = node.create_publisher::<sensor_msgs::msg::CameraInfo>("~/camera_info")?;
        let frame_id = std::env::var("VANTAGE_CAMERA_FRAME_ID")
            .unwrap_or_else(|_| "camera_optical_frame".to_string());

        std::thread::spawn(move || {
            let mut executor = executor;
            let errs = executor.spin(SpinOptions::default());
            if !errs.is_empty() {
                tracing::warn!("rclrs executor stopped: {errs:?}");
            }
        });

        Ok(Self { _node: node, image_pub, info_pub, frame_id })
    }

    /// Publish one frame as Image + CameraInfo, sharing one timestamp + frame_id.
    pub fn publish(&self, frame: RawFrame) -> Result<(), RclrsError> {
        let ci = camera_info_parts(&frame); // borrow first...
        let parts = image_parts(frame); // ...then move the pixel buffer.
        let (sec, nanosec) = now_secs_nanos();

        // Build via Default + field mutation so we only ever name `sensor_msgs`
        // (header/stamp nested types come from Default).
        let mut img = sensor_msgs::msg::Image::default();
        img.header.stamp.sec = sec;
        img.header.stamp.nanosec = nanosec;
        img.header.frame_id = self.frame_id.clone();
        img.height = parts.height;
        img.width = parts.width;
        img.encoding = parts.encoding;
        img.is_bigendian = parts.is_bigendian;
        img.step = parts.step;
        img.data = parts.data;
        self.image_pub.publish(img)?;

        let mut info = sensor_msgs::msg::CameraInfo::default();
        info.header.stamp.sec = sec;
        info.header.stamp.nanosec = nanosec;
        info.header.frame_id = self.frame_id.clone();
        info.height = ci.height;
        info.width = ci.width;
        info.distortion_model = ci.distortion_model;
        self.info_pub.publish(info)?;
        Ok(())
    }
}

/// Wall-clock stamp as (sec, nanosec). Uses the system clock rather than the
/// rclrs node clock to avoid the `lyrical -> rolling` clock-API drift risk;
/// sufficient for 4b (placeholder calibration).
fn now_secs_nanos() -> (i32, u32) {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_secs() as i32, d.subsec_nanos())
}
