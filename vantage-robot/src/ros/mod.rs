//! Feature-gated ROS 2 camera bridge. The ONLY place `r2r` appears. Maps the
//! pure parts from `crate::convert` onto ROS messages and publishes
//! `~/image_raw` + `~/camera_info`.

use std::sync::Mutex;
use std::time::Duration;

use r2r::sensor_msgs::msg::{CameraInfo, Image};
use r2r::{Context, Error, Node, Publisher, QosProfile};
use vantage_signalling::peer::RawFrame;

use crate::convert::{camera_info_parts, image_parts};

pub struct CameraBridge {
    // r2r's Publisher is Send but !Sync. main.rs shares the bridge as
    // Arc<CameraBridge> across tokio tasks, which needs Sync, so wrap each
    // publisher in a Mutex (Mutex<T: Send> is Sync). No await is held across the
    // lock in publish(), so a std Mutex is correct here.
    image_pub: Mutex<Publisher<Image>>,
    info_pub: Mutex<Publisher<CameraInfo>>,
    frame_id: String,
}

impl CameraBridge {
    /// Create the ROS context/node/publishers and start servicing the node by
    /// spinning it on a dedicated OS thread (so the tokio runtime is never
    /// blocked). Publishing does not depend on the spin, but spinning keeps the
    /// node live for discovery.
    pub fn new() -> Result<Self, Error> {
        let ctx = Context::create()?;
        let node_name =
            std::env::var("VANTAGE_CAMERA_NODE").unwrap_or_else(|_| "vantage_camera".to_string());
        let mut node = Node::create(ctx, &node_name, "")?;
        // Relative (`~/`) topics so they remap/namespace. Default QoS; rmw-agnostic.
        let image_pub = node.create_publisher::<Image>("~/image_raw", QosProfile::default())?;
        let info_pub =
            node.create_publisher::<CameraInfo>("~/camera_info", QosProfile::default())?;
        let frame_id = std::env::var("VANTAGE_CAMERA_FRAME_ID")
            .unwrap_or_else(|_| "camera_optical_frame".to_string());

        std::thread::spawn(move || {
            let mut node = node;
            loop {
                node.spin_once(Duration::from_millis(100));
            }
        });

        Ok(Self {
            image_pub: Mutex::new(image_pub),
            info_pub: Mutex::new(info_pub),
            frame_id,
        })
    }

    /// Publish one frame as Image + CameraInfo, sharing one timestamp + frame_id.
    pub fn publish(&self, frame: RawFrame) -> Result<(), Error> {
        let ci = camera_info_parts(&frame); // borrow first...
        let parts = image_parts(frame); // ...then move the pixel buffer.
        let (sec, nanosec) = now_secs_nanos();

        // Build via Default + field mutation so we only ever name the message
        // types (header/stamp nested types come from Default).
        let mut img = Image::default();
        img.header.stamp.sec = sec;
        img.header.stamp.nanosec = nanosec;
        img.header.frame_id = self.frame_id.clone();
        img.height = parts.height;
        img.width = parts.width;
        img.encoding = parts.encoding;
        img.is_bigendian = parts.is_bigendian;
        img.step = parts.step;
        img.data = parts.data;
        self.image_pub.lock().unwrap().publish(&img)?;

        let mut info = CameraInfo::default();
        info.header.stamp.sec = sec;
        info.header.stamp.nanosec = nanosec;
        info.header.frame_id = self.frame_id.clone();
        info.height = ci.height;
        info.width = ci.width;
        info.distortion_model = ci.distortion_model;
        self.info_pub.lock().unwrap().publish(&info)?;
        Ok(())
    }
}

/// Wall-clock stamp as (sec, nanosec). Uses the system clock rather than the ROS
/// node clock; sufficient for 4b (placeholder calibration).
fn now_secs_nanos() -> (i32, u32) {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_secs() as i32, d.subsec_nanos())
}
