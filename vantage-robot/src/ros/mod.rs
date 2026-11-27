//! Feature-gated ROS 2 bridge, the only place rclrs appears. With
//! `VANTAGE_VIDEO_SOURCE=ros` it subscribes to an image topic and feeds the
//! pipeline; otherwise it publishes the camera's raw frames as `~/image_raw` +
//! `~/camera_info`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Result, anyhow};
use rclrs::{
    Context, CreateBasicExecutor, Executor, IntoPrimitiveOptions, Node, Publisher, SpinOptions,
    Subscription,
};
use ros_env::sensor_msgs::msg::rmw::{CameraInfo, Image};
use rosidl_runtime_rs::Sequence;
use vantage_signalling::peer::RawFrame;
use vantage_signalling::robot_media::{FrameInput, FrameLayout, RobotMedia};

use crate::convert::{camera_info_parts, image_parts, pixel_format};
use crate::metrics::RobotMetrics;

/// Holds the node and its subscription or publishers; dropping it unsubscribes.
pub struct Bridge {
    _node: Node,
    _subscription: Option<Subscription<Image>>,
}

/// Create the node, wire it to `media` and spin it on its own thread so the
/// tokio runtime is never blocked.
pub fn start(media: &Arc<RobotMedia>, metrics: &RobotMetrics) -> Result<Bridge> {
    let executor = Context::default_from_env()?.create_basic_executor();
    let name = std::env::var("VANTAGE_CAMERA_NODE").unwrap_or_else(|_| "vantage_camera".into());
    let node = executor.create_node(name.as_str())?;
    let subscription = match media.frame_input() {
        Some(input) => Some(subscribe(&node, input)?),
        None => {
            publish(&node, media.clone(), metrics.clone())?;
            None
        }
    };
    std::thread::Builder::new()
        .name("ros-spin".into())
        .spawn(move || spin(executor))?;
    Ok(Bridge {
        _node: node,
        _subscription: subscription,
    })
}

/// Spin until shutdown. A failed take is a spurious wake-up; other errors are
/// logged, not fatal, since video is not worth stopping the robot for.
fn spin(mut executor: Executor) {
    loop {
        let errors = executor.spin(SpinOptions::default());
        if errors.is_empty() {
            return;
        }
        let real: Vec<_> = errors.iter().filter(|e| !e.is_take_failed()).collect();
        if !real.is_empty() {
            tracing::warn!("ros spin: {real:?}");
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// The message's own buffer, handed to GStreamer without a copy.
struct ImageData(Image);

impl AsRef<[u8]> for ImageData {
    fn as_ref(&self) -> &[u8] {
        self.0.data.as_slice()
    }
}

fn layout(msg: &Image) -> Result<FrameLayout> {
    let encoding = msg.encoding.to_string();
    let format =
        pixel_format(&encoding).ok_or_else(|| anyhow!("unsupported encoding {encoding:?}"))?;
    Ok(FrameLayout {
        format,
        width: msg.width,
        height: msg.height,
        stride: msg.step,
    })
}

fn subscribe(node: &Node, input: FrameInput) -> Result<Subscription<Image>> {
    let topic = std::env::var("VANTAGE_ROS_IMAGE_TOPIC").unwrap_or_else(|_| "image_raw".into());
    tracing::info!("subscribing to ros images on {topic}");
    let received = AtomicU64::new(0);
    // The rmw type skips rclrs's per-element conversion. Best effort, depth 1: a
    // slow encoder skips frames instead of queueing them.
    let subscription = node.create_subscription::<Image, _>(
        topic.as_str().best_effort().keep_last(1),
        move |msg: Image| {
            let n = received.fetch_add(1, Ordering::Relaxed) + 1;
            let (w, h) = (msg.width, msg.height);
            let pushed = layout(&msg).and_then(|l| input.push(l, ImageData(msg)));
            if n == 1 || n.is_multiple_of(300) {
                match pushed {
                    Ok(()) => tracing::info!("ros image {w}x{h} (#{n})"),
                    Err(e) => tracing::warn!("ros image #{n} dropped: {e}"),
                }
            }
        },
    )?;
    Ok(subscription)
}

/// One reused message pair, so a steady stream costs one copy a frame.
struct Publishers {
    image: Publisher<Image>,
    info: Publisher<CameraInfo>,
    msgs: Mutex<(Image, CameraInfo)>,
}

/// Publish the raw (pre-encode) branch, drained once for the engine's lifetime.
fn publish(node: &Node, media: Arc<RobotMedia>, metrics: RobotMetrics) -> Result<()> {
    let frame_id = std::env::var("VANTAGE_CAMERA_FRAME_ID")
        .unwrap_or_else(|_| "camera_optical_frame".to_string());
    let (mut image, mut info) = (Image::default(), CameraInfo::default());
    image.header.frame_id = frame_id.as_str().into();
    info.header.frame_id = frame_id.as_str().into();
    // Relative (`~/`) topics so they remap/namespace. Default QoS; rmw-agnostic.
    let publishers = Publishers {
        image: node.create_publisher::<Image>("~/image_raw")?,
        info: node.create_publisher::<CameraInfo>("~/camera_info")?,
        msgs: Mutex::new((image, info)),
    };
    tokio::spawn(async move {
        let mut n: u64 = 0;
        while let Some(frame) = media.recv_raw_frame().await {
            n += 1;
            metrics.frame_published();
            let (w, h) = (frame.width, frame.height);
            match publishers.publish(frame) {
                Ok(()) => {
                    if n == 1 || n.is_multiple_of(30) {
                        tracing::info!("published ros image {w}x{h} (#{n})");
                    }
                }
                Err(e) => tracing::warn!("ros publish failed: {e}"),
            }
        }
        tracing::warn!("raw-frame drain task exited");
    });
    Ok(())
}

impl Publishers {
    /// Publish one frame as Image + CameraInfo, sharing one timestamp + frame_id.
    fn publish(&self, frame: RawFrame) -> Result<()> {
        let (sec, nanosec) = secs_nanos(frame.stamp);
        let ci = camera_info_parts(&frame); // borrow first...
        let parts = image_parts(frame); // ...then move the pixel buffer.
        let mut msgs = self.msgs.lock().unwrap();
        let (image, info) = &mut *msgs;

        image.header.stamp.sec = sec;
        image.header.stamp.nanosec = nanosec;
        image.height = parts.height;
        image.width = parts.width;
        image.encoding = parts.encoding.as_str().into();
        image.is_bigendian = parts.is_bigendian;
        image.step = parts.step;
        if image.data.as_slice().len() != parts.data.len() {
            image.data = Sequence::new(parts.data.len());
        }
        image.data.as_mut_slice().copy_from_slice(&parts.data);
        self.image.publish(&*image)?;

        info.header.stamp.sec = sec;
        info.header.stamp.nanosec = nanosec;
        info.height = ci.height;
        info.width = ci.width;
        info.distortion_model = ci.distortion_model.as_str().into();
        self.info.publish(&*info)?;
        Ok(())
    }
}

/// Wall-clock time as a ROS stamp (sec, nanosec).
fn secs_nanos(t: std::time::SystemTime) -> (i32, u32) {
    let d = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    (d.as_secs().min(i32::MAX as u64) as i32, d.subsec_nanos())
}
