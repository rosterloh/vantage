use anyhow::{Context, Result};
use gstreamer as gst;
use gstreamer::prelude::*;

/// Ordered H.264 encoder candidates: hardware first, software (`x264enc`) last.
/// design.md §8: nvv4l2h264enc (Jetson) → nvh264enc (desktop NVIDIA) →
/// vah264enc / vaapih264enc (Intel/AMD VAAPI) → qsvh264enc (Intel QSV) →
/// vtenc_h264 (macOS) → x264enc (software).
pub(crate) const CANDIDATES: &[&str] = &[
    "nvv4l2h264enc",
    "nvh264enc",
    "vah264enc",
    "vaapih264enc",
    "qsvh264enc",
    "vtenc_h264",
    "x264enc",
];

/// The factory name of the encoder that would be selected on this host, if any.
pub fn selected_encoder_name() -> Option<&'static str> {
    CANDIDATES
        .iter()
        .find(|n| gst::ElementFactory::find(n).is_some())
        .copied()
}

/// Build the first available H.264 encoder, configured for low latency (~1.5 Mbit/s,
/// ~1 s GOP at 30 fps, no B-frames). A downstream `h264parse` normalizes the output,
/// so the caps contract to `rtph264pay` is identical regardless of which is selected.
pub fn make_h264_encoder() -> Result<gst::Element> {
    let name = selected_encoder_name()
        .context("no H.264 encoder found (install gst-plugins-ugly for x264enc)")?;
    let enc = gst::ElementFactory::make(name)
        .build()
        .with_context(|| format!("failed to build encoder {name}"))?;
    configure_low_latency(&enc, name);
    tracing::info!("selected H.264 encoder: {name}");
    Ok(enc)
}

/// Set low-latency properties per encoder. Property names differ between encoders;
/// each arm sets only what that element understands. Bitrate UNITS differ (noted).
fn configure_low_latency(enc: &gst::Element, name: &str) {
    match name {
        "x264enc" => {
            enc.set_property_from_str("tune", "zerolatency");
            enc.set_property_from_str("speed-preset", "ultrafast");
            enc.set_property("bitrate", 1500u32); // kbit/s
            enc.set_property("key-int-max", 30u32);
        }
        "nvv4l2h264enc" => {
            // Jetson V4L2 encoder — bitrate in bits/s.
            enc.set_property("bitrate", 1_500_000u32);
            enc.set_property("iframeinterval", 30u32);
            enc.set_property("insert-sps-pps", true);
            enc.set_property("maxperf-enable", true);
        }
        "nvh264enc" => {
            // Desktop NVENC — bitrate in kbit/s.
            enc.set_property("bitrate", 1500u32);
            enc.set_property_from_str("preset", "low-latency-hp");
            enc.set_property("zerolatency", true);
        }
        "vah264enc" | "vaapih264enc" => {
            // VAAPI — bitrate in kbit/s.
            enc.set_property("bitrate", 1500u32);
            enc.set_property_from_str("rate-control", "cbr");
            enc.set_property("key-int-max", 30u32);
        }
        "qsvh264enc" => {
            // Intel QSV — bitrate in kbit/s.
            enc.set_property("bitrate", 1500u32);
            enc.set_property_from_str("rate-control", "cbr");
        }
        "vtenc_h264" => {
            // macOS VideoToolbox — bitrate in kbit/s.
            enc.set_property("bitrate", 1500u32);
            enc.set_property("realtime", true);
            enc.set_property("allow-frame-reordering", false);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_builds_an_available_encoder() {
        gst::init().unwrap();
        let enc = make_h264_encoder().expect("an H.264 encoder must be available");
        let factory_name = enc.factory().expect("element has a factory").name();
        assert!(
            CANDIDATES.contains(&factory_name.as_str()),
            "selected encoder {factory_name} not in the candidate list"
        );
    }

    #[test]
    fn selection_is_deterministic_and_present() {
        gst::init().unwrap();
        assert!(selected_encoder_name().is_some(), "expected at least x264enc");
    }
}
