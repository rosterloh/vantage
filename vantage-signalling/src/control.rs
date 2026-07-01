//! Reliability options for the operator→robot control data channel.

use gstreamer as gst;

/// Options for the `control` data channel: unreliable + unordered so a lost or
/// late teleop command never head-of-line-blocks the next one (latest-command-wins).
/// Safety comes from the robot's disconnect watchdog, not retransmission.
pub(crate) fn control_dc_options() -> gst::Structure {
    gst::Structure::builder("config")
        .field("ordered", false)
        .field("max-retransmits", 0i32)
        .build()
}
