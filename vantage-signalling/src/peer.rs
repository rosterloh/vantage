use anyhow::{Context, Result};
use gstreamer as gst;
use gstreamer::glib;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use gstreamer_sdp as gst_sdp;
use gstreamer_video as gst_video;
use gstreamer_video::prelude::*;
use gstreamer_webrtc as gst_webrtc;
use std::sync::Arc;
use tokio::sync::mpsc;
use vantage_protocol::control::{ControlMsg, CONTROL_LABEL};
use vantage_protocol::signalling::{IceServer, Signal};

#[derive(Debug)]
pub enum PeerEvent {
    /// Offer or Answer to forward via coordinator.
    LocalDescription(Signal),
    /// Signal::Ice to forward.
    LocalIce(Signal),
    DataChannelOpen,
    /// Bytes received on the telemetry data channel.
    DataMessage(Vec<u8>),
    /// Bytes received on the `control` data channel (operator→robot teleop).
    Control(Vec<u8>),
}

/// One decoded RGBA video frame handed to the UI.
pub struct VideoFrame {
    pub width: u32,
    pub height: u32,
    pub rgba: Vec<u8>, // tightly packed, width*height*4 bytes
}

/// One raw camera frame from the pre-encode tee branch (RGB888). Consumed by the
/// ROS2 bridge in Phase 4b; logged for concurrency verification in Phase 4a.
pub struct RawFrame {
    pub width: u32,
    pub height: u32,
    pub encoding: String, // "rgb8"
    pub data: Vec<u8>,    // tightly packed, width*height*3 bytes
}

pub struct Peer {
    pub pipeline: gst::Pipeline,
    pub webrtcbin: gst::Element,
    events_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<PeerEvent>>,
    events_tx: mpsc::UnboundedSender<PeerEvent>,
    data_channel: std::sync::Mutex<Option<gst_webrtc::WebRTCDataChannel>>,
    /// The operator→robot `control` channel, populated when the robot's control DC
    /// arrives via `on-data-channel` (shared with that closure, hence `Arc`).
    control_channel: Arc<std::sync::Mutex<Option<gst_webrtc::WebRTCDataChannel>>>,
    frames_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<VideoFrame>>,
    frames_tx: mpsc::UnboundedSender<VideoFrame>,
}

impl Peer {
    /// Build the client (ANSWERER) peer: receives the telemetry data channel and the
    /// incoming video track. The robot send/encode path now lives in `RobotMedia`.
    pub fn new(ice_servers: &[IceServer]) -> Result<Self> {
        gst::init()?;
        let pipeline = gst::Pipeline::new();
        let webrtcbin = gst::ElementFactory::make("webrtcbin")
            .name("sendrecv")
            .property("bundle-policy", gst_webrtc::WebRTCBundlePolicy::MaxBundle)
            .build()
            .context("webrtcbin missing — install gst-plugins-bad")?;

        configure_ice(&webrtcbin, ice_servers);

        pipeline.add(&webrtcbin)?;
        let (tx, rx) = mpsc::unbounded_channel();

        wire_local_ice(&webrtcbin, &tx);

        let (frames_tx, frames_rx) = mpsc::unbounded_channel::<VideoFrame>();

        let peer = Self {
            pipeline,
            webrtcbin,
            events_rx: tokio::sync::Mutex::new(rx),
            events_tx: tx.clone(),
            data_channel: std::sync::Mutex::new(None),
            control_channel: Arc::new(std::sync::Mutex::new(None)),
            frames_rx: tokio::sync::Mutex::new(frames_rx),
            frames_tx: frames_tx.clone(),
        };

        // The SCTP transport is only available once the pipeline is PLAYING; the decode
        // branch elements sync their state when the incoming pad is added.
        peer.pipeline.set_state(gst::State::Playing)?;

        let tx2 = tx.clone();
        let control_slot = peer.control_channel.clone();
        peer.webrtcbin.connect("on-data-channel", false, move |vals| {
            let dc = vals[1].get::<gst_webrtc::WebRTCDataChannel>().unwrap();
            // The robot creates two channels on one SCTP transport (no renegotiation);
            // route by label — `control` is our operator→robot send channel.
            if dc.property::<String>("label") == CONTROL_LABEL {
                *control_slot.lock().unwrap() = Some(dc);
            } else {
                wire_data_channel(&dc, &tx2);
            }
            None
        });
        wire_video_receiver(&peer.pipeline, &peer.webrtcbin, &peer.frames_tx);

        Ok(peer)
    }

    /// Await the next event the app must act on (forward signalling / handle data).
    pub async fn recv_event(&self) -> Option<PeerEvent> {
        self.events_rx.lock().await.recv().await
    }

    /// Await the next decoded video frame (RGBA). Returns None when the pipeline ends.
    pub async fn recv_frame(&self) -> Option<VideoFrame> {
        self.frames_rx.lock().await.recv().await
    }

    /// Apply a Signal received from the remote peer (offer/answer/ice).
    pub fn handle_signal(&self, signal: Signal) -> Result<()> {
        match signal {
            Signal::Offer { sdp } => {
                let desc = parse_sdp(&sdp, gst_webrtc::WebRTCSDPType::Offer)?;
                self.webrtcbin.emit_by_name::<()>(
                    "set-remote-description",
                    &[&desc, &None::<gst::Promise>],
                );
                let bin = self.webrtcbin.clone();
                let tx = self.events_tx.clone();
                let promise = gst::Promise::with_change_func(move |reply| {
                    let reply = match reply {
                        Ok(Some(r)) => r,
                        _ => return,
                    };
                    let answer = reply
                        .value("answer")
                        .unwrap()
                        .get::<gst_webrtc::WebRTCSessionDescription>()
                        .unwrap();
                    bin.emit_by_name::<()>(
                        "set-local-description",
                        &[&answer, &None::<gst::Promise>],
                    );
                    let sdp = answer.sdp().as_text().unwrap();
                    let _ = tx.send(PeerEvent::LocalDescription(Signal::Answer { sdp }));
                });
                self.webrtcbin
                    .emit_by_name::<()>("create-answer", &[&None::<gst::Structure>, &promise]);
            }
            Signal::Answer { sdp } => {
                let desc = parse_sdp(&sdp, gst_webrtc::WebRTCSDPType::Answer)?;
                self.webrtcbin.emit_by_name::<()>(
                    "set-remote-description",
                    &[&desc, &None::<gst::Promise>],
                );
            }
            Signal::Ice {
                candidate,
                sdp_mline_index,
            } => {
                self.webrtcbin
                    .emit_by_name::<()>("add-ice-candidate", &[&sdp_mline_index, &candidate]);
            }
        }
        Ok(())
    }

    /// Send bytes on the data channel (telemetry).
    pub fn send_data(&self, bytes: &[u8]) -> Result<()> {
        if let Some(dc) = self.data_channel.lock().unwrap().as_ref() {
            let glib_bytes = glib::Bytes::from(bytes);
            dc.emit_by_name::<()>("send-data", &[&glib_bytes]);
        }
        Ok(())
    }

    /// Send a teleop command on the operator→robot `control` channel. No-op (with a
    /// debug log) until the control channel has arrived and opened.
    pub fn send_control(&self, msg: &ControlMsg) -> Result<()> {
        if let Some(dc) = self.control_channel.lock().unwrap().as_ref() {
            let bytes = vantage_protocol::codec::encode(msg)?;
            let glib_bytes = glib::Bytes::from(&bytes);
            dc.emit_by_name::<()>("send-data", &[&glib_bytes]);
        } else {
            tracing::debug!("control channel not open yet; dropping {msg:?}");
        }
        Ok(())
    }
}

/// Apply the STUN/TURN config (and the relay-only diagnostic toggle) to a webrtcbin.
/// Shared by `Peer::new` (client) and `RobotMedia::add_consumer` (robot consumers).
pub(crate) fn configure_ice(webrtcbin: &gst::Element, ice_servers: &[IceServer]) {
    // Test/diagnostic toggle: force ICE onto the TURN relay so the relay path can
    // be validated on a single host (where host candidates would otherwise win).
    if std::env::var("VANTAGE_FORCE_RELAY").is_ok_and(|v| v != "0" && !v.is_empty()) {
        webrtcbin.set_property(
            "ice-transport-policy",
            gst_webrtc::WebRTCICETransportPolicy::Relay,
        );
        tracing::warn!("VANTAGE_FORCE_RELAY set — ICE restricted to relay candidates");
    }

    for s in ice_servers {
        for url in &s.urls {
            if url.starts_with("stun:") && !url.starts_with("stun://") {
                // webrtcbin/libnice require the stun://host:port form, not stun:host:port.
                let normalized = format!("stun://{}", url.trim_start_matches("stun:"));
                webrtcbin.set_property("stun-server", &normalized);
            } else if url.starts_with("stun://") {
                webrtcbin.set_property("stun-server", url);
            } else if url.starts_with("turn:") {
                // webrtcbin/libnice require the turn://[user:pass@]host:port form.
                let host = url.trim_start_matches("turn://").trim_start_matches("turn:");
                let with_creds = match (&s.username, &s.credential) {
                    (Some(u), Some(p)) => format!("turn://{u}:{p}@{host}"),
                    _ => format!("turn://{host}"),
                };
                let _ = webrtcbin.emit_by_name::<bool>("add-turn-server", &[&with_creds]);
            }
        }
    }
}

/// Forward webrtcbin's local ICE candidates as `PeerEvent::LocalIce`. Shared by
/// `Peer::new` (client) and `RobotMedia::add_consumer` (robot consumers).
pub(crate) fn wire_local_ice(webrtcbin: &gst::Element, tx: &mpsc::UnboundedSender<PeerEvent>) {
    let tx = tx.clone();
    webrtcbin.connect("on-ice-candidate", false, move |vals| {
        let mlineindex = vals[1].get::<u32>().unwrap();
        let candidate = vals[2].get::<String>().unwrap();
        let _ = tx.send(PeerEvent::LocalIce(Signal::Ice {
            candidate,
            sdp_mline_index: mlineindex,
        }));
        None
    });
}

pub(crate) fn wire_data_channel(
    dc: &gst_webrtc::WebRTCDataChannel,
    tx: &mpsc::UnboundedSender<PeerEvent>,
) {
    {
        let tx = tx.clone();
        dc.connect("on-open", false, move |_| {
            let _ = tx.send(PeerEvent::DataChannelOpen);
            None
        });
    }
    {
        let tx = tx.clone();
        dc.connect("on-message-data", false, move |vals| {
            if let Ok(bytes) = vals[1].get::<glib::Bytes>() {
                let _ = tx.send(PeerEvent::DataMessage(bytes.to_vec()));
            }
            None
        });
    }
}

pub(crate) fn parse_sdp(
    sdp: &str,
    ty: gst_webrtc::WebRTCSDPType,
) -> Result<gst_webrtc::WebRTCSessionDescription> {
    let msg = gst_sdp::SDPMessage::parse_buffer(sdp.as_bytes())?;
    Ok(gst_webrtc::WebRTCSessionDescription::new(ty, msg))
}

pub(crate) fn wire_on_negotiation_needed(
    webrtcbin: &gst::Element,
    tx: &mpsc::UnboundedSender<PeerEvent>,
) {
    let bin = webrtcbin.clone();
    let txn = tx.clone();
    webrtcbin.connect("on-negotiation-needed", false, move |_| {
        let bin2 = bin.clone();
        let tx2 = txn.clone();
        let promise = gst::Promise::with_change_func(move |reply| {
            let reply = match reply {
                Ok(Some(r)) => r,
                _ => return,
            };
            let offer = reply
                .value("offer")
                .unwrap()
                .get::<gst_webrtc::WebRTCSessionDescription>()
                .unwrap();
            bin2.emit_by_name::<()>("set-local-description", &[&offer, &None::<gst::Promise>]);
            let sdp = offer.sdp().as_text().unwrap();
            let _ = tx2.send(PeerEvent::LocalDescription(Signal::Offer { sdp }));
        });
        bin.emit_by_name::<()>("create-offer", &[&None::<gst::Structure>, &promise]);
        None
    });
}

/// On the answerer, decode the incoming H.264 track to RGBA frames.
fn wire_video_receiver(
    pipeline: &gst::Pipeline,
    webrtcbin: &gst::Element,
    frames_tx: &mpsc::UnboundedSender<VideoFrame>,
) {
    let pipeline = pipeline.clone();
    let frames_tx = frames_tx.clone();
    webrtcbin.connect_pad_added(move |_bin, pad| {
        if pad.direction() != gst::PadDirection::Src {
            return;
        }
        if let Err(e) = build_decode_branch(&pipeline, pad, &frames_tx) {
            tracing::error!("failed to build decode branch: {e}");
        }
    });
}

fn build_decode_branch(
    pipeline: &gst::Pipeline,
    src_pad: &gst::Pad,
    frames_tx: &mpsc::UnboundedSender<VideoFrame>,
) -> Result<()> {
    let depay = gst::ElementFactory::make("rtph264depay").build()?;
    let parse = gst::ElementFactory::make("h264parse").build()?;
    let dec = crate::decoder::make_h264_decoder()?;
    let convert = gst::ElementFactory::make("videoconvert").build()?;

    let appsink = gst_app::AppSink::builder()
        .caps(&gst::Caps::builder("video/x-raw").field("format", "RGBA").build())
        .max_buffers(2)
        .drop(true)
        .build();

    {
        let frames_tx = frames_tx.clone();
        appsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let sample = sink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                    let buffer = sample.buffer().ok_or(gst::FlowError::Error)?;
                    let caps = sample.caps().ok_or(gst::FlowError::Error)?;
                    let info = gst_video::VideoInfo::from_caps(caps)
                        .map_err(|_| gst::FlowError::Error)?;
                    let frame = gst_video::VideoFrameRef::from_buffer_ref_readable(buffer, &info)
                        .map_err(|_| gst::FlowError::Error)?;

                    let width = info.width() as usize;
                    let height = info.height() as usize;
                    let stride = frame.plane_stride()[0] as usize;
                    let src = frame.plane_data(0).map_err(|_| gst::FlowError::Error)?;

                    let mut rgba = Vec::with_capacity(width * height * 4);
                    for row in 0..height {
                        let start = row * stride;
                        rgba.extend_from_slice(&src[start..start + width * 4]);
                    }
                    let _ = frames_tx.send(VideoFrame {
                        width: width as u32,
                        height: height as u32,
                        rgba,
                    });
                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );
    }

    let sink_el: gst::Element = appsink.upcast();
    let elements = [&depay, &parse, &dec, &convert, &sink_el];
    pipeline.add_many(elements)?;
    gst::Element::link_many(elements)?;
    for e in elements {
        e.sync_state_with_parent()?;
    }

    let depay_sink = depay.static_pad("sink").context("depay has no sink pad")?;
    src_pad.link(&depay_sink)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Peer>();
    }
}
