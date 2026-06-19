use anyhow::{Context, Result};
use gstreamer as gst;
use gstreamer::glib;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use gstreamer_sdp as gst_sdp;
use gstreamer_video as gst_video;
use gstreamer_video::prelude::*;
use gstreamer_webrtc as gst_webrtc;
use tokio::sync::mpsc;
use vantage_protocol::signalling::{IceServer, Signal};

#[derive(Debug)]
pub enum PeerEvent {
    /// Offer or Answer to forward via coordinator.
    LocalDescription(Signal),
    /// Signal::Ice to forward.
    LocalIce(Signal),
    DataChannelOpen,
    /// Bytes received on the data channel.
    DataMessage(Vec<u8>),
}

/// Which end of the connection this peer is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Offerer: creates the telemetry data channel AND a send-only video branch.
    Robot,
    /// Answerer: receives the data channel and the incoming video track.
    Client,
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
    frames_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<VideoFrame>>,
    frames_tx: mpsc::UnboundedSender<VideoFrame>,
    raw_frames_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<RawFrame>>,
    raw_frames_tx: mpsc::UnboundedSender<RawFrame>,
}

impl Peer {
    /// `Role::Robot` is the OFFERER (creates data channel + video); `Role::Client` is the ANSWERER.
    pub fn new(ice_servers: &[IceServer], role: Role) -> Result<Self> {
        gst::init()?;
        let pipeline = gst::Pipeline::new();
        let webrtcbin = gst::ElementFactory::make("webrtcbin")
            .name("sendrecv")
            .property("bundle-policy", gst_webrtc::WebRTCBundlePolicy::MaxBundle)
            .build()
            .context("webrtcbin missing — install gst-plugins-bad")?;

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

        pipeline.add(&webrtcbin)?;
        let (tx, rx) = mpsc::unbounded_channel();

        // Emit local ICE candidates -> PeerEvent::LocalIce
        {
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

        let (frames_tx, frames_rx) = mpsc::unbounded_channel::<VideoFrame>();
        let (raw_frames_tx, raw_frames_rx) = mpsc::unbounded_channel::<RawFrame>();

        let peer = Self {
            pipeline,
            webrtcbin,
            events_rx: tokio::sync::Mutex::new(rx),
            events_tx: tx.clone(),
            data_channel: std::sync::Mutex::new(None),
            frames_rx: tokio::sync::Mutex::new(frames_rx),
            frames_tx: frames_tx.clone(),
            raw_frames_rx: tokio::sync::Mutex::new(raw_frames_rx),
            raw_frames_tx: raw_frames_tx.clone(),
        };

        // The SCTP transport (and thus create-data-channel) is only available once the
        // pipeline is PLAYING. Newly-added video elements sync their state below.
        peer.pipeline.set_state(gst::State::Playing)?;

        match role {
            Role::Robot => {
                let dc = peer.webrtcbin.emit_by_name_with_values(
                    "create-data-channel",
                    &["telemetry".to_value(), None::<gst::Structure>.to_value()],
                );
                let dc = dc
                    .context("create-data-channel returned no value (pipeline not ready?)")?
                    .get::<gst_webrtc::WebRTCDataChannel>()
                    .context("create-data-channel returned null (pipeline not ready?)")?;
                wire_data_channel(&dc, &tx);
                *peer.data_channel.lock().unwrap() = Some(dc);

                add_video_source(&peer.pipeline, &peer.webrtcbin, &peer.raw_frames_tx)?;
                wire_on_negotiation_needed(&peer.webrtcbin, &tx);
            }
            Role::Client => {
                let tx2 = tx.clone();
                peer.webrtcbin.connect("on-data-channel", false, move |vals| {
                    let dc = vals[1].get::<gst_webrtc::WebRTCDataChannel>().unwrap();
                    wire_data_channel(&dc, &tx2);
                    None
                });
                wire_video_receiver(&peer.pipeline, &peer.webrtcbin, &peer.frames_tx);
            }
        }

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

    /// Await the next raw camera frame (RGB888) from the pre-encode tee branch.
    pub async fn recv_raw_frame(&self) -> Option<RawFrame> {
        self.raw_frames_rx.lock().await.recv().await
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
}

fn wire_data_channel(dc: &gst_webrtc::WebRTCDataChannel, tx: &mpsc::UnboundedSender<PeerEvent>) {
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

fn parse_sdp(sdp: &str, ty: gst_webrtc::WebRTCSDPType) -> Result<gst_webrtc::WebRTCSessionDescription> {
    let msg = gst_sdp::SDPMessage::parse_buffer(sdp.as_bytes())?;
    Ok(gst_webrtc::WebRTCSessionDescription::new(ty, msg))
}

fn wire_on_negotiation_needed(webrtcbin: &gst::Element, tx: &mpsc::UnboundedSender<PeerEvent>) {
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

/// Build the robot send graph: source → tee, with an H.264 encode branch into
/// webrtcbin (send-only) and a raw RGB branch into an appsink emitting RawFrames.
fn add_video_source(
    pipeline: &gst::Pipeline,
    webrtcbin: &gst::Element,
    raw_tx: &mpsc::UnboundedSender<RawFrame>,
) -> Result<()> {
    // --- source → videoconvert → caps(640x480@30) → tee ---
    let source = build_source()?;
    let srcconvert = gst::ElementFactory::make("videoconvert").build()?;
    let srccaps = gst::ElementFactory::make("capsfilter")
        .property("caps", &gst::Caps::builder("video/x-raw")
            // Force I420 (4:2:0): cameras often negotiate YUYV (4:2:2), which x264enc
            // baseline rejects ("baseline profile doesn't support 4:2:2"). I420 also
            // converts cleanly to RGB on the raw branch.
            .field("format", "I420")
            .field("width", 640i32).field("height", 480i32)
            .field("framerate", gst::Fraction::new(30, 1))
            .build())
        .build()?;
    let tee = gst::ElementFactory::make("tee").name("t").build()?;

    pipeline.add_many([&source, &srcconvert, &srccaps, &tee])?;
    gst::Element::link_many([&source, &srcconvert, &srccaps, &tee])?;

    // --- encode branch: tee → queue(leaky) → encoder → h264parse → rtph264pay → webrtcbin ---
    let equeue = gst::ElementFactory::make("queue")
        .property_from_str("leaky", "downstream")
        .build()?;
    let enc = crate::encoder::make_h264_encoder()?;
    let parse = gst::ElementFactory::make("h264parse").build()?;
    let pay = gst::ElementFactory::make("rtph264pay")
        .property("config-interval", -1i32)
        .property("pt", 96u32)
        .build()?;
    let rtpcaps = gst::ElementFactory::make("capsfilter")
        .property("caps", &gst::Caps::builder("application/x-rtp")
            .field("media", "video").field("encoding-name", "H264").field("payload", 96i32)
            .build())
        .build()?;

    pipeline.add_many([&equeue, &enc, &parse, &pay, &rtpcaps])?;
    gst::Element::link_many([&equeue, &enc, &parse, &pay, &rtpcaps])?;
    link_tee(&tee, &equeue)?;

    let src_pad = rtpcaps.static_pad("src").context("rtpcaps has no src pad")?;
    let sink_pad = webrtcbin.request_pad_simple("sink_%u")
        .context("webrtcbin refused a sink pad")?;
    src_pad.link(&sink_pad)?;

    let transceiver = webrtcbin
        .emit_by_name::<gst_webrtc::WebRTCRTPTransceiver>("get-transceiver", &[&0i32]);
    transceiver.set_property("direction", gst_webrtc::WebRTCRTPTransceiverDirection::Sendonly);

    // --- raw branch: tee → queue → videoconvert → RGB appsink → RawFrame ---
    let rqueue = gst::ElementFactory::make("queue").build()?;
    let rconvert = gst::ElementFactory::make("videoconvert").build()?;
    let rawsink = gst_app::AppSink::builder()
        .caps(&gst::Caps::builder("video/x-raw").field("format", "RGB").build())
        .max_buffers(2)
        .drop(true)
        .build();
    {
        let raw_tx = raw_tx.clone();
        rawsink.set_callbacks(
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
                    let mut data = Vec::with_capacity(width * height * 3);
                    for row in 0..height {
                        let start = row * stride;
                        data.extend_from_slice(&src[start..start + width * 3]);
                    }
                    let _ = raw_tx.send(RawFrame {
                        width: width as u32,
                        height: height as u32,
                        encoding: "rgb8".into(),
                        data,
                    });
                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );
    }
    let rawsink_el: gst::Element = rawsink.upcast();
    pipeline.add_many([&rqueue, &rconvert, &rawsink_el])?;
    gst::Element::link_many([&rqueue, &rconvert, &rawsink_el])?;
    link_tee(&tee, &rqueue)?;

    // Elements added to an already-PLAYING pipeline must sync their state.
    for e in [
        &source, &srcconvert, &srccaps, &tee, &equeue, &enc, &parse, &pay, &rtpcaps,
        &rqueue, &rconvert, &rawsink_el,
    ] {
        e.sync_state_with_parent()?;
    }
    Ok(())
}

/// Request a `src_%u` pad off the tee and link it to a downstream element's sink.
fn link_tee(tee: &gst::Element, downstream: &gst::Element) -> Result<()> {
    let tee_src = tee.request_pad_simple("src_%u").context("tee has no src pad")?;
    let sink = downstream.static_pad("sink").context("downstream has no sink pad")?;
    tee_src.link(&sink)?;
    Ok(())
}

/// `VANTAGE_VIDEO_SOURCE=camera` → v4l2src (`VANTAGE_CAMERA_DEVICE`, default /dev/video0);
/// anything else (default) → videotestsrc SMPTE pattern.
fn build_source() -> Result<gst::Element> {
    let kind = std::env::var("VANTAGE_VIDEO_SOURCE").unwrap_or_else(|_| "test".into());
    if kind == "camera" {
        let dev = std::env::var("VANTAGE_CAMERA_DEVICE").unwrap_or_else(|_| "/dev/video0".into());
        tracing::info!("video source: camera {dev}");
        gst::ElementFactory::make("v4l2src")
            .property("device", dev.as_str())
            .build()
            .context("v4l2src — camera source")
    } else {
        tracing::info!("video source: test pattern");
        gst::ElementFactory::make("videotestsrc")
            .property_from_str("pattern", "smpte")
            .property("is-live", true)
            .build()
            .context("videotestsrc")
    }
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
    let dec = gst::ElementFactory::make("avdec_h264").build()?;
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

    fn ice() -> Vec<IceServer> {
        vec![IceServer {
            urls: vec!["stun:stun.l.google.com:19302".into()],
            username: None,
            credential: None,
        }]
    }

    #[test]
    fn peer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Peer>();
    }

    #[tokio::test]
    async fn robot_offer_contains_video_mline() {
        let peer = Peer::new(&ice(), Role::Robot).expect("robot peer builds");
        let ev = tokio::time::timeout(std::time::Duration::from_secs(5), peer.recv_event())
            .await
            .expect("offer produced within 5s");
        match ev {
            Some(PeerEvent::LocalDescription(Signal::Offer { sdp })) => {
                assert!(sdp.contains("m=video"), "offer must contain a video m-line:\n{sdp}");
            }
            other => panic!("expected an Offer, got {other:?}"),
        }
    }
}
