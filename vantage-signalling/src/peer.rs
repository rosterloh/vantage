use anyhow::{Context, Result};
use gstreamer as gst;
use gstreamer::glib;
use gstreamer::prelude::*;
use gstreamer_sdp as gst_sdp;
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

pub struct Peer {
    pub pipeline: gst::Pipeline,
    pub webrtcbin: gst::Element,
    events_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<PeerEvent>>,
    events_tx: mpsc::UnboundedSender<PeerEvent>,
    data_channel: std::sync::Mutex<Option<gst_webrtc::WebRTCDataChannel>>,
    #[allow(dead_code)] // consumed in Task 2 (video receive branch)
    frames_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<VideoFrame>>,
    #[allow(dead_code)] // populated in Task 2 (video receive branch)
    frames_tx: mpsc::UnboundedSender<VideoFrame>,
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

        let peer = Self {
            pipeline,
            webrtcbin,
            events_rx: tokio::sync::Mutex::new(rx),
            events_tx: tx.clone(),
            data_channel: std::sync::Mutex::new(None),
            frames_rx: tokio::sync::Mutex::new(frames_rx),
            frames_tx: frames_tx.clone(),
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

                add_video_test_source(&peer.pipeline, &peer.webrtcbin)?;
                wire_on_negotiation_needed(&peer.webrtcbin, &tx);
            }
            Role::Client => {
                let tx2 = tx.clone();
                peer.webrtcbin.connect("on-data-channel", false, move |vals| {
                    let dc = vals[1].get::<gst_webrtc::WebRTCDataChannel>().unwrap();
                    wire_data_channel(&dc, &tx2);
                    None
                });
                // Video receive branch is added in Task 2; nothing else here yet.
            }
        }

        Ok(peer)
    }

    /// Await the next event the app must act on (forward signalling / handle data).
    pub async fn recv_event(&self) -> Option<PeerEvent> {
        self.events_rx.lock().await.recv().await
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

/// videotestsrc -> x264enc(H.264) -> rtph264pay -> webrtcbin, send-only.
fn add_video_test_source(pipeline: &gst::Pipeline, webrtcbin: &gst::Element) -> Result<()> {
    let src = gst::ElementFactory::make("videotestsrc")
        .property_from_str("pattern", "smpte")
        .property("is-live", true)
        .build()?;
    let caps = gst::ElementFactory::make("capsfilter")
        .property(
            "caps",
            &gst::Caps::builder("video/x-raw")
                .field("width", 640i32)
                .field("height", 480i32)
                .field("framerate", gst::Fraction::new(30, 1))
                .build(),
        )
        .build()?;
    let convert = gst::ElementFactory::make("videoconvert").build()?;
    let enc = gst::ElementFactory::make("x264enc")
        .property_from_str("tune", "zerolatency")
        .property_from_str("speed-preset", "ultrafast")
        .property("bitrate", 1500u32)
        .property("key-int-max", 30u32)
        .build()?;
    let pay = gst::ElementFactory::make("rtph264pay")
        .property("config-interval", -1i32)
        .property("pt", 96u32)
        .build()?;
    let rtpcaps = gst::ElementFactory::make("capsfilter")
        .property(
            "caps",
            &gst::Caps::builder("application/x-rtp")
                .field("media", "video")
                .field("encoding-name", "H264")
                .field("payload", 96i32)
                .build(),
        )
        .build()?;

    pipeline.add_many([&src, &caps, &convert, &enc, &pay, &rtpcaps])?;
    gst::Element::link_many([&src, &caps, &convert, &enc, &pay, &rtpcaps])?;

    let src_pad = rtpcaps.static_pad("src").context("rtpcaps has no src pad")?;
    let sink_pad = webrtcbin
        .request_pad_simple("sink_%u")
        .context("webrtcbin refused a sink pad")?;
    src_pad.link(&sink_pad)?;

    // The pipeline is already PLAYING when this runs; bring the new elements up to speed.
    for el in [&src, &caps, &convert, &enc, &pay, &rtpcaps] {
        el.sync_state_with_parent()?;
    }

    // Make the transceiver send-only. Best-effort: if it can't be retrieved, sendrecv
    // still yields one-way flow (client answers recvonly).
    let transceiver = webrtcbin
        .emit_by_name::<gst_webrtc::WebRTCRTPTransceiver>("get-transceiver", &[&0i32]);
    transceiver.set_property(
        "direction",
        gst_webrtc::WebRTCRTPTransceiverDirection::Sendonly,
    );

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
