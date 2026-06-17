use anyhow::{Context, Result};
use gstreamer as gst;
use gstreamer::glib;
use gstreamer::prelude::*;
use gstreamer_sdp as gst_sdp;
use gstreamer_webrtc as gst_webrtc;
use tokio::sync::mpsc;
use vantage_protocol::signalling::{IceServer, Signal};

pub enum PeerEvent {
    /// Offer or Answer to forward via coordinator.
    LocalDescription(Signal),
    /// Signal::Ice to forward.
    LocalIce(Signal),
    DataChannelOpen,
    /// Bytes received on the data channel.
    DataMessage(Vec<u8>),
}

pub struct Peer {
    pub pipeline: gst::Pipeline,
    pub webrtcbin: gst::Element,
    events_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<PeerEvent>>,
    events_tx: mpsc::UnboundedSender<PeerEvent>,
    data_channel: std::sync::Mutex<Option<gst_webrtc::WebRTCDataChannel>>,
}

impl Peer {
    /// `create_data_channel=true` for the OFFERER (robot); false for the ANSWERER (client).
    pub fn new(ice_servers: &[IceServer], create_data_channel: bool) -> Result<Self> {
        gst::init()?;
        let pipeline = gst::Pipeline::new();
        let webrtcbin = gst::ElementFactory::make("webrtcbin")
            .name("sendrecv")
            .property("bundle-policy", gst_webrtc::WebRTCBundlePolicy::MaxBundle)
            .build()
            .context("webrtcbin missing — install gst-plugins-bad")?;

        for s in ice_servers {
            for url in &s.urls {
                if url.starts_with("stun:") {
                    webrtcbin.set_property("stun-server", url);
                } else if url.starts_with("turn:") {
                    let with_creds = match (&s.username, &s.credential) {
                        (Some(u), Some(p)) => {
                            format!("turn://{u}:{p}@{}", url.trim_start_matches("turn:"))
                        }
                        _ => url.clone(),
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

        let peer = Self {
            pipeline,
            webrtcbin,
            events_rx: tokio::sync::Mutex::new(rx),
            events_tx: tx.clone(),
            data_channel: std::sync::Mutex::new(None),
        };

        if create_data_channel {
            // OFFERER: react to negotiation-needed by making an offer. The data channel itself is
            // created below, once the pipeline is PLAYING and the SCTP transport is initialised.
            let bin = peer.webrtcbin.clone();
            let txn = tx.clone();
            peer.webrtcbin
                .connect("on-negotiation-needed", false, move |_| {
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
                        bin2.emit_by_name::<()>(
                            "set-local-description",
                            &[&offer, &None::<gst::Promise>],
                        );
                        let sdp = offer.sdp().as_text().unwrap();
                        let _ = tx2.send(PeerEvent::LocalDescription(Signal::Offer { sdp }));
                    });
                    bin.emit_by_name::<()>("create-offer", &[&None::<gst::Structure>, &promise]);
                    None
                });
        } else {
            // ANSWERER: react to the remote-created data channel.
            let tx2 = tx.clone();
            peer.webrtcbin.connect("on-data-channel", false, move |vals| {
                let dc = vals[1].get::<gst_webrtc::WebRTCDataChannel>().unwrap();
                wire_data_channel(&dc, &tx2);
                None
            });
        }

        peer.pipeline.set_state(gst::State::Playing)?;

        if create_data_channel {
            // Create the data channel now that the pipeline is PLAYING; this triggers
            // on-negotiation-needed which produces the offer.
            let dc = peer
                .webrtcbin
                .emit_by_name_with_values(
                    "create-data-channel",
                    &["telemetry".to_value(), None::<gst::Structure>.to_value()],
                )
                .expect("create-data-channel returned a value")
                .get::<gst_webrtc::WebRTCDataChannel>()
                .expect("create-data-channel returned a WebRTCDataChannel");
            wire_data_channel(&dc, &tx);
            *peer.data_channel.lock().unwrap() = Some(dc);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn _assert_send_sync<T: Send + Sync>() {}
    #[allow(dead_code)]
    fn peer_is_send_sync() {
        _assert_send_sync::<Peer>();
    }

    #[test]
    fn offerer_peer_constructs() {
        let ice = vec![IceServer {
            urls: vec!["stun:stun.l.google.com:19302".into()],
            username: None,
            credential: None,
        }];
        let peer = Peer::new(&ice, true).expect("peer builds");
        assert!(peer.webrtcbin.name().starts_with("sendrecv"));
    }
}
