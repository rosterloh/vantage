// webrtcbin in-process loopback baseline, measured identically to the MoQ probe:
// post-encode H.264 AU (before rtph264pay) -> through webrtcbin RTP/SRTP/ICE ->
// reconstructed AU (after rtph264depay), matched by PTS. Both webrtcbins live in
// one pipeline so they share a clock and base-time (PTS survives pay/depay).
use gstreamer as gst;
use gstreamer_webrtc as gst_webrtc;
use gstreamer_rtp::RTPBuffer;
use gst::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn none_promise() -> Option<gst::Promise> { None }

fn main() {
    gst::init().unwrap();
    let secs: u64 = std::env::var("SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(12);

    let desc = "videotestsrc is-live=true pattern=ball ! video/x-raw,width=640,height=480,framerate=30/1 ! \
        x264enc tune=zerolatency bitrate=1500 key-int-max=30 ! h264parse config-interval=-1 ! \
        rtph264pay name=pay pt=96 config-interval=1 aggregate-mode=zero-latency ! \
        application/x-rtp,media=video,encoding-name=H264,payload=96 ! \
        webrtcbin name=sendbin bundle-policy=max-bundle latency=0 \
        webrtcbin name=recvbin bundle-policy=max-bundle latency=0";

    let pipeline = gst::parse::launch(desc).unwrap().downcast::<gst::Pipeline>().unwrap();
    let sendbin = pipeline.by_name("sendbin").unwrap();
    let recvbin = pipeline.by_name("recvbin").unwrap();

    // webrtcbin re-times PTS on receive, so match by RTP timestamp instead — it
    // survives end-to-end. Record the marker packet (end of access unit) on each
    // side: frame-complete to frame-complete.
    let sent: Arc<Mutex<HashMap<u32, Instant>>> = Arc::new(Mutex::new(HashMap::new()));
    let lats: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::new()));
    let tx_n = Arc::new(Mutex::new(0u64));
    let rx_n = Arc::new(Mutex::new(0u64));

    // TX probe: RTP packets leaving the payloader.
    {
        let pad = pipeline.by_name("pay").unwrap().static_pad("src").unwrap();
        let sent = sent.clone();
        let tx_n = tx_n.clone();
        pad.add_probe(gst::PadProbeType::BUFFER, move |_, info| {
            if let Some(gst::PadProbeData::Buffer(ref b)) = info.data {
                if let Ok(rtp) = RTPBuffer::from_buffer_readable(b) {
                    if rtp.is_marker() {
                        let ts = rtp.timestamp();
                        *tx_n.lock().unwrap() += 1;
                        sent.lock().unwrap().entry(ts).or_insert_with(Instant::now);
                    }
                }
            }
            gst::PadProbeReturn::Ok
        });
    }

    // ICE: forward candidates between the two webrtcbins.
    {
        let recv = recvbin.clone();
        sendbin.connect("on-ice-candidate", false, move |vals| {
            let m = vals[1].get::<u32>().unwrap();
            let c = vals[2].get::<String>().unwrap();
            recv.emit_by_name::<()>("add-ice-candidate", &[&m, &c]);
            None
        });
        let send = sendbin.clone();
        recvbin.connect("on-ice-candidate", false, move |vals| {
            let m = vals[1].get::<u32>().unwrap();
            let c = vals[2].get::<String>().unwrap();
            send.emit_by_name::<()>("add-ice-candidate", &[&m, &c]);
            None
        });
    }

    // SDP offer/answer entirely in-process.
    {
        let send = sendbin.clone();
        let recv = recvbin.clone();
        sendbin.connect("on-negotiation-needed", false, move |_| {
            let send = send.clone();
            let recv = recv.clone();
            let send_for_offer = send.clone();
            let promise = gst::Promise::with_change_func(move |reply| {
                let offer = reply.unwrap().unwrap()
                    .value("offer").unwrap()
                    .get::<gst_webrtc::WebRTCSessionDescription>().unwrap();
                send.emit_by_name::<()>("set-local-description", &[&offer, &none_promise()]);
                recv.emit_by_name::<()>("set-remote-description", &[&offer, &none_promise()]);
                let send2 = send.clone();
                let recv2 = recv.clone();
                let answer_promise = gst::Promise::with_change_func(move |reply| {
                    let answer = reply.unwrap().unwrap()
                        .value("answer").unwrap()
                        .get::<gst_webrtc::WebRTCSessionDescription>().unwrap();
                    recv2.emit_by_name::<()>("set-local-description", &[&answer, &none_promise()]);
                    send2.emit_by_name::<()>("set-remote-description", &[&answer, &none_promise()]);
                });
                recv.emit_by_name::<()>("create-answer", &[&None::<gst::Structure>, &answer_promise]);
            });
            send_for_offer.emit_by_name::<()>("create-offer", &[&None::<gst::Structure>, &promise]);
            None
        });
    }

    // Incoming RTP from webrtcbin: probe the marker packet for arrival time, then
    // run it through depay -> decode so the session stays live.
    {
        let pipeline_weak = pipeline.downgrade();
        let sent = sent.clone();
        let lats = lats.clone();
        let rx_n = rx_n.clone();
        recvbin.connect_pad_added(move |_, pad| {
            if pad.direction() != gst::PadDirection::Src { return; }
            let Some(pipeline) = pipeline_weak.upgrade() else { return };

            let sent = sent.clone();
            let lats = lats.clone();
            let rx_n = rx_n.clone();
            pad.add_probe(gst::PadProbeType::BUFFER, move |_, info| {
                if let Some(gst::PadProbeData::Buffer(ref b)) = info.data {
                    if let Ok(rtp) = RTPBuffer::from_buffer_readable(b) {
                        if rtp.is_marker() {
                            let ts = rtp.timestamp();
                            *rx_n.lock().unwrap() += 1;
                            if let Some(t0) = sent.lock().unwrap().remove(&ts) {
                                lats.lock().unwrap().push(t0.elapsed().as_secs_f64() * 1000.0);
                            }
                        }
                    }
                }
                gst::PadProbeReturn::Ok
            });

            let bin = gst::parse::bin_from_description(
                "queue ! rtph264depay ! h264parse ! avdec_h264 ! fakesink sync=false",
                true,
            ).unwrap();
            pipeline.add(&bin).unwrap();
            bin.sync_state_with_parent().unwrap();
            pad.link(&bin.static_pad("sink").unwrap()).unwrap();
        });
    }

    pipeline.set_state(gst::State::Playing).unwrap();
    std::thread::sleep(Duration::from_secs(secs));
    pipeline.set_state(gst::State::Null).unwrap();

    println!("tx_frames={} rx_frames={}", *tx_n.lock().unwrap(), *rx_n.lock().unwrap());

    let mut v = lats.lock().unwrap().clone();
    let unmatched = sent.lock().unwrap().len();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n == 0 {
        println!("NO MATCHED FRAMES (unmatched_sent={unmatched})");
        return;
    }
    let pct = |q: f64| v[(((n - 1) as f64) * q).round() as usize];
    let mean = v.iter().sum::<f64>() / n as f64;
    println!("matched_frames={n} unmatched_sent={unmatched}");
    println!(
        "WebRTC transport latency (ms): min={:.1} p50={:.1} mean={:.1} p90={:.1} p99={:.1} max={:.1}",
        v[0], pct(0.50), mean, pct(0.90), pct(0.99), v[n - 1]
    );
}
