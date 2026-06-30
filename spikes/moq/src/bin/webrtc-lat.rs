// webrtcbin in-process loopback baseline, measured identically to the MoQ probe:
// post-encode H.264 AU (before rtph264pay) -> through webrtcbin RTP/SRTP/ICE ->
// reconstructed AU (after rtph264depay), matched by PTS. Both webrtcbins live in
// one pipeline so they share a clock and base-time (PTS survives pay/depay).
use gstreamer as gst;
use gstreamer_webrtc as gst_webrtc;
use gstreamer_rtp::RTPBuffer;
use gstreamer_rtp::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn none_promise() -> Option<gst::Promise> { None }

fn main() {
    gst::init().unwrap();
    let secs: u64 = std::env::var("SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(12);
    let bitrate: u32 = std::env::var("BITRATE").ok().and_then(|s| s.parse().ok()).unwrap_or(1500);
    let adapt: bool = std::env::var("ADAPT").map(|v| v == "1").unwrap_or(false);
    let pattern = std::env::var("PATTERN").unwrap_or_else(|_| "ball".into());

    let desc = format!(
        "videotestsrc is-live=true pattern={pattern} ! video/x-raw,width=640,height=480,framerate=30/1 ! \
        x264enc name=enc tune=zerolatency bitrate={bitrate} key-int-max=30 ! h264parse config-interval=-1 ! \
        rtph264pay name=pay pt=96 config-interval=1 aggregate-mode=zero-latency ! \
        application/x-rtp,media=video,encoding-name=H264,payload=96 ! \
        webrtcbin name=sendbin bundle-policy=max-bundle latency=0 \
        webrtcbin name=recvbin bundle-policy=max-bundle latency=0"
    );

    let pipeline = gst::parse::launch(&desc).unwrap().downcast::<gst::Pipeline>().unwrap();
    let sendbin = pipeline.by_name("sendbin").unwrap();
    let recvbin = pipeline.by_name("recvbin").unwrap();
    let enc = pipeline.by_name("enc").unwrap();
    let pay = pipeline.by_name("pay").unwrap();
    let cur_bitrate = Arc::new(AtomicU32::new(bitrate));

    // transport-cc header extension (ext-id=3) so the receiver returns TWCC
    // feedback — exactly as vantage-signalling wires it.
    if let Ok(twcc) = gst::ElementFactory::make("rtphdrexttwcc").build() {
        let ext = twcc.clone().dynamic_cast::<gstreamer_rtp::RTPHeaderExtension>().unwrap();
        ext.set_id(3);
        pay.emit_by_name::<()>("add-extension", &[&twcc]);
    }

    // ADAPT=1: attach rtpgccbwe as aux sender and drive x264enc bitrate from the
    // GCC estimate (clamped 300..target), same as vantage-signalling.
    if adapt {
        if gst::ElementFactory::find("rtpgccbwe").is_some() {
            let enc_weak = enc.downgrade();
            let cur = cur_bitrate.clone();
            sendbin.connect("request-aux-sender", false, move |_| {
                let gcc = gst::ElementFactory::make("rtpgccbwe").build().unwrap();
                gcc.set_property("min-bitrate", 300_000u32);
                gcc.set_property("max-bitrate", bitrate * 1000);
                let enc_weak = enc_weak.clone();
                let cur = cur.clone();
                gcc.connect("notify::estimated-bitrate", false, move |args| {
                    let gcc_el = args[0].get::<gst::Element>().ok()?;
                    let bps: u32 = gcc_el.property("estimated-bitrate");
                    let kbps = (bps / 1000).clamp(300, bitrate);
                    if let Some(enc) = enc_weak.upgrade() {
                        enc.set_property("bitrate", kbps);
                        cur.store(kbps, Ordering::Relaxed);
                    }
                    None
                });
                Some(gcc.to_value())
            });
            eprintln!("rtpgccbwe attached — adaptive bitrate active (target {bitrate} kbit/s)");
        } else {
            eprintln!("ADAPT=1 but rtpgccbwe MISSING — running static");
        }
    }

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

    println!("tx_frames={} rx_frames={} final_bitrate={}kbit/s",
        *tx_n.lock().unwrap(), *rx_n.lock().unwrap(), cur_bitrate.load(Ordering::Relaxed));

    let arr = lats.lock().unwrap().clone(); // arrival order
    let n = arr.len();
    if n == 0 {
        println!("NO MATCHED FRAMES");
        return;
    }
    let win = (n / 5).max(1);
    let mean_of = |s: &[f64]| s.iter().sum::<f64>() / s.len() as f64;
    let early = mean_of(&arr[..win]);
    let late = mean_of(&arr[n - win..]);
    let mut v = arr.clone();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |q: f64| v[(((n - 1) as f64) * q).round() as usize];
    println!("matched_frames={n}");
    println!(
        "WebRTC transport latency (ms): p50={:.1} p90={:.1} p99={:.1} max={:.1} | early={:.1} late={:.1} (bufferbloat if late>>early)",
        pct(0.50), pct(0.90), pct(0.99), v[n - 1], early, late
    );
}
