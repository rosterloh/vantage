// Single-process MoQ transport-latency probe.
// TX branch encodes H.264 and hands each access unit to moqsink; RX branch pulls
// it back through the relay via moqsrc. Both pad probes run in one process, so
// Instant timestamps share a clock. Frames are matched by PTS (ms) — this is
// correct even though the subscriber joins mid-stream and misses the first GOP.
use gstreamer as gst;
use gst::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn main() {
    gst::init().unwrap();
    let url = std::env::var("MOQ_URL").unwrap_or_else(|_| "https://localhost:4443".into());
    let bc = "spike.hang";
    let secs: u64 = std::env::var("SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(12);

    let sent: Arc<Mutex<HashMap<u64, Instant>>> = Arc::new(Mutex::new(HashMap::new()));
    let lats: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::new()));
    let tx_n = Arc::new(Mutex::new(0u64));
    let rx_n = Arc::new(Mutex::new(0u64));
    let tx_samp = Arc::new(Mutex::new(Vec::<u64>::new()));
    let rx_samp = Arc::new(Mutex::new(Vec::<u64>::new()));
    // moqsink stamps timestamps off the absolute monotonic clock; moqsrc rebases
    // the broadcast to 0. Subtract the first TX PTS (the stream epoch) so TX keys
    // land in the same timescale as RX.
    let epoch: Arc<Mutex<Option<gst::ClockTime>>> = Arc::new(Mutex::new(None));

    let tx_desc = format!(
        "videotestsrc is-live=true pattern=ball ! video/x-raw,width=640,height=480,framerate=30/1 ! \
         x264enc tune=zerolatency bitrate=1500 key-int-max=30 ! h264parse config-interval=-1 ! \
         video/x-h264,stream-format=byte-stream,alignment=au ! identity name=tx ! \
         moqsink url={url} broadcast={bc} tls-disable-verify=true"
    );
    let rx_desc = format!(
        "moqsrc url={url} broadcast={bc} tls-disable-verify=true ! identity name=rx ! \
         h264parse ! avdec_h264 ! fakesink sync=false"
    );

    let tx = gst::parse::launch(&tx_desc).unwrap().downcast::<gst::Pipeline>().unwrap();
    let rx = gst::parse::launch(&rx_desc).unwrap().downcast::<gst::Pipeline>().unwrap();

    {
        let pad = tx.by_name("tx").unwrap().static_pad("src").unwrap();
        let sent = sent.clone();
        let tx_n = tx_n.clone();
        let tx_samp = tx_samp.clone();
        let epoch = epoch.clone();
        pad.add_probe(gst::PadProbeType::BUFFER, move |_, info| {
            if let Some(gst::PadProbeData::Buffer(ref b)) = info.data {
                if let Some(p) = b.pts() {
                    let mut e = epoch.lock().unwrap();
                    let base = *e.get_or_insert(p);
                    let key = p.saturating_sub(base).mseconds();
                    *tx_n.lock().unwrap() += 1;
                    let mut s = tx_samp.lock().unwrap();
                    if s.len() < 5 { s.push(key); }
                    drop(s);
                    sent.lock().unwrap().insert(key, Instant::now());
                }
            }
            gst::PadProbeReturn::Ok
        });
    }
    {
        let pad = rx.by_name("rx").unwrap().static_pad("src").unwrap();
        let sent = sent.clone();
        let lats = lats.clone();
        let rx_n = rx_n.clone();
        let rx_samp = rx_samp.clone();
        pad.add_probe(gst::PadProbeType::BUFFER, move |_, info| {
            if let Some(gst::PadProbeData::Buffer(ref b)) = info.data {
                if let Some(p) = b.pts() {
                    *rx_n.lock().unwrap() += 1;
                    let mut s = rx_samp.lock().unwrap();
                    if s.len() < 5 { s.push(p.mseconds()); }
                    drop(s);
                    if let Some(t0) = sent.lock().unwrap().remove(&p.mseconds()) {
                        lats.lock().unwrap().push(t0.elapsed().as_secs_f64() * 1000.0);
                    }
                }
            }
            gst::PadProbeReturn::Ok
        });
    }

    rx.set_state(gst::State::Playing).unwrap();
    tx.set_state(gst::State::Playing).unwrap();
    std::thread::sleep(Duration::from_secs(secs));
    tx.set_state(gst::State::Null).unwrap();
    rx.set_state(gst::State::Null).unwrap();

    for (name, p) in [("TX", &tx), ("RX", &rx)] {
        if let Some(bus) = p.bus() {
            while let Some(msg) = bus.timed_pop(gst::ClockTime::ZERO) {
                if let gst::MessageView::Error(e) = msg.view() {
                    eprintln!("{name} ERROR: {} ({:?})", e.error(), e.debug());
                }
            }
        }
    }

    println!("tx_buffers={} rx_buffers={}", *tx_n.lock().unwrap(), *rx_n.lock().unwrap());
    println!("tx_pts_ms_sample={:?}", *tx_samp.lock().unwrap());
    println!("rx_pts_ms_sample={:?}", *rx_samp.lock().unwrap());
    let mut v = lats.lock().unwrap().clone();
    let unmatched = sent.lock().unwrap().len();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n == 0 {
        println!("NO MATCHED FRAMES (unmatched_sent={unmatched}) — PTS mismatch or no delivery");
        return;
    }
    let pct = |q: f64| v[(((n - 1) as f64) * q).round() as usize];
    let mean = v.iter().sum::<f64>() / n as f64;
    println!("matched_frames={n} unmatched_sent={unmatched}");
    println!(
        "MoQ transport latency (ms): min={:.1} p50={:.1} mean={:.1} p90={:.1} p99={:.1} max={:.1}",
        v[0], pct(0.50), mean, pct(0.90), pct(0.99), v[n - 1]
    );
}
