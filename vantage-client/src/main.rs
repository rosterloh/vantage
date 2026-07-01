mod session;
mod ui;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use session::{run_session, UiSink};
use slint::ComponentHandle;
use tokio::sync::mpsc;
use ui::{AppWindow, SlintSink};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_protocol::ControlMsg;
use vantage_signalling::peer::VideoFrame;

fn coordinator_url() -> String {
    std::env::var("VANTAGE_COORDINATOR").unwrap_or_else(|_| "ws://localhost:8080".into())
}

fn spawn_session(sink: Arc<dyn UiSink>, control_rx: mpsc::UnboundedReceiver<ControlMsg>) {
    let coord = coordinator_url();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        if let Err(e) = rt.block_on(run_session(coord, sink, control_rx)) {
            tracing::error!("session ended: {e}");
        }
    });
}

/// Headless control driver for display-less verification. Env-controlled so the
/// harness can exercise the robot watchdog without a window:
///   VANTAGE_CONTROL=move|keepalive  — what to send every 100ms (default: nothing)
///   VANTAGE_CONTROL_STOP_MS=<n>     — stop sending after n ms (simulate staleness
///                                     while staying connected)
async fn headless_control(control_tx: mpsc::UnboundedSender<ControlMsg>) {
    let kind = match std::env::var("VANTAGE_CONTROL") {
        Ok(k) if k == "move" || k == "keepalive" => k,
        _ => return, // no control input in this run
    };
    let stop_after = std::env::var("VANTAGE_CONTROL_STOP_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut elapsed = 0u64;
    loop {
        tick.tick().await;
        if stop_after.is_some_and(|limit| elapsed >= limit) {
            tracing::info!("headless control: stopped after {elapsed}ms (simulating staleness)");
            return;
        }
        let msg = if kind == "move" {
            ControlMsg::Move { linear: 0.5, angular: 0.0 }
        } else {
            ControlMsg::KeepAlive
        };
        if control_tx.send(msg).is_err() {
            return;
        }
        elapsed += 100;
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let (control_tx, control_rx) = mpsc::unbounded_channel::<ControlMsg>();

    // Headless mode for display-less verification (CI / sandboxes).
    if std::env::var("VANTAGE_HEADLESS").is_ok_and(|v| v != "0" && !v.is_empty()) {
        let sink = Arc::new(LogSink { frames: AtomicU64::new(0) });
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
        return rt.block_on(async move {
            tokio::spawn(headless_control(control_tx));
            run_session(coordinator_url(), sink, control_rx).await
        });
    }

    let ui = AppWindow::new()?;
    let sink = SlintSink::new(ui.as_weak());
    ui::wire_control_input(&ui, control_tx);
    spawn_session(sink, control_rx);
    ui.run()?;
    Ok(())
}

struct LogSink {
    frames: AtomicU64,
}
impl UiSink for LogSink {
    fn frame(&self, frame: VideoFrame) {
        let n = self.frames.fetch_add(1, Ordering::Relaxed) + 1;
        if n == 1 || n % 30 == 0 {
            tracing::info!("video frame {}x{} (#{n})", frame.width, frame.height);
        }
    }
    fn telemetry(&self, info: &DeviceInfo) {
        tracing::info!("telemetry: cpu={:.1}% mem={}/{}MB", info.cpu_percent, info.mem_used_mb, info.mem_total_mb);
    }
    fn status(&self, text: &str) { tracing::info!("status: {text}"); }
}
