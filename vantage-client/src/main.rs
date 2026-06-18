mod session;
mod ui;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use session::{run_session, UiSink};
use slint::ComponentHandle;
use ui::{AppWindow, SlintSink};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_signalling::peer::VideoFrame;

fn coordinator_url() -> String {
    std::env::var("VANTAGE_COORDINATOR").unwrap_or_else(|_| "ws://localhost:8080".into())
}

fn spawn_session(sink: Arc<dyn UiSink>) {
    let coord = coordinator_url();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        if let Err(e) = rt.block_on(run_session(coord, sink)) {
            tracing::error!("session ended: {e}");
        }
    });
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Headless mode for display-less verification (CI / sandboxes).
    if std::env::var("VANTAGE_HEADLESS").is_ok_and(|v| v != "0" && !v.is_empty()) {
        let sink = Arc::new(LogSink { frames: AtomicU64::new(0) });
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
        return rt.block_on(run_session(coordinator_url(), sink));
    }

    let ui = AppWindow::new()?;
    let sink = SlintSink::new(ui.as_weak());
    spawn_session(sink);
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
