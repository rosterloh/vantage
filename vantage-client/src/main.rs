mod session;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use session::{run_session, UiSink};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_signalling::peer::VideoFrame;

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let coord = std::env::var("VANTAGE_COORDINATOR").unwrap_or_else(|_| "ws://localhost:8080".into());

    let sink = Arc::new(LogSink { frames: AtomicU64::new(0) });
    run_session(coord, sink).await
}
