mod registry;
mod routes;
mod sessions;

use std::sync::Arc;

use routes::{router, AppState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env (TURN creds, bind addr, RUST_LOG) before reading any env var. Missing
    // file is fine — real environment still wins for anything not set here.
    dotenvy::dotenv().ok();

    let _otel = vantage_observability::init("vantage-coordinator");

    let state = Arc::new(AppState::from_env());

    // Background pruner so stale robots expire even with no traffic. Doubles as
    // the sampling point for fleet-size gauges (no-ops until OTLP is configured).
    {
        let state = state.clone();
        let meter = vantage_observability::opentelemetry::global::meter("vantage-coordinator");
        let robots = meter.u64_gauge("vantage.coordinator.robots_online").build();
        let sessions = meter.u64_gauge("vantage.coordinator.sessions_active").build();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tick.tick().await;
                let live = {
                    let mut reg = state.registry.lock().await;
                    reg.prune(std::time::Instant::now());
                    reg.len() as u64
                };
                robots.record(live, &[]);
                sessions.record(state.sessions.lock().await.consumer_count() as u64, &[]);
            }
        });
    }

    let addr = std::env::var("VANTAGE_BIND").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("coordinator listening on {addr}");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
