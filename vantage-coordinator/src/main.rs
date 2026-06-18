mod registry;
mod routes;
mod sessions;

use std::sync::Arc;

use routes::{router, AppState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let state = Arc::new(AppState::from_env());

    // Background pruner so stale robots expire even with no traffic.
    {
        let state = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tick.tick().await;
                state.registry.lock().await.prune(std::time::Instant::now());
            }
        });
    }

    let addr = std::env::var("VANTAGE_BIND").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("coordinator listening on {addr}");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
