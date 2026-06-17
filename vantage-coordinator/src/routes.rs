use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use tokio::sync::{mpsc, Mutex};
use vantage_protocol::signalling::{IceServer, ServerMsg};

use crate::registry::Registry;
use crate::sessions::Sessions;

/// One connected peer's outbound channel (robot or client).
pub type Outbound = mpsc::UnboundedSender<ServerMsg>;

pub struct AppState {
    pub registry: Mutex<Registry>,
    pub sessions: Mutex<Sessions>,
    /// session/robot id (as string) -> its outbound sender
    pub peers: Mutex<std::collections::HashMap<String, Outbound>>,
    pub ice_servers: Vec<IceServer>,
}

impl AppState {
    pub fn from_env() -> Self {
        let mut ice = vec![IceServer {
            urls: vec!["stun:stun.l.google.com:19302".into()],
            username: None,
            credential: None,
        }];
        if let Ok(turn_url) = std::env::var("VANTAGE_TURN_URL") {
            ice.push(IceServer {
                urls: vec![turn_url],
                username: std::env::var("VANTAGE_TURN_USER").ok(),
                credential: std::env::var("VANTAGE_TURN_PASS").ok(),
            });
        }
        Self {
            registry: Mutex::new(Registry::new(Duration::from_secs(15))),
            sessions: Mutex::new(Sessions::default()),
            peers: Mutex::new(std::collections::HashMap::new()),
            ice_servers: ice,
        }
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/ice", get(ice_servers))
        .route("/ws/robot", get(robot_ws))
        .route("/ws/client", get(client_ws))
        .with_state(state)
}

async fn ice_servers(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.ice_servers.clone())
}

async fn robot_ws(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| crate::routes::handle_robot(socket, state))
}

async fn client_ws(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| crate::routes::handle_client(socket, state))
}

// handle_robot / handle_client implemented in Task 8.
pub async fn handle_robot(_socket: WebSocket, _state: Arc<AppState>) { /* Task 8 */ }
pub async fn handle_client(_socket: WebSocket, _state: Arc<AppState>) { /* Task 8 */ }

// Helper kept here so it is unit-testable.
pub fn split_text(msg: Message) -> Option<String> {
    match msg {
        Message::Text(t) => Some(t.to_string()),
        _ => None,
    }
}
