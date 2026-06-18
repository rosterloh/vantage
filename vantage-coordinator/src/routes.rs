use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, Mutex};
use vantage_protocol::signalling::{ClientMsg, IceServer, RobotMsg, ServerMsg};
use vantage_protocol::{RobotId, SessionId};

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

pub async fn handle_robot(socket: WebSocket, state: Arc<AppState>) {
    let (mut tx_ws, mut rx_ws) = socket.split();
    let (tx_out, mut rx_out) = mpsc::unbounded_channel::<ServerMsg>();

    // Pump outbound ServerMsgs to the websocket.
    let pump = tokio::spawn(async move {
        while let Some(msg) = rx_out.recv().await {
            let txt = serde_json::to_string(&msg).unwrap();
            if tx_ws.send(Message::Text(txt.into())).await.is_err() {
                break;
            }
        }
    });

    let mut my_id: Option<RobotId> = None;

    while let Some(Ok(raw)) = rx_ws.next().await {
        let Some(text) = split_text(raw) else { continue };
        let Ok(msg) = serde_json::from_str::<RobotMsg>(&text) else {
            let _ = tx_out.send(ServerMsg::Error { message: "bad robot message".into() });
            continue;
        };
        match msg {
            RobotMsg::Register(info) => {
                let id = info.id.clone();
                state.registry.lock().await.register(info, std::time::Instant::now());
                state.peers.lock().await.insert(id.0.clone(), tx_out.clone());
                my_id = Some(id);
            }
            RobotMsg::Heartbeat => {
                if let Some(id) = &my_id {
                    state.registry.lock().await.heartbeat(id, std::time::Instant::now());
                }
            }
            RobotMsg::Signal { to, signal } => {
                relay_to(&state, &to.0, ServerMsg::Signal { from: None, signal }).await;
            }
        }
    }

    // Cleanup: robot disconnected.
    if let Some(id) = my_id {
        state.registry.lock().await.remove(&id);
        state.peers.lock().await.remove(&id.0);
        // Notify any clients attached to this robot.
        let orphaned = state.sessions.lock().await.sessions_for(&id);
        for s in orphaned {
            relay_to(&state, &s.0, ServerMsg::Error { message: "robot disconnected".into() }).await;
            state.sessions.lock().await.close(&s);
        }
    }
    pump.abort();
}

async fn relay_to(state: &Arc<AppState>, peer_key: &str, msg: ServerMsg) {
    if let Some(out) = state.peers.lock().await.get(peer_key) {
        let _ = out.send(msg);
    }
}

pub async fn handle_client(socket: WebSocket, state: Arc<AppState>) {
    let (mut tx_ws, mut rx_ws) = socket.split();
    let (tx_out, mut rx_out) = mpsc::unbounded_channel::<ServerMsg>();

    let pump = tokio::spawn(async move {
        while let Some(msg) = rx_out.recv().await {
            let txt = serde_json::to_string(&msg).unwrap();
            if tx_ws.send(Message::Text(txt.into())).await.is_err() {
                break;
            }
        }
    });

    // Each client connection is one session.
    let session = SessionId(format!("sess-{}", uuid_like()));
    state.peers.lock().await.insert(session.0.clone(), tx_out.clone());

    while let Some(Ok(raw)) = rx_ws.next().await {
        let Some(text) = split_text(raw) else { continue };
        let Ok(msg) = serde_json::from_str::<ClientMsg>(&text) else {
            let _ = tx_out.send(ServerMsg::Error { message: "bad client message".into() });
            continue;
        };
        match msg {
            ClientMsg::ListRobots => {
                let mut reg = state.registry.lock().await;
                reg.prune(std::time::Instant::now());
                let robots = reg.list();
                let _ = tx_out.send(ServerMsg::RobotList { robots });
            }
            ClientMsg::Connect { robot } => {
                state.sessions.lock().await.open(session.clone(), robot.clone());
                // Tell the robot a client arrived (so it builds its peer + offer).
                relay_to(&state, &robot.0, ServerMsg::ClientConnected { session: session.clone() }).await;
                let _ = tx_out.send(ServerMsg::Connected { robot, session: session.clone() });
            }
            ClientMsg::Signal { signal } => {
                // Route to the robot of this session, tagging our session id.
                let robot = state.sessions.lock().await.robot_for(&session).cloned();
                if let Some(robot) = robot {
                    relay_to(&state, &robot.0,
                        ServerMsg::Signal { from: Some(session.clone()), signal }).await;
                }
            }
        }
    }

    // Cleanup: client disconnected -> close session, notify robot.
    if let Some(robot) = state.sessions.lock().await.close(&session) {
        relay_to(&state, &robot.0, ServerMsg::ClientDisconnected { session: session.clone() }).await;
    }
    state.peers.lock().await.remove(&session.0);
    pump.abort();
}

/// Tiny unique-ish id without pulling in the uuid crate for the PoC.
fn uuid_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{n:x}")
}

// Helper kept here so it is unit-testable.
pub fn split_text(msg: Message) -> Option<String> {
    match msg {
        Message::Text(t) => Some(t.to_string()),
        _ => None,
    }
}
