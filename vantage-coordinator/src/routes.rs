use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{Mutex, mpsc};
use vantage_protocol::signalling::{ClientMsg, IceServer, RobotMsg, ServerMsg};
use vantage_protocol::{FleetStats, RobotId, SessionId};

use crate::registry::Registry;
use crate::sessions::Sessions;

/// One connected peer's outbound channel (robot or client). Bounded: a peer this
/// far behind is not reading its socket, so further messages to it are dropped
/// (the idle timeout reaps it) instead of growing coordinator memory.
pub type Outbound = mpsc::Sender<ServerMsg>;

const OUTBOUND_QUEUE_DEPTH: usize = 256;

fn send_or_drop(out: &Outbound, msg: ServerMsg) {
    if let Err(mpsc::error::TrySendError::Full(_)) = out.try_send(msg) {
        tracing::warn!("peer outbound queue full — dropping message (slow reader)");
    }
}

/// A peer socket idle (no inbound frame) longer than this is treated as dead and
/// reaped. Chosen well above the robot 5s heartbeat so a live robot never trips.
const PEER_IDLE_TIMEOUT: Duration = Duration::from_secs(20);

/// Signalling messages are small (an SDP is a few KB); anything bigger is abuse.
const MAX_WS_MESSAGE_BYTES: usize = 64 * 1024;

pub struct AppState {
    pub registry: Mutex<Registry>,
    pub sessions: Mutex<Sessions>,
    /// session/robot id (as string) -> (owning connection token, its outbound sender).
    /// The token lets cleanup tell whether the entry still belongs to the socket
    /// tearing itself down, or was already replaced by a reconnect.
    pub peers: Mutex<std::collections::HashMap<String, (u64, Outbound)>>,
    pub ice_servers: Vec<IceServer>,
    /// No inbound frame within this window on a peer socket ⇒ reap it.
    pub idle_timeout: Duration,
    /// When set, every route except /healthz requires `Authorization: Bearer <token>`.
    pub auth_token: Option<String>,
}

impl AppState {
    pub fn from_env() -> Self {
        let mut ice = vantage_protocol::signalling::default_ice();
        if let Ok(turn_url) = std::env::var("VANTAGE_TURN_URL") {
            ice.push(IceServer {
                urls: vec![turn_url],
                username: std::env::var("VANTAGE_TURN_USER").ok(),
                credential: std::env::var("VANTAGE_TURN_PASS").ok(),
            });
        }
        let auth_token = std::env::var("VANTAGE_COORDINATOR_TOKEN")
            .ok()
            .filter(|t| !t.is_empty());
        if auth_token.is_none() {
            tracing::warn!(
                "VANTAGE_COORDINATOR_TOKEN not set — coordinator is UNAUTHENTICATED: \
                 any peer can register, enumerate robots, and fetch TURN credentials"
            );
        }
        Self {
            registry: Mutex::new(Registry::new(Duration::from_secs(15))),
            sessions: Mutex::new(Sessions::default()),
            peers: Mutex::new(std::collections::HashMap::new()),
            ice_servers: ice,
            idle_timeout: PEER_IDLE_TIMEOUT,
            auth_token,
        }
    }

    /// Test/embedding constructor: explicit TTL, idle timeout, caller-supplied ICE
    /// servers, no environment reads.
    pub fn with_config(ttl: Duration, idle_timeout: Duration, ice_servers: Vec<IceServer>) -> Self {
        Self {
            registry: Mutex::new(Registry::new(ttl)),
            sessions: Mutex::new(Sessions::default()),
            peers: Mutex::new(std::collections::HashMap::new()),
            ice_servers,
            idle_timeout,
            auth_token: None,
        }
    }

    /// Enable bearer-token auth on all routes except /healthz.
    pub fn with_auth(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    let protected = Router::new()
        .route("/ice", get(ice_servers))
        .route("/stats", get(stats))
        .route("/ws/robot", get(robot_ws))
        .route("/ws/client", get(client_ws))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_auth,
        ))
        .with_state(state);
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .merge(protected)
}

async fn require_auth(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let Some(expected) = &state.auth_token else {
        return next.run(req).await;
    };
    let presented = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match presented {
        Some(token) if constant_time_eq(token.as_bytes(), expected.as_bytes()) => {
            next.run(req).await
        }
        _ => axum::http::StatusCode::UNAUTHORIZED.into_response(),
    }
}

/// Comparison time must not depend on where the first mismatching byte is.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn ice_servers(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.ice_servers.clone())
}

/// Session-derived fleet statistics. Prune first so the provider count is accurate
/// between the background pruner's ticks; the consumer count reconciles on socket drop.
async fn stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let providers_online = {
        let mut reg = state.registry.lock().await;
        reg.prune(std::time::Instant::now());
        reg.len()
    };
    let consumers_connected = state.sessions.lock().await.consumer_count();
    Json(FleetStats {
        providers_online,
        consumers_connected,
    })
}

async fn robot_ws(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.max_message_size(MAX_WS_MESSAGE_BYTES)
        .max_frame_size(MAX_WS_MESSAGE_BYTES)
        .on_upgrade(move |socket| crate::routes::handle_robot(socket, state))
}

async fn client_ws(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.max_message_size(MAX_WS_MESSAGE_BYTES)
        .max_frame_size(MAX_WS_MESSAGE_BYTES)
        .on_upgrade(move |socket| crate::routes::handle_client(socket, state))
}

#[tracing::instrument(skip_all)]
pub async fn handle_robot(socket: WebSocket, state: Arc<AppState>) {
    let conn = next_conn_id();
    let (mut tx_ws, mut rx_ws) = socket.split();
    let (tx_out, mut rx_out) = mpsc::channel::<ServerMsg>(OUTBOUND_QUEUE_DEPTH);

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

    loop {
        let raw = match tokio::time::timeout(state.idle_timeout, rx_ws.next()).await {
            Err(_) => {
                tracing::warn!("robot connection idle-timed-out; reaping");
                break;
            }
            Ok(None) | Ok(Some(Err(_))) => break,
            Ok(Some(Ok(raw))) => raw,
        };
        let Some(text) = split_text(raw) else {
            continue;
        };
        let Ok(msg) = serde_json::from_str::<RobotMsg>(&text) else {
            send_or_drop(
                &tx_out,
                ServerMsg::Error {
                    message: "bad robot message".into(),
                },
            );
            continue;
        };
        match msg {
            RobotMsg::Register(info) => {
                let id = info.id.clone();
                // Re-Register with a different id on the same socket: evict the
                // previous id's entries, but only if we still own them.
                if let Some(prev) = &my_id
                    && *prev != id
                {
                    let owned = state
                        .peers
                        .lock()
                        .await
                        .get(&prev.0)
                        .is_some_and(|(c, _)| *c == conn);
                    if owned {
                        state.registry.lock().await.remove(prev);
                        state.peers.lock().await.remove(&prev.0);
                    }
                }
                state
                    .registry
                    .lock()
                    .await
                    .register(info, std::time::Instant::now());
                state
                    .peers
                    .lock()
                    .await
                    .insert(id.0.clone(), (conn, tx_out.clone()));
                my_id = Some(id);
            }
            RobotMsg::Heartbeat => {
                if let Some(id) = &my_id {
                    state
                        .registry
                        .lock()
                        .await
                        .heartbeat(id, std::time::Instant::now());
                }
            }
            RobotMsg::Signal { to, signal } => {
                relay_to(&state, &to.0, ServerMsg::Signal { from: None, signal }).await;
            }
        }
    }

    // Cleanup: robot disconnected. Only tear down if this socket still owns the
    // entry — a reconnect on a newer socket may already have replaced it.
    if let Some(id) = my_id {
        let owned = state
            .peers
            .lock()
            .await
            .get(&id.0)
            .is_some_and(|(c, _)| *c == conn);
        if owned {
            state.registry.lock().await.remove(&id);
            state.peers.lock().await.remove(&id.0);
            // Notify any clients attached to this robot.
            let orphaned = state.sessions.lock().await.sessions_for(&id);
            for s in orphaned {
                relay_to(
                    &state,
                    &s.0,
                    ServerMsg::Error {
                        message: "robot disconnected".into(),
                    },
                )
                .await;
                state.sessions.lock().await.close(&s);
            }
        }
    }
    pump.abort();
}

async fn relay_to(state: &Arc<AppState>, peer_key: &str, msg: ServerMsg) {
    if let Some((_, out)) = state.peers.lock().await.get(peer_key) {
        send_or_drop(out, msg);
    }
}

#[tracing::instrument(skip_all)]
pub async fn handle_client(socket: WebSocket, state: Arc<AppState>) {
    let conn = next_conn_id();
    let (mut tx_ws, mut rx_ws) = socket.split();
    let (tx_out, mut rx_out) = mpsc::channel::<ServerMsg>(OUTBOUND_QUEUE_DEPTH);

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
    state
        .peers
        .lock()
        .await
        .insert(session.0.clone(), (conn, tx_out.clone()));

    loop {
        let raw = match tokio::time::timeout(state.idle_timeout, rx_ws.next()).await {
            Err(_) => {
                tracing::warn!("client {} idle-timed-out; reaping", session.0);
                break;
            }
            Ok(None) | Ok(Some(Err(_))) => break,
            Ok(Some(Ok(raw))) => raw,
        };
        let Some(text) = split_text(raw) else {
            continue;
        };
        let Ok(msg) = serde_json::from_str::<ClientMsg>(&text) else {
            send_or_drop(
                &tx_out,
                ServerMsg::Error {
                    message: "bad client message".into(),
                },
            );
            continue;
        };
        match msg {
            ClientMsg::ListRobots => {
                let mut reg = state.registry.lock().await;
                reg.prune(std::time::Instant::now());
                let robots = reg.list();
                send_or_drop(&tx_out, ServerMsg::RobotList { robots });
            }
            ClientMsg::Connect { robot } => {
                // Re-Connect on the same session: tear down the prior session's
                // robot link before opening the new one.
                let prior = state.sessions.lock().await.robot_for(&session).cloned();
                if let Some(prev_robot) = prior
                    && prev_robot != robot
                {
                    relay_to(
                        &state,
                        &prev_robot.0,
                        ServerMsg::ClientDisconnected {
                            session: session.clone(),
                        },
                    )
                    .await;
                }
                state
                    .sessions
                    .lock()
                    .await
                    .open(session.clone(), robot.clone());
                // Tell the robot a client arrived (so it builds its peer + offer).
                relay_to(
                    &state,
                    &robot.0,
                    ServerMsg::ClientConnected {
                        session: session.clone(),
                    },
                )
                .await;
                send_or_drop(
                    &tx_out,
                    ServerMsg::Connected {
                        robot,
                        session: session.clone(),
                    },
                );
            }
            ClientMsg::Signal { signal } => {
                // Route to the robot of this session, tagging our session id.
                let robot = state.sessions.lock().await.robot_for(&session).cloned();
                if let Some(robot) = robot {
                    relay_to(
                        &state,
                        &robot.0,
                        ServerMsg::Signal {
                            from: Some(session.clone()),
                            signal,
                        },
                    )
                    .await;
                }
            }
        }
    }

    // Cleanup: client disconnected -> close session, notify robot.
    if let Some(robot) = state.sessions.lock().await.close(&session) {
        relay_to(
            &state,
            &robot.0,
            ServerMsg::ClientDisconnected {
                session: session.clone(),
            },
        )
        .await;
    }
    // Remove this socket's own peers entry, but only if it still owns it.
    let owned = state
        .peers
        .lock()
        .await
        .get(&session.0)
        .is_some_and(|(c, _)| *c == conn);
    if owned {
        state.peers.lock().await.remove(&session.0);
    }
    pump.abort();
}

static SESSION_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static CONN_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Nanos since epoch captured once, so ids are unique across process restarts too;
/// falls back to 0 rather than panicking if the clock is somehow before the epoch.
fn start_nanos() -> u128 {
    static START: std::sync::OnceLock<u128> = std::sync::OnceLock::new();
    *START.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    })
}

/// Unique-per-process id: process-start nanos plus a monotonic counter, so two
/// connections in the same clock tick never collide.
fn uuid_like() -> String {
    let n = SESSION_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{:x}-{n}", start_nanos())
}

/// Monotonic per-connection token, used to scope cleanup to the socket that
/// owns a given peers/registry entry.
fn next_conn_id() -> u64 {
    CONN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

// Helper kept here so it is unit-testable.
pub fn split_text(msg: Message) -> Option<String> {
    match msg {
        Message::Text(t) => Some(t.to_string()),
        _ => None,
    }
}
