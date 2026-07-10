mod discovery;
mod metrics;
mod safety;
mod telemetry;

#[allow(dead_code)] // used by ros/mod.rs under --features ros; exercised by unit tests otherwise
mod convert;

#[cfg(feature = "ros")]
mod ros;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use vantage_protocol::codec;
use vantage_protocol::signalling::{IceServer, RobotInfo, RobotMsg, ServerMsg, default_ice};
use vantage_protocol::{RobotId, SessionId};
use vantage_signalling::peer::PeerEvent;
use vantage_signalling::robot_media::{Consumer, RobotMedia};
use vantage_signalling::ws::CoordinatorWs;

use metrics::RobotMetrics;
use safety::Watchdog;
use telemetry::Sampler;

#[tokio::main]
async fn main() -> Result<()> {
    let _otel = vantage_observability::init("vantage-robot");

    let coord =
        std::env::var("VANTAGE_COORDINATOR").unwrap_or_else(|_| "ws://localhost:8080".into());
    let id = RobotId(std::env::var("VANTAGE_ROBOT_ID").unwrap_or_else(|_| "robot-1".into()));
    let token = std::env::var("VANTAGE_COORDINATOR_TOKEN").ok();

    // ICE is fetched from the coordinator, but on the offline LAN path the coordinator
    // is unreachable — fall back to STUN-only (host candidates carry the LAN).
    let ice = fetch_ice(&coord, token.as_deref())
        .await
        .unwrap_or_else(|e| {
            tracing::warn!("ICE fetch failed ({e}) — using STUN-only defaults (LAN/direct path)");
            default_ice()
        });
    let robot_info = RobotInfo {
        id: id.clone(),
        name: std::env::var("VANTAGE_ROBOT_NAME").unwrap_or_else(|_| "Atlas".into()),
        capabilities: vec!["telemetry".into()],
    };

    // ONE shared capture/encode engine: camera → tee → encode-once → rtptee fan-out,
    // plus the raw pre-encode branch. Built (and PLAYed) once; encoder runs once.
    let media = Arc::new(RobotMedia::new(&ice)?);
    // Sessions whose telemetry data channel is open.
    let mut dc_open: std::collections::HashSet<SessionId> = std::collections::HashSet::new();
    // Teleop disconnect failsafe: no command is acted on unless its session is live.
    let mut watchdog = Watchdog::default();
    let mut sampler = Sampler::new();
    let metrics = RobotMetrics::new();

    // Shared, session-tagged event channel: every Consumer forwards its PeerEvents
    // here so the single loop below selects over all consumers at once.
    let (events_tx, mut events_rx) =
        tokio::sync::mpsc::unbounded_channel::<(SessionId, PeerEvent)>();

    // Construct the ROS bridge once and share it across client sessions. The
    // bridge spins its own executor on a dedicated thread (see CameraBridge::new).
    #[cfg(feature = "ros")]
    let ros_bridge = Arc::new(ros::CameraBridge::new()?);

    // Drain the raw (pre-encode) branch once, for the engine's lifetime, concurrently
    // with the WebRTC streams. recv_raw_frame locks one receiver, so exactly one drain
    // owns it — the active branch is selected at compile time by the `ros` feature.
    #[cfg(feature = "ros")]
    {
        let media_raw = media.clone();
        let bridge = ros_bridge.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let mut n: u64 = 0;
            while let Some(frame) = media_raw.recv_raw_frame().await {
                n += 1;
                metrics.frame_published();
                let (w, h) = (frame.width, frame.height);
                match bridge.publish(frame) {
                    Ok(()) => {
                        if n == 1 || n.is_multiple_of(30) {
                            tracing::info!("published ros image {w}x{h} (#{n})");
                        }
                    }
                    Err(e) => tracing::warn!("ros publish failed: {e}"),
                }
            }
            tracing::warn!("raw-frame drain task exited");
        });
    }
    #[cfg(not(feature = "ros"))]
    {
        let media_raw = media.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let mut n: u64 = 0;
            while let Some(frame) = media_raw.recv_raw_frame().await {
                n += 1;
                metrics.frame_published();
                if n == 1 || n.is_multiple_of(30) {
                    tracing::info!(
                        "raw frame {}x{} {} (#{n})",
                        frame.width,
                        frame.height,
                        frame.encoding
                    );
                }
            }
            tracing::warn!("raw-frame drain task exited");
        });
    }

    // LAN fast path: direct signalling listener + mDNS advertisement, independent of
    // the coordinator so an offline LAN client can still discover and connect.
    let direct_port = discovery::serve_direct(media.clone()).await?;
    let _mdns = match discovery::advertise(&robot_info, direct_port) {
        Ok(daemon) => Some(daemon),
        Err(e) => {
            tracing::warn!("mDNS advertise failed: {e}");
            None
        }
    };

    // Coordinator path (primary). If unreachable, keep running to serve LAN clients.
    let ws = match CoordinatorWs::connect_with_token(&format!("{coord}/ws/robot"), token.as_deref())
        .await
    {
        Ok(ws) => ws,
        Err(e) => {
            tracing::warn!(
                "coordinator unreachable ({e}) — serving LAN clients via mDNS/direct only"
            );
            shutdown_signal().await;
            tracing::info!("shutdown signal received (LAN-only mode)");
            media.shutdown();
            return Ok(());
        }
    };
    let (mut tx, mut rx) = ws.split();
    if let Err(e) = tx.send(&RobotMsg::Register(robot_info.clone())).await {
        tracing::warn!(
            "coordinator registration failed ({e}) — serving LAN clients via mDNS/direct only"
        );
        shutdown_signal().await;
        tracing::info!("shutdown signal received (LAN-only mode)");
        media.shutdown();
        return Ok(());
    }
    tracing::info!("registered as {id}");
    let mut consumers: HashMap<SessionId, Consumer> = HashMap::new();

    // Outbound writes are pumped through a bounded channel by a dedicated task so a
    // blocked coordinator socket write can never stall the select loop below (in
    // particular the watchdog_tick arm, which must always fire on schedule).
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<RobotMsg>(64);
    let pump = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if tx.send(&msg).await.is_err() {
                break;
            }
        }
    });

    let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
    let mut telemetry_tick = tokio::time::interval(Duration::from_secs(1));
    // Poll the failsafe often relative to CONTROL_TIMEOUT (500ms) so a stale session
    // trips promptly.
    let mut watchdog_tick = tokio::time::interval(Duration::from_millis(100));

    let mut received_shutdown = false;
    loop {
        tokio::select! {
            // Coordinator -> robot
            msg = rx.recv::<ServerMsg>() => {
                match msg {
                    Err(e) => { tracing::warn!("bad frame from coordinator ({e})"); break; }
                    Ok(None) => { tracing::info!("coordinator closed"); break; }
                    Ok(Some(ServerMsg::ClientConnected { session: s })) => {
                        tracing::info!("client connected: {s}");
                        if let Some(old) = consumers.remove(&s) {
                            tracing::warn!("duplicate ClientConnected for {s}; replacing consumer");
                            media.remove_consumer(old);
                        }
                        let c = match media.add_consumer(s.clone(), events_tx.clone()) {
                            Ok(c) => c,
                            Err(e) => { tracing::warn!("failed to add consumer for {s}: {e}"); continue; }
                        };
                        watchdog.arm(s.clone(), std::time::Instant::now());
                        consumers.insert(s, c);
                    }
                    Ok(Some(ServerMsg::ClientDisconnected { session: s })) => {
                        tracing::info!("client disconnected: {s}");
                        if let Some(c) = consumers.remove(&s) {
                            media.remove_consumer(c);
                        }
                        watchdog.disarm(&s);
                        tracing::warn!("safe-state entered: {s} (disconnect)");
                        dc_open.remove(&s);
                    }
                    Ok(Some(ServerMsg::Signal { from: Some(s), signal })) => {
                        if let Some(c) = consumers.get(&s)
                            && let Err(e) = c.handle_signal(signal)
                        {
                            tracing::warn!("failed to apply signal for {s}: {e}");
                        }
                    }
                    Ok(Some(ServerMsg::Signal { from: None, .. })) => {}
                    Ok(Some(ServerMsg::Error { message })) => tracing::warn!("coordinator error: {message}"),
                    Ok(_) => {}
                }
            }
            // Consumers -> coordinator / local  (merged, session-tagged)
            ev = events_rx.recv() => {
                match ev {
                    Some((s, PeerEvent::LocalDescription(sig))) | Some((s, PeerEvent::LocalIce(sig))) => {
                        if out_tx.try_send(RobotMsg::Signal { to: s.clone(), signal: sig }).is_err() {
                            tracing::warn!("coordinator outbound full/closed — dropping signal for {s}");
                        }
                    }
                    Some((s, PeerEvent::DataChannelOpen)) => {
                        tracing::info!("data channel open: {s}");
                        dc_open.insert(s);
                    }
                    Some((_, PeerEvent::DataMessage(_))) => { /* telemetry is robot->client only */ }
                    Some((s, PeerEvent::Control(bytes))) => {
                        match codec::decode::<vantage_protocol::ControlMsg>(&bytes) {
                            Ok(msg) => {
                                // A fresh command re-arms the watchdog (and clears any
                                // prior staleness trip — a momentary stall self-heals).
                                watchdog.feed(&s, std::time::Instant::now());
                                // SAFETY GATE: only act on the command while the session
                                // is live; otherwise hold the neutral command.
                                if watchdog.is_live(&s) {
                                    act_on_control(&s, msg);
                                }
                            }
                            Err(e) => tracing::warn!("bad control message from {s}: {e}"),
                        }
                    }
                    None => {}
                }
            }
            _ = watchdog_tick.tick() => {
                for (s, reason) in watchdog.tick(std::time::Instant::now()) {
                    // Neutral command held for this session (no actuator in the PoC).
                    tracing::warn!("safe-state entered: {s} ({reason})");
                }
            }
            _ = heartbeat.tick() => {
                let _ = out_tx.try_send(RobotMsg::Heartbeat);
            }
            _ = telemetry_tick.tick() => {
                let info = sampler.sample();
                metrics.record_device(&info);
                match codec::encode(&info) {
                    Ok(bytes) => {
                        for (s, c) in &consumers {
                            if dc_open.contains(s)
                                && let Err(e) = c.send_data(&bytes)
                            {
                                tracing::warn!("telemetry send failed for {s}: {e}");
                            }
                        }
                    }
                    Err(e) => tracing::warn!("telemetry encode failed: {e}"),
                }
            }
            _ = shutdown_signal() => {
                tracing::info!("shutdown signal received");
                received_shutdown = true;
                break;
            }
        }
    }

    pump.abort();
    for (s, c) in consumers.drain() {
        watchdog.disarm(&s);
        media.remove_consumer(c);
    }
    if !received_shutdown {
        tracing::warn!("coordinator link lost — continuing to serve LAN clients via mDNS/direct");
        shutdown_signal().await;
        tracing::info!("shutdown signal received (LAN-only mode)");
    }
    media.shutdown();
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut s) = signal(SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
}

/// Act on a live teleop command. The PoC has no actuator, so "acting" means logging
/// the command (a real robot would drive its motion layer here). KeepAlive carries no
/// motion — it only re-armed the watchdog at the call site.
fn act_on_control(session: &SessionId, msg: vantage_protocol::ControlMsg) {
    use vantage_protocol::ControlMsg;
    match msg {
        ControlMsg::Move { linear, angular } => {
            tracing::info!("acting on control {session}: move linear={linear} angular={angular}");
        }
        ControlMsg::KeepAlive => {}
    }
}

async fn fetch_ice(coord: &str, token: Option<&str>) -> Result<Vec<IceServer>> {
    let http = coord.replacen("ws", "http", 1); // ws://host -> http://host, wss:// -> https://
    let mut req = reqwest::Client::new().get(format!("{http}/ice"));
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    let servers = req
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<IceServer>>()
        .await?;
    Ok(servers)
}
