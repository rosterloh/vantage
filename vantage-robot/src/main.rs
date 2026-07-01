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
use vantage_protocol::signalling::{IceServer, RobotInfo, RobotMsg, ServerMsg};
use vantage_protocol::{RobotId, SessionId};
use vantage_signalling::peer::PeerEvent;
use vantage_signalling::robot_media::{Consumer, RobotMedia};
use vantage_signalling::ws::CoordinatorWs;

use telemetry::Sampler;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let coord =
        std::env::var("VANTAGE_COORDINATOR").unwrap_or_else(|_| "ws://localhost:8080".into());
    let id = RobotId(std::env::var("VANTAGE_ROBOT_ID").unwrap_or_else(|_| "robot-1".into()));

    let ice = fetch_ice(&coord).await?;

    let ws = CoordinatorWs::connect(&format!("{coord}/ws/robot")).await?;
    let (mut tx, mut rx) = ws.split();
    tx.send(&RobotMsg::Register(RobotInfo {
        id: id.clone(),
        name: std::env::var("VANTAGE_ROBOT_NAME").unwrap_or_else(|_| "Atlas".into()),
        capabilities: vec!["telemetry".into()],
    }))
    .await?;
    tracing::info!("registered as {id}");

    // ONE shared capture/encode engine: camera → tee → encode-once → rtptee fan-out,
    // plus the raw pre-encode branch. Built (and PLAYed) once; encoder runs once.
    let media = Arc::new(RobotMedia::new(&ice)?);
    let mut consumers: HashMap<SessionId, Consumer> = HashMap::new();
    // Sessions whose telemetry data channel is open.
    let mut dc_open: std::collections::HashSet<SessionId> = std::collections::HashSet::new();
    let mut sampler = Sampler::new();

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
        tokio::spawn(async move {
            let mut n: u64 = 0;
            while let Some(frame) = media_raw.recv_raw_frame().await {
                n += 1;
                let (w, h) = (frame.width, frame.height);
                match bridge.publish(frame) {
                    Ok(()) => {
                        if n == 1 || n % 30 == 0 {
                            tracing::info!("published ros image {w}x{h} (#{n})");
                        }
                    }
                    Err(e) => tracing::warn!("ros publish failed: {e}"),
                }
            }
        });
    }
    #[cfg(not(feature = "ros"))]
    {
        let media_raw = media.clone();
        tokio::spawn(async move {
            let mut n: u64 = 0;
            while let Some(frame) = media_raw.recv_raw_frame().await {
                n += 1;
                if n == 1 || n % 30 == 0 {
                    tracing::info!(
                        "raw frame {}x{} {} (#{n})",
                        frame.width,
                        frame.height,
                        frame.encoding
                    );
                }
            }
        });
    }

    let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
    let mut telemetry_tick = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            // Coordinator -> robot
            msg = rx.recv::<ServerMsg>() => {
                match msg? {
                    None => { tracing::info!("coordinator closed"); break; }
                    Some(ServerMsg::ClientConnected { session: s }) => {
                        tracing::info!("client connected: {s}");
                        let c = media.add_consumer(s.clone(), events_tx.clone())?;
                        consumers.insert(s, c);
                    }
                    Some(ServerMsg::ClientDisconnected { session: s }) => {
                        tracing::info!("client disconnected: {s}");
                        if let Some(c) = consumers.remove(&s) {
                            media.remove_consumer(c);
                        }
                        dc_open.remove(&s);
                    }
                    Some(ServerMsg::Signal { from: Some(s), signal }) => {
                        if let Some(c) = consumers.get(&s) { c.handle_signal(signal)?; }
                    }
                    Some(ServerMsg::Signal { from: None, .. }) => {}
                    Some(ServerMsg::Error { message }) => tracing::warn!("coordinator error: {message}"),
                    _ => {}
                }
            }
            // Consumers -> coordinator / local  (merged, session-tagged)
            ev = events_rx.recv() => {
                match ev {
                    Some((s, PeerEvent::LocalDescription(sig))) | Some((s, PeerEvent::LocalIce(sig))) => {
                        tx.send(&RobotMsg::Signal { to: s, signal: sig }).await?;
                    }
                    Some((s, PeerEvent::DataChannelOpen)) => {
                        tracing::info!("data channel open: {s}");
                        dc_open.insert(s);
                    }
                    Some((_, PeerEvent::DataMessage(_))) => { /* telemetry is robot->client only */ }
                    Some((s, PeerEvent::Control(bytes))) => {
                        // Task 2: log receipt only. Task 3 gates this behind the watchdog
                        // before any command is treated as live.
                        match codec::decode::<vantage_protocol::ControlMsg>(&bytes) {
                            Ok(msg) => tracing::debug!("control from {s}: {msg:?}"),
                            Err(e) => tracing::warn!("bad control message from {s}: {e}"),
                        }
                    }
                    None => {}
                }
            }
            _ = heartbeat.tick() => {
                tx.send(&RobotMsg::Heartbeat).await?;
            }
            _ = telemetry_tick.tick() => {
                let info = sampler.sample();
                let bytes = codec::encode(&info)?;
                for (s, c) in &consumers {
                    if dc_open.contains(s) {
                        c.send_data(&bytes)?;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn fetch_ice(coord: &str) -> Result<Vec<IceServer>> {
    let http = coord.replacen("ws", "http", 1); // ws://host -> http://host, wss:// -> https://
    let servers = reqwest::get(format!("{http}/ice"))
        .await?
        .json::<Vec<IceServer>>()
        .await?;
    Ok(servers)
}
