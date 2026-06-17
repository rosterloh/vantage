mod telemetry;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use vantage_protocol::codec;
use vantage_protocol::signalling::{IceServer, RobotInfo, RobotMsg, ServerMsg};
use vantage_protocol::{RobotId, SessionId};
use vantage_signalling::peer::{Peer, PeerEvent};
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

    let mut peer: Option<Arc<Peer>> = None;
    let mut session: Option<SessionId> = None;
    let mut dc_open = false;
    let mut sampler = Sampler::new();

    let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
    let mut telemetry_tick = tokio::time::interval(Duration::from_secs(1));

    loop {
        // Clone an Arc for the event future so it does not borrow `peer`.
        let peer_ev = peer.clone();
        tokio::select! {
            // Coordinator -> robot
            msg = rx.recv::<ServerMsg>() => {
                match msg? {
                    None => { tracing::info!("coordinator closed"); break; }
                    Some(ServerMsg::ClientConnected { session: s }) => {
                        tracing::info!("client connected: {s}");
                        let p = Arc::new(Peer::new(&ice, true)?); // offerer creates data channel + offer
                        peer = Some(p);
                        session = Some(s);
                        dc_open = false;
                    }
                    Some(ServerMsg::ClientDisconnected { .. }) => {
                        tracing::info!("client disconnected");
                        peer = None; session = None; dc_open = false;
                    }
                    Some(ServerMsg::Signal { signal, .. }) => {
                        if let Some(p) = &peer { p.handle_signal(signal)?; }
                    }
                    Some(ServerMsg::Error { message }) => tracing::warn!("coordinator error: {message}"),
                    _ => {}
                }
            }
            // Peer -> coordinator / local
            ev = async { match &peer_ev { Some(p) => p.recv_event().await, None => std::future::pending().await } } => {
                match ev {
                    Some(PeerEvent::LocalDescription(sig)) | Some(PeerEvent::LocalIce(sig)) => {
                        if let Some(s) = &session {
                            tx.send(&RobotMsg::Signal { to: s.clone(), signal: sig }).await?;
                        }
                    }
                    Some(PeerEvent::DataChannelOpen) => { tracing::info!("data channel open"); dc_open = true; }
                    Some(PeerEvent::DataMessage(_)) => { /* reserved: future control channel */ }
                    None => {}
                }
            }
            _ = heartbeat.tick() => {
                tx.send(&RobotMsg::Heartbeat).await?;
            }
            _ = telemetry_tick.tick() => {
                if dc_open {
                    if let Some(p) = &peer {
                        let info = sampler.sample();
                        let bytes = codec::encode(&info)?;
                        p.send_data(&bytes)?;
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
