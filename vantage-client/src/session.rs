use anyhow::Result;
use tokio::sync::mpsc;
use vantage_protocol::codec;
use vantage_protocol::signalling::{default_ice, ClientMsg, IceServer, ServerMsg};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_protocol::ControlMsg;
use vantage_signalling::peer::{Peer, PeerEvent, VideoFrame};
use vantage_signalling::ws::{CoordinatorWs, CoordinatorWsRx, CoordinatorWsTx};

use std::sync::Arc;
use std::time::Duration;

use crate::discovery;

/// Where the session pushes decoded frames, telemetry, and status. Implementations
/// are the headless logger (Task 3) and the Slint UI bridge (Task 4).
pub trait UiSink: Send + Sync + 'static {
    fn frame(&self, frame: VideoFrame);
    fn telemetry(&self, info: &DeviceInfo);
    fn status(&self, text: &str);
}

pub async fn run_session(
    coord: String,
    ui: Arc<dyn UiSink>,
    control_rx: mpsc::UnboundedReceiver<ControlMsg>,
) -> Result<()> {
    // Primary path: the coordinator. If it is unreachable, fall back to the mDNS
    // LAN fast path (offline direct connect).
    match CoordinatorWs::connect(&format!("{coord}/ws/client")).await {
        Ok(ws) => run_via_coordinator(coord, ws, ui, control_rx).await,
        Err(e) => {
            ui.status(&format!("coordinator unreachable ({e}); scanning LAN…"));
            tracing::warn!("coordinator unreachable ({e}) — trying mDNS LAN fast path");
            run_via_mdns(ui, control_rx).await
        }
    }
}

/// Coordinator path: discover via the robot list, then connect.
async fn run_via_coordinator(
    coord: String,
    mut ws: CoordinatorWs,
    ui: Arc<dyn UiSink>,
    control_rx: mpsc::UnboundedReceiver<ControlMsg>,
) -> Result<()> {
    ws.send(&ClientMsg::ListRobots).await?;
    let robots = loop {
        match ws.recv::<ServerMsg>().await? {
            Some(ServerMsg::RobotList { robots }) => break robots,
            Some(_) => continue,
            None => anyhow::bail!("coordinator closed before sending robot list"),
        }
    };
    let target = robots.into_iter().next().ok_or_else(|| anyhow::anyhow!("no robots online"))?;
    ui.status(&format!("connecting to {}", target.name));
    ws.send(&ClientMsg::Connect { robot: target.id.clone() }).await?;

    let ice = fetch_ice(&coord).await?;
    let peer = Arc::new(Peer::new(&ice)?);
    let (tx, rx) = ws.split();
    run_answer_loop(peer, tx, rx, ui, control_rx).await
}

/// mDNS LAN fast path: browse for a robot and connect directly to its signalling
/// endpoint (the robot offers immediately; no ListRobots/Connect needed).
async fn run_via_mdns(
    ui: Arc<dyn UiSink>,
    control_rx: mpsc::UnboundedReceiver<ControlMsg>,
) -> Result<()> {
    let robots = discovery::discover(Duration::from_secs(2)).await?;
    let target = robots
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("no robots found on the LAN via mDNS"))?;
    tracing::info!("mDNS: discovered {} at {}", target.id, target.addr);
    ui.status(&format!("connecting to {} (LAN) at {}", target.name, target.addr));

    let ws = CoordinatorWs::connect(&format!("ws://{}", target.addr)).await?;
    // No coordinator to fetch ICE from; host candidates carry the LAN.
    let peer = Arc::new(Peer::new(&default_ice())?);
    let (tx, rx) = ws.split();
    run_answer_loop(peer, tx, rx, ui, control_rx).await
}

/// The shared answerer loop: relay signalling, render frames/telemetry, forward
/// operator control. Identical over the coordinator and direct transports.
async fn run_answer_loop(
    peer: Arc<Peer>,
    mut tx: CoordinatorWsTx,
    mut rx: CoordinatorWsRx,
    ui: Arc<dyn UiSink>,
    mut control_rx: mpsc::UnboundedReceiver<ControlMsg>,
) -> Result<()> {
    // Frame pump: decoded frames -> UI.
    {
        let peer = peer.clone();
        let ui = ui.clone();
        tokio::spawn(async move {
            while let Some(frame) = peer.recv_frame().await {
                ui.frame(frame);
            }
        });
    }

    let mut control_closed = false;
    loop {
        tokio::select! {
            msg = rx.recv::<ServerMsg>() => {
                match msg? {
                    Some(ServerMsg::Signal { signal, .. }) => { peer.handle_signal(signal)?; }
                    Some(ServerMsg::Error { message }) => ui.status(&format!("error: {message}")),
                    Some(_) => {}
                    None => { ui.status("connection closed"); break; }
                }
            }
            ev = peer.recv_event() => {
                match ev {
                    Some(PeerEvent::LocalDescription(sig)) | Some(PeerEvent::LocalIce(sig)) => {
                        tx.send(&ClientMsg::Signal { signal: sig }).await?;
                    }
                    Some(PeerEvent::DataChannelOpen) => ui.status("connected"),
                    Some(PeerEvent::DataMessage(bytes)) => {
                        if let Ok(info) = codec::decode::<DeviceInfo>(&bytes) {
                            ui.telemetry(&info);
                        }
                    }
                    // Control is client->robot only; nothing arrives here.
                    Some(PeerEvent::Control(_)) => {}
                    None => {}
                }
            }
            // Operator input (keyboard / headless stub) -> robot control channel.
            cmd = control_rx.recv(), if !control_closed => {
                match cmd {
                    Some(msg) => peer.send_control(&msg)?,
                    None => control_closed = true,
                }
            }
        }
    }
    Ok(())
}

async fn fetch_ice(coord: &str) -> Result<Vec<IceServer>> {
    let http = coord.replacen("ws", "http", 1);
    let servers = reqwest::get(format!("{http}/ice")).await?.json::<Vec<IceServer>>().await?;
    Ok(servers)
}
