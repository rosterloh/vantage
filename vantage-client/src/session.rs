use anyhow::{Context, Result};
use tokio::sync::mpsc;
use vantage_protocol::ControlMsg;
use vantage_protocol::codec;
use vantage_protocol::signalling::{
    ClientMsg, DialReport, DialStage, IceServer, ServerMsg, Signal, default_ice,
};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_signalling::peer::{Peer, PeerEvent, VideoFrame};
use vantage_signalling::ws::{CoordinatorWs, CoordinatorWsRx, CoordinatorWsTx};

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::discovery;

/// Must stay well under the coordinator's 20s peer idle timeout; 5s matches the
/// robot's heartbeat cadence.
pub(crate) const WS_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// Consecutive keepalive send failures before the session is declared dead
/// (~15s at the 5s cadence). Covers the half-open case the read side can't see.
pub(crate) const MAX_PING_FAILURES: u32 = 3;

/// Where the session pushes decoded frames, telemetry, and status. Implementations
/// are the headless logger (Task 3) and the Slint UI bridge (Task 4).
pub trait UiSink: Send + Sync + 'static {
    fn frame(&self, frame: VideoFrame);
    fn telemetry(&self, info: &DeviceInfo);
    fn status(&self, text: &str);
}

/// A dial that has not reached an open data channel by now is abandoned.
const NEGOTIATE_TIMEOUT: Duration = Duration::from_secs(20);

/// A negotiated connection, ready for the session loop.
struct Link {
    peer: Arc<Peer>,
    tx: CoordinatorWsTx,
    rx: CoordinatorWsRx,
}

#[tracing::instrument(skip(ui, control_rx))]
pub async fn run_session(
    coord: String,
    ui: Arc<dyn UiSink>,
    control_rx: mpsc::UnboundedReceiver<ControlMsg>,
) -> Result<()> {
    let token = std::env::var("VANTAGE_COORDINATOR_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    // Race the coordinator and the mDNS LAN path; the first to open a data channel
    // wins and the other is dropped (closing its socket tears down its robot side).
    let (link, path) = first_ok(
        async {
            via_coordinator(&coord, token.as_deref(), ui.as_ref())
                .await
                .map(|l| (l, "coordinator"))
                .context("coordinator")
        },
        async {
            via_mdns(ui.as_ref())
                .await
                .map(|l| (l, "LAN"))
                .context("LAN")
        },
    )
    .await?;
    tracing::info!("connected via {path}");
    ui.status(&format!("connected ({path})"));
    run_answer_loop(link.peer, link.tx, link.rx, ui, control_rx).await
}

/// The first of `a` and `b` to succeed; if both fail, both errors.
async fn first_ok<T>(
    a: impl Future<Output = Result<T>>,
    b: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::pin!(a, b);
    let (first, other) = tokio::select! {
        r = &mut a => match r {
            Ok(v) => return Ok(v),
            Err(e) => (e, b.await),
        },
        r = &mut b => match r {
            Ok(v) => return Ok(v),
            Err(e) => (e, a.await),
        },
    };
    other.map_err(|e| anyhow::anyhow!("{first:#}; {e:#}"))
}

/// Coordinator path: pick a robot, negotiate, and report how far the dial got.
async fn via_coordinator(coord: &str, token: Option<&str>, ui: &dyn UiSink) -> Result<Link> {
    let started = Instant::now();
    let ws = CoordinatorWs::connect_with_token(&format!("{coord}/ws/client"), token).await?;
    let (mut tx, mut rx) = ws.split();
    let mut stage = DialStage::SignallingConnected;
    let result = async {
        tx.send(&ClientMsg::ListRobots).await?;
        let robots = loop {
            match rx.recv::<ServerMsg>().await? {
                Some(ServerMsg::RobotList { robots }) => break robots,
                Some(_) => continue,
                None => anyhow::bail!("coordinator closed before sending robot list"),
            }
        };
        let target = robots
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("no robots online"))?;
        ui.status(&format!("connecting to {}", target.name));
        tx.send(&ClientMsg::Connect {
            robot: target.id.clone(),
        })
        .await?;
        stage = DialStage::RobotSelected;
        let ice = fetch_ice(coord, token).await?;
        let peer = Arc::new(Peer::new(&ice)?);
        negotiate(&peer, &mut tx, &mut rx, &mut stage, ui).await?;
        Ok(peer)
    }
    .await;

    let (local_candidate, remote_candidate) = match &result {
        Ok(peer) => peer.selected_candidate_types().await.unzip(),
        Err(_) => (None, None),
    };
    let report = DialReport {
        stage,
        duration_ms: started.elapsed().as_millis() as u64,
        local_candidate,
        remote_candidate,
        error: result.as_ref().err().map(|e| format!("{e:#}")),
    };
    if let Err(e) = tx.send(&ClientMsg::DialReport(report)).await {
        tracing::debug!("dial report not sent: {e}");
    }
    Ok(Link {
        peer: result?,
        tx,
        rx,
    })
}

/// mDNS LAN fast path: browse for a robot and connect directly to its signalling
/// endpoint (the robot offers immediately; no ListRobots/Connect needed).
async fn via_mdns(ui: &dyn UiSink) -> Result<Link> {
    let robots = discovery::discover(Duration::from_secs(2)).await?;
    let target = robots
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("no robots found on the LAN via mDNS"))?;
    tracing::info!("mDNS: discovered {} at {}", target.id, target.addr);
    ui.status(&format!(
        "connecting to {} (LAN) at {}",
        target.name, target.addr
    ));

    let ws = CoordinatorWs::connect(&format!("ws://{}", target.addr)).await?;
    // No coordinator to fetch ICE from; host candidates carry the LAN.
    let peer = Arc::new(Peer::new(&default_ice())?);
    let (mut tx, mut rx) = ws.split();
    // No coordinator to report to on this path, so the stage is only tracked.
    let mut stage = DialStage::SignallingConnected;
    negotiate(&peer, &mut tx, &mut rx, &mut stage, ui).await?;
    Ok(Link { peer, tx, rx })
}

/// Relay signalling until the robot's data channel opens, advancing `stage`.
async fn negotiate(
    peer: &Peer,
    tx: &mut CoordinatorWsTx,
    rx: &mut CoordinatorWsRx,
    stage: &mut DialStage,
    ui: &dyn UiSink,
) -> Result<()> {
    tokio::time::timeout(NEGOTIATE_TIMEOUT, async {
        loop {
            tokio::select! {
                msg = rx.recv::<ServerMsg>() => match msg? {
                    Some(ServerMsg::Signal { signal, .. }) => {
                        if matches!(signal, Signal::Offer { .. }) {
                            *stage = (*stage).max(DialStage::OfferReceived);
                        }
                        if let Err(e) = peer.handle_signal(signal) {
                            tracing::warn!("failed to apply signal: {e}");
                        }
                    }
                    Some(ServerMsg::Error { message }) => ui.status(&format!("error: {message}")),
                    Some(ServerMsg::RobotDisconnected { robot }) => {
                        anyhow::bail!("robot {robot} disconnected")
                    }
                    Some(_) => {}
                    None => anyhow::bail!("signalling closed"),
                },
                ev = peer.recv_event() => match ev {
                    Some(PeerEvent::LocalDescription(sig)) => {
                        tx.send(&ClientMsg::Signal { signal: sig }).await?;
                        *stage = (*stage).max(DialStage::AnswerSent);
                    }
                    Some(PeerEvent::LocalIce(sig)) => {
                        tx.send(&ClientMsg::Signal { signal: sig }).await?;
                    }
                    Some(PeerEvent::DataChannelOpen) => {
                        *stage = DialStage::Ready;
                        return Ok(());
                    }
                    Some(_) => {}
                    None => anyhow::bail!("peer closed"),
                },
            }
        }
    })
    .await
    .context("data channel did not open in time")?
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
    // Metric instruments — no-ops until a global meter provider is installed.
    let meter = vantage_observability::opentelemetry::global::meter("vantage-client");
    let frames_rx = meter.u64_counter("vantage.client.frames_received").build();
    let telemetry_rx = meter
        .u64_counter("vantage.client.telemetry_received")
        .build();

    // Frame pump: decoded frames -> UI.
    let pump = {
        let peer = peer.clone();
        let ui = ui.clone();
        let frames_rx = frames_rx.clone();
        tokio::spawn(async move {
            while let Some(frame) = peer.recv_frame().await {
                frames_rx.add(1, &[]);
                ui.frame(frame);
            }
        })
    };

    let mut control_closed = false;
    let mut keepalive = tokio::time::interval(WS_KEEPALIVE_INTERVAL);
    let mut ping_failures = 0u32;
    loop {
        tokio::select! {
            msg = rx.recv::<ServerMsg>() => {
                match msg? {
                    Some(ServerMsg::Signal { signal, .. }) => {
                        if let Err(e) = peer.handle_signal(signal) {
                            ui.status(&format!("signal error: {e}"));
                            tracing::warn!("failed to apply signal: {e}");
                        }
                    }
                    Some(ServerMsg::Error { message }) => ui.status(&format!("error: {message}")),
                    Some(ServerMsg::RobotDisconnected { robot }) => {
                        ui.status(&format!("robot {robot} disconnected"));
                        tracing::warn!("robot {robot} disconnected — ending session");
                        break;
                    }
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
                            telemetry_rx.add(1, &[]);
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
                    Some(msg) => {
                        if let Err(e) = peer.send_control(&msg) {
                            tracing::warn!("control send failed: {e}");
                        }
                    }
                    None => control_closed = true,
                }
            }
            _ = keepalive.tick() => {
                match tx.send_ping().await {
                    Ok(()) => ping_failures = 0,
                    Err(e) => {
                        ping_failures += 1;
                        tracing::warn!("ws keepalive failed ({ping_failures}/{MAX_PING_FAILURES}): {e}");
                        if ping_failures >= MAX_PING_FAILURES {
                            ui.status("connection lost (keepalive failed)");
                            break;
                        }
                    }
                }
            }
        }
    }
    pump.abort();
    peer.shutdown();
    Ok(())
}

pub(crate) async fn fetch_ice(coord: &str, token: Option<&str>) -> Result<Vec<IceServer>> {
    let http = coord.replacen("ws", "http", 1);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn first_ok_takes_the_first_success() {
        let slow_ok = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok("slow")
        };
        let fast_err = async { anyhow::bail!("down") };
        assert_eq!(first_ok(fast_err, slow_ok).await.unwrap(), "slow");

        let fast_ok = async { Ok("fast") };
        let never = std::future::pending::<Result<&str>>();
        assert_eq!(first_ok(never, fast_ok).await.unwrap(), "fast");
    }

    #[tokio::test]
    async fn first_ok_reports_both_errors() {
        let a = async { Err::<(), _>(anyhow::anyhow!("a failed")) };
        let b = async { Err::<(), _>(anyhow::anyhow!("b failed")) };
        let msg = first_ok(a, b).await.unwrap_err().to_string();
        assert!(
            msg.contains("a failed") && msg.contains("b failed"),
            "{msg}"
        );
    }
}
