//! `vantage-client ssh-proxy <robot>`: an OpenSSH `ProxyCommand` that carries one
//! SSH connection over a WebRTC data channel to the robot's own `sshd`.
//!
//! ```text
//! Host robot-*
//!   ProxyCommand vantage-client ssh-proxy %h
//! ```
//!
//! stdin/stdout carry the SSH byte stream, so all logging goes to stderr. The
//! session is headless: video frames are received but discarded.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use vantage_protocol::SSH_CAPABILITY;
use vantage_protocol::signalling::{ClientMsg, ServerMsg, default_ice};
use vantage_signalling::peer::{Peer, PeerEvent};
use vantage_signalling::ws::{CoordinatorWs, CoordinatorWsRx, CoordinatorWsTx};

use crate::discovery;
use crate::session::{MAX_PING_FAILURES, WS_KEEPALIVE_INTERVAL, fetch_ice};

/// How long the WebRTC session may take to come up before giving up.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Tunnel stdin/stdout to `robot`'s sshd. `robot` matches a robot id or name.
pub async fn run(coord: String, robot: String) -> Result<()> {
    let token = std::env::var("VANTAGE_COORDINATOR_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    match CoordinatorWs::connect_with_token(&format!("{coord}/ws/client"), token.as_deref()).await {
        Ok(ws) => via_coordinator(coord, token, ws, &robot).await,
        Err(e) => {
            tracing::warn!("coordinator unreachable ({e}) — trying mDNS LAN fast path");
            via_mdns(&robot).await
        }
    }
}

async fn via_coordinator(
    coord: String,
    token: Option<String>,
    mut ws: CoordinatorWs,
    robot: &str,
) -> Result<()> {
    ws.send(&ClientMsg::ListRobots).await?;
    let robots = loop {
        match ws.recv::<ServerMsg>().await? {
            Some(ServerMsg::RobotList { robots }) => break robots,
            Some(_) => continue,
            None => anyhow::bail!("coordinator closed before sending robot list"),
        }
    };
    let target = robots
        .into_iter()
        .find(|r| r.id.0 == robot || r.name == robot)
        .with_context(|| format!("robot '{robot}' is not online"))?;
    if !target.capabilities.iter().any(|c| c == SSH_CAPABILITY) {
        anyhow::bail!(
            "robot '{robot}' does not accept SSH tunnels (set VANTAGE_SSH_ENABLE=1 on the robot)"
        );
    }
    ws.send(&ClientMsg::Connect {
        robot: target.id.clone(),
    })
    .await?;

    let ice = fetch_ice(&coord, token.as_deref()).await?;
    let peer = Arc::new(Peer::new(&ice)?);
    let (tx, rx) = ws.split();
    tunnel_stdio(peer, tx, rx).await
}

async fn via_mdns(robot: &str) -> Result<()> {
    let target = discovery::discover(Duration::from_secs(2))
        .await?
        .into_iter()
        .find(|r| r.id == robot || r.name == robot)
        .with_context(|| format!("robot '{robot}' not found on the LAN via mDNS"))?;
    tracing::info!("mDNS: discovered {} at {}", target.id, target.addr);
    let ws = CoordinatorWs::connect(&format!("ws://{}", target.addr)).await?;
    let peer = Arc::new(Peer::new(&default_ice())?);
    let (tx, rx) = ws.split();
    tunnel_stdio(peer, tx, rx).await
}

/// Relay signalling until the session is up, then open an `ssh` channel and copy
/// it to/from stdio until either side closes.
async fn tunnel_stdio(
    peer: Arc<Peer>,
    mut tx: CoordinatorWsTx,
    mut rx: CoordinatorWsRx,
) -> Result<()> {
    // Headless: discard frames so the decode queue never backs up.
    let drain = {
        let peer = peer.clone();
        tokio::spawn(async move { while peer.recv_frame().await.is_some() {} })
    };

    let mut bridge: Option<tokio::task::JoinHandle<std::io::Result<(u64, u64)>>> = None;
    let connect_deadline = tokio::time::sleep(CONNECT_TIMEOUT);
    tokio::pin!(connect_deadline);
    let mut keepalive = tokio::time::interval(WS_KEEPALIVE_INTERVAL);
    let mut ping_failures = 0u32;

    let result = loop {
        tokio::select! {
            msg = rx.recv::<ServerMsg>() => match msg {
                Ok(Some(ServerMsg::Signal { signal, .. })) => {
                    if let Err(e) = peer.handle_signal(signal) {
                        tracing::warn!("failed to apply signal: {e}");
                    }
                }
                Ok(Some(ServerMsg::Error { message })) => break Err(anyhow::anyhow!("coordinator error: {message}")),
                Ok(Some(ServerMsg::RobotDisconnected { robot })) => break Err(anyhow::anyhow!("robot {robot} disconnected")),
                Ok(Some(_)) => {}
                Ok(None) => break Err(anyhow::anyhow!("signalling connection closed")),
                Err(e) => break Err(e),
            },
            ev = peer.recv_event() => match ev {
                Some(PeerEvent::LocalDescription(sig)) | Some(PeerEvent::LocalIce(sig)) => {
                    tx.send(&ClientMsg::Signal { signal: sig }).await?;
                }
                // The robot's telemetry channel opening means SCTP is up: open ours.
                Some(PeerEvent::DataChannelOpen) if bridge.is_none() => {
                    let mut stream = peer.open_ssh_channel()?;
                    tracing::info!("ssh channel requested");
                    bridge = Some(tokio::spawn(async move {
                        let mut stdio = tokio::io::join(tokio::io::stdin(), tokio::io::stdout());
                        tokio::io::copy_bidirectional(&mut stdio, &mut stream).await
                    }));
                }
                _ => {}
            },
            res = async { bridge.as_mut().unwrap().await }, if bridge.is_some() => {
                break match res {
                    Ok(Ok((up, down))) => {
                        tracing::info!("ssh tunnel closed ({up} B up, {down} B down)");
                        Ok(())
                    }
                    Ok(Err(e)) => Err(anyhow::anyhow!("ssh tunnel ended: {e}")),
                    Err(e) => Err(anyhow::anyhow!("ssh tunnel task failed: {e}")),
                };
            }
            _ = &mut connect_deadline, if bridge.is_none() => {
                break Err(anyhow::anyhow!("WebRTC session did not come up within {CONNECT_TIMEOUT:?}"));
            }
            _ = keepalive.tick() => {
                match tx.send_ping().await {
                    Ok(()) => ping_failures = 0,
                    Err(e) => {
                        ping_failures += 1;
                        tracing::warn!("ws keepalive failed ({ping_failures}/{MAX_PING_FAILURES}): {e}");
                        if ping_failures >= MAX_PING_FAILURES {
                            break Err(anyhow::anyhow!("signalling connection lost"));
                        }
                    }
                }
            }
        }
    };

    drain.abort();
    peer.shutdown();
    result
}
