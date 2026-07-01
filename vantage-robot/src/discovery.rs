//! LAN fast path: advertise this robot over mDNS and serve a direct signalling
//! WebSocket so a client can connect peer-to-peer when the coordinator is
//! unreachable. The direct path reuses the shared `RobotMedia` engine and applies
//! the same teleop watchdog as the coordinator path — no safety regression offline.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use mdns_sd::{ServiceDaemon, ServiceInfo};
use tokio::net::TcpListener;
use vantage_protocol::discovery::{advert_txt, MDNS_SERVICE_TYPE};
use vantage_protocol::signalling::{ClientMsg, IceServer, RobotInfo, ServerMsg};
use vantage_protocol::SessionId;
use vantage_signalling::peer::PeerEvent;
use vantage_signalling::robot_media::RobotMedia;
use vantage_signalling::ws::PeerWs;

use crate::safety::{SafeState, Watchdog};
use crate::telemetry::Sampler;

/// Bind the direct signalling listener on an ephemeral port and spawn its accept
/// loop. Returns the bound port for the mDNS SRV record.
pub async fn serve_direct(media: Arc<RobotMedia>, ice: Vec<IceServer>) -> Result<u16> {
    let listener = TcpListener::bind("0.0.0.0:0")
        .await
        .context("bind direct signalling listener")?;
    let port = listener.local_addr()?.port();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let media = media.clone();
                    let ice = ice.clone();
                    tokio::spawn(async move {
                        tracing::info!("direct client connecting from {addr}");
                        if let Err(e) = handle_direct_client(media, ice, stream).await {
                            tracing::warn!("direct client {addr} ended: {e}");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("direct accept failed: {e}");
                    return;
                }
            }
        }
    });
    tracing::info!("direct LAN signalling listener on port {port}");
    Ok(port)
}

/// Advertise this robot as a `_vantage._tcp` service. The returned daemon must be
/// kept alive for the advertisement to persist.
pub fn advertise(info: &RobotInfo, port: u16) -> Result<ServiceDaemon> {
    let daemon = ServiceDaemon::new().context("start mDNS daemon")?;
    let instance = info.id.0.clone();
    let host = format!("vantage-{}.local.", info.id.0);
    let service = ServiceInfo::new(
        MDNS_SERVICE_TYPE,
        &instance,
        &host,
        "", // addresses auto-detected below
        port,
        advert_txt(info),
    )
    .context("build mDNS service info")?
    .enable_addr_auto();
    daemon.register(service).context("register mDNS service")?;
    tracing::info!("advertised {MDNS_SERVICE_TYPE} instance {instance} on port {port}");
    Ok(daemon)
}

/// Drive one directly-connected client: create a consumer off the shared engine,
/// offer immediately (robot is offerer), relay signalling, and enforce the teleop
/// watchdog. Mirrors the coordinator path but scoped to a single peer.
async fn handle_direct_client(
    media: Arc<RobotMedia>,
    _ice: Vec<IceServer>,
    stream: tokio::net::TcpStream,
) -> Result<()> {
    let ws = PeerWs::accept(stream).await?;
    let (mut tx, mut rx) = ws.split();

    // Unique session id per direct peer (a counter, so concurrent clients never collide).
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let session = SessionId(format!("direct-{}", NEXT.fetch_add(1, Ordering::Relaxed)));
    let (events_tx, mut events_rx) = tokio::sync::mpsc::unbounded_channel::<(SessionId, PeerEvent)>();

    // Creating the consumer builds the offer and both data channels immediately.
    let consumer = media.add_consumer(session.clone(), events_tx)?;
    let mut watchdog = Watchdog::default();
    watchdog.arm(session.clone(), Instant::now());
    let mut dc_open = false;
    let mut sampler = Sampler::new();
    let mut watchdog_tick = tokio::time::interval(Duration::from_millis(100));
    let mut telemetry_tick = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            msg = rx.recv::<ClientMsg>() => {
                match msg {
                    Ok(Some(ClientMsg::Signal { signal })) => { consumer.handle_signal(signal)?; }
                    // 1:1 direct link: ListRobots/Connect are unnecessary and ignored.
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
            ev = events_rx.recv() => {
                match ev {
                    Some((_, PeerEvent::LocalDescription(sig))) | Some((_, PeerEvent::LocalIce(sig))) => {
                        tx.send(&ServerMsg::Signal { from: None, signal: sig }).await?;
                    }
                    Some((_, PeerEvent::DataChannelOpen)) => { dc_open = true; }
                    Some((s, PeerEvent::Control(bytes))) => {
                        if let Ok(cmd) = vantage_protocol::codec::decode::<vantage_protocol::ControlMsg>(&bytes) {
                            watchdog.feed(&s, Instant::now());
                            if watchdog.is_live(&s) {
                                crate::act_on_control(&s, cmd);
                            }
                        }
                    }
                    Some((_, PeerEvent::DataMessage(_))) => {}
                    None => {}
                }
            }
            _ = watchdog_tick.tick() => {
                for (s, state) in watchdog.tick(Instant::now()) {
                    let SafeState::Entered { reason } = state;
                    tracing::warn!("safe-state entered: {s} ({reason})");
                }
            }
            _ = telemetry_tick.tick() => {
                if dc_open {
                    let bytes = vantage_protocol::codec::encode(&sampler.sample())?;
                    consumer.send_data(&bytes)?;
                }
            }
        }
    }

    watchdog.disarm(&session);
    media.remove_consumer(consumer);
    tracing::warn!("safe-state entered: {session} (disconnect)");
    Ok(())
}
