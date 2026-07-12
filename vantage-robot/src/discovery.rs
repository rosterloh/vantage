//! LAN fast path: advertise this robot over mDNS and serve a direct signalling
//! WebSocket so a client can connect peer-to-peer when the coordinator is
//! unreachable. The direct path reuses the shared `RobotMedia` engine and applies
//! the same teleop watchdog as the coordinator path — no safety regression offline.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use mdns_sd::{ServiceDaemon, ServiceInfo};
use tokio::net::TcpListener;
use vantage_protocol::SessionId;
use vantage_protocol::discovery::{MDNS_SERVICE_TYPE, advert_txt};
use vantage_protocol::signalling::{ClientMsg, RobotInfo, ServerMsg};
use vantage_signalling::peer::PeerEvent;
use vantage_signalling::robot_media::RobotMedia;
use vantage_signalling::ws::PeerWs;

use crate::safety::{Admit, Freshness, Watchdog};
use crate::telemetry::Sampler;

/// Bind the direct signalling listener on an ephemeral port and spawn its accept
/// loop. Returns the bound port for the mDNS SRV record.
pub async fn serve_direct(media: Arc<RobotMedia>) -> Result<u16> {
    let listener = TcpListener::bind("0.0.0.0:0")
        .await
        .context("bind direct signalling listener")?;
    let port = listener.local_addr()?.port();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let media = media.clone();
                    tokio::spawn(async move {
                        tracing::info!("direct client connecting from {addr}");
                        if let Err(e) = handle_direct_client(media, stream).await {
                            tracing::warn!("direct client {addr} ended: {e}");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("direct accept failed: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
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
async fn handle_direct_client(media: Arc<RobotMedia>, stream: tokio::net::TcpStream) -> Result<()> {
    let ws = PeerWs::accept(stream).await?;
    let (mut tx, mut rx) = ws.split();

    // Unique session id per direct peer (a counter, so concurrent clients never collide).
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let session = SessionId(format!("direct-{}", NEXT.fetch_add(1, Ordering::Relaxed)));
    let (events_tx, mut events_rx) =
        tokio::sync::mpsc::unbounded_channel::<(SessionId, PeerEvent)>();

    // Outbound writes are pumped through a bounded channel by a dedicated task so a
    // blocked peer socket write can never stall the select loop below (in particular
    // the watchdog_tick arm, which must always fire on schedule).
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<ServerMsg>(64);
    let pump = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if tx.send(&msg).await.is_err() {
                break;
            }
        }
    });

    // Creating the consumer builds the offer and both data channels immediately.
    let consumer = media.add_consumer(session.clone(), events_tx)?;
    let mut watchdog = Watchdog::default();
    watchdog.arm(session.clone(), Instant::now());
    let mut freshness = Freshness::default();
    let control_epoch = Instant::now();
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
                        if out_tx.try_send(ServerMsg::Signal { from: None, signal: sig }).is_err() {
                            tracing::warn!("direct peer outbound full/closed — dropping signal for {session}");
                        }
                    }
                    Some((_, PeerEvent::DataChannelOpen)) => { dc_open = true; }
                    Some((s, PeerEvent::Control(bytes))) => {
                        match vantage_protocol::codec::decode::<vantage_protocol::ControlEnvelope>(&bytes) {
                            Ok(env) => {
                                let arrival_ms = control_epoch.elapsed().as_millis() as u64;
                                match freshness.admit(&s, env.seq, env.sent_ms, arrival_ms) {
                                    Admit::Fresh => {
                                        watchdog.feed(&s, Instant::now());
                                        if watchdog.is_live(&s) {
                                            crate::act_on_control(&s, env.msg);
                                        }
                                    }
                                    verdict => tracing::warn!(
                                        "dropping control from {s}: {verdict:?} (seq {})",
                                        env.seq
                                    ),
                                }
                            }
                            Err(e) => tracing::warn!("bad control message from {s}: {e}"),
                        }
                    }
                    Some((_, PeerEvent::DataMessage(_))) => {}
                    None => {}
                }
            }
            _ = watchdog_tick.tick() => {
                for (s, reason) in watchdog.tick(Instant::now()) {
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

    pump.abort();
    watchdog.disarm(&session);
    media.remove_consumer(consumer);
    tracing::warn!("safe-state entered: {session} (disconnect)");
    Ok(())
}
