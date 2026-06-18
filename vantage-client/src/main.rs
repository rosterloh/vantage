use anyhow::Result;
use vantage_protocol::codec;
use vantage_protocol::signalling::{ClientMsg, IceServer, ServerMsg};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_signalling::peer::{Peer, PeerEvent, Role};
use vantage_signalling::ws::CoordinatorWs;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let coord = std::env::var("VANTAGE_COORDINATOR").unwrap_or_else(|_| "ws://localhost:8080".into());

    let mut ws = CoordinatorWs::connect(&format!("{coord}/ws/client")).await?;

    // 1. Discover
    ws.send(&ClientMsg::ListRobots).await?;
    let robots = loop {
        match ws.recv::<ServerMsg>().await? {
            Some(ServerMsg::RobotList { robots }) => break robots,
            Some(_) => continue,
            None => anyhow::bail!("coordinator closed before sending robot list"),
        }
    };
    let target = robots.into_iter().next().ok_or_else(|| anyhow::anyhow!("no robots online"))?;
    tracing::info!("connecting to {} ({})", target.name, target.id);

    // 2. Connect (robot will create the peer + offer on its side)
    ws.send(&ClientMsg::Connect { robot: target.id.clone() }).await?;

    // 3. Build the answerer peer
    let ice = fetch_ice(&coord).await?;
    let peer = Peer::new(&ice, Role::Client)?;

    // 4. Split and run the signalling/data loop
    let (mut tx, mut rx) = ws.split();
    loop {
        tokio::select! {
            msg = rx.recv::<ServerMsg>() => {
                match msg? {
                    Some(ServerMsg::Signal { signal, .. }) => { peer.handle_signal(signal)?; }
                    Some(ServerMsg::Error { message }) => tracing::warn!("coordinator error: {message}"),
                    Some(_) => {}
                    None => { tracing::info!("coordinator closed"); break; }
                }
            }
            ev = peer.recv_event() => {
                match ev {
                    Some(PeerEvent::LocalDescription(sig)) | Some(PeerEvent::LocalIce(sig)) => {
                        tx.send(&ClientMsg::Signal { signal: sig }).await?;
                    }
                    Some(PeerEvent::DataChannelOpen) => tracing::info!("data channel open"),
                    Some(PeerEvent::DataMessage(bytes)) => {
                        match codec::decode::<DeviceInfo>(&bytes) {
                            Ok(info) => tracing::info!(
                                "telemetry: cpu={:.1}% mem={}/{}MB temps={} uptime={}s",
                                info.cpu_percent, info.mem_used_mb, info.mem_total_mb,
                                info.temps.len(), info.uptime_s),
                            Err(e) => tracing::warn!("bad telemetry: {e}"),
                        }
                    }
                    None => {}
                }
            }
        }
    }
    Ok(())
}

async fn fetch_ice(coord: &str) -> Result<Vec<IceServer>> {
    let http = coord.replacen("ws", "http", 1); // ws->http, wss->https
    let servers = reqwest::get(format!("{http}/ice")).await?.json::<Vec<IceServer>>().await?;
    Ok(servers)
}
