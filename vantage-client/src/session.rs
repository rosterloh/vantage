use anyhow::Result;
use vantage_protocol::codec;
use vantage_protocol::signalling::{ClientMsg, IceServer, ServerMsg};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_signalling::peer::{Peer, PeerEvent, VideoFrame};
use vantage_signalling::ws::CoordinatorWs;

use std::sync::Arc;

/// Where the session pushes decoded frames, telemetry, and status. Implementations
/// are the headless logger (Task 3) and the Slint UI bridge (Task 4).
pub trait UiSink: Send + Sync + 'static {
    fn frame(&self, frame: VideoFrame);
    fn telemetry(&self, info: &DeviceInfo);
    fn status(&self, text: &str);
}

pub async fn run_session(coord: String, ui: Arc<dyn UiSink>) -> Result<()> {
    let mut ws = CoordinatorWs::connect(&format!("{coord}/ws/client")).await?;

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

    let (mut tx, mut rx) = ws.split();
    loop {
        tokio::select! {
            msg = rx.recv::<ServerMsg>() => {
                match msg? {
                    Some(ServerMsg::Signal { signal, .. }) => { peer.handle_signal(signal)?; }
                    Some(ServerMsg::Error { message }) => ui.status(&format!("error: {message}")),
                    Some(_) => {}
                    None => { ui.status("coordinator closed"); break; }
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
                    None => {}
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
