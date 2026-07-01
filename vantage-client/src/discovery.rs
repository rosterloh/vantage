//! Client-side mDNS browse for the LAN fast path: find `_vantage._tcp` robots so
//! the client can connect directly when the coordinator is unreachable.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent};
use vantage_protocol::discovery::{robot_from_txt, MDNS_SERVICE_TYPE};

/// A robot discovered on the LAN, with its direct signalling endpoint.
pub struct Discovered {
    pub id: String,
    pub name: String,
    pub addr: SocketAddr,
}

/// Browse for Vantage robots for up to `timeout`, returning every resolved instance.
pub async fn discover(timeout: Duration) -> Result<Vec<Discovered>> {
    let daemon = ServiceDaemon::new().context("start mDNS daemon")?;
    let receiver = daemon.browse(MDNS_SERVICE_TYPE).context("browse mDNS")?;
    let mut found = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, receiver.recv_async()).await {
            Ok(Ok(ServiceEvent::ServiceResolved(info))) => {
                let props: HashMap<String, String> = info
                    .get_properties()
                    .iter()
                    .map(|p| (p.key().to_string(), p.val_str().to_string()))
                    .collect();
                if let Some((id, name)) = robot_from_txt(&props) {
                    if let Some(ip) = info.get_addresses().iter().next() {
                        found.push(Discovered {
                            id,
                            name,
                            addr: SocketAddr::new(*ip, info.get_port()),
                        });
                    }
                }
            }
            Ok(Ok(_)) => {} // other service events (found/removed) — keep waiting
            Ok(Err(_)) => break, // browse channel closed
            Err(_) => break,     // timeout elapsed
        }
    }
    let _ = daemon.shutdown();
    Ok(found)
}
