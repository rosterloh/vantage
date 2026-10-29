//! Byte tunnels over WebRTC data channels (SSH today).
//!
//! [`attach`] turns a reliable, ordered `WebRTCDataChannel` into a plain
//! `AsyncRead + AsyncWrite` stream. Everything above that — bridging to `sshd` on
//! the robot, or to stdio in `vantage-client ssh-proxy` — is transport-agnostic
//! byte copying, so the same callers would work unchanged over e.g. a QUIC stream.
//!
//! One channel is one TCP connection: there is no framing, and since data channels
//! have no half-close, EOF in either direction closes the channel.

use std::sync::Arc;
use std::time::Duration;

use gstreamer::glib;
use gstreamer_webrtc as gst_webrtc;
use gstreamer_webrtc::WebRTCDataChannelState;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::{Notify, mpsc};

/// Largest single `send-data` payload. Safe across SCTP implementations regardless
/// of the negotiated `max-message-size`.
pub const MAX_CHUNK: usize = 16 * 1024;
/// Stop reading the local side while this much is queued in the channel...
pub const HIGH_WATER: u64 = 1024 * 1024;
/// ...and resume once `on-buffered-amount-low` reports it has drained below this.
pub const LOW_WATER: u64 = 256 * 1024;
/// In-process buffer between the channel pump and the caller's stream.
const DUPLEX_CAPACITY: usize = 64 * 1024;
/// How long a freshly created channel may take to open before it is abandoned.
const OPEN_TIMEOUT: Duration = Duration::from_secs(10);
/// How long to wait for queued bytes to drain before closing on local EOF.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Robot-side SSH tunnel settings. `None` from [`SshTunnelConfig::from_env`] means
/// SSH is disabled and every operator-created channel is closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshTunnelConfig {
    /// Where tunnelled connections go. Fixed by the robot, never chosen by the
    /// operator, so the robot cannot be used as a pivot into its LAN.
    pub target: String,
    /// Concurrent SSH channels allowed per operator session.
    pub max_channels: usize,
}

impl SshTunnelConfig {
    pub const DEFAULT_TARGET: &str = "127.0.0.1:22";
    pub const DEFAULT_MAX_CHANNELS: usize = 4;

    /// `VANTAGE_SSH_ENABLE=1` turns SSH tunnels on; `VANTAGE_SSH_TARGET` (default
    /// `127.0.0.1:22`) and `VANTAGE_SSH_MAX_CHANNELS` (default 4) tune them.
    pub fn from_env() -> Option<Self> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Option<Self> {
        let enabled = get("VANTAGE_SSH_ENABLE").is_some_and(|v| v != "0" && !v.is_empty());
        if !enabled {
            return None;
        }
        let target = get("VANTAGE_SSH_TARGET")
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| Self::DEFAULT_TARGET.into());
        let max_channels = get("VANTAGE_SSH_MAX_CHANNELS")
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(Self::DEFAULT_MAX_CHANNELS);
        Some(Self {
            target,
            max_channels,
        })
    }
}

/// True when a channel delivers every byte in order — the only kind a byte
/// tunnel can run over (`-1` means the limit is unset).
pub fn is_reliable_ordered(dc: &gst_webrtc::WebRTCDataChannel) -> bool {
    dc.is_ordered() && dc.max_retransmits() == -1 && dc.max_packet_lifetime() == -1
}

/// Bytes moved by one tunnel, for logging when it ends.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TunnelStats {
    /// Received from the remote peer and written to the local stream.
    pub bytes_in: u64,
    /// Read from the local stream and sent to the remote peer.
    pub bytes_out: u64,
}

enum Inbound {
    Data(Vec<u8>),
    Closed,
}

/// Wire `dc`'s signals and return the local end of a byte stream bridged to it,
/// plus the pump task moving bytes between the two (it resolves when the tunnel
/// ends). Must be called before the channel can deliver messages — synchronously
/// in `on-data-channel`, or right after `create-data-channel` — so none are missed.
///
/// Dropping or shutting down the returned stream closes the channel; the channel
/// closing makes the stream read EOF and fail writes.
pub fn attach(
    dc: gst_webrtc::WebRTCDataChannel,
    rt: &tokio::runtime::Handle,
) -> (DuplexStream, tokio::task::JoinHandle<TunnelStats>) {
    // Inbound is unbounded: SCTP gives the receiver no way to push back, so a
    // slow local writer can only be absorbed here. Both local sinks (sshd on
    // loopback, stdout) drain far faster than the link can fill this.
    let (in_tx, in_rx) = mpsc::unbounded_channel();
    let opened = Arc::new(Notify::new());
    let low = Arc::new(Notify::new());

    dc.set_buffered_amount_low_threshold(LOW_WATER);
    {
        let in_tx = in_tx.clone();
        dc.connect_on_message_data(move |_, bytes| {
            if let Some(b) = bytes {
                let _ = in_tx.send(Inbound::Data(b.to_vec()));
            }
        });
    }
    {
        // A text message means the peer is not speaking the byte-tunnel protocol.
        let in_tx = in_tx.clone();
        dc.connect_on_message_string(move |_, _| {
            tracing::warn!("text message on a byte tunnel — closing");
            let _ = in_tx.send(Inbound::Closed);
        });
    }
    {
        let in_tx = in_tx.clone();
        dc.connect_on_close(move |_| {
            let _ = in_tx.send(Inbound::Closed);
        });
    }
    {
        let in_tx = in_tx.clone();
        dc.connect_on_error(move |_, e| {
            tracing::warn!("tunnel data channel error: {e}");
            let _ = in_tx.send(Inbound::Closed);
        });
    }
    {
        let opened = opened.clone();
        dc.connect_on_open(move |_| opened.notify_one());
    }
    {
        let low = low.clone();
        dc.connect_on_buffered_amount_low(move |_| low.notify_one());
    }

    let (local, remote) = tokio::io::duplex(DUPLEX_CAPACITY);
    let task = rt.spawn(pump(dc, in_rx, opened, low, remote));
    (local, task)
}

async fn pump(
    dc: gst_webrtc::WebRTCDataChannel,
    mut inbound: mpsc::UnboundedReceiver<Inbound>,
    opened: Arc<Notify>,
    low: Arc<Notify>,
    io: DuplexStream,
) -> TunnelStats {
    let mut stats = TunnelStats::default();

    if !wait_open(&dc, &opened).await {
        tracing::warn!("tunnel data channel did not open within {OPEN_TIMEOUT:?}");
        dc.close();
        return stats;
    }

    let (mut rd, mut wr) = tokio::io::split(io);
    let mut buf = vec![0u8; MAX_CHUNK];
    loop {
        tokio::select! {
            ev = inbound.recv() => match ev {
                Some(Inbound::Data(bytes)) => {
                    if wr.write_all(&bytes).await.is_err() {
                        // Local side is gone; nothing left to deliver to.
                        dc.close();
                        break;
                    }
                    stats.bytes_in += bytes.len() as u64;
                }
                // Remote closed: dropping `io` gives the caller EOF.
                Some(Inbound::Closed) | None => break,
            },
            read = async {
                wait_for_room(&dc, &low).await;
                rd.read(&mut buf).await
            } => match read {
                Ok(0) | Err(_) => {
                    // Local EOF: let queued bytes reach the peer, then close.
                    drain(&dc).await;
                    dc.close();
                    break;
                }
                Ok(n) => {
                    let chunk = glib::Bytes::from(&buf[..n]);
                    if let Err(e) = dc.send_data_full(Some(&chunk)) {
                        tracing::warn!("tunnel send failed: {e}");
                        dc.close();
                        break;
                    }
                    stats.bytes_out += n as u64;
                }
            },
        }
    }
    stats
}

/// Wait for the channel to reach `Open`. Returns false on timeout or if it closed.
async fn wait_open(dc: &gst_webrtc::WebRTCDataChannel, opened: &Notify) -> bool {
    let deadline = tokio::time::Instant::now() + OPEN_TIMEOUT;
    loop {
        match dc.ready_state() {
            WebRTCDataChannelState::Open => return true,
            WebRTCDataChannelState::Closing | WebRTCDataChannelState::Closed => return false,
            _ => {}
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        // `on-open` may race the state check above; the short timeout covers it.
        let _ = tokio::time::timeout(Duration::from_millis(100), opened.notified()).await;
    }
}

/// Backpressure: hold off reading the local side while the channel is full.
async fn wait_for_room(dc: &gst_webrtc::WebRTCDataChannel, low: &Notify) {
    while dc.buffered_amount() > HIGH_WATER {
        // `on-buffered-amount-low` may fire between the check and the wait; the
        // timeout re-checks rather than relying on that edge.
        let _ = tokio::time::timeout(Duration::from_millis(50), low.notified()).await;
    }
}

/// Wait (bounded) for everything queued on the channel to be handed to SCTP.
async fn drain(dc: &gst_webrtc::WebRTCDataChannel) {
    let deadline = tokio::time::Instant::now() + DRAIN_TIMEOUT;
    while dc.buffered_amount() > 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Bridge a tunnel stream to a TCP connection to `target` until either side ends.
/// Used by the robot for each accepted SSH channel.
pub async fn bridge_to_tcp(mut stream: DuplexStream, target: &str) -> std::io::Result<(u64, u64)> {
    let mut tcp = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect(target),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timed out"))??;
    let _ = tcp.set_nodelay(true);
    let res = tokio::io::copy_bidirectional(&mut stream, &mut tcp).await;
    let _ = stream.shutdown().await;
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| map.get(k).cloned()
    }

    #[test]
    fn ssh_is_disabled_by_default() {
        assert_eq!(SshTunnelConfig::from_lookup(lookup(&[])), None);
        assert_eq!(
            SshTunnelConfig::from_lookup(lookup(&[("VANTAGE_SSH_ENABLE", "0")])),
            None
        );
        // A target alone does not enable SSH.
        assert_eq!(
            SshTunnelConfig::from_lookup(lookup(&[("VANTAGE_SSH_TARGET", "10.0.0.1:22")])),
            None
        );
    }

    #[test]
    fn ssh_enabled_uses_loopback_defaults() {
        let cfg = SshTunnelConfig::from_lookup(lookup(&[("VANTAGE_SSH_ENABLE", "1")])).unwrap();
        assert_eq!(cfg.target, "127.0.0.1:22");
        assert_eq!(cfg.max_channels, 4);
    }

    #[test]
    fn ssh_config_overrides_and_rejects_zero_channels() {
        let cfg = SshTunnelConfig::from_lookup(lookup(&[
            ("VANTAGE_SSH_ENABLE", "1"),
            ("VANTAGE_SSH_TARGET", "127.0.0.1:2222"),
            ("VANTAGE_SSH_MAX_CHANNELS", "0"),
        ]))
        .unwrap();
        assert_eq!(cfg.target, "127.0.0.1:2222");
        assert_eq!(cfg.max_channels, SshTunnelConfig::DEFAULT_MAX_CHANNELS);
    }
}
