use anyhow::Result;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, accept_async, connect_async, tungstenite::Message,
};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type ServerWsStream = WebSocketStream<TcpStream>;

pub struct CoordinatorWs {
    inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

pub struct CoordinatorWsTx {
    sink: SplitSink<WsStream, Message>,
}
pub struct CoordinatorWsRx {
    stream: SplitStream<WsStream>,
}

impl CoordinatorWs {
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_token(url, None).await
    }

    /// Connect presenting `Authorization: Bearer <token>` when a token is given
    /// (the coordinator's VANTAGE_COORDINATOR_TOKEN). None connects plain — for
    /// open coordinators and the direct LAN path.
    pub async fn connect_with_token(url: &str, token: Option<&str>) -> Result<Self> {
        let (inner, _resp) = connect_async(auth_request(url, token)?).await?;
        Ok(Self { inner })
    }

    pub async fn send<T: serde::Serialize>(&mut self, msg: &T) -> Result<()> {
        let txt = serde_json::to_string(msg)?;
        self.inner.send(Message::Text(txt.into())).await?;
        Ok(())
    }

    pub async fn recv<T: serde::de::DeserializeOwned>(&mut self) -> Result<Option<T>> {
        while let Some(item) = self.inner.next().await {
            if let Message::Text(t) = item? {
                return Ok(Some(serde_json::from_str(&t)?));
            }
        }
        Ok(None)
    }

    /// Split into independent send/receive halves for use in a select loop.
    pub fn split(self) -> (CoordinatorWsTx, CoordinatorWsRx) {
        let (sink, stream) = self.inner.split();
        (CoordinatorWsTx { sink }, CoordinatorWsRx { stream })
    }
}

fn auth_request(
    url: &str,
    token: Option<&str>,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = url.into_client_request()?;
    if let Some(t) = token {
        let value = format!("Bearer {t}")
            .parse()
            .map_err(|e| anyhow::anyhow!("token is not header-safe: {e}"))?;
        req.headers_mut().insert(
            tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
            value,
        );
    }
    Ok(req)
}

impl CoordinatorWsTx {
    pub async fn send<T: serde::Serialize>(&mut self, msg: &T) -> Result<()> {
        let txt = serde_json::to_string(msg)?;
        self.sink.send(Message::Text(txt.into())).await?;
        Ok(())
    }
}

impl CoordinatorWsRx {
    pub async fn recv<T: serde::de::DeserializeOwned>(&mut self) -> Result<Option<T>> {
        while let Some(item) = self.stream.next().await {
            if let Message::Text(t) = item? {
                return Ok(Some(serde_json::from_str(&t)?));
            }
        }
        Ok(None)
    }
}

/// Server side of a signalling WebSocket: the robot's direct LAN listener accepts a
/// client here and speaks the same ServerMsg/ClientMsg the coordinator relays.
pub struct PeerWs {
    inner: ServerWsStream,
}

pub struct PeerWsTx {
    sink: SplitSink<ServerWsStream, Message>,
}
pub struct PeerWsRx {
    stream: SplitStream<ServerWsStream>,
}

impl PeerWs {
    /// Complete the WebSocket handshake on an accepted TCP connection.
    pub async fn accept(stream: TcpStream) -> Result<Self> {
        let inner = accept_async(stream).await?;
        Ok(Self { inner })
    }

    pub fn split(self) -> (PeerWsTx, PeerWsRx) {
        let (sink, stream) = self.inner.split();
        (PeerWsTx { sink }, PeerWsRx { stream })
    }
}

impl PeerWsTx {
    pub async fn send<T: serde::Serialize>(&mut self, msg: &T) -> Result<()> {
        let txt = serde_json::to_string(msg)?;
        self.sink.send(Message::Text(txt.into())).await?;
        Ok(())
    }
}

impl PeerWsRx {
    pub async fn recv<T: serde::de::DeserializeOwned>(&mut self) -> Result<Option<T>> {
        while let Some(item) = self.stream.next().await {
            if let Message::Text(t) = item? {
                return Ok(Some(serde_json::from_str(&t)?));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_request_carries_bearer_header() {
        let req = auth_request("ws://localhost:8080/ws/robot", Some("tok")).unwrap();
        assert_eq!(req.headers().get("authorization").unwrap(), "Bearer tok");
    }

    #[test]
    fn auth_request_without_token_has_no_header() {
        let req = auth_request("ws://localhost:8080/ws/robot", None).unwrap();
        assert!(req.headers().get("authorization").is_none());
    }
}
