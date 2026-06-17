use anyhow::Result;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

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
        let (inner, _resp) = connect_async(url).await?;
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
