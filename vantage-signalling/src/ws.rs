use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

pub struct CoordinatorWs {
    inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
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
}
