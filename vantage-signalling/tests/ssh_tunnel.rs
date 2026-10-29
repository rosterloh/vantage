//! End-to-end SSH tunnel over a real in-process WebRTC session: a robot
//! `RobotMedia` consumer and an operator `Peer` signal each other directly, the
//! operator opens an `ssh` channel, and the robot bridges it to a local TCP echo
//! server standing in for `sshd`.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use vantage_protocol::SessionId;
use vantage_signalling::peer::{Peer, PeerEvent};
use vantage_signalling::robot_media::{Consumer, RobotMedia};
use vantage_signalling::tunnel::SshTunnelConfig;

/// A TCP echo server; returns its address.
async fn echo_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (mut r, mut w) = sock.split();
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    addr
}

/// Connect a robot consumer and an operator peer, relaying signalling between
/// them in the background. Resolves once the operator's telemetry channel is open
/// (the SCTP association is up).
async fn connect(media: &RobotMedia) -> (Arc<Consumer>, Arc<Peer>) {
    let (robot_tx, mut robot_rx) = mpsc::unbounded_channel();
    let consumer = media
        .add_consumer(SessionId("ssh-test".into()), robot_tx)
        .expect("consumer builds");
    let peer = Arc::new(Peer::new(&[]).expect("peer builds"));

    // Robot -> operator signalling.
    {
        let peer = peer.clone();
        tokio::spawn(async move {
            while let Some((_, ev)) = robot_rx.recv().await {
                if let PeerEvent::LocalDescription(sig) | PeerEvent::LocalIce(sig) = ev {
                    peer.handle_signal(sig).unwrap();
                }
            }
        });
    }
    // Frames are irrelevant here; drain so the decode queue never backs up.
    {
        let peer = peer.clone();
        tokio::spawn(async move { while peer.recv_frame().await.is_some() {} });
    }

    // Operator -> robot signalling, until the data channel opens.
    let (open_tx, mut open_rx) = mpsc::unbounded_channel();
    let (to_robot_tx, mut to_robot_rx) = mpsc::unbounded_channel();
    {
        let peer = peer.clone();
        tokio::spawn(async move {
            while let Some(ev) = peer.recv_event().await {
                match ev {
                    PeerEvent::LocalDescription(sig) | PeerEvent::LocalIce(sig) => {
                        let _ = to_robot_tx.send(sig);
                    }
                    PeerEvent::DataChannelOpen => {
                        let _ = open_tx.send(());
                    }
                    _ => {}
                }
            }
        });
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            tokio::select! {
                Some(sig) = to_robot_rx.recv() => consumer.handle_signal(sig).unwrap(),
                Some(()) = open_rx.recv() => break,
            }
        }
    })
    .await
    .expect("data channel opens within 20s");
    // Keep relaying late ICE candidates for the rest of the test.
    let consumer = Arc::new(consumer);
    {
        let c = consumer.clone();
        tokio::spawn(async move {
            while let Some(sig) = to_robot_rx.recv().await {
                let _ = c.handle_signal(sig);
            }
        });
    }
    (consumer, peer)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ssh_channel_round_trips_bytes_through_robot_bridge() {
    let target = echo_server().await;
    let media = RobotMedia::new(&[])
        .expect("robot media builds")
        .with_ssh(Some(SshTunnelConfig {
            target,
            max_channels: 4,
        }));
    let (_consumer, peer) = connect(&media).await;

    let stream = peer.open_ssh_channel().expect("ssh channel created");
    let (mut rd, mut wr) = tokio::io::split(stream);

    // Large enough to cross the 16 KiB chunk size many times over.
    let payload: Vec<u8> = (0..512 * 1024u32).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();
    let writer = tokio::spawn(async move {
        wr.write_all(&payload).await.unwrap();
        wr
    });
    let mut got = vec![0u8; expected.len()];
    tokio::time::timeout(Duration::from_secs(20), rd.read_exact(&mut got))
        .await
        .expect("echo returns within 20s")
        .expect("read echoed bytes");
    assert!(got == expected, "echoed bytes differ from what was sent");
    drop(writer.await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ssh_channel_is_refused_when_disabled() {
    let media = RobotMedia::new(&[]).expect("robot media builds");
    let (_consumer, peer) = connect(&media).await;

    let mut stream = peer.open_ssh_channel().expect("ssh channel created");
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(20), stream.read(&mut buf))
        .await
        .expect("robot closes the channel within 20s")
        .expect("read succeeds");
    assert_eq!(n, 0, "a refused channel reads EOF");
}
