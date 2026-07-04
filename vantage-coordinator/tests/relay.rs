use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use vantage_coordinator::{AppState, router};
use vantage_protocol::RobotId;
use vantage_protocol::signalling::{ClientMsg, RobotInfo, RobotMsg, ServerMsg, Signal};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn spawn_server() -> SocketAddr {
    let state = Arc::new(AppState::with_config(Duration::from_secs(15), vec![]));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    addr
}

async fn connect(addr: SocketAddr, path: &str) -> Ws {
    let (ws, _) = connect_async(format!("ws://{addr}{path}")).await.unwrap();
    ws
}

async fn send_json(ws: &mut Ws, msg: &impl serde::Serialize) {
    let txt = serde_json::to_string(msg).unwrap();
    ws.send(Message::Text(txt)).await.unwrap();
}

async fn recv_json<T: DeserializeOwned>(ws: &mut Ws) -> T {
    let msg = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .expect("timed out waiting for message")
        .expect("stream ended")
        .expect("websocket error");
    match msg {
        Message::Text(txt) => serde_json::from_str(&txt).unwrap(),
        other => panic!("expected text frame, got {other:?}"),
    }
}

fn robot_info(id: &str) -> RobotInfo {
    RobotInfo {
        id: RobotId(id.into()),
        name: id.into(),
        capabilities: vec![],
    }
}

/// Registers a robot and connects a client to it, draining the ClientConnected/Connected
/// handshake. Returns the two sockets plus the session id the coordinator assigned.
async fn connected_pair(addr: SocketAddr, robot_id: &str) -> (Ws, Ws, vantage_protocol::SessionId) {
    let mut robot_ws = connect(addr, "/ws/robot").await;
    send_json(&mut robot_ws, &RobotMsg::Register(robot_info(robot_id))).await;

    let mut client_ws = connect(addr, "/ws/client").await;
    send_json(
        &mut client_ws,
        &ClientMsg::Connect {
            robot: RobotId(robot_id.into()),
        },
    )
    .await;

    let robot_msg: ServerMsg = recv_json(&mut robot_ws).await;
    let session = match robot_msg {
        ServerMsg::ClientConnected { session } => session,
        other => panic!("expected ClientConnected, got {other:?}"),
    };

    let client_msg: ServerMsg = recv_json(&mut client_ws).await;
    match client_msg {
        ServerMsg::Connected { session: s, .. } => assert_eq!(s, session),
        other => panic!("expected Connected, got {other:?}"),
    }

    (robot_ws, client_ws, session)
}

#[tokio::test]
async fn robot_registers_and_appears_in_list() {
    let addr = spawn_server().await;

    let mut robot_ws = connect(addr, "/ws/robot").await;
    send_json(&mut robot_ws, &RobotMsg::Register(robot_info("robot-1"))).await;

    let mut client_ws = connect(addr, "/ws/client").await;
    send_json(&mut client_ws, &ClientMsg::ListRobots).await;

    let msg: ServerMsg = recv_json(&mut client_ws).await;
    match msg {
        ServerMsg::RobotList { robots } => {
            assert!(robots.iter().any(|r| r.id == RobotId("robot-1".into())));
        }
        other => panic!("expected RobotList, got {other:?}"),
    }
}

#[tokio::test]
async fn client_connect_relays_client_connected_to_robot() {
    let addr = spawn_server().await;
    // connected_pair already asserts ClientConnected/Connected are received correctly.
    let _ = connected_pair(addr, "robot-1").await;
}

#[tokio::test]
async fn signal_relays_client_to_robot_and_back() {
    let addr = spawn_server().await;
    let (mut robot_ws, mut client_ws, session) = connected_pair(addr, "robot-1").await;

    send_json(
        &mut client_ws,
        &ClientMsg::Signal {
            signal: Signal::Offer {
                sdp: "offer-sdp".into(),
            },
        },
    )
    .await;

    let msg: ServerMsg = recv_json(&mut robot_ws).await;
    match msg {
        ServerMsg::Signal { from, signal } => {
            assert_eq!(from, Some(session.clone()));
            assert_eq!(
                signal,
                Signal::Offer {
                    sdp: "offer-sdp".into()
                }
            );
        }
        other => panic!("expected Signal, got {other:?}"),
    }

    send_json(
        &mut robot_ws,
        &RobotMsg::Signal {
            to: session,
            signal: Signal::Answer {
                sdp: "answer-sdp".into(),
            },
        },
    )
    .await;

    let msg: ServerMsg = recv_json(&mut client_ws).await;
    match msg {
        ServerMsg::Signal { from, signal } => {
            assert_eq!(from, None);
            assert_eq!(
                signal,
                Signal::Answer {
                    sdp: "answer-sdp".into()
                }
            );
        }
        other => panic!("expected Signal, got {other:?}"),
    }
}

#[tokio::test]
async fn robot_disconnect_notifies_attached_client() {
    let addr = spawn_server().await;
    let (robot_ws, mut client_ws, _session) = connected_pair(addr, "robot-1").await;

    drop(robot_ws);

    let msg: ServerMsg = recv_json(&mut client_ws).await;
    match msg {
        ServerMsg::Error { message } => assert_eq!(message, "robot disconnected"),
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn client_disconnect_notifies_robot() {
    let addr = spawn_server().await;
    let (mut robot_ws, client_ws, session) = connected_pair(addr, "robot-1").await;

    drop(client_ws);

    let msg: ServerMsg = recv_json(&mut robot_ws).await;
    match msg {
        ServerMsg::ClientDisconnected { session: s } => assert_eq!(s, session),
        other => panic!("expected ClientDisconnected, got {other:?}"),
    }
}

#[tokio::test]
async fn bad_message_yields_error_not_disconnect() {
    let addr = spawn_server().await;
    let mut robot_ws = connect(addr, "/ws/robot").await;

    robot_ws
        .send(Message::Text("not json".into()))
        .await
        .unwrap();

    let msg: ServerMsg = recv_json(&mut robot_ws).await;
    match msg {
        ServerMsg::Error { message } => assert_eq!(message, "bad robot message"),
        other => panic!("expected Error, got {other:?}"),
    }

    // Socket should still be open: a subsequent valid Register must work.
    send_json(&mut robot_ws, &RobotMsg::Register(robot_info("robot-2"))).await;

    let mut client_ws = connect(addr, "/ws/client").await;
    send_json(&mut client_ws, &ClientMsg::ListRobots).await;
    let msg: ServerMsg = recv_json(&mut client_ws).await;
    match msg {
        ServerMsg::RobotList { robots } => {
            assert!(robots.iter().any(|r| r.id == RobotId("robot-2".into())));
        }
        other => panic!("expected RobotList, got {other:?}"),
    }
}
