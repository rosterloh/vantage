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
    spawn_server_with_idle(Duration::from_secs(20)).await
}

async fn spawn_server_with_idle(idle_timeout: Duration) -> SocketAddr {
    let state = Arc::new(AppState::with_config(
        Duration::from_secs(15),
        idle_timeout,
        vec![],
    ));
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
    ws.send(Message::Text(txt.into())).await.unwrap();
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

#[tokio::test]
async fn two_clients_get_distinct_sessions() {
    let addr = spawn_server().await;

    let mut robot_ws = connect(addr, "/ws/robot").await;
    send_json(&mut robot_ws, &RobotMsg::Register(robot_info("robot-1"))).await;

    let mut client_a = connect(addr, "/ws/client").await;
    send_json(
        &mut client_a,
        &ClientMsg::Connect {
            robot: RobotId("robot-1".into()),
        },
    )
    .await;
    let msg: ServerMsg = recv_json(&mut robot_ws).await;
    let session_a = match msg {
        ServerMsg::ClientConnected { session } => session,
        other => panic!("expected ClientConnected, got {other:?}"),
    };
    let _: ServerMsg = recv_json(&mut client_a).await; // Connected

    let mut client_b = connect(addr, "/ws/client").await;
    send_json(
        &mut client_b,
        &ClientMsg::Connect {
            robot: RobotId("robot-1".into()),
        },
    )
    .await;
    let msg: ServerMsg = recv_json(&mut robot_ws).await;
    let session_b = match msg {
        ServerMsg::ClientConnected { session } => session,
        other => panic!("expected ClientConnected, got {other:?}"),
    };
    let _: ServerMsg = recv_json(&mut client_b).await; // Connected

    assert_ne!(session_a, session_b);

    // A signal targeted at session_a must reach client_a only.
    send_json(
        &mut robot_ws,
        &RobotMsg::Signal {
            to: session_a.clone(),
            signal: Signal::Answer {
                sdp: "for-a".into(),
            },
        },
    )
    .await;
    let msg: ServerMsg = recv_json(&mut client_a).await;
    match msg {
        ServerMsg::Signal { signal, .. } => assert_eq!(
            signal,
            Signal::Answer {
                sdp: "for-a".into()
            }
        ),
        other => panic!("expected Signal, got {other:?}"),
    }

    // client_b must not have received anything meant for client_a.
    let nothing = tokio::time::timeout(Duration::from_millis(200), client_b.next()).await;
    assert!(
        nothing.is_err(),
        "client_b unexpectedly received a message meant for client_a"
    );
}

#[tokio::test]
async fn robot_reconnect_keeps_new_registration() {
    let addr = spawn_server().await;

    let mut robot_a = connect(addr, "/ws/robot").await;
    send_json(&mut robot_a, &RobotMsg::Register(robot_info("robot-1"))).await;

    let mut robot_b = connect(addr, "/ws/robot").await;
    send_json(&mut robot_b, &RobotMsg::Register(robot_info("robot-1"))).await;

    drop(robot_a);

    // Settle: this round trip gives the dropped socket's cleanup task a chance
    // to run before we assert on the registry/peers state.
    let mut client_ws = connect(addr, "/ws/client").await;
    send_json(&mut client_ws, &ClientMsg::ListRobots).await;
    let msg: ServerMsg = recv_json(&mut client_ws).await;
    match msg {
        ServerMsg::RobotList { robots } => {
            assert!(robots.iter().any(|r| r.id == RobotId("robot-1".into())));
        }
        other => panic!("expected RobotList, got {other:?}"),
    }

    send_json(
        &mut client_ws,
        &ClientMsg::Connect {
            robot: RobotId("robot-1".into()),
        },
    )
    .await;
    let msg: ServerMsg = recv_json(&mut robot_b).await;
    match msg {
        ServerMsg::ClientConnected { .. } => {}
        other => panic!("expected ClientConnected on socket B, got {other:?}"),
    }
}

#[tokio::test]
async fn reconnect_client_to_second_robot_notifies_first() {
    let addr = spawn_server().await;

    let mut robot1 = connect(addr, "/ws/robot").await;
    send_json(&mut robot1, &RobotMsg::Register(robot_info("robot-1"))).await;
    let mut robot2 = connect(addr, "/ws/robot").await;
    send_json(&mut robot2, &RobotMsg::Register(robot_info("robot-2"))).await;

    let mut client_ws = connect(addr, "/ws/client").await;
    send_json(
        &mut client_ws,
        &ClientMsg::Connect {
            robot: RobotId("robot-1".into()),
        },
    )
    .await;
    let msg: ServerMsg = recv_json(&mut robot1).await;
    let session = match msg {
        ServerMsg::ClientConnected { session } => session,
        other => panic!("expected ClientConnected, got {other:?}"),
    };
    let _: ServerMsg = recv_json(&mut client_ws).await; // Connected to robot-1

    send_json(
        &mut client_ws,
        &ClientMsg::Connect {
            robot: RobotId("robot-2".into()),
        },
    )
    .await;

    // robot1 must be told its session went away.
    let msg: ServerMsg = recv_json(&mut robot1).await;
    match msg {
        ServerMsg::ClientDisconnected { session: s } => assert_eq!(s, session),
        other => panic!("expected ClientDisconnected, got {other:?}"),
    }

    let msg: ServerMsg = recv_json(&mut robot2).await;
    match msg {
        ServerMsg::ClientConnected { .. } => {}
        other => panic!("expected ClientConnected on robot2, got {other:?}"),
    }
}

#[tokio::test]
async fn re_register_different_id_evicts_old() {
    let addr = spawn_server().await;

    let mut robot_ws = connect(addr, "/ws/robot").await;
    send_json(&mut robot_ws, &RobotMsg::Register(robot_info("a"))).await;

    // Force ordering: a Connect round trip proves Register("a") is fully processed.
    let mut client_ws = connect(addr, "/ws/client").await;
    send_json(
        &mut client_ws,
        &ClientMsg::Connect {
            robot: RobotId("a".into()),
        },
    )
    .await;
    let msg: ServerMsg = recv_json(&mut robot_ws).await;
    match msg {
        ServerMsg::ClientConnected { .. } => {}
        other => panic!("expected ClientConnected, got {other:?}"),
    }
    let _: ServerMsg = recv_json(&mut client_ws).await; // Connected

    send_json(&mut robot_ws, &RobotMsg::Register(robot_info("b"))).await;

    // Force ordering again: a Connect round trip to "b" proves Register("b")
    // (and the eviction of "a") is fully processed before we query the list.
    send_json(
        &mut client_ws,
        &ClientMsg::Connect {
            robot: RobotId("b".into()),
        },
    )
    .await;
    let msg: ServerMsg = recv_json(&mut robot_ws).await;
    match msg {
        ServerMsg::ClientConnected { .. } => {}
        other => panic!("expected ClientConnected, got {other:?}"),
    }
    let _: ServerMsg = recv_json(&mut client_ws).await; // Connected

    send_json(&mut client_ws, &ClientMsg::ListRobots).await;
    let msg: ServerMsg = recv_json(&mut client_ws).await;
    match msg {
        ServerMsg::RobotList { robots } => {
            let ids: Vec<_> = robots.iter().map(|r| r.id.clone()).collect();
            assert_eq!(ids, vec![RobotId("b".into())]);
        }
        other => panic!("expected RobotList, got {other:?}"),
    }
}

#[tokio::test]
async fn robot_notified_when_silent_client_is_reaped() {
    let addr = spawn_server_with_idle(Duration::from_millis(300)).await;
    let (robot_ws, client_ws, session) = connected_pair(addr, "robot-1").await;

    // The robot's own idle clock started at Register, before the client's clock
    // started at Connect, so it must keep heartbeating (as a real robot would)
    // or it would race the silent client to be reaped first. Only the client
    // goes silent here.
    let (mut robot_tx, mut robot_rx) = robot_ws.split();
    let heartbeat = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let txt = serde_json::to_string(&RobotMsg::Heartbeat).unwrap();
            if robot_tx.send(Message::Text(txt.into())).await.is_err() {
                break;
            }
        }
    });

    let msg = tokio::time::timeout(Duration::from_secs(2), robot_rx.next())
        .await
        .expect("timed out waiting for message")
        .expect("stream ended")
        .expect("websocket error");
    let msg: ServerMsg = match msg {
        Message::Text(txt) => serde_json::from_str(&txt).unwrap(),
        other => panic!("expected text frame, got {other:?}"),
    };
    match msg {
        ServerMsg::ClientDisconnected { session: s } => assert_eq!(s, session),
        other => panic!("expected ClientDisconnected, got {other:?}"),
    }

    heartbeat.abort();
    drop(client_ws);
}

const TEST_TOKEN: &str = "test-token-123";

async fn spawn_server_with_auth() -> SocketAddr {
    let state = Arc::new(
        AppState::with_config(Duration::from_secs(15), Duration::from_secs(20), vec![])
            .with_auth(TEST_TOKEN),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    addr
}

async fn connect_with_token(addr: SocketAddr, path: &str, token: &str) -> Ws {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = format!("ws://{addr}{path}").into_client_request().unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    let (ws, _) = connect_async(req).await.unwrap();
    ws
}

#[tokio::test]
async fn ws_without_token_is_rejected_when_auth_configured() {
    let addr = spawn_server_with_auth().await;
    let err = connect_async(format!("ws://{addr}/ws/robot"))
        .await
        .unwrap_err();
    match err {
        tokio_tungstenite::tungstenite::Error::Http(resp) => assert_eq!(resp.status(), 401),
        other => panic!("expected HTTP 401 rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn ws_with_wrong_token_is_rejected() {
    let addr = spawn_server_with_auth().await;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = format!("ws://{addr}/ws/robot")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer wrong-token".parse().unwrap());
    let err = connect_async(req).await.unwrap_err();
    match err {
        tokio_tungstenite::tungstenite::Error::Http(resp) => assert_eq!(resp.status(), 401),
        other => panic!("expected HTTP 401 rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn ws_with_token_registers_and_lists() {
    let addr = spawn_server_with_auth().await;
    let mut robot_ws = connect_with_token(addr, "/ws/robot", TEST_TOKEN).await;
    send_json(&mut robot_ws, &RobotMsg::Register(robot_info("robot-1"))).await;

    let mut client_ws = connect_with_token(addr, "/ws/client", TEST_TOKEN).await;
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
async fn ice_requires_token_when_configured() {
    let addr = spawn_server_with_auth().await;
    let http = reqwest::Client::new();
    let resp = http.get(format!("http://{addr}/ice")).send().await.unwrap();
    assert_eq!(resp.status(), 401);
    let resp = http
        .get(format!("http://{addr}/ice"))
        .bearer_auth(TEST_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn healthz_stays_open_with_auth_configured() {
    let addr = spawn_server_with_auth().await;
    let resp = reqwest::get(format!("http://{addr}/healthz"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn oversized_message_gets_the_peer_disconnected() {
    let addr = spawn_server().await;
    let mut robot_ws = connect(addr, "/ws/robot").await;
    send_json(&mut robot_ws, &RobotMsg::Register(robot_info("robot-1"))).await;

    let huge = "x".repeat(128 * 1024);
    robot_ws.send(Message::Text(huge.into())).await.unwrap();

    let end = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match robot_ws.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => continue,
            }
        }
    })
    .await;
    assert!(
        end.is_ok(),
        "connection must terminate after an oversized message"
    );

    // The coordinator itself must stay healthy for other peers.
    let mut client_ws = connect(addr, "/ws/client").await;
    send_json(&mut client_ws, &ClientMsg::ListRobots).await;
    let _: ServerMsg = recv_json(&mut client_ws).await;
}

#[tokio::test]
async fn flooding_a_stalled_client_does_not_wedge_the_coordinator() {
    let addr = spawn_server().await;
    let (mut robot_ws, client_ws, session) = connected_pair(addr, "robot-1").await;

    // client_ws is never read again; large payloads defeat kernel socket buffering
    // so the coordinator-side outbound queue for this peer genuinely fills.
    let fat_sdp = "x".repeat(32 * 1024);
    for _ in 0..600 {
        send_json(
            &mut robot_ws,
            &RobotMsg::Signal {
                to: session.clone(),
                signal: Signal::Offer {
                    sdp: fat_sdp.clone(),
                },
            },
        )
        .await;
    }

    // A second, healthy pair must still relay correctly.
    let (mut robot2, mut client2, session2) = connected_pair(addr, "robot-2").await;
    send_json(
        &mut client2,
        &ClientMsg::Signal {
            signal: Signal::Offer { sdp: "o".into() },
        },
    )
    .await;
    let msg: ServerMsg = recv_json(&mut robot2).await;
    match msg {
        ServerMsg::Signal { from, .. } => assert_eq!(from, Some(session2)),
        other => panic!("expected Signal, got {other:?}"),
    }
    drop(client_ws);
}

#[tokio::test]
async fn pinging_client_is_not_reaped() {
    let addr = spawn_server_with_idle(Duration::from_millis(300)).await;
    let (robot_ws, mut client_ws, _session) = connected_pair(addr, "robot-1").await;

    // Keep the robot alive exactly as robot_notified_when_silent_client_is_reaped does.
    let (mut robot_tx, mut robot_rx) = robot_ws.split();
    let heartbeat = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let txt = serde_json::to_string(&RobotMsg::Heartbeat).unwrap();
            if robot_tx.send(Message::Text(txt.into())).await.is_err() {
                break;
            }
        }
    });

    // The client sends ONLY WS pings — no text frames — across 3+ idle windows.
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        client_ws.send(Message::Ping(vec![].into())).await.unwrap();
    }

    // Still alive: a ListRobots round trip works. The server may have queued Pong
    // frames for our pings — skip non-Text frames while waiting for the reply.
    send_json(&mut client_ws, &ClientMsg::ListRobots).await;
    let reply = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match client_ws.next().await {
                Some(Ok(Message::Text(txt))) => {
                    break serde_json::from_str::<ServerMsg>(&txt).unwrap();
                }
                Some(Ok(_)) => continue, // Pong etc.
                other => panic!("client socket ended unexpectedly: {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for RobotList");
    match reply {
        ServerMsg::RobotList { robots } => {
            assert!(robots.iter().any(|r| r.id == RobotId("robot-1".into())));
        }
        other => panic!("expected RobotList, got {other:?}"),
    }

    // And the robot must NOT have been told the client disconnected.
    let nothing = tokio::time::timeout(Duration::from_millis(300), robot_rx.next()).await;
    assert!(
        nothing.is_err(),
        "robot unexpectedly received a message (ClientDisconnected?) for a pinging client"
    );

    heartbeat.abort();
}
