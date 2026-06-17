# Vantage PoC — Foundation Milestone Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Vantage Cargo workspace, the shared `vantage-protocol` types, and the signalling spine so that a robot and a client discover each other through the coordinator and exchange device telemetry over a WebRTC **data channel** — including when forced onto the TURN relay.

**Architecture:** A single Rust Cargo workspace with five crates. `vantage-protocol` holds all shared `serde` types (signalling + telemetry) plus a one-place codec wrapper. `vantage-coordinator` is an `axum` WebSocket service doing registration, heartbeat/expiry, discovery, ICE-server provisioning, and SDP/ICE relay. `vantage-signalling` wraps a `webrtcbin` element (data-channel-only at this milestone) and the coordinator WebSocket client, shared by robot and client. `vantage-robot` registers and pushes `DeviceInfo` (via `sysinfo`); `vantage-client` discovers, connects, and prints telemetry.

**Tech Stack:** Rust 2021, `serde`/`serde_json`, `tokio`, `axum` (ws), `tokio-tungstenite`, `gstreamer-rs` (`gstreamer`, `gstreamer-webrtc`, `gstreamer-sdp`) with `webrtcbin` from gst-plugins-bad, `sysinfo`, `thiserror`/`anyhow`, `tracing`.

**Scope:** This is the **foundation milestone only** — `tasks.md` Phase 1 (protocol skeleton) and Phase 2 (signalling spine, no media). Phases 3–6 (video, real camera + ROS2 tee, hardware decode + fan-out, fleet stats + teleop groundwork) get their own plans once the two riskiest pieces — traversal here, and the client decode→texture path in Phase 3 — are proven.

**Spec coverage at this milestone (deltas in `openspec/changes/add-vantage-poc/specs/`):**
- `discovery`: Robot registration and liveness; Client discovery. (mDNS LAN fast path deferred to a later plan.)
- `telemetry`: Device telemetry over the data channel; Bidirectional connection from day one; Shared message types. (Reliability-mode-matched-to-type is exercised lightly; full high-rate path lands with joint states.)
- `fleet-management`: TURN credential provisioning (the ICE-server endpoint). (Provider/consumer counts are stubbed here and finished in the Phase 6 plan.)
- `video-streaming`, `camera-sharing`: **not** in this milestone — they begin in Phase 3.

---

## Prerequisites (one-time, before Task 9)

Tasks 1–8 need only the Rust toolchain. Tasks 9–12 add live WebRTC and therefore GStreamer with the `webrtcbin`/`dtls`/`srtp`/`nice` plugins (gst-plugins-bad + libnice).

```bash
# Debian/Ubuntu
sudo apt-get install -y \
  libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
  libgstreamer-plugins-bad1.0-dev gstreamer1.0-plugins-bad \
  gstreamer1.0-nice gstreamer1.0-plugins-good libnice-dev

# Verify webrtcbin is present (must print element details, not "No such element")
gst-inspect-1.0 webrtcbin | head -5
```

For the relay test in Task 12 you also need a TURN server. Use a local `coturn` or the metered.ca free tier named in the proposal. Local coturn:

```bash
sudo apt-get install -y coturn
# minimal static-credential config for the PoC
turnserver -n -a -u vantage:vantagepoc -r vantage --no-tls --no-dtls \
  --listening-port 3478 --min-port 49160 --max-port 49200 -v
```

---

## File Structure

```
vantage/
├── Cargo.toml                         # workspace manifest + shared deps
├── vantage-protocol/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                     # re-exports
│       ├── ids.rs                     # RobotId, SessionId newtypes
│       ├── telemetry.rs              # DeviceInfo, TempReading
│       ├── signalling.rs             # Signal, RobotMsg, ClientMsg, ServerMsg, RobotInfo, IceServer
│       └── codec.rs                  # encode/decode wrapper (json now, bincode later)
├── vantage-signalling/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── ws.rs                     # coordinator WebSocket client (tungstenite)
│       └── peer.rs                   # webrtcbin driver (data-channel-only at this milestone)
├── vantage-coordinator/
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs                   # axum server bootstrap
│       ├── registry.rs              # Registry: register/heartbeat/prune/list (pure, TDD)
│       ├── sessions.rs             # session routing table robot<->client
│       └── routes.rs               # /ws, /ice handlers
├── vantage-robot/
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs
│       └── telemetry.rs            # sysinfo -> DeviceInfo sampler
└── vantage-client/
    ├── Cargo.toml
    └── src/
        └── main.rs                  # discover, connect, print telemetry
```

Responsibility split: shared types live only in `vantage-protocol`; nothing redefines a wire type elsewhere (satisfies telemetry "Shared message types"). Coordinator pure logic (`registry.rs`, `sessions.rs`) is separated from transport (`routes.rs`) so it can be unit-tested without a running server. The `webrtcbin` complexity is quarantined in `vantage-signalling/peer.rs`.

---

# Phase 1 — Protocol skeleton

### Task 1: Cargo workspace and five crate skeletons

**Files:**
- Create: `Cargo.toml`
- Create: `vantage-protocol/Cargo.toml`, `vantage-protocol/src/lib.rs`
- Create: `vantage-signalling/Cargo.toml`, `vantage-signalling/src/lib.rs`
- Create: `vantage-coordinator/Cargo.toml`, `vantage-coordinator/src/main.rs`
- Create: `vantage-robot/Cargo.toml`, `vantage-robot/src/main.rs`
- Create: `vantage-client/Cargo.toml`, `vantage-client/src/main.rs`

- [ ] **Step 1: Write the workspace manifest**

`Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = [
    "vantage-protocol",
    "vantage-signalling",
    "vantage-coordinator",
    "vantage-robot",
    "vantage-client",
]

[workspace.package]
edition = "2021"
license = "MIT OR Apache-2.0"
rust-version = "1.90"

[workspace.dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
anyhow = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync", "time", "signal"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

- [ ] **Step 2: Create the library crate manifests and entry points**

`vantage-protocol/Cargo.toml`:

```toml
[package]
name = "vantage-protocol"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
```

`vantage-protocol/src/lib.rs`:

```rust
pub mod codec;
pub mod ids;
pub mod signalling;
pub mod telemetry;

pub use ids::{RobotId, SessionId};
```

`vantage-signalling/Cargo.toml`:

```toml
[package]
name = "vantage-signalling"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
vantage-protocol = { path = "../vantage-protocol" }
serde.workspace = true
serde_json.workspace = true
tokio.workspace = true
anyhow.workspace = true
thiserror.workspace = true
tracing.workspace = true
tokio-tungstenite = "0.24"
futures-util = "0.3"
gstreamer = "0.23"
gstreamer-webrtc = "0.23"
gstreamer-sdp = "0.23"
```

`vantage-signalling/src/lib.rs`:

```rust
pub mod peer;
pub mod ws;
```

(Leave `peer.rs` and `ws.rs` as empty files with `// filled in Phase 2` for now so the crate compiles; they are written in Tasks 9–11.)

- [ ] **Step 3: Create the three binary crate manifests and entry points**

`vantage-coordinator/Cargo.toml`:

```toml
[package]
name = "vantage-coordinator"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
vantage-protocol = { path = "../vantage-protocol" }
serde.workspace = true
serde_json.workspace = true
tokio.workspace = true
anyhow.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
axum = { version = "0.8", features = ["ws"] }
tower-http = { version = "0.6", features = ["trace"] }
```

`vantage-robot/Cargo.toml`:

```toml
[package]
name = "vantage-robot"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
vantage-protocol = { path = "../vantage-protocol" }
vantage-signalling = { path = "../vantage-signalling" }
serde.workspace = true
serde_json.workspace = true
tokio.workspace = true
anyhow.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
sysinfo = "0.33"
```

`vantage-client/Cargo.toml`: identical to `vantage-robot/Cargo.toml` but `name = "vantage-client"` and **without** the `sysinfo` line.

Each of `vantage-coordinator/src/main.rs`, `vantage-robot/src/main.rs`, `vantage-client/src/main.rs`:

```rust
fn main() {
    println!("vantage placeholder");
}
```

- [ ] **Step 4: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: all five crates compile (network deps download on first run).

- [ ] **Step 5: Commit**

```bash
git init -q
git add .
git commit -m "chore: scaffold vantage cargo workspace with five crates"
```

---

### Task 2: Telemetry types (`DeviceInfo`)

**Files:**
- Create: `vantage-protocol/src/telemetry.rs`
- Test: inline `#[cfg(test)]` module in the same file

- [ ] **Step 1: Write the failing test**

In `vantage-protocol/src/telemetry.rs`:

```rust
use serde::{Deserialize, Serialize};

/// A single temperature sensor reading.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TempReading {
    pub label: String,
    pub celsius: f32,
}

/// Host/device metrics sampled by the robot and shown beside the video.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub cpu_percent: f32,
    pub mem_used_mb: u64,
    pub mem_total_mb: u64,
    pub temps: Vec<TempReading>,
    pub uptime_s: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_info_json_round_trips() {
        let info = DeviceInfo {
            cpu_percent: 12.5,
            mem_used_mb: 2048,
            mem_total_mb: 8192,
            temps: vec![TempReading { label: "cpu".into(), celsius: 47.0 }],
            uptime_s: 3600,
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: DeviceInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info, back);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p vantage-protocol telemetry`
Expected: FAIL — `telemetry` module not yet wired (or a compile error until `lib.rs` from Task 1 references it). If `lib.rs` already declares `pub mod telemetry;`, the test compiles and passes immediately; in that case this is a confirming test, proceed.

- [ ] **Step 3: (Implementation already written above)**

The types in Step 1 are the minimal implementation. No further code needed.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p vantage-protocol telemetry`
Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add vantage-protocol/src/telemetry.rs
git commit -m "feat(protocol): add DeviceInfo telemetry types"
```

---

### Task 3: Identity newtypes and signalling messages

**Files:**
- Create: `vantage-protocol/src/ids.rs`
- Create: `vantage-protocol/src/signalling.rs`
- Test: inline `#[cfg(test)]` in `signalling.rs`

- [ ] **Step 1: Write the id newtypes**

`vantage-protocol/src/ids.rs`:

```rust
use serde::{Deserialize, Serialize};

/// Stable identity a robot advertises to the coordinator.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RobotId(pub String);

/// Coordinator-assigned id for one client viewing session.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub String);

impl std::fmt::Display for RobotId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
```

- [ ] **Step 2: Write the failing signalling test (and the types)**

`vantage-protocol/src/signalling.rs`:

```rust
use serde::{Deserialize, Serialize};

use crate::ids::{RobotId, SessionId};

/// What a robot advertises and what clients see in the discovery list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RobotInfo {
    pub id: RobotId,
    pub name: String,
    /// Free-form capability tags, e.g. ["h264", "telemetry"].
    pub capabilities: Vec<String>,
}

/// One ICE server entry handed to peers (STUN has no creds; TURN does).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IceServer {
    pub urls: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential: Option<String>,
}

/// The SDP/ICE payloads that the coordinator relays verbatim between peers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Signal {
    Offer { sdp: String },
    Answer { sdp: String },
    Ice { candidate: String, sdp_mline_index: u32 },
}

/// robot -> coordinator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RobotMsg {
    Register(RobotInfo),
    Heartbeat,
    /// Signalling aimed at a specific client session.
    Signal { to: SessionId, signal: Signal },
}

/// client -> coordinator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    ListRobots,
    Connect { robot: RobotId },
    /// Signalling aimed at the robot of the client's active session.
    Signal { signal: Signal },
}

/// coordinator -> robot or client
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    /// Sent to a client in reply to ListRobots.
    RobotList { robots: Vec<RobotInfo> },
    /// Sent to a robot when a client opens a session with it.
    ClientConnected { session: SessionId },
    /// Sent to a robot when a client session ends.
    ClientDisconnected { session: SessionId },
    /// Sent to a client once its Connect is accepted.
    Connected { robot: RobotId, session: SessionId },
    /// ICE servers for either peer.
    IceServers { servers: Vec<IceServer> },
    /// Relayed signalling. `from` is the peer session for the robot side.
    Signal { from: Option<SessionId>, signal: Signal },
    /// Coordinator-side error string.
    Error { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_round_trips_each_variant() {
        for s in [
            Signal::Offer { sdp: "v=0...".into() },
            Signal::Answer { sdp: "v=0...".into() },
            Signal::Ice { candidate: "candidate:1 1 udp ...".into(), sdp_mline_index: 0 },
        ] {
            let json = serde_json::to_string(&s).unwrap();
            assert_eq!(s, serde_json::from_str::<Signal>(&json).unwrap());
        }
    }

    #[test]
    fn robot_register_round_trips() {
        let msg = RobotMsg::Register(RobotInfo {
            id: RobotId("robot-1".into()),
            name: "Atlas".into(),
            capabilities: vec!["h264".into(), "telemetry".into()],
        });
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(msg, serde_json::from_str::<RobotMsg>(&json).unwrap());
    }

    #[test]
    fn ice_server_omits_empty_creds() {
        let stun = IceServer { urls: vec!["stun:stun.l.google.com:19302".into()], username: None, credential: None };
        let json = serde_json::to_string(&stun).unwrap();
        assert!(!json.contains("username"), "STUN entry must not serialize null creds: {json}");
    }
}
```

- [ ] **Step 3: Run tests to verify they fail then pass**

Run: `cargo test -p vantage-protocol signalling`
Expected: PASS (3 tests). If `lib.rs` does not yet declare `pub mod ids;`/`pub mod signalling;`, add them (Task 1 Step 2 already did) — without the declarations you get a compile error, which is the "failing" state; add the declarations to make it pass.

- [ ] **Step 4: Commit**

```bash
git add vantage-protocol/src/ids.rs vantage-protocol/src/signalling.rs vantage-protocol/src/lib.rs
git commit -m "feat(protocol): add signalling message + ICE server types"
```

---

### Task 4: Codec wrapper (one place to swap json→bincode)

**Files:**
- Create: `vantage-protocol/src/codec.rs`
- Test: inline `#[cfg(test)]` in `codec.rs`

- [ ] **Step 1: Write the failing test (and the wrapper)**

`vantage-protocol/src/codec.rs`:

```rust
use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("encode failed: {0}")]
    Encode(String),
    #[error("decode failed: {0}")]
    Decode(String),
}

/// Encode a wire value. JSON during bring-up (readable in logs); swapping the two
/// bodies below to `bincode` is the entire codec change (see design.md §7).
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    serde_json::to_vec(value).map_err(|e| CodecError::Encode(e.to_string()))
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    serde_json::from_slice(bytes).map_err(|e| CodecError::Decode(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{DeviceInfo, TempReading};

    #[test]
    fn encode_decode_round_trips() {
        let info = DeviceInfo {
            cpu_percent: 3.0,
            mem_used_mb: 100,
            mem_total_mb: 200,
            temps: vec![TempReading { label: "soc".into(), celsius: 40.0 }],
            uptime_s: 10,
        };
        let bytes = encode(&info).unwrap();
        let back: DeviceInfo = decode(&bytes).unwrap();
        assert_eq!(info, back);
    }

    #[test]
    fn decode_garbage_is_an_error_not_a_panic() {
        let err = decode::<DeviceInfo>(b"not json").unwrap_err();
        assert!(matches!(err, CodecError::Decode(_)));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p vantage-protocol codec`
Expected: PASS (2 tests).

- [ ] **Step 3: Run the whole protocol crate**

Run: `cargo test -p vantage-protocol`
Expected: PASS (all Phase 1 tests).

- [ ] **Step 4: Commit**

```bash
git add vantage-protocol/src/codec.rs
git commit -m "feat(protocol): add codec wrapper (json now, bincode later)"
```

---

# Phase 2 — Signalling spine (no media)

### Task 5: Coordinator registry with TTL expiry (pure logic, TDD)

**Files:**
- Create: `vantage-coordinator/src/registry.rs`
- Test: inline `#[cfg(test)]` in `registry.rs`

Time is injected as a parameter so expiry is testable without sleeping — good test design (satisfies discovery "Stale robot expires").

- [ ] **Step 1: Write the failing test (and the registry)**

`vantage-coordinator/src/registry.rs`:

```rust
use std::collections::HashMap;
use std::time::{Duration, Instant};

use vantage_protocol::signalling::RobotInfo;
use vantage_protocol::RobotId;

struct Entry {
    info: RobotInfo,
    last_seen: Instant,
}

/// Live set of registered robots with heartbeat-based expiry.
pub struct Registry {
    robots: HashMap<RobotId, Entry>,
    ttl: Duration,
}

impl Registry {
    pub fn new(ttl: Duration) -> Self {
        Self { robots: HashMap::new(), ttl }
    }

    pub fn register(&mut self, info: RobotInfo, now: Instant) {
        self.robots.insert(info.id.clone(), Entry { info, last_seen: now });
    }

    /// Returns false if the robot was unknown (e.g. already expired).
    pub fn heartbeat(&mut self, id: &RobotId, now: Instant) -> bool {
        match self.robots.get_mut(id) {
            Some(e) => { e.last_seen = now; true }
            None => false,
        }
    }

    pub fn remove(&mut self, id: &RobotId) {
        self.robots.remove(id);
    }

    /// Drop entries whose last heartbeat is older than the TTL.
    pub fn prune(&mut self, now: Instant) {
        let ttl = self.ttl;
        self.robots.retain(|_, e| now.duration_since(e.last_seen) < ttl);
    }

    /// Current live robots (call `prune` first for accuracy).
    pub fn list(&self) -> Vec<RobotInfo> {
        self.robots.values().map(|e| e.info.clone()).collect()
    }

    pub fn len(&self) -> usize {
        self.robots.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn robot(id: &str) -> RobotInfo {
        RobotInfo { id: RobotId(id.into()), name: id.into(), capabilities: vec![] }
    }

    #[test]
    fn registered_robot_is_listed() {
        let t0 = Instant::now();
        let mut r = Registry::new(Duration::from_secs(10));
        r.register(robot("a"), t0);
        assert_eq!(r.list().len(), 1);
    }

    #[test]
    fn stale_robot_is_pruned() {
        let t0 = Instant::now();
        let mut r = Registry::new(Duration::from_secs(10));
        r.register(robot("a"), t0);
        r.prune(t0 + Duration::from_secs(11));
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn heartbeat_keeps_robot_alive() {
        let t0 = Instant::now();
        let mut r = Registry::new(Duration::from_secs(10));
        r.register(robot("a"), t0);
        assert!(r.heartbeat(&RobotId("a".into()), t0 + Duration::from_secs(8)));
        r.prune(t0 + Duration::from_secs(11)); // 3s since last heartbeat -> alive
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn heartbeat_for_unknown_robot_returns_false() {
        let mut r = Registry::new(Duration::from_secs(10));
        assert!(!r.heartbeat(&RobotId("ghost".into()), Instant::now()));
    }
}
```

Add `mod registry;` to `vantage-coordinator/src/main.rs` (replace the placeholder body for now):

```rust
mod registry;

fn main() {
    println!("vantage-coordinator placeholder");
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p vantage-coordinator registry`
Expected: PASS (4 tests).

- [ ] **Step 3: Commit**

```bash
git add vantage-coordinator/src/registry.rs vantage-coordinator/src/main.rs
git commit -m "feat(coordinator): registry with heartbeat TTL expiry"
```

---

### Task 6: Session routing table (robot ↔ client), pure logic

**Files:**
- Create: `vantage-coordinator/src/sessions.rs`
- Test: inline `#[cfg(test)]` in `sessions.rs`

Tracks which client session is talking to which robot so the relay (Task 8) and fleet counts (Phase 6) can be derived from session lifecycle (satisfies fleet-management "Session-derived statistics").

- [ ] **Step 1: Write the failing test (and the table)**

`vantage-coordinator/src/sessions.rs`:

```rust
use std::collections::HashMap;

use vantage_protocol::{RobotId, SessionId};

/// Maps a client session to the robot it is connected to, and back.
#[derive(Default)]
pub struct Sessions {
    by_session: HashMap<SessionId, RobotId>,
}

impl Sessions {
    pub fn open(&mut self, session: SessionId, robot: RobotId) {
        self.by_session.insert(session, robot);
    }

    pub fn robot_for(&self, session: &SessionId) -> Option<&RobotId> {
        self.by_session.get(session)
    }

    /// Remove a session; returns the robot it was attached to, if any.
    pub fn close(&mut self, session: &SessionId) -> Option<RobotId> {
        self.by_session.remove(session)
    }

    /// All open client sessions for a given robot (used when the robot drops).
    pub fn sessions_for(&self, robot: &RobotId) -> Vec<SessionId> {
        self.by_session
            .iter()
            .filter(|(_, r)| *r == robot)
            .map(|(s, _)| s.clone())
            .collect()
    }

    pub fn consumer_count(&self) -> usize {
        self.by_session.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_then_lookup() {
        let mut s = Sessions::default();
        s.open(SessionId("sess-1".into()), RobotId("r1".into()));
        assert_eq!(s.robot_for(&SessionId("sess-1".into())), Some(&RobotId("r1".into())));
        assert_eq!(s.consumer_count(), 1);
    }

    #[test]
    fn close_returns_robot_and_drops_count() {
        let mut s = Sessions::default();
        s.open(SessionId("sess-1".into()), RobotId("r1".into()));
        assert_eq!(s.close(&SessionId("sess-1".into())), Some(RobotId("r1".into())));
        assert_eq!(s.consumer_count(), 0);
    }

    #[test]
    fn sessions_for_robot() {
        let mut s = Sessions::default();
        s.open(SessionId("a".into()), RobotId("r1".into()));
        s.open(SessionId("b".into()), RobotId("r1".into()));
        s.open(SessionId("c".into()), RobotId("r2".into()));
        let mut got = s.sessions_for(&RobotId("r1".into()));
        got.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(got, vec![SessionId("a".into()), SessionId("b".into())]);
    }
}
```

Add `mod sessions;` to `vantage-coordinator/src/main.rs`.

- [ ] **Step 2: Run tests**

Run: `cargo test -p vantage-coordinator sessions`
Expected: PASS (3 tests).

- [ ] **Step 3: Commit**

```bash
git add vantage-coordinator/src/sessions.rs vantage-coordinator/src/main.rs
git commit -m "feat(coordinator): session routing table"
```

---

### Task 7: Coordinator server — shared state, ICE endpoint, WebSocket scaffold

**Files:**
- Create: `vantage-coordinator/src/routes.rs`
- Modify: `vantage-coordinator/src/main.rs`

Static TURN config satisfies fleet-management "TURN credential provisioning". The TURN URL/creds come from env so the relay test in Task 12 can point at local coturn or metered.ca.

- [ ] **Step 1: Write the shared state and ICE config**

`vantage-coordinator/src/routes.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use tokio::sync::{mpsc, Mutex};
use vantage_protocol::signalling::{IceServer, ServerMsg};

use crate::registry::Registry;
use crate::sessions::Sessions;

/// One connected peer's outbound channel (robot or client).
pub type Outbound = mpsc::UnboundedSender<ServerMsg>;

pub struct AppState {
    pub registry: Mutex<Registry>,
    pub sessions: Mutex<Sessions>,
    /// session/robot id (as string) -> its outbound sender
    pub peers: Mutex<std::collections::HashMap<String, Outbound>>,
    pub ice_servers: Vec<IceServer>,
}

impl AppState {
    pub fn from_env() -> Self {
        let mut ice = vec![IceServer {
            urls: vec!["stun:stun.l.google.com:19302".into()],
            username: None,
            credential: None,
        }];
        if let Ok(turn_url) = std::env::var("VANTAGE_TURN_URL") {
            ice.push(IceServer {
                urls: vec![turn_url],
                username: std::env::var("VANTAGE_TURN_USER").ok(),
                credential: std::env::var("VANTAGE_TURN_PASS").ok(),
            });
        }
        Self {
            registry: Mutex::new(Registry::new(Duration::from_secs(15))),
            sessions: Mutex::new(Sessions::default()),
            peers: Mutex::new(std::collections::HashMap::new()),
            ice_servers: ice,
        }
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/ice", get(ice_servers))
        .route("/ws/robot", get(robot_ws))
        .route("/ws/client", get(client_ws))
        .with_state(state)
}

async fn ice_servers(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.ice_servers.clone())
}

async fn robot_ws(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| crate::routes::handle_robot(socket, state))
}

async fn client_ws(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| crate::routes::handle_client(socket, state))
}

// handle_robot / handle_client implemented in Task 8.
pub async fn handle_robot(_socket: WebSocket, _state: Arc<AppState>) { /* Task 8 */ }
pub async fn handle_client(_socket: WebSocket, _state: Arc<AppState>) { /* Task 8 */ }

// Helper kept here so it is unit-testable.
pub fn split_text(msg: Message) -> Option<String> {
    match msg {
        Message::Text(t) => Some(t.to_string()),
        _ => None,
    }
}
```

Rewrite `vantage-coordinator/src/main.rs`:

```rust
mod registry;
mod routes;
mod sessions;

use std::sync::Arc;

use routes::{router, AppState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let state = Arc::new(AppState::from_env());

    // Background pruner so stale robots expire even with no traffic.
    {
        let state = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tick.tick().await;
                state.registry.lock().await.prune(std::time::Instant::now());
            }
        });
    }

    let addr = std::env::var("VANTAGE_BIND").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("coordinator listening on {addr}");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
```

- [ ] **Step 2: Verify it builds and runs**

Run: `cargo run -p vantage-coordinator`
Expected: logs `coordinator listening on 0.0.0.0:8080`. In another shell: `curl -s localhost:8080/ice` returns the STUN entry as JSON. Stop with Ctrl-C.

- [ ] **Step 3: Commit**

```bash
git add vantage-coordinator/src/routes.rs vantage-coordinator/src/main.rs
git commit -m "feat(coordinator): axum app, shared state, /ice endpoint, ws scaffold"
```

---

### Task 8: Coordinator WebSocket handlers — register/heartbeat/discovery/relay

**Files:**
- Modify: `vantage-coordinator/src/routes.rs` (replace the two `handle_*` stubs)

This implements discovery (register/heartbeat/list) and the SDP/ICE relay. Verification is by integration in Task 12 (live sockets); here the exit check is that the handlers compile and a manual `websocat` exchange works.

- [ ] **Step 1: Implement `handle_robot`**

Replace the `handle_robot` stub in `routes.rs`:

```rust
use futures_util::{SinkExt, StreamExt};
use vantage_protocol::signalling::{ClientMsg, RobotMsg, Signal};
use vantage_protocol::{RobotId, SessionId};

pub async fn handle_robot(socket: WebSocket, state: Arc<AppState>) {
    let (mut tx_ws, mut rx_ws) = socket.split();
    let (tx_out, mut rx_out) = mpsc::unbounded_channel::<ServerMsg>();

    // Pump outbound ServerMsgs to the websocket.
    let pump = tokio::spawn(async move {
        while let Some(msg) = rx_out.recv().await {
            let txt = serde_json::to_string(&msg).unwrap();
            if tx_ws.send(Message::Text(txt.into())).await.is_err() {
                break;
            }
        }
    });

    let mut my_id: Option<RobotId> = None;

    while let Some(Ok(raw)) = rx_ws.next().await {
        let Some(text) = split_text(raw) else { continue };
        let Ok(msg) = serde_json::from_str::<RobotMsg>(&text) else {
            let _ = tx_out.send(ServerMsg::Error { message: "bad robot message".into() });
            continue;
        };
        match msg {
            RobotMsg::Register(info) => {
                let id = info.id.clone();
                state.registry.lock().await.register(info, std::time::Instant::now());
                state.peers.lock().await.insert(id.0.clone(), tx_out.clone());
                my_id = Some(id);
            }
            RobotMsg::Heartbeat => {
                if let Some(id) = &my_id {
                    state.registry.lock().await.heartbeat(id, std::time::Instant::now());
                }
            }
            RobotMsg::Signal { to, signal } => {
                relay_to(&state, &to.0, ServerMsg::Signal { from: None, signal }).await;
            }
        }
    }

    // Cleanup: robot disconnected.
    if let Some(id) = my_id {
        state.registry.lock().await.remove(&id);
        state.peers.lock().await.remove(&id.0);
        // Notify any clients attached to this robot.
        let orphaned = state.sessions.lock().await.sessions_for(&id);
        for s in orphaned {
            relay_to(&state, &s.0, ServerMsg::Error { message: "robot disconnected".into() }).await;
            state.sessions.lock().await.close(&s);
        }
    }
    pump.abort();
}

async fn relay_to(state: &Arc<AppState>, peer_key: &str, msg: ServerMsg) {
    if let Some(out) = state.peers.lock().await.get(peer_key) {
        let _ = out.send(msg);
    }
}
```

- [ ] **Step 2: Implement `handle_client`**

Replace the `handle_client` stub:

```rust
pub async fn handle_client(socket: WebSocket, state: Arc<AppState>) {
    let (mut tx_ws, mut rx_ws) = socket.split();
    let (tx_out, mut rx_out) = mpsc::unbounded_channel::<ServerMsg>();

    let pump = tokio::spawn(async move {
        while let Some(msg) = rx_out.recv().await {
            let txt = serde_json::to_string(&msg).unwrap();
            if tx_ws.send(Message::Text(txt.into())).await.is_err() {
                break;
            }
        }
    });

    // Each client connection is one session.
    let session = SessionId(format!("sess-{}", uuid_like()));
    state.peers.lock().await.insert(session.0.clone(), tx_out.clone());

    while let Some(Ok(raw)) = rx_ws.next().await {
        let Some(text) = split_text(raw) else { continue };
        let Ok(msg) = serde_json::from_str::<ClientMsg>(&text) else {
            let _ = tx_out.send(ServerMsg::Error { message: "bad client message".into() });
            continue;
        };
        match msg {
            ClientMsg::ListRobots => {
                let mut reg = state.registry.lock().await;
                reg.prune(std::time::Instant::now());
                let robots = reg.list();
                let _ = tx_out.send(ServerMsg::RobotList { robots });
            }
            ClientMsg::Connect { robot } => {
                state.sessions.lock().await.open(session.clone(), robot.clone());
                // Tell the robot a client arrived (so it builds its peer + offer).
                relay_to(&state, &robot.0, ServerMsg::ClientConnected { session: session.clone() }).await;
                let _ = tx_out.send(ServerMsg::Connected { robot, session: session.clone() });
            }
            ClientMsg::Signal { signal } => {
                // Route to the robot of this session, tagging our session id.
                let robot = state.sessions.lock().await.robot_for(&session).cloned();
                if let Some(robot) = robot {
                    relay_to(&state, &robot.0,
                        ServerMsg::Signal { from: Some(session.clone()), signal }).await;
                }
            }
        }
    }

    // Cleanup: client disconnected -> close session, notify robot.
    if let Some(robot) = state.sessions.lock().await.close(&session) {
        relay_to(&state, &robot.0, ServerMsg::ClientDisconnected { session: session.clone() }).await;
    }
    state.peers.lock().await.remove(&session.0);
    pump.abort();
}

/// Tiny unique-ish id without pulling in the uuid crate for the PoC.
fn uuid_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{n:x}")
}
```

> Note on `from` tagging: the client's `Signal` carries the robot's reply via `ServerMsg::Signal { from: Some(session) }` so the robot knows which client to answer. The robot's `RobotMsg::Signal { to: session }` addresses the client directly. SDP/ICE bodies are relayed verbatim — the coordinator never parses them.

- [ ] **Step 3: Verify it builds**

Run: `cargo build -p vantage-coordinator`
Expected: compiles clean.

- [ ] **Step 4: Manual smoke (optional but recommended)**

```bash
cargo run -p vantage-coordinator &
# install websocat if needed: cargo install websocat
echo '{"type":"register","id":"r1","name":"Atlas","capabilities":[]}' | websocat -n1 ws://localhost:8080/ws/robot
echo '{"type":"list_robots"}' | websocat -n1 ws://localhost:8080/ws/client
# second command should return {"type":"robot_list","robots":[{"id":"r1",...}]}
kill %1
```

- [ ] **Step 5: Commit**

```bash
git add vantage-coordinator/src/routes.rs
git commit -m "feat(coordinator): ws handlers for registration, discovery, signalling relay"
```

---

### Task 9: `vantage-signalling` — coordinator WS client + webrtcbin data-channel driver

**Files:**
- Create: `vantage-signalling/src/ws.rs`
- Create: `vantage-signalling/src/peer.rs`

> **Verification note:** Live WebRTC/ICE is async, network-dependent, and not amenable to fast unit TDD. Tasks 9–11 are written as integration code with real `webrtcbin` calls; the behavioural test is the end-to-end exit criteria in Task 12. This is deliberate — fabricating unit tests around live ICE would be dishonest and low-value. Requires the GStreamer prerequisites at the top of this plan.

- [ ] **Step 1: WebSocket client to the coordinator**

`vantage-signalling/src/ws.rs`:

```rust
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

    /// Serialize and send any protocol message (RobotMsg or ClientMsg).
    pub async fn send<T: serde::Serialize>(&mut self, msg: &T) -> Result<()> {
        let txt = serde_json::to_string(msg)?;
        self.inner.send(Message::Text(txt.into())).await?;
        Ok(())
    }

    /// Receive and deserialize the next ServerMsg, if any.
    pub async fn recv<T: serde::de::DeserializeOwned>(&mut self) -> Result<Option<T>> {
        while let Some(item) = self.inner.next().await {
            if let Message::Text(t) = item? {
                return Ok(Some(serde_json::from_str(&t)?));
            }
        }
        Ok(None)
    }
}
```

- [ ] **Step 2: webrtcbin peer driver (data-channel-only)**

`vantage-signalling/src/peer.rs`:

```rust
use anyhow::{Context, Result};
use gstreamer as gst;
use gstreamer::prelude::*;
use tokio::sync::mpsc;
use vantage_protocol::signalling::{IceServer, Signal};

/// Events the peer raises that the app must relay through the coordinator.
pub enum PeerEvent {
    LocalDescription(Signal),       // Offer or Answer to forward
    LocalIce(Signal),               // Signal::Ice to forward
    DataChannelOpen,
    DataMessage(Vec<u8>),           // bytes received on the data channel
}

pub struct Peer {
    pub pipeline: gst::Pipeline,
    pub webrtcbin: gst::Element,
    pub events: mpsc::UnboundedReceiver<PeerEvent>,
    data_channel: std::sync::Mutex<Option<glib::Object>>,
}

impl Peer {
    /// `polite=true` for the answerer (client); `false` for the offerer (robot).
    pub fn new(ice_servers: &[IceServer], create_data_channel: bool) -> Result<Self> {
        gst::init()?;
        let pipeline = gst::Pipeline::new();
        let webrtcbin = gst::ElementFactory::make("webrtcbin")
            .name("sendrecv")
            .property("bundle-policy", gst_webrtc::WebRTCBundlePolicy::MaxBundle)
            .build()
            .context("webrtcbin missing — install gst-plugins-bad")?;

        // STUN + TURN from coordinator (first STUN -> stun-server; TURN -> add-turn-server).
        for s in ice_servers {
            for url in &s.urls {
                if url.starts_with("stun:") {
                    webrtcbin.set_property("stun-server", url);
                } else if url.starts_with("turn:") {
                    let with_creds = match (&s.username, &s.credential) {
                        (Some(u), Some(p)) => format!("turn://{u}:{p}@{}", url.trim_start_matches("turn:")),
                        _ => url.clone(),
                    };
                    let _ = webrtcbin.emit_by_name::<bool>("add-turn-server", &[&with_creds]);
                }
            }
        }

        pipeline.add(&webrtcbin)?;

        let (tx, rx) = mpsc::unbounded_channel();

        // on-negotiation-needed -> create offer (offerer only).
        if !create_data_channel {
            // answerer reacts to remote offer instead (see handle_signal).
        }

        // Emit local ICE candidates.
        {
            let tx = tx.clone();
            webrtcbin.connect("on-ice-candidate", false, move |vals| {
                let mlineindex = vals[1].get::<u32>().unwrap();
                let candidate = vals[2].get::<String>().unwrap();
                let _ = tx.send(PeerEvent::LocalIce(Signal::Ice {
                    candidate,
                    sdp_mline_index: mlineindex,
                }));
                None
            });
        }

        let peer = Self {
            pipeline,
            webrtcbin,
            events: rx,
            data_channel: std::sync::Mutex::new(None),
        };

        if create_data_channel {
            peer.create_data_channel("telemetry", &tx)?;
            peer.wire_on_negotiation_needed(&tx);
        } else {
            peer.wire_on_data_channel(&tx);
        }

        peer.pipeline.set_state(gst::State::Playing)?;
        Ok(peer)
    }

    fn create_data_channel(&self, label: &str, tx: &mpsc::UnboundedSender<PeerEvent>) -> Result<()> {
        // Reliable ordered for discrete telemetry events at this milestone
        // (design.md / telemetry spec: unreliable mode arrives with high-rate streams).
        let dc = self.webrtcbin
            .emit_by_name::<glib::Object>("create-data-channel", &[&label, &None::<gst::Structure>]);
        wire_data_channel(&dc, tx);
        *self.data_channel.lock().unwrap() = Some(dc);
        Ok(())
    }

    fn wire_on_negotiation_needed(&self, tx: &mpsc::UnboundedSender<PeerEvent>) {
        let bin = self.webrtcbin.clone();
        let tx = tx.clone();
        self.webrtcbin.connect("on-negotiation-needed", false, move |_| {
            let bin2 = bin.clone();
            let tx2 = tx.clone();
            let promise = gst::Promise::with_change_func(move |reply| {
                let reply = reply.unwrap().unwrap();
                let offer = reply.value("offer").unwrap()
                    .get::<gst_webrtc::WebRTCSessionDescription>().unwrap();
                bin2.emit_by_name::<()>("set-local-description", &[&offer, &None::<gst::Promise>]);
                let sdp = offer.sdp().as_text().unwrap();
                let _ = tx2.send(PeerEvent::LocalDescription(Signal::Offer { sdp }));
            });
            bin.emit_by_name::<()>("create-offer", &[&None::<gst::Structure>, &promise]);
            None
        });
    }

    fn wire_on_data_channel(&self, tx: &mpsc::UnboundedSender<PeerEvent>) {
        let tx = tx.clone();
        let slot = self as *const _;
        let _ = slot;
        self.webrtcbin.connect("on-data-channel", false, move |vals| {
            let dc = vals[1].get::<glib::Object>().unwrap();
            wire_data_channel(&dc, &tx);
            None
        });
    }

    /// Apply a Signal received from the remote peer via the coordinator.
    pub fn handle_signal(&self, signal: Signal, tx: &mpsc::UnboundedSender<PeerEvent>) -> Result<()> {
        match signal {
            Signal::Offer { sdp } => {
                let desc = parse_sdp(&sdp, gst_webrtc::WebRTCSDPType::Offer)?;
                self.webrtcbin.emit_by_name::<()>("set-remote-description", &[&desc, &None::<gst::Promise>]);
                // create answer
                let bin = self.webrtcbin.clone();
                let tx = tx.clone();
                let promise = gst::Promise::with_change_func(move |reply| {
                    let reply = reply.unwrap().unwrap();
                    let answer = reply.value("answer").unwrap()
                        .get::<gst_webrtc::WebRTCSessionDescription>().unwrap();
                    bin.emit_by_name::<()>("set-local-description", &[&answer, &None::<gst::Promise>]);
                    let sdp = answer.sdp().as_text().unwrap();
                    let _ = tx.send(PeerEvent::LocalDescription(Signal::Answer { sdp }));
                });
                self.webrtcbin.emit_by_name::<()>("create-answer", &[&None::<gst::Structure>, &promise]);
            }
            Signal::Answer { sdp } => {
                let desc = parse_sdp(&sdp, gst_webrtc::WebRTCSDPType::Answer)?;
                self.webrtcbin.emit_by_name::<()>("set-remote-description", &[&desc, &None::<gst::Promise>]);
            }
            Signal::Ice { candidate, sdp_mline_index } => {
                self.webrtcbin.emit_by_name::<()>("add-ice-candidate", &[&sdp_mline_index, &candidate]);
            }
        }
        Ok(())
    }

    /// Send bytes on the open data channel (telemetry).
    pub fn send_data(&self, bytes: &[u8]) -> Result<()> {
        if let Some(dc) = self.data_channel.lock().unwrap().as_ref() {
            let glib_bytes = glib::Bytes::from(bytes);
            dc.emit_by_name::<()>("send-data", &[&glib_bytes]);
        }
        Ok(())
    }
}

fn wire_data_channel(dc: &glib::Object, tx: &mpsc::UnboundedSender<PeerEvent>) {
    {
        let tx = tx.clone();
        dc.connect("on-open", false, move |_| {
            let _ = tx.send(PeerEvent::DataChannelOpen);
            None
        });
    }
    {
        let tx = tx.clone();
        dc.connect("on-message-data", false, move |vals| {
            if let Ok(bytes) = vals[1].get::<glib::Bytes>() {
                let _ = tx.send(PeerEvent::DataMessage(bytes.to_vec()));
            }
            None
        });
    }
}

fn parse_sdp(sdp: &str, ty: gst_webrtc::WebRTCSDPType) -> Result<gst_webrtc::WebRTCSessionDescription> {
    let msg = gst_sdp::SDPMessage::parse_buffer(sdp.as_bytes())?;
    Ok(gst_webrtc::WebRTCSessionDescription::new(ty, msg))
}

use gstreamer_sdp as gst_sdp;
use gstreamer_webrtc as gst_webrtc;
```

Add the matching imports to `vantage-signalling/src/lib.rs` if needed (`gst_webrtc`/`gst_sdp` are referenced from `peer.rs` via the `use` at file end). Keep the data-channel handle behind a `Mutex` because GStreamer signal callbacks are `Send`-hostile otherwise; the data channel is only touched from the app's single task.

> Implementation reality check for the executor: `webrtcbin`'s exact signal signatures vary slightly across `gstreamer-rs` 0.23.x. If `connect("on-ice-candidate", ...)` closure arg extraction or `emit_by_name` generics don't match your installed version, consult Context7 for the pinned `gstreamer-rs` docs (`mcp__plugin_context7_context7__resolve-library-id` → `query-docs` for "gstreamer-rs webrtcbin create-offer on-data-channel") rather than guessing. The control flow (offer/answer/ICE relay, data-channel open/message) is correct; only the binding spelling may drift.

- [ ] **Step 3: Verify it builds**

Run: `cargo build -p vantage-signalling`
Expected: compiles (requires the GStreamer dev packages from Prerequisites).

- [ ] **Step 4: Commit**

```bash
git add vantage-signalling/src/ws.rs vantage-signalling/src/peer.rs vantage-signalling/src/lib.rs
git commit -m "feat(signalling): coordinator ws client + webrtcbin data-channel peer"
```

---

### Task 10: `vantage-robot` — register, build offerer peer, stream telemetry

**Files:**
- Create: `vantage-robot/src/telemetry.rs`
- Modify: `vantage-robot/src/main.rs`

- [ ] **Step 1: sysinfo → DeviceInfo sampler (with a unit test)**

`vantage-robot/src/telemetry.rs`:

```rust
use sysinfo::{Components, System};
use vantage_protocol::telemetry::{DeviceInfo, TempReading};

pub struct Sampler {
    sys: System,
    components: Components,
}

impl Sampler {
    pub fn new() -> Self {
        Self { sys: System::new_all(), components: Components::new_with_refreshed_list() }
    }

    pub fn sample(&mut self) -> DeviceInfo {
        self.sys.refresh_cpu_usage();
        self.sys.refresh_memory();
        self.components.refresh();

        let cpu_percent = self.sys.global_cpu_usage();
        let temps = self.components.iter()
            .map(|c| TempReading { label: c.label().to_string(), celsius: c.temperature() })
            .collect();

        DeviceInfo {
            cpu_percent,
            mem_used_mb: self.sys.used_memory() / (1024 * 1024),
            mem_total_mb: self.sys.total_memory() / (1024 * 1024),
            temps,
            uptime_s: System::uptime(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_reports_nonzero_total_memory() {
        let mut s = Sampler::new();
        let info = s.sample();
        assert!(info.mem_total_mb > 0, "total memory should be discoverable");
    }
}
```

Run: `cargo test -p vantage-robot telemetry`
Expected: PASS (1 test). (`sysinfo` 0.33 API: `global_cpu_usage`, `Components`. If your pinned version differs, check Context7 for `sysinfo`.)

- [ ] **Step 2: Robot main loop**

`vantage-robot/src/main.rs`:

```rust
mod telemetry;

use std::time::Duration;

use anyhow::Result;
use vantage_protocol::codec;
use vantage_protocol::signalling::{RobotInfo, RobotMsg, ServerMsg, Signal};
use vantage_protocol::{RobotId, SessionId};
use vantage_signalling::peer::{Peer, PeerEvent};
use vantage_signalling::ws::CoordinatorWs;

use telemetry::Sampler;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    let coord = std::env::var("VANTAGE_COORDINATOR").unwrap_or_else(|_| "ws://localhost:8080".into());

    let mut ws = CoordinatorWs::connect(&format!("{coord}/ws/robot")).await?;
    let id = RobotId(std::env::var("VANTAGE_ROBOT_ID").unwrap_or_else(|_| "robot-1".into()));
    ws.send(&RobotMsg::Register(RobotInfo {
        id: id.clone(),
        name: "Atlas".into(),
        capabilities: vec!["telemetry".into()],
    })).await?;

    // Heartbeat task.
    // (Real impl shares the ws via a channel; for the PoC, open a second ws for heartbeat.)
    spawn_heartbeat(coord.clone(), id.clone());

    // Wait for a client to connect, then build the offerer peer.
    let ice = fetch_ice(&coord).await?;
    let (evt_tx, _evt_tx_keep) = tokio::sync::mpsc::unbounded_channel::<PeerEvent>();
    let mut peer: Option<Peer> = None;
    let mut current_client: Option<SessionId> = None;

    loop {
        tokio::select! {
            // Coordinator -> robot messages.
            msg = ws.recv::<ServerMsg>() => {
                match msg? {
                    Some(ServerMsg::ClientConnected { session }) => {
                        current_client = Some(session);
                        let p = Peer::new(&ice, /*create_data_channel=*/true)?;
                        peer = Some(p);
                        // on-negotiation-needed fires -> offer emitted as PeerEvent below.
                    }
                    Some(ServerMsg::Signal { from: _, signal }) => {
                        if let Some(p) = &peer {
                            p.handle_signal(signal, &evt_tx)?;
                        }
                    }
                    Some(ServerMsg::ClientDisconnected { .. }) => {
                        peer = None;
                        current_client = None;
                    }
                    _ => {}
                }
            }
            // Peer -> coordinator events.
            event = recv_peer_event(&mut peer) => {
                if let (Some(ev), Some(client)) = (event, &current_client) {
                    match ev {
                        PeerEvent::LocalDescription(sig) | PeerEvent::LocalIce(sig) => {
                            ws.send(&RobotMsg::Signal { to: client.clone(), signal: sig }).await?;
                        }
                        PeerEvent::DataChannelOpen => {
                            // Start the telemetry pump.
                            if let Some(p) = &peer {
                                pump_telemetry(p);
                            }
                        }
                        PeerEvent::DataMessage(_) => { /* reserved: future control channel */ }
                    }
                }
            }
        }
    }
}

async fn recv_peer_event(peer: &mut Option<Peer>) -> Option<PeerEvent> {
    match peer {
        Some(p) => p.events.recv().await,
        None => { std::future::pending::<()>().await; None }
    }
}

fn pump_telemetry(peer: &Peer) {
    // Sample once per second and send over the data channel.
    // NOTE: Peer is not Send across .await freely; sample synchronously on a std thread
    // that holds a clone of the data-channel sender. For the PoC, sample inline on a timer.
    let mut sampler = Sampler::new();
    let info = sampler.sample();
    let bytes = codec::encode(&info).unwrap();
    let _ = peer.send_data(&bytes);
    // Executor: replace this one-shot with a 1s interval loop wired to peer.send_data.
}

fn spawn_heartbeat(coord: String, id: RobotId) {
    tokio::spawn(async move {
        if let Ok(mut hb) = CoordinatorWs::connect(&format!("{coord}/ws/robot")).await {
            // Re-register on the heartbeat socket so the coordinator binds id->socket.
            let _ = hb.send(&RobotMsg::Register(RobotInfo {
                id: id.clone(), name: "Atlas".into(), capabilities: vec!["telemetry".into()],
            })).await;
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                if hb.send(&RobotMsg::Heartbeat).await.is_err() { break; }
            }
        }
    });
}

async fn fetch_ice(coord: &str) -> Result<Vec<vantage_protocol::signalling::IceServer>> {
    let http = coord.replacen("ws", "http", 1);
    let body = reqwest_get(&format!("{http}/ice")).await?;
    Ok(serde_json::from_str(&body)?)
}

// Minimal GET without adding reqwest: reuse the ws crate's http? For the PoC, add
// `reqwest = { version = "0.12", features = ["json"] }` to vantage-robot/Cargo.toml
// and replace this with reqwest::get(...).text().await.
async fn reqwest_get(_url: &str) -> Result<String> {
    unimplemented!("executor: add reqwest and implement; see comment above");
}
```

> **Executor notes for Task 10 (intentional rough edges to resolve, not placeholders to skip):**
> 1. Add `reqwest = { version = "0.12" }` to `vantage-robot/Cargo.toml` and implement `fetch_ice` with `reqwest::get(url).await?.text().await?`. (Kept out of the shared deps because only the binaries need HTTP.)
> 2. Replace the one-shot `pump_telemetry` with a `tokio::time::interval(Duration::from_secs(1))` loop that calls `peer.send_data`. Because `Peer` holds GStreamer objects, run the sampler on the main task and only pass the encoded `Vec<u8>` to `send_data` (which is cheap and thread-safe via the data channel).
> 3. The dual-socket heartbeat is a PoC shortcut. The clean version shares one `CoordinatorWs` behind an `mpsc` writer task; do that if the dual registration causes id/socket churn in the coordinator's `peers` map. If you unify, also unify the `peers` insert so the relay always targets the live socket.

- [ ] **Step 3: Verify it builds**

Run: `cargo build -p vantage-robot`
Expected: compiles after the reqwest dep and the two TODO bodies are filled.

- [ ] **Step 4: Commit**

```bash
git add vantage-robot/
git commit -m "feat(robot): register, build offerer peer, stream DeviceInfo over data channel"
```

---

### Task 11: `vantage-client` — discover, connect, receive telemetry

**Files:**
- Modify: `vantage-client/src/main.rs`

- [ ] **Step 1: Client main loop (answerer)**

`vantage-client/src/main.rs`:

```rust
use anyhow::Result;
use vantage_protocol::codec;
use vantage_protocol::signalling::{ClientMsg, ServerMsg};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_protocol::RobotId;
use vantage_signalling::peer::{Peer, PeerEvent};
use vantage_signalling::ws::CoordinatorWs;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    let coord = std::env::var("VANTAGE_COORDINATOR").unwrap_or_else(|_| "ws://localhost:8080".into());

    let mut ws = CoordinatorWs::connect(&format!("{coord}/ws/client")).await?;

    // 1. Discover.
    ws.send(&ClientMsg::ListRobots).await?;
    let robots = match ws.recv::<ServerMsg>().await? {
        Some(ServerMsg::RobotList { robots }) => robots,
        other => anyhow::bail!("expected robot list, got {other:?}"),
    };
    let target = robots.into_iter().next().ok_or_else(|| anyhow::anyhow!("no robots online"))?;
    tracing::info!("connecting to {}", target.name);

    // 2. Connect -> robot will send the offer through the coordinator.
    ws.send(&ClientMsg::Connect { robot: RobotId(target.id.0.clone()) }).await?;

    let ice = fetch_ice(&coord).await?;
    let (evt_tx, _keep) = tokio::sync::mpsc::unbounded_channel::<PeerEvent>();
    let peer = Peer::new(&ice, /*create_data_channel=*/false)?; // answerer

    loop {
        tokio::select! {
            msg = ws.recv::<ServerMsg>() => {
                match msg? {
                    Some(ServerMsg::Signal { from: _, signal }) => {
                        peer.handle_signal(signal, &evt_tx)?;
                    }
                    Some(ServerMsg::Error { message }) => {
                        tracing::warn!("coordinator error: {message}");
                    }
                    None => break,
                    _ => {}
                }
            }
            event = recv_event(&peer) => {
                match event {
                    Some(PeerEvent::LocalDescription(sig)) | Some(PeerEvent::LocalIce(sig)) => {
                        ws.send(&ClientMsg::Signal { signal: sig }).await?;
                    }
                    Some(PeerEvent::DataMessage(bytes)) => {
                        match codec::decode::<DeviceInfo>(&bytes) {
                            Ok(info) => tracing::info!(
                                "telemetry: cpu={:.1}% mem={}/{}MB temps={}",
                                info.cpu_percent, info.mem_used_mb, info.mem_total_mb, info.temps.len()
                            ),
                            Err(e) => tracing::warn!("bad telemetry: {e}"),
                        }
                    }
                    Some(PeerEvent::DataChannelOpen) => tracing::info!("data channel open"),
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

async fn recv_event(peer: &Peer) -> Option<PeerEvent> {
    // events is &mut behind the Peer; for the PoC, store events in a Mutex or
    // restructure Peer to expose a take_events() once. Executor: wrap
    // `events` in `tokio::sync::Mutex` inside Peer and lock here.
    let _ = peer;
    std::future::pending().await
}

async fn fetch_ice(coord: &str) -> Result<Vec<vantage_protocol::signalling::IceServer>> {
    let http = coord.replacen("ws", "http", 1);
    let body = reqwest::get(format!("{http}/ice")).await?.text().await?;
    Ok(serde_json::from_str(&body)?)
}
```

Add `reqwest = { version = "0.12" }` to `vantage-client/Cargo.toml`.

> **Executor note:** `Peer.events` is an owned `mpsc::UnboundedReceiver`, which needs `&mut` to `recv`. The cleanest fix is to change `Peer.events` to `tokio::sync::Mutex<UnboundedReceiver<PeerEvent>>` and have `recv_event` lock-and-`recv`. Apply the same change in the robot. Make this edit in `vantage-signalling/src/peer.rs` and update both call sites; it is the one structural change needed to make Tasks 10–11 compile cleanly.

- [ ] **Step 2: Verify it builds**

Run: `cargo build --workspace`
Expected: whole workspace compiles.

- [ ] **Step 3: Commit**

```bash
git add vantage-client/ vantage-signalling/src/peer.rs vantage-robot/
git commit -m "feat(client): discover, connect, receive telemetry over data channel"
```

---

### Task 12: End-to-end integration — same-LAN, then forced TURN relay

**Files:**
- Create: `docs/superpowers/plans/notes/2026-06-17-milestone-exit.md` (capture evidence)

This is the milestone exit gate (tasks.md Phase 2 exit criteria + the explicit relay requirement). No new product code — run the three binaries and verify behaviour.

- [ ] **Step 1: Same-LAN end-to-end**

```bash
# terminal 1
RUST_LOG=info cargo run -p vantage-coordinator
# terminal 2
RUST_LOG=info VANTAGE_COORDINATOR=ws://localhost:8080 cargo run -p vantage-robot
# terminal 3
RUST_LOG=info VANTAGE_COORDINATOR=ws://localhost:8080 cargo run -p vantage-client
```

Expected: client logs `connecting to Atlas`, then `data channel open`, then recurring `telemetry: cpu=... mem=.../...MB ...` lines. This proves discovery + WebRTC data channel + shared `DeviceInfo` types end to end (discovery + telemetry specs).

- [ ] **Step 2: Verify the direct (host) path was used**

With `GST_DEBUG=webrtcbin:5` (or inspect ICE state), confirm the selected candidate pair is host/srflx, not relay, when both peers are on the same machine/LAN. Record the selected pair.

- [ ] **Step 3: Force the TURN relay path**

Start the coordinator with TURN env so peers receive relay creds, and block direct paths to force ICE onto the relay:

```bash
# coordinator with TURN (local coturn from Prerequisites, or metered.ca creds)
VANTAGE_TURN_URL=turn:localhost:3478 \
VANTAGE_TURN_USER=vantage VANTAGE_TURN_PASS=vantagepoc \
RUST_LOG=info cargo run -p vantage-coordinator
```

Force relay by setting `webrtcbin`'s ICE transport policy to relay-only for this test: temporarily set `ice-transport-policy = relay` on `webrtcbin` in `Peer::new` (gate behind `VANTAGE_FORCE_RELAY=1`), or firewall-drop host/srflx candidates. Re-run robot + client.

Expected: telemetry still flows; the selected candidate pair is `relay`. This proves ICE traversal and the static TURN credentials (the explicit Phase 2 requirement and fleet-management "TURN credential provisioning").

- [ ] **Step 4: Verify expiry**

Kill the robot (Ctrl-C). Within ~15s (the TTL), a fresh `cargo run -p vantage-client` (or a re-issued `ListRobots`) returns an empty robot list — proving heartbeat expiry (discovery "Stale robot expires").

- [ ] **Step 5: Record evidence and commit**

Write the observed candidate pairs (host and relay), a sample telemetry line, and the expiry observation into `docs/superpowers/plans/notes/2026-06-17-milestone-exit.md`.

```bash
git add docs/superpowers/plans/notes/2026-06-17-milestone-exit.md
git commit -m "test: foundation milestone end-to-end evidence (direct + relay)"
```

---

## Milestone Exit Criteria (gate before Phase 3)

- [ ] `cargo test --workspace` is green (protocol round-trips, registry expiry, sessions, sysinfo sampler).
- [ ] Robot and client discover each other through the coordinator and exchange `DeviceInfo` over a WebRTC **data channel**.
- [ ] The same exchange works when **forced onto the TURN relay** (evidence recorded).
- [ ] A stopped robot disappears from discovery within the TTL.
- [ ] The peer connection establishes a data channel from day one with a reserved return path (the answerer-side `on-data-channel` plumbing exists), satisfying telemetry "Bidirectional connection from day one" at the connection level. (Wiring an actual operator→robot channel and its failsafe is Phase 6.)

Once this gate is green, the Phase 3 plan (`video with videotestsrc + x264enc`, then the client `appsink`→Slint texture path) can start from a proven traversal + signalling base.

---

## Deferred to later plans (explicitly out of this milestone)

- **Phase 3:** `webrtcbin` video transceiver (`sendonly`/`recvonly`), `videotestsrc ! x264enc`, client decode → `slint::Image::from_rgba8`. (video-streaming "One-way video"; the client texture path is the next big risk.)
- **Phase 4:** real `v4l2src`/`nvarguscamerasrc`, the `tee`, raw `sensor_msgs/Image` + `camera_info` via `rclrs`, encoder factory. (all of camera-sharing; video-streaming under real capture.)
- **Phase 5:** hardware decode, RTP `tee` encode-once fan-out, demand-driven sink branches via pad probes, `GstForceKeyUnit` on join, `transport-cc` adaptive bitrate. (video-streaming "Encode once…", "Immediate startup…", "Demand-driven…", "Adaptive bitrate".)
- **Phase 6:** session-derived provider/consumer counts surfaced (fleet-management "Provider count"/"Consumer count" reporting), mDNS LAN fast path (discovery "LAN-local fast path"), reserved control channel wired + **teleop disconnect failsafe specified before any control acts**, ROS topic bridge, `bincode` codec swap + unreliable/unordered channel mode for high-rate data (telemetry "Channel reliability matched to data type").
