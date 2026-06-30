# Vantage Phase 6 — Fleet stats, mDNS LAN fast-path, teleop control channel + disconnect failsafe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close out the PoC's coordinator/control surface. (1) Expose **session-derived fleet stats** (providers-online / consumers-connected) that stay accurate through ungraceful disconnects. (2) Add an **mDNS LAN-local fast path** so a client on the same LAN can discover and connect to a robot when the coordinator is unreachable. (3) **Reserve and wire the bidirectional control (teleop) data channel** alongside telemetry. (4) **Specify and implement the teleop disconnect failsafe/watchdog** — the safety gate that MUST be in place *before any control command is acted on*.

**Architecture:** Four largely-independent slices over the existing spine, ordered so the safety gate lands before the robot acts on any command:

- **Fleet stats** are read off state the coordinator already owns: `Registry` (live robots, TTL-expired by the existing 5 s pruner) and `Sessions` (open viewer sessions). The only new behaviour is *reconciliation* — closing sessions when a client or robot socket drops without a clean message — plus a `/stats` HTTP endpoint mirroring the existing `/ice` JSON pattern.
- **Control channel** reuses the day-one bidirectional contract (design §5): the robot, already the offerer, creates a *second* `control` data channel next to `telemetry` in the same SDP, so no renegotiation is needed. The client receives it via `webrtcbin`'s `on-data-channel`, routes by label, and sends `ControlMsg` upstream. Control is **unreliable/unordered** (latest-command-wins; the watchdog covers loss) per the telemetry spec's high-rate rule.
- **Failsafe/watchdog** is a per-session timer in the robot. Any control message (or a periodic control keepalive) resets it; expiry — or DataChannel close / `ClientDisconnected` — drives the robot into a **safe state** (neutral command, logged `SafeState` event). The robot only *acts on* (for the PoC: logs/echoes) a control command once the watchdog is proven to trip. This ordering is the whole point of the §6 wording.
- **mDNS** advertises each robot as a `_vantage._tcp` service carrying its `RobotInfo` and a direct LAN signalling port. When the coordinator is unreachable, the client browses mDNS, picks a robot, and connects to the robot's small **direct signalling listener** (the robot brokers its own SDP/ICE for one peer — no TURN needed on a LAN, host candidates suffice).

**Tech Stack:** Rust (workspace, edition 2024, rust 1.90), tokio, axum 0.8, `gstreamer-webrtc` 0.23 (`create-data-channel` with reliability options, `on-data-channel`), `mdns-sd` (pure-Rust mDNS/DNS-SD, no Avahi/Bonjour dependency), `serde_json` codec.

**Scope:** This is the spec's **Phase 6** (`openspec/changes/add-vantage-poc/tasks.md` §6). It delivers fleet stats, the mDNS fast path, the control channel, and the failsafe. Explicitly **out of scope** (deferred, not silently dropped):

- **Actual robot actuation.** The PoC has no motors; "act on a control command" means *log/echo the decoded `ControlMsg` and feed the watchdog*. Wiring commands to a real robot (ros2_control / joint commands) is post-PoC. The watchdog is built and verified now precisely so that wiring is safe later.
- **The `(Later)` §6 bullet** — bridging ROS joint-state topics into telemetry and swapping the data-channel codec to `bincode` for high-rate data. Tracked as carry-over; the `vantage-protocol::codec` wrapper already localises that swap to one place.
- **Ephemeral TURN credentials** (design §6) — still static for the PoC.
- 4b/5 carry-over: live-camera-on-hardware and the Docker/CI two-lane harness — unchanged by this plan.

**Builds on:** Phase 5 (`docs/superpowers/plans/2026-06-26-vantage-phase5-multiconsumer-hwdecode.md`, merged to `main`). `RobotMedia` + per-session `Consumer`, the session-tagged `(SessionId, PeerEvent)` event channel in `vantage-robot/src/main.rs`, the coordinator's `Sessions`/`Registry`, and the client `Peer` + `UiSink` are all reused. The telemetry data channel is the template for the control channel.

---

## Spec coverage (this plan)

- **fleet-management "Provider count"** → Task 1 (`Registry::len` surfaced via `/stats`; TTL pruner keeps it live).
- **fleet-management "Consumer count"** → Task 1 (`Sessions::consumer_count` surfaced via `/stats`).
- **fleet-management "Session-derived statistics" / "Ungraceful disconnect"** → Task 1 Step 2 (close the session on client/robot socket drop so the count reconciles without a clean `ClientDisconnected`).
- **discovery "LAN-local fast path" / "Coordinator unreachable on a LAN"** → Task 4 (robot mDNS advertisement + client browse + direct signalling listener).
- **telemetry "Bidirectional connection from day one"** → Task 2 (the `control` DC is created in the same SDP as `telemetry`; no renegotiation).
- **telemetry "Channel reliability matched to data type"** → Task 2 (control DC is unreliable/unordered; telemetry stays reliable-ordered).
- **telemetry "Shared message types"** → Task 2 (`ControlMsg` defined once in `vantage-protocol`, used by robot + client; coordinator never inspects it).
- **Project safety non-negotiable / design §11 "Teleop failsafe"** → Task 3 (watchdog → safe state before any command is acted on).

---

## Prerequisites — what this box can and cannot verify

| Capability | Verifiable on dev host? |
|---|---|
| `/stats` endpoint + session reconciliation (multi-client headless harness) | ✅ |
| Control DC negotiated bidirectionally + `ControlMsg` round-trip robot↔client | ✅ (loopback, headless) |
| Watchdog trips → safe state on stale/disconnect | ✅ (drive a control keepalive, then stop it / kill the client) |
| mDNS advertise + browse on loopback/LAN (`mdns-sd`) | ✅ on a real LAN or a host where multicast on `lo`/a NIC works; ⚠️ some CI sandboxes block multicast — record host-deferred if so, exactly as Phase 5 did for `rtpgccbwe` |
| Offline direct-connect WebRTC with the coordinator killed | ✅ on a LAN (host ICE candidates); the relay/TURN path is irrelevant offline |

> If multicast is unavailable in the build sandbox, Task 4's *discovery* is structured and unit-tested (TXT-record encode/decode, listener wiring) and the live browse is recorded **host-deferred** — not claimed as passed. This mirrors Phase 5's honesty rule for absent elements.

---

## File Structure

| File | Responsibility | Tasks |
|------|----------------|-------|
| `vantage-protocol/src/control.rs` | **Create.** `ControlMsg` (teleop command + keepalive) + the control-channel label constant. Unit-tested round-trip via `codec`. | 2 |
| `vantage-protocol/src/fleet.rs` | **Create.** `FleetStats { providers_online, consumers_connected }` (the `/stats` body). | 1 |
| `vantage-protocol/src/lib.rs` | **Modify.** `pub mod control; pub mod fleet;` + re-exports. | 1, 2 |
| `vantage-coordinator/src/routes.rs` | **Modify.** Add `GET /stats` → `Json(FleetStats)`. Ensure the client-WS and robot-WS handlers close sessions on socket drop (reconciliation). | 1 |
| `vantage-coordinator/src/sessions.rs` | **Modify.** Add `provider`/`consumer` accessors if needed; ensure `close`/orphan-on-robot-drop paths are exercised. | 1 |
| `vantage-signalling/src/control.rs` | **Create.** Reliability-options helper for the unreliable/unordered control DC (the one place the `ordered=false, max-retransmits=0` Structure is built). | 2 |
| `vantage-signalling/src/robot_media.rs` | **Modify.** In `add_consumer`, create the `control` DC next to `telemetry`; surface inbound control as a new `PeerEvent`; add `Consumer::send_control` is **not** needed (robot receives control), but expose the inbound control bytes per session. | 2, 3 |
| `vantage-signalling/src/peer.rs` | **Modify.** Listen on `webrtcbin`'s `on-data-channel`, route by label; hold the `control` DC; add `Peer::send_control(&ControlMsg)`. Extend `PeerEvent` with a labelled data variant (or add `ControlMessage(Vec<u8>)`). | 2 |
| `vantage-robot/src/main.rs` | **Modify.** Per-session watchdog (`HashMap<SessionId, Instant>` of last-control); a watchdog tick that trips stale sessions to safe state; on inbound control, reset the timer and log/echo the command **only after** the watchdog exists; safe-state on `ClientDisconnected`/DC-close. mDNS advertise on startup; direct signalling listener. | 2, 3, 4 |
| `vantage-robot/src/safety.rs` | **Create.** `Watchdog` (per-session deadline + `SafeState` decision) and `SafeState`/neutral-command logic. Pure + unit-tested (no GStreamer, no network). | 3 |
| `vantage-robot/src/discovery.rs` | **Create.** mDNS advertisement (`_vantage._tcp`, TXT = robot id/name/port) + the direct signalling listener used when a LAN client connects without the coordinator. | 4 |
| `vantage-client/src/session.rs` | **Modify.** Receive the `control` DC by label; expose a control sender; on coordinator-unreachable, fall back to mDNS browse → direct connect. | 2, 4 |
| `vantage-client/src/discovery.rs` | **Create.** mDNS browse → `Vec<RobotInfo + endpoint>` for the offline fast path. | 4 |
| `vantage-client/src/ui.rs` | **Modify (minimal).** Capture arrow-key / WASD `FocusScope` input and forward to a control sender (the only UI change; renders nothing new). | 2 |
| `Cargo.toml` (workspace) + robot/client `Cargo.toml` | **Modify.** Add `mdns-sd` to `[workspace.dependencies]` and to robot + client. | 4 |
| `docs/superpowers/plans/notes/2026-06-30-phase6-exit.md` | **Create.** Exit evidence. | 5 |

---

## Failsafe specification (read before Task 3 — this is the safety gate)

The §6 task says *"Specify and implement the teleop disconnect failsafe/watchdog **before any control command is acted on**."* Specification first:

- **Trigger conditions** (any one ⇒ enter safe state for that session):
  1. **Staleness:** no `ControlMsg` (command *or* keepalive) received from the session within `CONTROL_TIMEOUT` (default **500 ms**; the client sends a keepalive at **100 ms** so three consecutive losses trip it).
  2. **Channel loss:** the `control` DataChannel reports closed/errored.
  3. **Session loss:** `ServerMsg::ClientDisconnected{session}` (coordinator-observed) or the direct-connect peer drops.
- **Safe state:** the robot discards the session's last command and adopts the **neutral command** (all-zero velocity / no motion). For the PoC with no actuator this means: stop echoing the last command, log `safe-state entered: <session> (<reason>)`, and emit a `SafeState` marker the exit harness can grep. The neutral command is what a real actuator layer would consume — the watchdog's output is a *command*, not a side effect, so it stays testable.
- **Ordering invariant (the gate):** the robot MUST NOT forward/act on any decoded command for a session until that session has an armed watchdog. In code: `add_consumer` arms the watchdog (records `last_control = now`, marks the session "not yet live") and the inbound-control handler refuses to act while the timer is already expired. Task 3 proves the trip *before* Task 5 exercises commands end to end.
- **Recovery:** receiving a fresh `ControlMsg` after a staleness trip re-arms the watchdog and clears safe state (a momentary stall self-heals). A channel/session loss does not recover — the consumer is torn down.

`safety.rs` holds this as a pure `Watchdog` (deadlines + a `tick(now) -> Vec<(SessionId, SafeState)>` and `feed(session, now)`), unit-tested with an injected clock — no GStreamer, no sockets — so the safety logic is verified deterministically, independent of the media stack.

---

## Design decisions (read before Task 1)

1. **Stats are derived, never self-reported.** `providers_online = Registry::len()` (already TTL-pruned every 5 s) and `consumers_connected = Sessions::consumer_count()`. The spec's "session-derived … stays accurate as connections drop" is satisfied by making *socket drop* close the session, not by trusting a robot or client to announce its own departure. This is the single behavioural change in Task 1; the endpoint itself is trivial.
2. **Reconciliation lives where the socket dies.** The client-WS handler already runs a receive loop; when it ends (clean or not) it MUST `sessions.close(session)` and notify the robot. The robot-WS handler MUST, on robot drop, close *all* sessions for that robot (orphaned viewers). Both are small additions to existing teardown blocks in `routes.rs`.
3. **Two data channels, one negotiation.** The robot (offerer) creates `telemetry` *and* `control` before emitting its offer, so both are in the first SDP — honouring design §5 "bidirectional-ready, no renegotiation." The client routes incoming channels by `dc.property::<String>("label")`. No new transceiver, no SDP churn.
4. **Control is unreliable/unordered; telemetry stays reliable-ordered.** Teleop is latest-command-wins: a lost command must not head-of-line-block the next (telemetry spec). The control DC is built with `ordered=false, max-retransmits=0`. The watchdog — not retransmission — provides the safety guarantee. Telemetry keeps the default reliable-ordered channel unchanged.
5. **The coordinator is control-blind.** `ControlMsg` travels peer-to-peer over the DC; the coordinator never sees or relays it (it only brokers SDP/ICE and observes session lifecycle). So `ControlMsg` is a `vantage-protocol` type shared by robot + client only — no coordinator code touches it.
6. **mDNS is additive, not a rewrite.** The coordinator path stays primary. The robot *also* advertises over mDNS and *also* runs a direct signalling listener; the client tries the coordinator first and falls back to mDNS only when the coordinator WS connect fails. Offline, ICE uses host candidates (same LAN) so no STUN/TURN is needed — the relay machinery is simply unused. `mdns-sd` is pure-Rust (no system Avahi), keeping the build self-contained.
7. **`ControlMsg` shape is deliberately minimal.** A normalized 2-DOF teleop command plus a keepalive is enough to prove the channel + watchdog; richer command schemas are a post-PoC concern.
   ```rust
   // vantage-protocol/src/control.rs
   pub const CONTROL_LABEL: &str = "control";
   #[serde(tag = "kind", rename_all = "snake_case")]
   pub enum ControlMsg {
       /// Normalized teleop command, each in [-1.0, 1.0]. Latest wins.
       Move { linear: f32, angular: f32 },
       /// Liveness beat so the watchdog does not trip during an idle hold.
       KeepAlive,
   }
   ```

---

## Task 1: Fleet stats — `/stats` endpoint + session reconciliation

Surface providers-online / consumers-connected and make the consumer count reconcile on ungraceful disconnects. Self-contained, no media, lands first.

**Files:**
- Create: `vantage-protocol/src/fleet.rs`
- Modify: `vantage-protocol/src/lib.rs`, `vantage-coordinator/src/routes.rs`, `vantage-coordinator/src/sessions.rs`

- [ ] **Step 1: Define `FleetStats` in the protocol crate**

```rust
// vantage-protocol/src/fleet.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FleetStats {
    pub providers_online: usize,
    pub consumers_connected: usize,
}
```
Add `pub mod fleet;` and `pub use fleet::FleetStats;` to `vantage-protocol/src/lib.rs`.

- [ ] **Step 2: Reconcile sessions on socket drop (the behavioural change)**

In `vantage-coordinator/src/routes.rs`:
- **Client WS handler** (`client_ws`): wrap the receive loop so that when it returns *for any reason* (clean close, error, or drop), the handler runs the existing disconnect teardown — `sessions.close(session)` and send `ClientDisconnected{session}` to the robot. Confirm this teardown is in a `finally`-style path (e.g. after the `loop`/`select` exits), not only inside a clean-`Close`-message arm. If it is currently only reached on an explicit close message, move it so an ungraceful drop also reaches it.
- **Robot WS handler** (`robot_ws`): on robot disconnect, for every session in `sessions.sessions_for(&robot_id)` call `sessions.close(s)` (orphaned viewers) in addition to removing the robot from the registry. (The registry TTL pruner is the backstop if the robot vanishes without a socket close.)

> Verify the current teardown placement before editing — if reconciliation already happens on drop, this step is a no-op to confirm with a test, and that should be recorded (Surgical Changes: don't "fix" what isn't broken).

- [ ] **Step 3: Add the `/stats` route**

Mirror the existing `/ice` JSON handler (`routes.rs:60`):
```rust
// in the Router builder, next to /healthz and /ice:
.route("/stats", get(stats))

async fn stats(State(state): State<Arc<AppState>>) -> Json<FleetStats> {
    let providers_online = state.registry.lock().await.len();
    let consumers_connected = state.sessions.lock().await.consumer_count();
    Json(FleetStats { providers_online, consumers_connected })
}
```

- [ ] **Step 4: Unit + integration coverage**

- Unit: `FleetStats` round-trips through `codec` (or `serde_json`).
- Integration (headless, extends the Phase 5 harness): start coordinator + robot + two clients; `curl /stats` → `{"providers_online":1,"consumers_connected":2}`. **Kill one client with `SIGKILL`** (ungraceful) and poll `/stats` until `consumers_connected` drops to `1` (reconciliation). Kill the robot and confirm `providers_online` → `0` within the pruner interval and orphaned sessions are closed.

```bash
export RUST_LOG=info; BIN=$(pwd)/target/debug
VANTAGE_BIND=127.0.0.1:8130 $BIN/vantage-coordinator >/tmp/p6_coord.log 2>&1 & CP=$!
for i in $(seq 1 40); do curl -sf http://127.0.0.1:8130/healthz >/dev/null 2>&1 && break; sleep 0.25; done
VANTAGE_COORDINATOR=ws://127.0.0.1:8130 $BIN/vantage-robot >/tmp/p6_robot.log 2>&1 & RP=$!
sleep 2
VANTAGE_HEADLESS=1 VANTAGE_COORDINATOR=ws://127.0.0.1:8130 $BIN/vantage-client >/tmp/p6_c1.log 2>&1 & C1=$!
VANTAGE_HEADLESS=1 VANTAGE_COORDINATOR=ws://127.0.0.1:8130 $BIN/vantage-client >/tmp/p6_c2.log 2>&1 & C2=$!
sleep 3
echo "stats (expect 1 provider / 2 consumers): $(curl -s http://127.0.0.1:8130/stats)"
kill -9 $C2; sleep 3                                   # UNGRACEFUL drop
echo "stats after SIGKILL c2 (expect consumers=1): $(curl -s http://127.0.0.1:8130/stats)"
kill $C1 $RP 2>/dev/null; sleep 6                      # > pruner interval
echo "stats after robot gone (expect providers=0): $(curl -s http://127.0.0.1:8130/stats)"
kill $CP 2>/dev/null; wait 2>/dev/null
```

- [ ] **Step 5: Commit**
```bash
git add vantage-protocol/src/fleet.rs vantage-protocol/src/lib.rs vantage-coordinator/src/routes.rs vantage-coordinator/src/sessions.rs
git commit -m "feat(coordinator): /stats endpoint + session-derived fleet stats with drop reconciliation

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Bidirectional control data channel (reserve + wire, no action yet)

Add the `control` DC next to `telemetry` in the same SDP and round-trip `ControlMsg` client→robot. **No command is acted on in this task** — the robot only logs receipt. Acting on commands is gated behind Task 3's watchdog.

**Files:**
- Create: `vantage-protocol/src/control.rs`, `vantage-signalling/src/control.rs`
- Modify: `vantage-protocol/src/lib.rs`, `vantage-signalling/src/robot_media.rs`, `vantage-signalling/src/peer.rs`, `vantage-robot/src/main.rs`, `vantage-client/src/session.rs`, `vantage-client/src/ui.rs`

- [ ] **Step 1: Define `ControlMsg` + label (protocol crate)**

Create `vantage-protocol/src/control.rs` with the `CONTROL_LABEL` constant and `ControlMsg` enum from Design decision §7. Add `pub mod control; pub use control::{ControlMsg, CONTROL_LABEL};` to `lib.rs`. Unit-test the `codec` round-trip for both variants.

- [ ] **Step 2: Control-channel reliability helper (signalling crate)**

Create `vantage-signalling/src/control.rs` with the single place that builds the unreliable/unordered options Structure:
```rust
use gstreamer as gst;
/// Options for the control DC: latest-command-wins, no HOL blocking.
pub(crate) fn control_dc_options() -> gst::Structure {
    gst::Structure::builder("config")
        .field("ordered", false)
        .field("max-retransmits", 0i32)
        .build()
}
```
> Verify the exact field names against the installed `gstreamer-webrtc` version (`create-data-channel`'s options Structure keys). If `max-retransmits` is rejected, the absence of options (reliable-ordered) is the safe fallback — but record the deviation; the watchdog still provides the safety guarantee.

- [ ] **Step 3: Robot creates the `control` DC in `add_consumer`**

In `vantage-signalling/src/robot_media.rs::add_consumer`, immediately after the existing `telemetry` DC creation (around `robot_media.rs:232`), create a second channel and wire its inbound messages to a *labelled* event so the robot loop can distinguish control from anything else:
```rust
let control_dc = webrtcbin
    .emit_by_name_with_values(
        "create-data-channel",
        &[crate::control::CONTROL_LABEL.to_value(),
          crate::control::control_dc_options().to_value()],
    )?
    .get::<gst_webrtc::WebRTCDataChannel>()?;
wire_control_channel(&control_dc, &session, &tx);  // inbound bytes -> PeerEvent::Control
```
Extend `PeerEvent` (`peer.rs:13`) with a control variant carrying the raw bytes, e.g. `Control(Vec<u8>)`, and add a `wire_control_channel` that forwards `on-message-data` as `PeerEvent::Control(bytes)` (mirror of `wire_data_channel`). Because the robot already tags events with `SessionId` via the shared `(SessionId, PeerEvent)` channel, the loop knows which session a command came from.

> The robot is the offerer and creates the DC; it does **not** need to hold a sender for control (control flows up, telemetry flows down). Keep the `telemetry` send path exactly as Phase 5 left it.

- [ ] **Step 4: Client receives the `control` DC by label + can send**

In `vantage-signalling/src/peer.rs`: connect to `webrtcbin`'s `on-data-channel` signal (the answerer receives offerer-created channels). For each incoming DC, branch on `dc.property::<String>("label")`:
- `"telemetry"` → existing `wire_data_channel` (unchanged).
- `CONTROL_LABEL` → store the DC in a new `control_dc: Mutex<Option<WebRTCDataChannel>>` field on `Peer`.

Add `Peer::send_control(&self, msg: &ControlMsg) -> Result<()>` that encodes via `codec` and sends on the stored control DC (no-op with a debug log if the channel is not yet open). Confirm whether the client currently *creates* the telemetry DC or *receives* it; the recon shows the robot creates `telemetry`, so the client must already be on the receive side — extend that same `on-data-channel` handler rather than adding a new one.

- [ ] **Step 5: Minimal UI input → control sender (client)**

In `vantage-client/src/ui.rs`, wrap the video area in a Slint `FocusScope` and map arrow keys / WASD to a normalized `ControlMsg::Move { linear, angular }`, forwarding through a channel the session loop drains. Also send `ControlMsg::KeepAlive` on a 100 ms timer whenever connected (feeds the robot watchdog during idle holds). In **headless** mode (`VANTAGE_HEADLESS=1`), drive the same sender from an env/stdin stub so the harness can exercise control without a window.

> This is the *only* UI change — no new widgets, no layout change. Keep `UiSink` and the existing video/telemetry rendering untouched.

- [ ] **Step 6: Robot logs inbound control (no action yet)**

In `vantage-robot/src/main.rs`, handle `(session, PeerEvent::Control(bytes))` by decoding to `ControlMsg` and **logging only** (`debug!("control from {session}: {msg:?}")`). Do NOT move a motor, echo to ROS, or treat it as live — Task 3 adds the watchdog that gates real handling. This step exists to prove the channel round-trips.

- [ ] **Step 7: Verify the round-trip (headless)**

Extend the harness: one client sends a `ControlMsg::Move`; confirm the robot logs `control from <session>: Move { .. }`. Assert the SDP offer now advertises **two** data channels (grep the offer for the `control` m-line / sctp negotiation, or assert both DCs reach `on-data-channel` on the client). `cargo test --workspace` green (new `control` round-trip unit test).

- [ ] **Step 8: Commit**
```bash
git add vantage-protocol/src/control.rs vantage-protocol/src/lib.rs vantage-signalling/src/control.rs \
        vantage-signalling/src/robot_media.rs vantage-signalling/src/peer.rs \
        vantage-robot/src/main.rs vantage-client/src/session.rs vantage-client/src/ui.rs
git commit -m "feat(signalling): bidirectional control data channel (ControlMsg, unreliable/unordered)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Teleop disconnect failsafe/watchdog (the safety gate)

Implement the Failsafe specification above. Pure watchdog logic is unit-tested with an injected clock; the robot loop wires it to control receipt and disconnect. **Only after this lands does the robot act on (log-as-live/echo) a command.**

**Files:**
- Create: `vantage-robot/src/safety.rs`
- Modify: `vantage-robot/src/main.rs`

- [ ] **Step 1: Pure `Watchdog` + `SafeState` with injected clock**

```rust
// vantage-robot/src/safety.rs
use std::collections::HashMap;
use std::time::{Duration, Instant};
use vantage_protocol::SessionId;

pub const CONTROL_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, PartialEq)]
pub enum SafeState { Entered { reason: &'static str } }

#[derive(Default)]
pub struct Watchdog { last: HashMap<SessionId, Instant>, tripped: HashMap<SessionId, bool> }

impl Watchdog {
    pub fn arm(&mut self, s: SessionId, now: Instant) { self.last.insert(s.clone(), now); self.tripped.insert(s, false); }
    pub fn disarm(&mut self, s: &SessionId) { self.last.remove(s); self.tripped.remove(s); }
    /// A fresh control msg/keepalive: re-arm and clear any staleness trip.
    pub fn feed(&mut self, s: &SessionId, now: Instant) { if let Some(t) = self.last.get_mut(s) { *t = now; } self.tripped.insert(s.clone(), false); }
    /// Sessions newly stale this tick → drive to safe state (idempotent).
    pub fn tick(&mut self, now: Instant) -> Vec<(SessionId, SafeState)> {
        let mut out = vec![];
        for (s, &t) in &self.last {
            let stale = now.duration_since(t) > CONTROL_TIMEOUT;
            let was = *self.tripped.get(s).unwrap_or(&false);
            if stale && !was { out.push((s.clone(), SafeState::Entered { reason: "control stale" })); }
        }
        for (s, _) in &out { self.tripped.insert(s.clone(), true); }
        out
    }
    /// Whether commands for this session may currently be acted on.
    pub fn is_live(&self, s: &SessionId) -> bool { matches!(self.tripped.get(s), Some(false)) }
}
```
Unit tests (deterministic, `Instant`-injected): feed within timeout ⇒ stays live; no feed past `CONTROL_TIMEOUT` ⇒ `tick` returns one `Entered` then nothing on subsequent ticks (idempotent); `feed` after a trip clears it; `disarm` removes the session.

- [ ] **Step 2: Wire the watchdog into the robot loop**

In `vantage-robot/src/main.rs`:
- On `ClientConnected` / `add_consumer`: `watchdog.arm(session, now)`. The session is armed-but-live; commands may be acted on only while `watchdog.is_live(&session)`.
- On `(session, PeerEvent::Control(bytes))`: `watchdog.feed(&session, now)`, decode, and **now** act on it — for the PoC, log `acting on control <session>: <ControlMsg>` and update that session's last command. If `!watchdog.is_live(&session)` (mid-trip), drop the command and keep the neutral command (it will re-arm on the *next* fed message).
- On `ClientDisconnected` / DC-close: `watchdog.disarm(&session)` and log `safe-state entered: <session> (disconnect)`.
- Add a **watchdog tick** to the `tokio::select!` (e.g. `tokio::time::interval(100ms)`): `for (s, st) in watchdog.tick(now) { neutralize(s); warn!("safe-state entered: {s} ({st:?})"); }` where `neutralize` resets the session's command to neutral (zero). Emit a distinct, greppable `SafeState` log line.

> `Instant::now()` is fine in the robot binary (only *workflow scripts* forbid it; this is application code). The pure `safety.rs` takes the clock as a parameter so its tests stay deterministic.

- [ ] **Step 3: Verify the failsafe (headless) — both trip paths**

- **Staleness:** client connects, sends `Move` + 100 ms keepalives, then **stops sending** (without disconnecting). Assert the robot logs `acting on control` while fed, then `safe-state entered: <session> (control stale)` ~500 ms after the last keepalive, and stops acting. Resume keepalives ⇒ assert it goes live again (recovery).
- **Disconnect:** `SIGKILL` the client mid-command; assert `safe-state entered: <session> (disconnect)` and the consumer is torn down (Phase 5 teardown still holds; survivor unaffected).
- `cargo test --workspace` green (the `safety` unit tests + everything prior).

- [ ] **Step 4: Commit**
```bash
git add vantage-robot/src/safety.rs vantage-robot/src/main.rs
git commit -m "feat(robot): teleop disconnect watchdog → safe state (gates control before any command is acted on)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: mDNS LAN fast-path + offline direct connect

Advertise each robot over mDNS and let a client discover + connect directly when the coordinator is unreachable. Riskiest (new dependency + multicast environment), so it lands last; its absence does not regress the coordinator path.

**Files:**
- Create: `vantage-robot/src/discovery.rs`, `vantage-client/src/discovery.rs`
- Modify: `Cargo.toml` (workspace), `vantage-robot/Cargo.toml`, `vantage-client/Cargo.toml`, `vantage-robot/src/main.rs`, `vantage-client/src/session.rs`

- [ ] **Step 1: Add the `mdns-sd` dependency**

Add `mdns-sd = "0.11"` (or current) to `[workspace.dependencies]` and reference it from `vantage-robot` and `vantage-client`. Pure-Rust; no Avahi/Bonjour. `cargo build --workspace` to confirm it resolves on this toolchain.

- [ ] **Step 2: Robot advertises `_vantage._tcp` + runs a direct signalling listener**

In `vantage-robot/src/discovery.rs`:
- Register a `_vantage._tcp.local.` service with TXT records `id`, `name`, `caps` (from `RobotInfo`) and the port of a small **direct signalling listener** the robot binds on the LAN.
- The direct listener accepts **one** client over WebSocket and speaks the *same* `ClientMsg`/`ServerMsg`/`Signal` vocabulary the coordinator relays — but the robot brokers its own SDP/ICE for that single peer (it is offerer; reuse `RobotMedia::add_consumer` exactly as in the coordinator path). No registration/heartbeat/relay — just the per-peer signalling. Provide `IceServers` with only STUN (host candidates carry the LAN; TURN is irrelevant offline).
- Start advertising in `main.rs` after `RobotMedia::new`, regardless of coordinator reachability (the fast path is *additive*).

> Keep the direct listener tiny and single-purpose. It is the LAN twin of the coordinator's `robot_ws` signalling, minus registry/discovery. If sharing code with the coordinator's relay is clean, factor the per-peer signalling step; if not, a small duplicate is acceptable for one peer (Simplicity First — don't build a second coordinator).

- [ ] **Step 3: Client mDNS browse + coordinator-unreachable fallback**

In `vantage-client/src/discovery.rs`: browse `_vantage._tcp.local.`, collect `(RobotInfo, socket_addr)` from resolved services. In `vantage-client/src/session.rs`, change discovery to:
1. Try the coordinator WS (`CoordinatorWs::connect`). On success → existing path, unchanged.
2. On connect failure → browse mDNS for a bounded window (e.g. 2 s), present/select a robot, and connect to its **direct** signalling endpoint, then run the same `Peer` answerer flow.

> The selection UX is out of scope beyond "pick the first / env-named robot" for the PoC — the spec scenario only requires *discover and proceed to connect*. Record the chosen robot in the log for the harness.

- [ ] **Step 4: Verify (LAN / loopback; honest host-deferral if multicast blocked)**

- **Discovery unit test:** TXT-record encode (robot) → decode (client) yields the original `RobotInfo` + port. Runs without multicast.
- **Live fast path:** with the **coordinator not started** (or killed), start the robot and one client on the same host/LAN; assert the client logs `mDNS: discovered <robot id> at <addr>`, connects directly, and decodes video — *with no coordinator running*. If the sandbox blocks multicast, record this scenario **host-deferred** (structure + unit test pass; live browse pending a multicast-capable host) — do not claim it passed.

- [ ] **Step 5: Commit**
```bash
git add Cargo.toml vantage-robot/Cargo.toml vantage-client/Cargo.toml \
        vantage-robot/src/discovery.rs vantage-client/src/discovery.rs \
        vantage-robot/src/main.rs vantage-client/src/session.rs
git commit -m "feat(discovery): mDNS LAN fast-path + offline direct connect (coordinator-optional)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Integration evidence

Prove the Phase 6 exit criteria end to end and capture the note.

**Files:**
- Create: `docs/superpowers/plans/notes/2026-06-30-phase6-exit.md`

- [ ] **Step 1: Run the full scenario** — coordinator + robot + two clients: `/stats` shows `1/2`; SIGKILL one client → `/stats` reconciles to `1/1`; one client sends `ControlMsg::Move` → robot logs `acting on control`; stop its keepalives → `safe-state entered (control stale)` ~500 ms later, then recovery on resume; SIGKILL the controlling client → `safe-state entered (disconnect)` + survivor unaffected.

- [ ] **Step 2: mDNS fast path** — coordinator **stopped**; robot + client on the LAN; client discovers via mDNS and connects directly and decodes video. Record host-deferred if multicast is unavailable in the sandbox (with the unit-test proof of TXT encode/decode).

- [ ] **Step 3: Record exit evidence** — create `docs/superpowers/plans/notes/2026-06-30-phase6-exit.md` modelled on the 4a/4b/5 notes: `cargo test --workspace` result, the `/stats` reconciliation transcript, the watchdog trip+recovery timeline (with measured trip latency vs the 500 ms target), the control round-trip line, and the mDNS result (verified or host-deferred with the multicast caveat). State the `(Later)` ROS-joint-state/bincode bullet as **not** attempted.

- [ ] **Step 4: Commit**
```bash
git add docs/superpowers/plans/notes/2026-06-30-phase6-exit.md
git commit -m "test: phase 6 exit evidence (fleet stats reconciliation, control channel, watchdog, mDNS)"
```

---

## Phase 6 exit criteria (gate)

- [ ] `cargo test --workspace` green (new `fleet`, `control`, and `safety` tests + existing suites).
- [ ] **Fleet stats:** `/stats` reports `providers_online`/`consumers_connected` derived from `Registry`/`Sessions`; an **ungraceful** client kill reconciles the consumer count downward; a robot kill drives providers to 0 and closes orphaned sessions.
- [ ] **Control channel:** a `control` DC is negotiated **in the same SDP** as `telemetry` (no renegotiation), unreliable/unordered; `ControlMsg` round-trips client→robot.
- [ ] **Failsafe (gate):** the robot acts on a command **only** while its watchdog is live; it enters safe state on control staleness (~500 ms) **and** on disconnect, recovers from a staleness stall when commands resume, and the safe-state command is neutral. Verified before any command is treated as live.
- [ ] **mDNS fast path:** with the coordinator down, a LAN client discovers a robot via mDNS and connects directly (or host-deferred with the multicast caveat + passing TXT encode/decode unit test).

Once green, the PoC's §6 surface is complete; the remaining `(Later)` work (ROS joint-state telemetry + `bincode` codec swap) and the standing 4b/5 carry-overs (live-camera-on-hardware, `rtpgccbwe` adaptive-bitrate scenario, Docker/CI two-lane harness) are the candidate backlog for a Phase 7 / hardening pass.

---

## Self-review

**Spec coverage:** every `tasks.md` §6 bullet maps to a task — fleet stats → Task 1; mDNS LAN fast path → Task 4; reserve+wire control channel → Task 2; failsafe/watchdog before control is acted on → Task 3 — and each is verified in Task 5. The `(Later)` ROS-joint-state/bincode bullet is explicitly out of scope, not silently dropped. fleet-management's "session-derived / ungraceful disconnect", discovery's "coordinator unreachable on a LAN", and telemetry's "bidirectional from day one" + "reliability matched to data type" + "shared message types" each map to a concrete step.

**Safety ordering is explicit (the §6 emphasis):** Task 2 wires the channel but the robot only *logs* receipt; Task 3 adds the watchdog and is the first point the robot *acts on* a command, and only while `watchdog.is_live`. The Failsafe specification is written before the code, the trip is proven (staleness + disconnect + recovery) before Task 5 exercises live commands, and the safe-state output is a neutral *command* (testable) rather than a hidden side effect — so the gate is demonstrable, not assumed.

**Architecture honesty / grounding:** the central claims are grounded in the current tree — `Registry::len()`/`Sessions::consumer_count()` already exist (`registry.rs`, `sessions.rs`), the 5 s pruner already runs (`coordinator/src/main.rs`), the robot is already the offerer creating the `telemetry` DC (`robot_media.rs:232`), and the session-tagged `(SessionId, PeerEvent)` loop already exists (`vantage-robot/src/main.rs`). The only new coordinator *behaviour* is reconciliation-on-drop (Task 1 Step 2), flagged to verify-before-editing in case it is already present.

**Known-uncertain points (surfaced, not hidden):**
1. `create-data-channel` reliability-options Structure field names vary by `gstreamer-webrtc` version — Task 2 Step 2 isolates them in one helper with a documented reliable-ordered fallback (and the watchdog, not retransmission, is the safety guarantee).
2. Whether the coordinator already reconciles sessions on ungraceful drop — Task 1 Step 2 says verify first and record a no-op if so (Surgical Changes).
3. mDNS multicast may be blocked in the build sandbox — Task 4/5 keep the structure unit-tested and mark the live browse host-deferred, mirroring Phase 5's `rtpgccbwe` handling.
4. Offline direct-connect duplicating vs sharing the coordinator's per-peer signalling — Task 4 Step 2 prefers a small single-peer listener over building a second coordinator (Simplicity First).

**Type/interface consistency:** `FleetStats` (protocol) is produced by `/stats` and consumed by the harness; `ControlMsg`/`CONTROL_LABEL` (protocol) are produced by the client UI and consumed by the robot, coordinator-blind (telemetry spec "shared message types"); `PeerEvent::Control(Vec<u8>)` is emitted in `robot_media.rs`/`peer.rs` and matched in `vantage-robot/src/main.rs`; `safety::Watchdog::{arm,feed,disarm,tick,is_live}` is defined in Task 3 Step 1 and consumed in Step 2 with matching signatures. `RobotMedia::add_consumer`, the `(SessionId, PeerEvent)` channel, `Sessions`, and `Registry` are reused unchanged from Phase 5.
