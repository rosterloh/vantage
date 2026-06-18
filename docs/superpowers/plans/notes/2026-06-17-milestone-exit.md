# Foundation Milestone — Exit Evidence (2026-06-17)

Plan: `docs/superpowers/plans/2026-06-17-vantage-poc-foundation.md`
Branch: `vantage-poc-foundation`. GStreamer 1.28.2 (system).

## Automated tests — `cargo test --workspace`

16 tests, all passing:
- `vantage-protocol`: 6 (telemetry round-trip, signalling round-trips ×3, codec ×2)
- `vantage-coordinator`: 7 (registry TTL expiry ×4, sessions ×3)
- `vantage-signalling`: 1 (offerer peer constructs — exercises `gst::init`, `webrtcbin`, data-channel creation, pipeline PLAYING)
- `vantage-robot`: 1 (sysinfo sampler reports nonzero total memory)
- `vantage-client`: 0 (glue; verified live below)

## Coordinator signalling — live WebSocket smoke (Node)

All passed against a running coordinator:
- discovery: client lists a registered robot
- relay: robot receives `client_connected`; client receives `connected`
- relay: client receives the robot's offer (`from: null`)
- relay: robot receives the client's answer, tagged with the client's session id (`from: Some(session)`)
- lifecycle: robot receives `client_disconnected` on client close
- lifecycle: robot removed from the discovery list after it disconnects

## End-to-end — coordinator + robot + client (direct/host path)

Single machine, ICE host candidates over loopback. Observed:

```
CLIENT: connecting to Atlas (robot-1)
CLIENT: data channel open
CLIENT: telemetry: cpu=7.7% mem=14574/31798MB temps=26 uptime=2789408s
ROBOT:  registered as robot-1
ROBOT:  client connected: sess-18b9fa78e33a4dc3
ROBOT:  data channel open
```

This proves end-to-end: discovery → WebRTC peer connection → bidirectional data
channel → shared `DeviceInfo` types serialized on the robot and deserialized on the
client (telemetry "Device telemetry over the data channel" + "Shared message types").

### Bug found and fixed during integration
`webrtcbin`/libnice rejected the ICE config's STUN URL:
`Stun server 'stun:stun.l.google.com:19302' has no host, must be of the form stun://<host>:<port>`.
The connection still succeeded on loopback (host candidates), but STUN was dead —
no server-reflexive candidates, i.e. the NAT-traversal tier was broken. Fixed in
`vantage-signalling/src/peer.rs` to normalize `stun:`→`stun://` and `turn:`→`turn://`
(commit `4e539d2`). Re-ran: STUN error gone, telemetry still flows.

## TURN relay path — verified (2026-06-18)

Ran forced-relay end-to-end against the **metered.ca** free tier (static
username/password supplied via `.env`, served over `/ice`). Both peers launched with
`VANTAGE_FORCE_RELAY=1`, which sets `webrtcbin`'s `ice-transport-policy=relay` so host
and server-reflexive candidates are excluded. Observed:

```
robot:  registered as robot-1
robot:  VANTAGE_FORCE_RELAY set — ICE restricted to relay candidates
robot:  client connected: sess-18ba1855bde322b7
robot:  data channel open
client: connecting to Atlas (robot-1)
client: VANTAGE_FORCE_RELAY set — ICE restricted to relay candidates
client: data channel open
client: telemetry: cpu=6.1% mem=15942/31798MB temps=26 uptime=2822192s
```

No TURN/ICE auth errors (a bad credential would fail the allocation with 401).
Because relay-only policy excludes every direct candidate, an established data
channel is conclusive proof media traversed the metered TURN relay — and that the
metered credentials are valid. This closes plan Task 12 step 3.

The relay toggle is `VANTAGE_FORCE_RELAY` (read in `vantage-signalling/src/peer.rs`).

## Still worth doing later (non-blocking)

- **srflx candidate over public STUN on a real NAT** (the middle ICE tier): not
  explicitly captured; the loopback/relay runs don't exercise it.
- **TURN-over-TLS (`turns:`)** support in the URL normalizer, for restrictive
  firewalls. Currently only `stun:`/`turn:` are handled.
