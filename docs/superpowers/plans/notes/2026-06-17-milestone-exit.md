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

## Not yet verified in this environment

- **TURN relay path** (plan Task 12 step 3): requires a TURN server. `coturn` is not
  installed here. The STUN/TURN URL normalization is fixed and unit-safe, but the
  forced-relay end-to-end run is still pending a TURN server.
  To run it: install `coturn`, start it with static creds, launch the coordinator with
  `VANTAGE_TURN_URL/USER/PASS`, and force relay (e.g. `ice-transport-policy=relay`
  gated behind an env flag, or firewall-drop host/srflx candidates).
- **srflx candidate over public STUN**: not explicitly captured (loopback run doesn't
  need it); worth confirming on a real NAT once relay infra is up.
