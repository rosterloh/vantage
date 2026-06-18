# TURN / STUN setup

WebRTC tries to connect peers directly. When both peers are behind NAT/firewalls
with no reachable direct path, media and the data channel must be **relayed** through
a TURN server. STUN (cheap, used first) lets a peer discover its own public
address; TURN (more expensive, last resort) relays traffic when nothing else works.

For the PoC, STUN is a public Google server and TURN is either the **metered.ca**
free tier (fastest to start) or a **self-hosted coturn** on a VPS (full control).

## How Vantage consumes TURN config

The coordinator reads TURN settings from three environment variables and serves the
combined STUN+TURN list to every peer over `GET /ice`:

| Env var             | Required | Example                               |
|---------------------|----------|---------------------------------------|
| `VANTAGE_TURN_URL`  | yes      | `turn:standard.relay.metered.ca:80`   |
| `VANTAGE_TURN_USER` | yes      | `your-metered-username`               |
| `VANTAGE_TURN_PASS` | yes      | `your-metered-credential`             |

A peer fetches `/ice`, and `vantage-signalling` normalizes each entry into the form
`webrtcbin`/libnice require:

- `stun:host:port`  → `stun://host:port`
- `turn:host:port` (+ user/pass) → `turn://user:pass@host:port`

### Current limitations (PoC) — relevant when choosing a metered URL

1. **One TURN URL.** `from_env` accepts a single `VANTAGE_TURN_URL`. Metered's
   dashboard lists several (ports 80, 443, TLS). Pick **one plain `turn:` URL**
   (e.g. `turn:...:80` or `turn:...:443`).
2. **No TLS TURN (`turns:`).** The URL normalizer handles `stun:` and `turn:` only,
   not `turns:` (TURN-over-TLS). Use a non-TLS `turn:` URL for now. Adding `turns:`
   is a small follow-up in `vantage-signalling/src/peer.rs` if a TLS relay is needed
   to punch through restrictive corporate firewalls.
3. **Static credentials only.** Ephemeral/time-limited credentials (metered's
   HMAC/REST API, or coturn's `use-auth-secret`) are deferred per the spec.
4. **Credentials are passed as a `turn://user:pass@host` URI.** If a password
   contains `@`, `:`, or `/`, that URI breaks. Metered's *static* dashboard
   credentials are alphanumeric and fine; metered's *HMAC/API* credentials are
   base64 (can contain `/ + =`) and would need percent-encoding or a non-URI API —
   another reason the static-credential path is the simple one for the PoC.

### The coordinator loads `.env`
`vantage-coordinator` calls `dotenvy::dotenv()` at startup, so a `.env` in the
working directory is picked up automatically (the real process environment still
overrides it). See `.env.example` for the keys. `.env` is gitignored — never commit
real credentials. The robot and client read `VANTAGE_COORDINATOR` from the process
environment (export it or prefix the command); they don't need the TURN vars (they
fetch ICE config from the coordinator's `/ice`).

`vantage-protocol::IceServer` already models multiple `urls` and the `/ice` payload
is a JSON array, so lifting limitation 1 (and adding more STUN/TURN entries) is a
coordinator-only change when you want it.

---

## Option A — metered.ca (free 20 GB/month tier)

Metered gives you credentials two ways. **Use the static pair** — it works with the
current code and needs no API key:

- **Static username/password (use this).** Your metered app dashboard shows a fixed
  `username` + `credential` plus a list of ICE server URLs. These plug straight into
  the `VANTAGE_TURN_*` vars.
- **API key + REST endpoint (alternative, not implemented).** Metered also exposes
  `https://<yourapp>.metered.live/api/v1/turn/credentials?apiKey=<API_KEY>`, which
  returns an `iceServers` array of (often rotating) credentials. The right way to use
  this is to give the **coordinator** the API key and have it fetch + serve those over
  `/ice` (keeps the key server-side). That's a small future enhancement; you do **not**
  need it if you have the static pair.

### Setup with static credentials

1. From the dashboard, copy your `username`, `credential`, and pick **one plain
   `turn:` URL** (e.g. `turn:standard.relay.metered.ca:443`). Do **not** use the
   `turns:...` (TLS) URL — see limitation 2. Example URLs metered lists:
   - `stun:stun.relay.metered.ca:80`
   - `turn:standard.relay.metered.ca:80`
   - `turn:standard.relay.metered.ca:443`
   - `turns:standard.relay.metered.ca:443?transport=tcp`  ← do **not** use (TLS)
2. Put them in `.env` at the repo root (gitignored; see `.env.example`):
   ```bash
   VANTAGE_TURN_URL=turn:standard.relay.metered.ca:443
   VANTAGE_TURN_USER=<username-from-dashboard>
   VANTAGE_TURN_PASS=<credential-from-dashboard>
   ```
3. Run the coordinator (it loads `.env` automatically):
   ```bash
   RUST_LOG=info cargo run -p vantage-coordinator
   ```
   `curl -s localhost:8080/ice` should now show both the STUN entry and your TURN
   entry (with `username`/`credential`).

---

## Option B — self-hosted coturn on a VPS

Full control, no third-party dependency. Assumes a VPS with a **public IP** and root.

### 1. Install

```bash
sudo apt-get update && sudo apt-get install -y coturn
```

### 2. Open firewall ports

TURN needs the signalling port plus a UDP range for relay allocations:

| Port / range        | Proto    | Purpose                         |
|---------------------|----------|---------------------------------|
| `3478`              | UDP+TCP  | STUN/TURN listening port        |
| `49152–65535`       | UDP      | Relay allocation range          |
| `5349`              | TCP      | (optional) TLS/DTLS listener    |

Cloud security groups **and** any host firewall (`ufw`) must allow these:

```bash
sudo ufw allow 3478/tcp
sudo ufw allow 3478/udp
sudo ufw allow 49152:65535/udp
```

### 3. Configure `/etc/turnserver.conf`

Static long-term credentials (matches what Vantage sends):

```ini
# Listening
listening-port=3478
fingerprint
lt-cred-mech

# A single static user. The realm is required for long-term credentials.
realm=vantage.example.com
user=vantage:CHANGE_ME_STRONG_PASSWORD

# Relay address. On most VPSes the public IP is bound directly:
external-ip=YOUR.PUBLIC.IP.ADDR
# If the box is behind 1:1 NAT (e.g. some clouds), use PUBLIC/PRIVATE:
# external-ip=YOUR.PUBLIC.IP/YOUR.PRIVATE.IP

# Relay port range (must match the firewall range above)
min-port=49152
max-port=65535

# PoC: no TLS (Vantage uses plain turn: — see limitation 2)
no-tls
no-dtls

# Hygiene
no-cli
no-multicast-peers
# Optional: block relaying to private ranges from the internet
denied-peer-ip=10.0.0.0-10.255.255.255
denied-peer-ip=192.168.0.0-192.168.255.255
denied-peer-ip=172.16.0.0-172.31.255.255
```

Enable the service:

```bash
# Debian ships coturn disabled by default
echo 'TURNSERVER_ENABLED=1' | sudo tee /etc/default/coturn
sudo systemctl enable --now coturn
sudo systemctl status coturn --no-pager
```

### 4. Point Vantage at it

```bash
export VANTAGE_TURN_URL=turn:YOUR.PUBLIC.IP.ADDR:3478
export VANTAGE_TURN_USER=vantage
export VANTAGE_TURN_PASS=CHANGE_ME_STRONG_PASSWORD
RUST_LOG=info cargo run -p vantage-coordinator
```

### 5. Verify the TURN server independently

From any machine (coturn ships `turnutils_uclient`):

```bash
turnutils_uclient -v -u vantage -w CHANGE_ME_STRONG_PASSWORD -p 3478 YOUR.PUBLIC.IP.ADDR
```

A successful run allocates a relay address and exchanges test packets. You can also
paste the `turn://` URL + creds into the WebRTC samples
[Trickle ICE](https://webrtc.github.io/samples/src/content/peerconnection/trickle-ice/)
page and confirm a `relay` candidate appears.

---

## Forcing the relay path in an end-to-end test

ICE prefers direct paths, so on a LAN it will never pick the relay on its own. To
prove the relay actually works, force `relay`-only ICE (see the foundation plan,
Task 12, step 3): gate `ice-transport-policy=relay` on `webrtcbin` behind an env
flag (e.g. `VANTAGE_FORCE_RELAY=1`), or firewall-drop host/srflx candidates. Then run
coordinator + robot + client with the TURN env set and confirm telemetry still
flows and the selected candidate pair is `relay`.
