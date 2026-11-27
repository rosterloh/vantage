# Observability — Grafana Cloud

Example configs for shipping Vantage's OpenTelemetry logs, metrics, and traces to
Grafana Cloud and viewing them.

## Files

| File | What it is |
|------|------------|
| [`otel-collector-grafana-cloud.yaml`](otel-collector-grafana-cloud.yaml) | OpenTelemetry Collector config: OTLP in (from the Vantage apps) → Grafana Cloud's unified OTLP gateway out. |
| [`grafana-dashboard.json`](grafana-dashboard.json) | Importable Grafana dashboard for the emitted metrics. |

## Wiring

```
vantage-{coordinator,robot,client}  --OTLP/gRPC :4317-->  Collector  --OTLP/HTTP-->  Grafana Cloud
```

1. Start the collector with your Grafana Cloud credentials (see the header of the
   YAML for the three env vars and a `docker run` one-liner).
2. Run the Vantage binaries with `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317`
   (see the repo README's *Observability* section).
3. In Grafana, **Dashboards → New → Import**, upload `grafana-dashboard.json`, and
   pick your Grafana Cloud Prometheus data source when prompted.

You can also skip the collector and set `OTEL_EXPORTER_OTLP_ENDPOINT` /
`OTEL_EXPORTER_OTLP_HEADERS` on the apps to point straight at the Grafana Cloud
OTLP gateway — the collector is preferred because it batches, adds host resource
attributes, and keeps the gateway token out of every robot.

## A note on metric names

Grafana Cloud normalizes OTLP metric names for Prometheus: dots become
underscores and monotonic counters gain a `_total` suffix. The dashboard queries
already use the normalized names, e.g.:

| OTLP instrument | Dashboard query |
|-----------------|-----------------|
| `vantage.robot.cpu_percent` (gauge) | `vantage_robot_cpu_percent` |
| `vantage.robot.frames_published` (counter) | `vantage_robot_frames_published_total` |
| `vantage.coordinator.robots_online` (gauge) | `vantage_coordinator_robots_online` |

If your stack is configured to append OTLP **unit** suffixes as well, names may
differ (e.g. `..._percent`, `..._mb`); adjust the panel queries or disable that
option on the OTLP endpoint.

## Connection and stream metrics

Each client dial reports when it reached each checkpoint, in ms from dial start:
the robot's offer, the client's answer, ICE connected, DTLS connected, data
channel open (`ready`) and the first decoded video frame. The client sends these
to the coordinator in its `DialReport` (`timings`), and both sides record:

| Instrument | Attributes | Meaning |
|------------|------------|---------|
| `vantage.{client,coordinator}.dial_stage_duration` (histogram, ms) | `stage` (+ `path` on the client) | Time from the previous checkpoint to this one |
| `vantage.{client,coordinator}.time_to_first_frame` (histogram, ms) | `path` on the client | Dial start to first decoded frame |

A slow `offer` stage points at the coordinator or robot; slow `ice`/`dtls` at the
network path. The coordinator only sees dials that went through it, while the
client's own metrics also include LAN (`path=lan`) dials. A dial waits up to 5 s
for the first frame before it reports.

While streaming, the client samples webrtcbin stats every 2 s and the robot
samples each viewer every second:

| Instrument | Meaning |
|------------|---------|
| `vantage.client.video_bitrate` (gauge, kbit/s) | Received video bitrate |
| `vantage.client.video_packets_lost` (counter) | Lost video RTP packets |
| `vantage.client.video_jitter` (gauge, ms) | Inbound RTP jitter |
| `vantage.robot.stream_rtt` (histogram, ms, `path`) | RTT from RTCP receiver reports |
| `vantage.robot.stream_estimated_bitrate` (histogram, kbit/s, `path`) | GCC bandwidth estimate (only with `rtpgccbwe`) |

To check that signalling delay shows up where expected, add delay to the
coordinator's port and dial again:

```bash
mise run netem -- on 250   # +250 ms each way on TCP 8080 via lo (sudo)
mise run client            # offer stage grows ~250 ms per signalling hop
mise run netem -- off
```
