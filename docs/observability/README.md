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
