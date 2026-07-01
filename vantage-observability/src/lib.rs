//! OpenTelemetry wiring shared by every Vantage binary.
//!
//! [`init`] sets up the process-wide `tracing` subscriber. When
//! `OTEL_EXPORTER_OTLP_ENDPOINT` is set it additionally exports logs, metrics,
//! and traces over OTLP/gRPC (default port 4317) to a collector — Grafana
//! Alloy, the OpenTelemetry Collector, or a Grafana Cloud OTLP endpoint. When
//! the variable is unset it falls back to the previous behaviour: plain
//! `RUST_LOG`-filtered logging on stderr, with zero export machinery started.
//!
//! Standard OTLP environment variables apply, e.g. `OTEL_EXPORTER_OTLP_HEADERS`
//! (auth for Grafana Cloud) and `OTEL_SERVICE_NAME` (overrides the name passed
//! to [`init`]).
//!
//! The returned [`OtelGuard`] must be kept alive for the whole process; dropping
//! it flushes and shuts down the exporters.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::Layer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

/// Re-export so binaries record metrics against the exact same `opentelemetry`
/// version this crate links, without adding their own (drift-prone) dependency.
pub use opentelemetry;

/// Keeps the OTLP providers alive and flushes them on drop. Bind it in `main`.
pub struct OtelGuard {
    providers: Option<Providers>,
}

struct Providers {
    tracer: SdkTracerProvider,
    logger: SdkLoggerProvider,
    meter: SdkMeterProvider,
    /// Dedicated runtime that drives the tonic gRPC export IO. Bound to the
    /// exporters at build time (via `enter()`) and kept alive here so the
    /// background batch/periodic exporters always have a reactor to run on —
    /// this is what lets the non-async client binary export too.
    runtime: tokio::runtime::Runtime,
}

/// Initialise logging and, if configured, OpenTelemetry export. Call once, early
/// in `main`, and hold the returned guard until the process exits.
pub fn init(service_name: &str) -> OtelGuard {
    match otlp_endpoint() {
        Some(endpoint) => match init_otlp(service_name, &endpoint) {
            Ok(guard) => guard,
            Err(e) => {
                // Setup failed before the subscriber was installed — fall back to
                // stderr so the operator still gets logs (and hears about it).
                init_fmt();
                tracing::error!(error = %e, %endpoint, "OTLP setup failed; using stderr logging");
                OtelGuard { providers: None }
            }
        },
        None => {
            init_fmt();
            OtelGuard { providers: None }
        }
    }
}

fn otlp_endpoint() -> Option<String> {
    parse_endpoint(std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
}

/// A blank or whitespace-only endpoint counts as "not configured".
fn parse_endpoint(raw: Option<String>) -> Option<String> {
    raw.filter(|v| !v.trim().is_empty())
}

/// Plain stderr logging, matching the pre-OpenTelemetry behaviour.
fn init_fmt() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}

fn init_otlp(service_name: &str, endpoint: &str) -> Result<OtelGuard, Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .thread_name("otel-export")
        .build()?;
    let _enter = runtime.enter();

    let resource = Resource::builder().with_service_name(service_name.to_string()).build();

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter)
        .build();

    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(log_exporter)
        .build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(metric_exporter)
        .build();

    // Publish globally so `global::meter(..)` and any library instrumentation
    // route to our providers.
    opentelemetry::global::set_meter_provider(meter_provider.clone());
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    let trace_layer = tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("vantage"));
    let log_layer = OpenTelemetryTracingBridge::new(&logger_provider);

    tracing_subscriber::registry()
        // Unchanged stderr behaviour: still governed by RUST_LOG.
        .with(tracing_subscriber::fmt::layer().with_filter(EnvFilter::from_default_env()))
        // Export layers get their own filter so they work even without RUST_LOG,
        // and so the exporter's own transport chatter never feeds back into it.
        .with(trace_layer.with_filter(otel_filter()))
        .with(log_layer.with_filter(otel_filter()))
        .init();

    tracing::info!(%endpoint, service = service_name, "opentelemetry export enabled (otlp/grpc)");

    Ok(OtelGuard {
        providers: Some(Providers { tracer: tracer_provider, logger: logger_provider, meter: meter_provider, runtime }),
    })
}

/// Filter for the export layers: honour RUST_LOG (default `info`) but silence the
/// exporter's own transport stack and the SDK's internal logs, so an export
/// error can never feed back through the log bridge into another export. (These
/// targets still reach the stderr layer, which has no such filter.)
fn otel_filter() -> EnvFilter {
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
        .add_directive("h2=off".parse().unwrap())
        .add_directive("tonic=off".parse().unwrap())
        .add_directive("hyper=off".parse().unwrap())
        .add_directive("tower=off".parse().unwrap())
        .add_directive("reqwest=off".parse().unwrap())
        .add_directive("opentelemetry=off".parse().unwrap())
        .add_directive("opentelemetry_sdk=off".parse().unwrap())
        .add_directive("opentelemetry_otlp=off".parse().unwrap())
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        let Some(p) = self.providers.take() else { return };
        // Flush in the documented order (tracer, logger, meter) while the
        // export runtime is still alive, then tear the runtime down without
        // blocking — a blocking drop would panic when `main` is itself async.
        let _ = p.tracer.shutdown();
        let _ = p.logger.shutdown();
        let _ = p.meter.shutdown();
        p.runtime.shutdown_background();
    }
}

#[cfg(test)]
mod tests {
    use super::parse_endpoint;

    #[test]
    fn blank_endpoint_is_disabled() {
        assert_eq!(parse_endpoint(None), None);
        assert_eq!(parse_endpoint(Some(String::new())), None);
        assert_eq!(parse_endpoint(Some("   ".into())), None);
    }

    #[test]
    fn real_endpoint_is_kept() {
        assert_eq!(
            parse_endpoint(Some("http://localhost:4317".into())),
            Some("http://localhost:4317".into())
        );
    }
}
