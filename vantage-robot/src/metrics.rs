//! OpenTelemetry metrics for the robot. Instruments are no-ops until a global
//! meter provider is installed (i.e. when OTLP export is configured), so this is
//! safe to construct and call unconditionally.

use vantage_observability::opentelemetry::KeyValue;
use vantage_observability::opentelemetry::global;
use vantage_observability::opentelemetry::metrics::{Counter, Gauge};
use vantage_protocol::telemetry::DeviceInfo;

/// Cloneable handle to the robot's metric instruments (each wraps an `Arc`).
#[derive(Clone)]
pub struct RobotMetrics {
    cpu_percent: Gauge<f64>,
    mem_used_mb: Gauge<u64>,
    mem_total_mb: Gauge<u64>,
    uptime_s: Gauge<u64>,
    temperature: Gauge<f64>,
    frames_published: Counter<u64>,
}

impl RobotMetrics {
    pub fn new() -> Self {
        let m = global::meter("vantage-robot");
        Self {
            cpu_percent: m.f64_gauge("vantage.robot.cpu_percent").with_unit("percent").build(),
            mem_used_mb: m.u64_gauge("vantage.robot.mem_used_mb").with_unit("MB").build(),
            mem_total_mb: m.u64_gauge("vantage.robot.mem_total_mb").with_unit("MB").build(),
            uptime_s: m.u64_gauge("vantage.robot.uptime_s").with_unit("s").build(),
            temperature: m
                .f64_gauge("vantage.robot.temperature_celsius")
                .with_unit("Cel")
                .build(),
            frames_published: m
                .u64_counter("vantage.robot.frames_published")
                .with_description("Raw camera frames drained from the capture engine")
                .build(),
        }
    }

    /// Publish the latest device sample as gauge values.
    pub fn record_device(&self, info: &DeviceInfo) {
        self.cpu_percent.record(info.cpu_percent as f64, &[]);
        self.mem_used_mb.record(info.mem_used_mb, &[]);
        self.mem_total_mb.record(info.mem_total_mb, &[]);
        self.uptime_s.record(info.uptime_s, &[]);
        for t in &info.temps {
            self.temperature.record(t.celsius as f64, &[KeyValue::new("sensor", t.label.clone())]);
        }
    }

    pub fn frame_published(&self) {
        self.frames_published.add(1, &[]);
    }
}
