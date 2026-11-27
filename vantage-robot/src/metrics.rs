//! OpenTelemetry metrics for the robot. Instruments are no-ops until a global
//! meter provider is installed (i.e. when OTLP export is configured), so this is
//! safe to construct and call unconditionally.

use vantage_observability::opentelemetry::KeyValue;
use vantage_observability::opentelemetry::global;
use vantage_observability::opentelemetry::metrics::{Counter, Gauge, Histogram};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_signalling::robot_media::Consumer;

/// Cloneable handle to the robot's metric instruments (each wraps an `Arc`).
#[derive(Clone)]
pub struct RobotMetrics {
    cpu_percent: Gauge<f64>,
    mem_used_mb: Gauge<u64>,
    mem_total_mb: Gauge<u64>,
    uptime_s: Gauge<u64>,
    temperature: Gauge<f64>,
    frames_published: Counter<u64>,
    stream_rtt: Histogram<f64>,
    stream_estimated_bitrate: Histogram<u64>,
}

impl RobotMetrics {
    pub fn new() -> Self {
        let m = global::meter("vantage-robot");
        Self {
            cpu_percent: m
                .f64_gauge("vantage.robot.cpu_percent")
                .with_unit("percent")
                .build(),
            mem_used_mb: m
                .u64_gauge("vantage.robot.mem_used_mb")
                .with_unit("MB")
                .build(),
            mem_total_mb: m
                .u64_gauge("vantage.robot.mem_total_mb")
                .with_unit("MB")
                .build(),
            uptime_s: m.u64_gauge("vantage.robot.uptime_s").with_unit("s").build(),
            temperature: m
                .f64_gauge("vantage.robot.temperature_celsius")
                .with_unit("Cel")
                .build(),
            frames_published: m
                .u64_counter("vantage.robot.frames_published")
                .with_description("Raw camera frames drained from the capture engine")
                .build(),
            stream_rtt: m
                .f64_histogram("vantage.robot.stream_rtt")
                .with_unit("ms")
                .with_description("Video RTT per viewer, from RTCP receiver reports")
                .build(),
            stream_estimated_bitrate: m
                .u64_histogram("vantage.robot.stream_estimated_bitrate")
                .with_unit("kbit/s")
                .with_description("GCC bandwidth estimate per viewer")
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
            self.temperature.record(
                t.celsius as f64,
                &[KeyValue::new("sensor", t.label.clone())],
            );
        }
    }

    /// Sample one viewer's stream. `path` is `coordinator` or `lan`. Never waits:
    /// the RTT lands when webrtcbin answers.
    pub fn record_stream(&self, consumer: &Consumer, path: &'static str) {
        let attrs = [KeyValue::new("path", path)];
        if let Some(bps) = consumer.estimated_bitrate() {
            self.stream_estimated_bitrate
                .record(bps as u64 / 1000, &attrs);
        }
        let rtt = self.stream_rtt.clone();
        consumer.sample_rtp_stats(move |s| {
            if let Some(r) = s.rtt_s {
                rtt.record(r * 1000.0, &attrs);
            }
        });
    }

    pub fn frame_published(&self) {
        self.frames_published.add(1, &[]);
    }
}
