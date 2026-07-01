use std::sync::Arc;

use slint::{Image, Rgba8Pixel, SharedPixelBuffer, SharedString, Weak};
use tokio::sync::mpsc;
use vantage_protocol::ControlMsg;
use vantage_protocol::telemetry::DeviceInfo;
use vantage_signalling::peer::VideoFrame;

use crate::session::UiSink;

slint::slint! {
    import { VerticalBox, HorizontalBox } from "std-widgets.slint";
    export component AppWindow inherits Window {
        in property <image> video-frame;
        in property <string> telemetry-text: "waiting for telemetry…";
        in property <string> status-text: "starting…";
        // Teleop key events, forwarded to Rust for mapping (WASD → Move, release → stop).
        callback key-pressed-text(string);
        callback key-released-text(string);
        title: "Vantage";
        preferred-width: 960px;
        preferred-height: 540px;
        forward-focus: input;
        input := FocusScope {
            key-pressed(event) => { root.key-pressed-text(event.text); accept }
            key-released(event) => { root.key-released-text(event.text); accept }
            HorizontalBox {
                Image {
                    source: root.video-frame;
                    horizontal-stretch: 7;
                    image-fit: contain;
                }
                VerticalBox {
                    horizontal-stretch: 3;
                    Text { text: root.status-text; font-size: 14px; }
                    Text { text: root.telemetry-text; font-size: 13px; wrap: word-wrap; }
                }
            }
        }
    }
}

/// Map a pressed WASD key to a normalized teleop command; other keys ⇒ None.
fn key_to_move(text: &str) -> Option<ControlMsg> {
    let (linear, angular) = match text {
        "w" | "W" => (1.0, 0.0),
        "s" | "S" => (-1.0, 0.0),
        "a" | "A" => (0.0, -1.0),
        "d" | "D" => (0.0, 1.0),
        _ => return None,
    };
    Some(ControlMsg::Move { linear, angular })
}

/// Wire keyboard teleop input and a 100 ms keepalive onto the control channel.
/// A key press sends a directional Move; release sends a neutral stop; the
/// keepalive beat keeps the robot watchdog armed during an idle hold.
pub fn wire_control_input(ui: &AppWindow, control_tx: mpsc::UnboundedSender<ControlMsg>) {
    {
        let tx = control_tx.clone();
        ui.on_key_pressed_text(move |text| {
            if let Some(msg) = key_to_move(&text) {
                let _ = tx.send(msg);
            }
        });
    }
    {
        let tx = control_tx.clone();
        ui.on_key_released_text(move |_| {
            let _ = tx.send(ControlMsg::Move { linear: 0.0, angular: 0.0 });
        });
    }
    // Keepalive so an idle hold does not trip the watchdog. The timer lives on the
    // UI thread for the app's lifetime.
    let timer = Box::leak(Box::new(slint::Timer::default()));
    timer.start(
        slint::TimerMode::Repeated,
        std::time::Duration::from_millis(100),
        move || {
            let _ = control_tx.send(ControlMsg::KeepAlive);
        },
    );
}

/// Bridges session callbacks (any thread) onto the Slint event loop.
pub struct SlintSink {
    ui: Weak<AppWindow>,
}

impl SlintSink {
    pub fn new(ui: Weak<AppWindow>) -> Arc<Self> {
        Arc::new(Self { ui })
    }
}

impl UiSink for SlintSink {
    fn frame(&self, frame: VideoFrame) {
        // SharedPixelBuffer is Send; Image is not, so build it inside the event loop.
        let buf = SharedPixelBuffer::<Rgba8Pixel>::clone_from_slice(
            &frame.rgba, frame.width, frame.height);
        let _ = self
            .ui
            .upgrade_in_event_loop(move |ui| ui.set_video_frame(Image::from_rgba8(buf)));
    }
    fn telemetry(&self, info: &DeviceInfo) {
        let text: SharedString = format!(
            "CPU {:.1}%\nMem {}/{} MB\nTemps {}\nUptime {}s",
            info.cpu_percent, info.mem_used_mb, info.mem_total_mb, info.temps.len(), info.uptime_s
        ).into();
        let _ = self.ui.upgrade_in_event_loop(move |ui| ui.set_telemetry_text(text));
    }
    fn status(&self, text: &str) {
        let text: SharedString = text.to_string().into();
        let _ = self.ui.upgrade_in_event_loop(move |ui| ui.set_status_text(text));
    }
}
