use std::sync::Arc;

use slint::{
    ComponentHandle, Image, Rgba8Pixel, SharedPixelBuffer, SharedString, StyledText, Weak,
};
use tokio::sync::mpsc;
use vantage_protocol::ControlMsg;
use vantage_protocol::telemetry::DeviceInfo;
use vantage_signalling::peer::VideoFrame;

use crate::session::UiSink;

slint::slint! {
    #[edition = "1.19"]
    import { VerticalBox } from "std-widgets.slint";
    export component AppWindow inherits Window {
        in property <image> video-frame;
        in property <styled-text> telemetry-text: @markdown("waiting for telemetry…");
        in property <string> status-text: "starting…";
        in property <string> drive-text: "idle";
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
            // Selecting telemetry text takes the focus, so the key release never
            // arrives here: stop instead of leaving a held move running.
            focus-lost(reason) => { root.key-released-text(""); }
            HorizontalLayout {
                padding: 8px;
                spacing: 8px;
                Rectangle {
                    horizontal-stretch: 7;
                    Image {
                        width: parent.width;
                        height: parent.height;
                        source: root.video-frame;
                        image-fit: contain;
                    }
                    Rectangle {
                        x: 8px;
                        y: 8px;
                        width: overlay.preferred-width;
                        height: overlay.preferred-height;
                        background: #00000066;
                        border-radius: 8px;
                        backdrop-blur: 12px;
                        overlay := VerticalLayout {
                            padding: 8px;
                            spacing: 4px;
                            Text { text: root.status-text; color: white; font-size: 14px; }
                            Text { text: "Drive: " + root.drive-text; color: white; font-size: 13px; }
                        }
                    }
                }
                VerticalBox {
                    horizontal-stretch: 3;
                    StyledText {
                        text: root.telemetry-text;
                        default-font-size: 13px;
                        selectable: true;
                    }
                }
            }
        }
    }
}

/// Map a pressed WASD key to a normalized teleop command and its on-screen
/// label; other keys ⇒ None.
fn key_to_move(text: &str) -> Option<(ControlMsg, &'static str)> {
    let (linear, angular, label) = match text {
        "w" | "W" => (1.0, 0.0, "▲ forward"),
        "s" | "S" => (-1.0, 0.0, "▼ reverse"),
        "a" | "A" => (0.0, -1.0, "◀ left"),
        "d" | "D" => (0.0, 1.0, "▶ right"),
        _ => return None,
    };
    Some((ControlMsg::Move { linear, angular }, label))
}

/// Wire keyboard teleop input and a 100 ms keepalive onto the control channel.
/// A key press sends a directional Move; release sends a neutral stop; the
/// keepalive beat keeps the robot watchdog armed during an idle hold.
pub fn wire_control_input(ui: &AppWindow, control_tx: mpsc::UnboundedSender<ControlMsg>) {
    {
        let tx = control_tx.clone();
        let weak = ui.as_weak();
        ui.on_key_pressed_text(move |text| {
            if let Some((msg, label)) = key_to_move(&text) {
                let _ = tx.send(msg);
                if let Some(ui) = weak.upgrade() {
                    ui.set_drive_text(label.into());
                }
            }
        });
    }
    {
        let tx = control_tx.clone();
        let weak = ui.as_weak();
        ui.on_key_released_text(move |_| {
            let _ = tx.send(ControlMsg::Move {
                linear: 0.0,
                angular: 0.0,
            });
            if let Some(ui) = weak.upgrade() {
                ui.set_drive_text("idle".into());
            }
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
            &frame.rgba,
            frame.width,
            frame.height,
        );
        let _ = self
            .ui
            .upgrade_in_event_loop(move |ui| ui.set_video_frame(Image::from_rgba8(buf)));
    }
    fn telemetry(&self, info: &DeviceInfo) {
        let md = format!(
            "- **CPU** {:.1}%\n- **Mem** {}/{} MB\n- **Temps** {}\n- **Uptime** {}s",
            info.cpu_percent,
            info.mem_used_mb,
            info.mem_total_mb,
            info.temps.len(),
            info.uptime_s
        );
        let _ = self.ui.upgrade_in_event_loop(move |ui| {
            let text =
                StyledText::from_markdown(&md).unwrap_or_else(|_| StyledText::from_plain_text(&md));
            ui.set_telemetry_text(text);
        });
    }
    fn status(&self, text: &str) {
        let text: SharedString = text.to_string().into();
        let _ = self
            .ui
            .upgrade_in_event_loop(move |ui| ui.set_status_text(text));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wasd_maps_to_move_and_label() {
        let (msg, label) = key_to_move("w").unwrap();
        assert!(
            matches!(msg, ControlMsg::Move { linear, angular } if linear == 1.0 && angular == 0.0)
        );
        assert_eq!(label, "▲ forward");
        assert!(key_to_move("x").is_none());
    }

    #[test]
    fn telemetry_markdown_parses() {
        assert!(StyledText::from_markdown("- **CPU** 1.0%\n- **Mem** 1/2 MB").is_ok());
    }
}
