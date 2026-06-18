use std::sync::Arc;

use slint::{Image, Rgba8Pixel, SharedPixelBuffer, SharedString, Weak};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_signalling::peer::VideoFrame;

use crate::session::UiSink;

slint::slint! {
    import { VerticalBox, HorizontalBox } from "std-widgets.slint";
    export component AppWindow inherits Window {
        in property <image> video-frame;
        in property <string> telemetry-text: "waiting for telemetry…";
        in property <string> status-text: "starting…";
        title: "Vantage";
        preferred-width: 960px;
        preferred-height: 540px;
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
