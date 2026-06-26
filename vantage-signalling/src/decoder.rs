use anyhow::{Context, Result};
use gstreamer as gst;
use gstreamer::prelude::*;

/// Ordered H.264 decoder candidates: hardware first, software (`avdec_h264`) last.
/// nvh264dec / nvdec (NVIDIA) → vah264dec / vaapih264dec (Intel/AMD VAAPI) →
/// d3d11h264dec (Windows) → vtdec_h264 (macOS) → avdec_h264 (software fallback).
pub(crate) const CANDIDATES: &[&str] = &[
    "nvh264dec",
    "nvdec",
    "vah264dec",
    "vaapih264dec",
    "d3d11h264dec",
    "vtdec_h264",
    "avdec_h264",
];

/// The factory name of the decoder that would be selected on this host, if any.
pub fn selected_decoder_name() -> Option<&'static str> {
    CANDIDATES
        .iter()
        .find(|n| gst::ElementFactory::find(n).is_some())
        .copied()
}

/// Build the first available H.264 decoder. Output caps are normalized by a
/// downstream `videoconvert` to RGBA, so the contract to the appsink is identical
/// regardless of which decoder is selected. Some hardware decoders emit
/// vendor-surface caps (NVMM / VASurface); on such hosts a hardware colour
/// converter (`vapostproc` / `nvvideoconvert`) may be needed before `videoconvert`
/// — see peer.rs build_decode_branch note.
pub fn make_h264_decoder() -> Result<gst::Element> {
    let name = selected_decoder_name()
        .context("no H.264 decoder found (install gst-plugins-libav for avdec_h264)")?;
    let dec = gst::ElementFactory::make(name)
        .build()
        .with_context(|| format!("failed to build decoder {name}"))?;
    tracing::info!("selected H.264 decoder: {name}");
    Ok(dec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_builds_an_available_decoder() {
        gst::init().unwrap();
        let dec = make_h264_decoder().expect("an H.264 decoder must be available");
        let factory_name = dec.factory().expect("element has a factory").name();
        assert!(
            CANDIDATES.contains(&factory_name.as_str()),
            "selected decoder {factory_name} not in the candidate list"
        );
    }

    #[test]
    fn selection_is_deterministic_and_present() {
        gst::init().unwrap();
        assert!(selected_decoder_name().is_some(), "expected at least avdec_h264");
    }
}
