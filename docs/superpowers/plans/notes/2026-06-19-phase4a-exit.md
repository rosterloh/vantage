# Phase 4a (camera + tee + encoder factory) — Exit Evidence (2026-06-19)

Plan: `docs/superpowers/plans/2026-06-19-vantage-phase4a-camera-tee-encoder.md`
Branch: `vantage-phase4a-camera`. GStreamer 1.28.2.

## Automated tests — `cargo test --workspace`

All green (7 test binaries), including the two new `encoder` tests
(`factory_builds_an_available_encoder`, `selection_is_deterministic_and_present`)
and `robot_offer_contains_video_mline` (now built through the tee + factory encoder).

## Encoder factory

Runtime selection over `nvv4l2h264enc → nvh264enc → vah264enc → vaapih264enc →
qsvh264enc → vtenc_h264 → x264enc`. This host has no hardware encoder, so it selects
`x264enc` (logged: `selected H.264 encoder: x264enc`). Hardware arms are configured
from GStreamer docs and would be exercised on a Jetson/GPU host.

## Concurrent tee — test source (headless)

`source → videoconvert → caps(I420,640x480@30) → tee` fanning out to
`queue(leaky) → x264enc → h264parse → rtph264pay → webrtcbin` and
`queue → videoconvert → RGB appsink`. Ran coordinator + robot + client (test source):

```
client video frames: ~210 (#210, ~30 fps)   # encode branch → WebRTC → decode
robot  raw  frames:  ~240 (#240, ~30 fps)    # raw RGB branch
encoder: x264enc
```

Both branches run simultaneously from one `tee` — the camera/source is not
monopolised by the stream (camera-sharing "Camera not monopolised", mechanism).

## Live camera (`/dev/video0`)

`/dev/video0` is a real MJPEG webcam (default `image/jpeg` 1920×1080@60) that also
exposes a YUYV (4:2:2) raw mode. Ran with `VANTAGE_VIDEO_SOURCE=camera`:

```
video source: camera /dev/video0
client video frames: #210 (~30 fps)
robot  raw  frames:  #210 (~30 fps)
no x264 / 4:2:2 / negotiation errors
```

**Live camera renders on the client AND the raw branch produces frames
simultaneously** — the full Phase 4a exit criterion, with a real camera.

### Bug found and fixed during integration
First camera run hit `x264 [error]: baseline profile doesn't support 4:2:2`: a
`video/x-raw` request without a `format` let the camera's YUYV (4:2:2) reach
`x264enc`, which baseline can't encode (only ~10 frames in 7 s). Fixed by forcing
`format=I420` (4:2:0) in the tee's source caps (commit `d35255a`) — x264 accepts
I420 and the raw branch still converts I420→RGB cleanly. Re-ran: full 30 fps, no
errors.

### Note for pure-MJPEG cameras
This camera offered a YUYV raw mode, so `videoconvert` could feed the tee directly.
A camera that is **MJPEG-only** (no raw mode) would need `v4l2src ! jpegdec` in
`build_source` — documented in the plan; not required here.

## Phase 4a exit criteria

- [x] `cargo test --workspace` green (incl. encoder tests + m=video through the tee).
- [x] Concurrent tee: client video frames AND robot raw frames flow simultaneously.
- [x] Encoder factory selects at runtime (`x264enc` here) and the stream decodes.
- [x] Live camera (`VANTAGE_VIDEO_SOURCE=camera`, `/dev/video0`) renders on the
      client with the concurrent raw branch — full criterion met with a real camera.

Next: **Phase 4b** consumes `Peer::recv_raw_frame()` to publish `sensor_msgs/Image`
+ `sensor_msgs/CameraInfo` over `rclrs` (colcon/ament build), completing
camera-sharing "Raw image availability" / "Camera info published".
