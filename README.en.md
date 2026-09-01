# streamer — the Bobi.Studio multi-destination encoder

*[Version française](README.md)*

Reads a video source from the [MXL](https://github.com/dmf-mxl/mxl) bus, encodes it **once**
(H.264 or H.265) and delivers it to several destinations at the same time: **UDP**, **SRT**,
**WebRTC**.

A component of [Bobi.Studio](https://github.com/bob-integration/bobistudio).

---

## How it works

A single ffmpeg encode, fanned out to the destinations by the `tee` muxer. Adding a
destination therefore does not cost another encoder — which matters when the same programme
feeds a broadcaster, a remote gallery and a browser preview.

Every branch carries `onfail=ignore`: **a dead destination does not take the others with it**.
That is the behaviour you want when an SRT link drops while the local UDP keeps running.

**Multi-track audio**: the input is an 8-channel flow, split into mono or stereo tracks. The
codec follows the destination — **AAC** for UDP and SRT (all tracks), **Opus** for WebRTC (the
first one only, the specification offering nothing better). Routing is done with
`tee select=`, without re-encoding the video.

The audio feeder writes **silence** when no fresh frame arrives, rather than blocking: a fault
on the audio side must not stop the video.

---

## What to know

**Audio/video alignment is decided when ffmpeg starts**, and it varies from one launch to the
next. The `av_offset_ms` setting corrects a constant offset, not that variation. Two
measurement programs are provided, to observe it rather than estimate it:

```bash
python3 tools/av_mesure_pts.py       # video and audio pts as they come out
python3 tools/av_origine_bench.py    # pts origin across several start-ups
```

**Resolution can be inferred.** If `video.width` and `video.height` are `0`, the encoder
derives the dimensions from the shared segment's size — useful to preview a source whose
format is unknown.

**WebRTC goes through a gateway.** The plugin pushes WHIP or RTSP to a MediaMTX deployed
separately; it does not serve WebRTC itself.

---

## Using it

This repository is a **plugin** of Bobi.Studio, mounted at `plugins/streamer/`. It is
configured from the orchestrator's **Streams** page — encoding on one side, the list of
destinations on the other — and wired to a source from the Cabling page.

It is not usable on its own: its configuration and wiring live in the orchestrator. `help.md`
is the help article rendered inside the product.

---

## Licence

GPL-3.0-or-later — see [LICENSE](LICENSE). Copyright © 2026 BOBI SAS, France.
