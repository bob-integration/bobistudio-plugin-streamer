from app.scripts import normalize_worker_udp_params


def before_deploy(params, context):
    """Normalise le schéma multi-destinations et résout les URLs WebRTC."""
    hot = bool(params.get("hot_input", False))
    params = normalize_worker_udp_params(params)
    params["hot_input"] = hot
    _resolve_webrtc(params, context.get("settings") or {})
    return params


def _resolve_webrtc(params, settings):
    """Injecte ingest_url/whep_url/embed_url dans chaque destination WebRTC active."""
    dests = params.get("destinations") or []
    if not any(d.get("type") == "webrtc" and d.get("enabled") for d in dests):
        return
    enabled  = settings.get("webrtc_enabled")
    gw_ip    = settings.get("webrtc_gateway_ip") or ""
    proto    = settings.get("webrtc_ingest_proto") or "rtsp"
    rtsp_p   = int(settings.get("webrtc_rtsp_port") or 8554)
    http_p   = int(settings.get("webrtc_http_port") or 8889)
    for d in dests:
        if d.get("type") != "webrtc" or not d.get("enabled"):
            continue
        path = d.get("path") or ""
        if not (enabled and gw_ip and path):
            d.pop("ingest_url", None)
            continue
        if proto == "whip":
            d["ingest_url"] = f"http://{gw_ip}:{http_p}/{path}/whip"
        else:
            d["ingest_url"] = f"rtsp://{gw_ip}:{rtsp_p}/{path}"
        d["whep_url"]  = f"http://{gw_ip}:{http_p}/{path}/whep"
        d["embed_url"] = f"http://{gw_ip}:{http_p}/{path}"
