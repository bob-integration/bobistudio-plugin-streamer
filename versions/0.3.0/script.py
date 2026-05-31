import mmap, struct, time, subprocess, threading, json, signal, os, math
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler

# ─── Config injectée (contrat plugin) ───────────────────────────────
# Les params sont DÉJÀ normalisés (normalize_worker_udp_params) et les destinations
# WebRTC résolues (deploy._resolve_webrtc_destinations) côté orchestrateur avant rendu.
CONFIG         = {config}
HOSTNAME       = "{hostname}"
PLUGIN_VERSION = "{plugin_version}"

SHM_NAME     = CONFIG.get("shm_name")
AUDIO_SHM    = CONFIG.get("audio_shm")
VIDEO_CFG    = CONFIG.get("video") or {{}}
AUDIO_CFG    = CONFIG.get("audio") or {{}}
DESTINATIONS = CONFIG.get("destinations") or []
HOT_INPUT    = bool(CONFIG.get("hot_input"))   # mode moniteur : source re-câblable à chaud (dims FIXES)

SHM_PATH = f"/dev/shm/{{SHM_NAME}}"
# ── Sémantique format : le format CONFIGURÉ est la SORTIE souhaitée — TOUJOURS honorée,
# jamais ignorée ni modifiée. L'ENTRÉE (signal reçu) est ce qui arrive réellement dans le
# shm ; si elle diffère, l'encodeur l'AJUSTE (mise à l'échelle + rééchantillonnage). ──
OUT_WIDTH  = int(VIDEO_CFG.get("width") or 0)     # 0 => suivre l'entrée (pas de mise à l'échelle)
OUT_HEIGHT = int(VIDEO_CFG.get("height") or 0)
OUT_FPS    = int(round(float(VIDEO_CFG.get("fps") or 0))) or 0   # 0 => cadence d'entrée
IN_FPS     = 25                                   # cadence native du pipeline simulé (receivers/mixer/…)
EFF_FPS    = OUT_FPS or IN_FPS                    # cadence réellement encodée
GOP        = int(VIDEO_CFG.get("gop") or EFF_FPS) or EFF_FPS
# WIDTH/HEIGHT = géométrie d'ENTRÉE lue dans le shm. Initialisées au format configuré
# (candidat), puis RÉSOLUES depuis le shm réel par _detect_dims (mode non-hot) ; canvas
# fixe en mode moniteur (hot). FPS = cadence d'entrée déclarée à ffmpeg.
WIDTH  = int(VIDEO_CFG.get("width") or 0)
HEIGHT = int(VIDEO_CFG.get("height") or 0)
FPS    = EFF_FPS
HEADER_SIZE = 64
RING_SIZE   = 10
# ── Chroma : IN = layout du shm source (doit matcher le producteur) ; OUT = encode souhaité.
_CHROMA_DIV = {{"420": (2, 2), "422": (2, 1), "444": (1, 1)}}
_PIX_FMT    = {{"420": "yuv420p", "422": "yuv422p", "444": "yuv444p"}}
IN_CHROMA   = str(CONFIG.get("chroma") or "422")
IN_CHROMA   = IN_CHROMA if IN_CHROMA in _CHROMA_DIV else "422"
OUT_CHROMA  = str(VIDEO_CFG.get("chroma") or "422")
OUT_CHROMA  = OUT_CHROMA if OUT_CHROMA in _CHROMA_DIV else "422"
IN_PIX_FMT  = _PIX_FMT[IN_CHROMA]
OUT_PIX_FMT = _PIX_FMT[OUT_CHROMA]
_ICW, _ICH  = _CHROMA_DIV[IN_CHROMA]         # diviseurs chroma de l'ENTRÉE
_IN_BPP     = 1.0 + 2.0 / (_ICW * _ICH)      # octets/pixel du layout d'entrée (1.5 / 2.0 / 3.0)
FRAME_SIZE = int(WIDTH * HEIGHT * _IN_BPP)    # recalculé après _detect_dims (entrée réelle)
TOTAL_SIZE = HEADER_SIZE + (FRAME_SIZE * RING_SIZE)

def _detect_dims():
    """Attend l'apparition du shm et résout la géométrie d'ENTRÉE réelle depuis sa taille.
    YUV420 → px = frame*2/3. On RESPECTE une résolution configurée (WIDTH/HEIGHT) si elle
    correspond EXACTEMENT au nombre de pixels du shm (gère un non-16:9 légitime) ; sinon
    (config absente OU périmée) on déduit en 16:9 → l'encodeur colle toujours au vrai
    signal reçu, peu importe ce qui a été enregistré. Renvoie (w,h) pairs."""
    while True:
        try:
            if os.path.exists(SHM_PATH) and os.path.getsize(SHM_PATH) > HEADER_SIZE:
                frame = (os.path.getsize(SHM_PATH) - HEADER_SIZE) // RING_SIZE
                px = int(frame / _IN_BPP)
                if px > 0:
                    if WIDTH > 0 and HEIGHT > 0 and WIDTH * HEIGHT == px:
                        return WIDTH, HEIGHT
                    h = int(round(math.sqrt(px * 9 / 16)));  h -= h % 2
                    w = (px // h) if h else 0;               w -= w % 2
                    if w > 0 and h > 0:
                        return w, h
        except Exception:
            pass
        print(f"detect-dims: attente du shm {{SHM_PATH}}…")
        time.sleep(1)
VCODEC = {{"h264": "libx264", "h265": "libx265"}}.get(VIDEO_CFG.get("codec", "h264"), "libx264")

# ─── Format audio (PCM L24 / 48kHz / 8ch) — identique au sender 2110-30 ──
A_SAMPLE_RATE = 48000
A_CHANNELS    = 8
A_BYTES_PER_SAMPLE = 3
A_CHUNK_SIZE  = (A_SAMPLE_RATE // 1000) * A_CHANNELS * A_BYTES_PER_SAMPLE  # 1152 (1 ms)
A_HEADER_SIZE = 64
A_RING_SIZE   = 100
A_TOTAL_SIZE  = A_HEADER_SIZE + A_RING_SIZE * A_CHUNK_SIZE
AUDIO_FIFO    = "/tmp/wudp_audio.raw"
AUDIO_ENABLED = bool(AUDIO_CFG.get("enabled")) and bool(AUDIO_SHM)

# ─── Mapping canaux→pistes mutable À CHAUD (page Streams) ─────────────
# La FORME est figée au lancement : nombre de pistes + largeur (mono=1 / stéréo=2).
# Seuls les INDICES de canaux source (0..7) changent à chaud (POST :8082/audiomap).
# Le feeder ré-aiguille les canaux du shm vers des "slots" de sortie FIXES que les
# filtres pan de ffmpeg lisent (positions constantes) → aucun restart, zéro coupure.
try:
    import numpy as _np
except Exception:
    _np = None

def _track_widths(tracks):
    return [2 if len(((t or {{}}).get("channels") or [0])) >= 2 else 1 for t in (tracks or [])]

def _flatten_channels(tracks):
    """Aplati les pistes en une liste de canaux source, dans l'ordre des slots."""
    flat = []
    for t in (tracks or []):
        chs = (t or {{}}).get("channels") or [0]
        if len(chs) >= 2:
            flat += [int(chs[0]) % A_CHANNELS, int(chs[1]) % A_CHANNELS]
        else:
            flat += [int(chs[0]) % A_CHANNELS]
    return flat

_INIT_TRACKS = AUDIO_CFG.get("tracks") or [{{"channels": [0, 1]}}]
AUDIO_WIDTHS = _track_widths(_INIT_TRACKS)
OUT_CHANNELS = sum(AUDIO_WIDTHS) or A_CHANNELS          # canaux écrits dans le fifo (≠ 8)
A_OUT_CHUNK  = (A_SAMPLE_RATE // 1000) * OUT_CHANNELS * A_BYTES_PER_SAMPLE

_amap_lock = threading.Lock()
_amap = {{"slot_src": _flatten_channels(_INIT_TRACKS)}}  # slot_src[k] = canal source du slot k

def _remap(buf, idx):
    """Ré-aiguille les canaux : entrée 8ch interleave s24le → sortie len(idx)ch.
    idx[k] = canal source (0..7) écrit dans le slot de sortie k. Identité si numpy
    absent et idx == range(8)."""
    fr = A_CHANNELS * A_BYTES_PER_SAMPLE
    if _np is not None:
        a = _np.frombuffer(buf, dtype=_np.uint8).reshape(-1, A_CHANNELS, A_BYTES_PER_SAMPLE)
        return a[:, idx, :].tobytes()
    n = len(buf) // fr
    out = bytearray(n * len(idx) * A_BYTES_PER_SAMPLE)
    op = 0
    for f in range(n):
        b = f * fr
        for s in idx:
            ip = b + s * A_BYTES_PER_SAMPLE
            out[op:op + A_BYTES_PER_SAMPLE] = buf[ip:ip + A_BYTES_PER_SAMPLE]; op += A_BYTES_PER_SAMPLE
    return bytes(out)

# ─── Latence : âge du frame consommé (rolling avg sur 30 frames) ──
class RollingMs:
    def __init__(self, n=30):
        self.d = deque(maxlen=n); self.last_ns = 0
    def push(self, ms_value):
        self.d.append(ms_value); self.last_ns = time.time_ns()
    def avg(self):
        if not self.d: return None
        if time.time_ns() - self.last_ns > 2_000_000_000: return None
        return round(sum(self.d) / len(self.d), 1)

lat_in = RollingMs()
lat_audio = RollingMs()   # latence du signal audio reçu (âge du chunk consommé)

def _dest_target(d):
    t = d.get("type")
    if t == "udp":    return f"udp://{{d.get('host')}}:{{d.get('port')}}"
    if t == "srt":    return f"srt://{{d.get('host')}}:{{d.get('port')}}"
    if t == "webrtc": return d.get("ingest_url") or f"webrtc:{{d.get('path')}}"
    return t or "?"

def _dests_summary(up):
    out = []
    for d in DESTINATIONS:
        if d.get("type") == "webrtc" and not (d.get("enabled") and d.get("ingest_url")):
            continue
        out.append({{"type": d.get("type"), "target": _dest_target(d), "up": bool(up)}})
    return out

metrics = {{"fps": 0.0, "in_width": 0, "in_height": 0,
            "out_width": OUT_WIDTH, "out_height": OUT_HEIGHT, "out_fps": OUT_FPS,
            "in_fps_seen": 0.0, "pushed_fps": 0.0, "dropped_stale_fps": 0.0,
            "inputs_latency_ms": {{}}, "out_bitrate_kbps": 0.0, "destinations": []}}

# ─── Instrumentation : tail du stderr ffmpeg (diagnostic backpressure encode/RTSP) ──
ffmpeg_log = []
ffmpeg_log_lock = threading.Lock()
def _drain_stderr(p):
    try:
        for line in iter(p.stderr.readline, b""):
            s = line.decode("utf-8", "replace").rstrip()
            if not s:
                continue
            with ffmpeg_log_lock:
                ffmpeg_log.append(s); del ffmpeg_log[:-20]
    except Exception:
        pass

# ─── Débit de sortie RÉEL = octets TX réseau du container ────────────
# Le muxer `tee` ne rapporte pas total_size (ffmpeg -progress renvoie N/A), donc on
# mesure côté réseau : delta des octets émis (TX) sur les interfaces hors `lo`, par
# seconde → kbps. Couvre UDP/SRT/WebRTC indépendamment du muxer, sans toucher à ffmpeg
# ni risquer de bloquer le flux. Inclut l'overhead réseau (débit fil réel).
def _tx_bytes():
    total = None
    try:
        with open("/proc/net/dev") as f:
            for line in f:
                if ":" not in line:
                    continue
                name, _, rest = line.partition(":")
                if name.strip() == "lo":
                    continue
                cols = rest.split()
                if len(cols) >= 9:
                    total = (total or 0) + int(cols[8])   # col 8 = octets TX
    except Exception:
        return None
    return total

def _net_meter():
    prev, prev_t = _tx_bytes(), time.time()
    while True:
        time.sleep(2)
        cur, now = _tx_bytes(), time.time()
        if cur is not None and prev is not None and now > prev_t and cur >= prev:
            metrics["out_bitrate_kbps"] = round((cur - prev) * 8.0 / (now - prev_t) / 1000.0, 1)
        prev, prev_t = cur, now

threading.Thread(target=_net_meter, daemon=True).start()

def _refresh_metrics(fps=None, up=True):
    if fps is not None:
        metrics["fps"] = fps
    v = lat_in.avg()
    lat = {{SHM_NAME: v}} if SHM_NAME else {{}}
    if AUDIO_ENABLED and AUDIO_SHM:
        lat[AUDIO_SHM] = lat_audio.avg()   # latence audio (None si pas de chunk frais récent)
    metrics["inputs_latency_ms"] = lat
    metrics["destinations"] = _dests_summary(up)

# Flag pour signaler une Bus error
bus_error = threading.Event()

def handle_sigbus(signum, frame):
    print("SIGBUS reçu — SHM invalidé, reconnexion...")
    bus_error.set()

signal.signal(signal.SIGBUS, handle_sigbus)

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        payload = dict(metrics)
        with ffmpeg_log_lock: payload["ffmpeg_log"] = list(ffmpeg_log)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode())
    def log_message(self, *a): pass

threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", 8080), MetricsHandler).serve_forever(),
    daemon=True).start()

# ─── Construction de la commande ffmpeg : un seul encode → fan-out ──
def _has_ts():
    return any(d.get("type") in ("udp", "srt") for d in DESTINATIONS)

def _has_webrtc():
    return any(d.get("type") == "webrtc" and d.get("enabled") and d.get("ingest_url")
               for d in DESTINATIONS)

def _pan_slots(base, w):
    """Filtre pan lisant des SLOTS de sortie FIXES de l'entrée ré-aiguillée par le
    feeder : 1 → mono, ≥2 → stéréo. Les positions ne changent jamais (la forme est
    figée) ; seuls les canaux source aboutissant dans ces slots changent à chaud."""
    if w >= 2:
        return f"pan=stereo|c0=c{{base}}|c1=c{{base+1}}"
    return f"pan=mono|c0=c{{base}}"

def _audio_plan():
    """Construit le plan audio : filtres pan (avec asplit si plusieurs consommateurs),
    maps et codecs par flux, et les index de flux de sortie pour le routage tee.
    Sortie : stream 0 = vidéo, puis pistes AAC (si TS), puis 1 piste Opus (si WebRTC).
    Les pan lisent des slots fixes (cf. _pan_slots) → remap canaux sans restart."""
    tracks = AUDIO_CFG.get("tracks") or [{{"channels": [0, 1]}}]
    has_ts, has_webrtc = _has_ts(), _has_webrtc()
    n_ts = len(tracks) if has_ts else 0
    consumers = n_ts + (1 if has_webrtc else 0)
    filters, maps, codecs = [], [], []
    ts_idx, webrtc_idx = [], None
    out_idx = 1   # 0 = vidéo
    a_idx = 0     # index relatif audio (a:0, a:1, …)
    br = str(AUDIO_CFG.get("bitrate", "128k"))

    # Bornes de slots FIXES par piste (forme figée). track i ↦ slots [bases[i], +widths[i]).
    widths = _track_widths(tracks)
    bases, _b = [], 0
    for w in widths:
        bases.append(_b); _b += w

    # Une copie de l'entrée audio par consommateur (asplit) — [1:a] non réutilisable tel quel.
    if consumers > 1:
        labels = [f"[s{{i}}]" for i in range(consumers)]
        filters.append(f"[1:a]asplit={{consumers}}{{''.join(labels)}}")
    else:
        labels = ["[1:a]"]
    li = 0

    # aresample=async : maintient une timeline 48 kHz CONTINUE (insère/retire des samples
    # pour absorber le jitter/drift entre l'horloge du producteur (fifo) et l'encodeur).
    # Corrige l'audio « haché » dû au franchissement de frontière d'horloge.
    asy = "aresample=async=1:min_hard_comp=0.100:first_pts=0"
    if has_ts:
        for i, t in enumerate(tracks):
            filters.append(f"{{labels[li]}}{{_pan_slots(bases[i], widths[i])}},{{asy}}[a{{i}}]"); li += 1
            maps += ["-map", f"[a{{i}}]"]
            codecs += [f"-c:a:{{a_idx}}", "aac", f"-b:a:{{a_idx}}", br]
            ts_idx.append(out_idx); out_idx += 1; a_idx += 1
    if has_webrtc:
        filters.append(f"{{labels[li]}}{{_pan_slots(bases[0] if bases else 0, widths[0] if widths else 2)}},{{asy}}[aop]"); li += 1
        maps += ["-map", "[aop]"]
        codecs += [f"-c:a:{{a_idx}}", "libopus", f"-b:a:{{a_idx}}", br]
        webrtc_idx = out_idx; out_idx += 1; a_idx += 1

    ts_sel = ",".join(str(i) for i in ([0] + ts_idx))
    webrtc_sel = ",".join(str(i) for i in ([0] + ([webrtc_idx] if webrtc_idx is not None else [])))
    return filters, maps, codecs, ts_sel, webrtc_sel

def _build_outputs(ts_sel=None, webrtc_sel=None):
    """Branches du muxer `tee`. `*_sel` = liste d'index de flux à router (select) quand
    l'audio est multi-codec ; None → tous les flux (vidéo seule). onfail=ignore : une
    destination morte n'affecte pas les autres."""
    def opt(sel):
        s = "onfail=ignore"
        if sel:
            s += f":select={{sel}}"
        return s
    branches = []
    for d in DESTINATIONS:
        t = d.get("type")
        if t == "udp":
            branches.append(f"[{{opt(ts_sel)}}:f=mpegts]udp://{{d['host']}}:{{d['port']}}?pkt_size=1316")
        elif t == "srt":
            q = f"mode=caller&latency={{int(d.get('latency_ms') or 120)}}"
            if d.get("passphrase"): q += f"&passphrase={{d['passphrase']}}"
            if d.get("streamid"):   q += f"&streamid={{d['streamid']}}"
            branches.append(f"[{{opt(ts_sel)}}:f=mpegts]srt://{{d['host']}}:{{d['port']}}?{{q}}")
        elif t == "webrtc" and d.get("enabled") and d.get("ingest_url"):
            url = d["ingest_url"]
            if url.startswith("rtsp://"):
                branches.append(f"[{{opt(webrtc_sel)}}:f=rtsp:rtsp_transport=tcp]{{url}}")
            else:  # WHIP (ffmpeg >= 7.1)
                branches.append(f"[{{opt(webrtc_sel)}}:f=whip]{{url}}")
    return branches

def _video_filter():
    """Chaîne de filtre d'ADAPTATION entrée→sortie, pour faire coller le signal reçu au
    format de SORTIE configuré. `scale` seulement si une taille de sortie est demandée ET
    diffère de l'entrée détectée (WIDTH/HEIGHT) ; `fps` seulement hors mode moniteur (le
    moniteur cadence déjà à la sortie) ET si la sortie diffère de l'entrée. Chaîne vide si
    sortie == entrée → aucun coût ni recompression inutile."""
    parts = []
    if OUT_WIDTH and OUT_HEIGHT and (OUT_WIDTH != WIDTH or OUT_HEIGHT != HEIGHT):
        parts.append(f"scale={{OUT_WIDTH}}:{{OUT_HEIGHT}}:flags=bicubic")
    if (not HOT_INPUT) and OUT_FPS and OUT_FPS != IN_FPS:
        parts.append(f"fps={{OUT_FPS}}")
    return ",".join(parts)

def creer_ffmpeg():
    # -r d'entrée = cadence du signal reçu : EFF_FPS en mode moniteur (le feeder cadence
    # lui-même à cette valeur), sinon la cadence native du pipeline (IN_FPS).
    in_rate = EFF_FPS if HOT_INPUT else IN_FPS
    cmd = ["ffmpeg",
           "-f", "rawvideo", "-pix_fmt", IN_PIX_FMT,   # layout du shm d'entrée
           "-s", f"{{WIDTH}}x{{HEIGHT}}", "-r", str(in_rate), "-i", "pipe:0"]
    ts_sel = webrtc_sel = None
    vchain = _video_filter()
    vopts = ["-c:v", VCODEC,
             "-preset", str(VIDEO_CFG.get("preset", "ultrafast")),
             "-tune", "zerolatency",
             "-b:v", str(VIDEO_CFG.get("bitrate", "4M")),
             "-pix_fmt", OUT_PIX_FMT,                   # chroma de SORTIE (ffmpeg convertit si ≠ entrée)
             "-g", str(GOP), "-keyint_min", str(GOP), "-sc_threshold", "0"]
    # Colorimétrie : flags optionnels (vides => laissés à l'auto ffmpeg).
    for _flag, _key in (("-color_primaries", "color_primaries"),
                        ("-color_trc", "color_trc"),
                        ("-colorspace", "colorspace")):
        _val = str(VIDEO_CFG.get(_key) or "").strip()
        if _val:
            vopts += [_flag, _val]
    if VCODEC == "libx265":
        vopts += ["-tag:v", "hvc1"]

    if AUDIO_ENABLED:
        cmd += ["-thread_queue_size", "512",
                "-f", "s24le", "-ar", str(A_SAMPLE_RATE), "-ac", str(OUT_CHANNELS),
                "-i", AUDIO_FIFO]
        filters, amaps, acodecs, ts_sel, webrtc_sel = _audio_plan()
        # Filtre vidéo dans le même filter_complex que l'audio → on mappe [vout].
        if vchain:
            filters = [f"[0:v]{{vchain}}[vout]"] + filters
            vmap = ["-map", "[vout]"]
        else:
            vmap = ["-map", "0:v"]
        if filters:
            cmd += ["-filter_complex", ";".join(filters)]
        cmd += vmap + amaps + vopts + acodecs
    else:
        if vchain:
            cmd += ["-filter_complex", f"[0:v]{{vchain}}[vout]", "-map", "[vout]"]
        else:
            cmd += ["-map", "0:v"]
        cmd += vopts

    cmd += ["-flush_packets", "1"]
    branches = _build_outputs(ts_sel, webrtc_sel)
    if branches:
        cmd += ["-f", "tee", "|".join(branches)]
    else:
        cmd += ["-f", "null", "-"]
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    threading.Thread(target=_drain_stderr, args=(proc,), daemon=True).start()
    return proc

# ─── Thread lecteur audio → fifo (2e entrée ffmpeg) ──────────────────
# IMPORTANT : on ouvre le fifo IMMÉDIATEMENT (sans attendre le shm audio) puis on
# l'alimente en continu, à ~temps réel (1 chunk = 1 ms). Chunk réel si disponible et
# frais, sinon SILENCE. Ainsi l'entrée audio de ffmpeg ne starve jamais → la vidéo
# n'est jamais bloquée, même si la source audio est absente/muette.
SILENCE = bytes(A_OUT_CHUNK)   # 1 ms de silence à la largeur de SORTIE (OUT_CHANNELS)

def _open_audio_shm():
    shm_path = f"/dev/shm/{{AUDIO_SHM}}"
    try:
        if not AUDIO_SHM or not os.path.exists(shm_path) or os.path.getsize(shm_path) < A_TOTAL_SIZE:
            return None, None
        af = open(shm_path, "r+b")
        shm = mmap.mmap(af.fileno(), A_TOTAL_SIZE)
        _ = shm[0:24]
        return af, shm
    except Exception:
        return None, None

def audio_feeder():
    try:
        if not os.path.exists(AUDIO_FIFO):
            os.mkfifo(AUDIO_FIFO)
    except Exception as e:
        print(f"audio fifo non créé : {{e}}")
        return
    while True:
        try:
            fifo = open(AUDIO_FIFO, "wb")   # rendez-vous : débloque quand ffmpeg ouvre la lecture
        except Exception as e:
            print(f"audio fifo open échec : {{e}}"); time.sleep(1); continue
        print("audio fifo connecté à ffmpeg")
        af = shm = None
        last_index = 0
        last_shm_try = 0.0
        sil_start = time.monotonic()
        sil_written = 0
        try:
            while True:
                if bus_error.is_set():
                    break
                if shm is None and (time.monotonic() - last_shm_try) > 1.0:
                    last_shm_try = time.monotonic()
                    af, shm = _open_audio_shm()
                    last_index = 0
                if shm is not None:
                    # Streaming FIDÈLE : on écrit chaque nouveau chunk dans l'ordre
                    # (comme le sender 2110-30) → pas de ré-échantillonnage ni de
                    # silence intercalé qui déformerait l'audio.
                    try:
                        ci, ts_a = struct.unpack("QQ", shm[0:16])
                    except Exception:
                        shm = None
                        continue
                    if ci > last_index:
                        n = ci - last_index
                        if n > A_RING_SIZE:   # gros retard → on saute au plus récent
                            n = 1
                        buf = bytearray()
                        for j in range(ci - n + 1, ci + 1):
                            off = A_HEADER_SIZE + (j % A_RING_SIZE) * A_CHUNK_SIZE
                            buf += memoryview(shm)[off:off + A_CHUNK_SIZE]
                        last_index = ci
                        with _amap_lock:
                            idx = list(_amap["slot_src"])
                        fifo.write(_remap(bytes(buf), idx)); fifo.flush()  # BrokenPipe → handler externe
                        age_ms = (time.time_ns() - ts_a) / 1e6   # âge du chunk consommé
                        if 0 <= age_ms < 5000: lat_audio.push(age_ms)
                        sil_start = time.monotonic(); sil_written = 0
                    time.sleep(0.0005)
                else:
                    # Pas de source audio : silence cadencé (~temps réel) pour ne jamais
                    # bloquer la vidéo. 1 chunk = 1 ms.
                    due = int((time.monotonic() - sil_start) * 1000)
                    guard = 0
                    while sil_written < due and guard < 1000:
                        fifo.write(SILENCE); sil_written += 1; guard += 1
                    fifo.flush()
                    time.sleep(0.004)
        except (BrokenPipeError, ValueError):
            print("audio fifo cassé (ffmpeg redémarré ?), reconnexion...")
        except Exception as e:
            print(f"audio feeder erreur : {{e}}")
        finally:
            for c in (fifo, shm, af):
                try:
                    if c: c.close()
                except Exception: pass
            time.sleep(1)

if AUDIO_ENABLED:
    threading.Thread(target=audio_feeder, daemon=True).start()

# ─── Ouverture du shm vidéo ──────────────────────────────────────────
def ouvrir_shm():
    while True:
        try:
            if not os.path.exists(SHM_PATH):
                raise FileNotFoundError(f"{{SHM_PATH}} n'existe pas")
            if os.path.getsize(SHM_PATH) < TOTAL_SIZE:
                raise ValueError(f"{{SHM_PATH}} trop petit")
            f = open(SHM_PATH, "r+b")
            shm = mmap.mmap(f.fileno(), TOTAL_SIZE)
            _ = shm[0:16]
            print(f"SHM ouvert : {{SHM_PATH}}")
            return f, shm
        except Exception as e:
            print(f"SHM indisponible, attente... ({{e}})")
            time.sleep(1)

def fermer_shm(shm_f, shm):
    try: shm.close()
    except Exception: pass
    try: shm_f.close()
    except Exception: pass

# ─── Mode hot-input (moniteur) : change de source sans redéployer ────────────
# La boucle lit une source courante muable (POST :8082/input {{shm}}), rouvre le
# mmap quand elle change, et alimente ffmpeg à cadence FPS (frame noire si pas
# d'image fraîche → le même process ffmpeg et le path WebRTC restent vivants :
# zéro coupure). Les dimensions sont FIXES ; un changement de résolution est géré
# par un redéploiement côté orchestrateur (il choisit hot vs redeploy via la DB).
_hot_lock = threading.Lock()
_hot_cur  = {{"shm": SHM_NAME or ""}}

def _open_named(name):
    path = f"/dev/shm/{{name}}"
    try:
        if not name or not os.path.exists(path) or os.path.getsize(path) < TOTAL_SIZE:
            return None, None
        f = open(path, "r+b")
        m = mmap.mmap(f.fileno(), TOTAL_SIZE)
        _ = m[0:16]
        return f, m
    except Exception:
        return None, None

class ControlHandler(BaseHTTPRequestHandler):
    """Contrôle à chaud sur :8082.
    - POST /input    {{shm}}             → re-câble la source vidéo (mode moniteur HOT_INPUT)
    - POST /audiomap {{tracks:[…]}}      → ré-aiguille les canaux audio (forme figée) sans restart
    - GET  /audiomap                     → mapping courant + forme
    - GET  /                             → état source vidéo (compat moniteur)
    """
    def _read_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n: return {{}}
        try: return json.loads(self.rfile.read(n).decode())
        except Exception: return {{}}
    def _reply(self, code, payload):
        self.send_response(code); self.send_header("Content-Type", "application/json")
        self.end_headers(); self.wfile.write(json.dumps(payload).encode())

    def do_POST(self):
        body = self._read_json()
        if self.path == "/input":
            # La re-câblage vidéo à chaud n'a de sens qu'en mode hot (boucle _run_hot).
            # En mode « Adaptation auto » (non-hot), la boucle lit un SHM_NAME fixe et ignore
            # _hot_cur → on REFUSE (409) pour que l'appelant (câbles) retombe sur un
            # redéploiement plutôt que de croire à une bascule réussie silencieusement.
            if not HOT_INPUT:
                self._reply(409, {{"error": "encodeur non-hot (adaptation auto) : redeploiement requis"}})
                return
            shm = (body.get("shm") or "").strip()
            with _hot_lock:
                _hot_cur["shm"] = shm
            self._reply(200, {{"ok": True, "shm": shm}})
        elif self.path == "/audiomap":
            # Remap canaux à chaud. La FORME doit être identique (mêmes largeurs de
            # pistes) — sinon 409 et l'orchestrateur retombe sur un redéploiement.
            tracks = body.get("tracks")
            if not isinstance(tracks, list):
                self._reply(400, {{"error": "tracks doit être une liste"}}); return
            if _track_widths(tracks) != AUDIO_WIDTHS:
                self._reply(409, {{"error": "forme audio changee, redeploiement requis",
                                   "widths": AUDIO_WIDTHS}}); return
            for t in tracks:
                for ci in ((t or {{}}).get("channels") or []):
                    if not isinstance(ci, int) or ci < 0 or ci >= A_CHANNELS:
                        self._reply(400, {{"error": "canaux invalides (0..7)"}}); return
            with _amap_lock:
                _amap["slot_src"] = _flatten_channels(tracks)
                src = list(_amap["slot_src"])
            self._reply(200, {{"ok": True, "slot_src": src}})
        else:
            self._reply(404, {{"error": "not found"}})

    def do_GET(self):
        if self.path == "/audiomap":
            with _amap_lock: src = list(_amap["slot_src"])
            self._reply(200, {{"slot_src": src, "widths": AUDIO_WIDTHS,
                              "channels": OUT_CHANNELS, "enabled": AUDIO_ENABLED}})
        else:
            with _hot_lock: shm = _hot_cur["shm"]
            self._reply(200, {{"shm": shm, "width": WIDTH, "height": HEIGHT}})
    def log_message(self, *a): pass

# Le serveur de contrôle tourne dès qu'il y a quelque chose à piloter à chaud :
# l'audio (remap canaux) en mode normal, et/ou la source vidéo en mode moniteur.
if AUDIO_ENABLED or HOT_INPUT:
    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", 8082), ControlHandler).serve_forever(),
        daemon=True).start()

def _run_hot():
    global ffmpeg_out
    black = b"\x10" * (WIDTH * HEIGHT) + b"\x80" * ((WIDTH // _ICW) * (HEIGHT // _ICH) * 2)
    ffmpeg_out = creer_ffmpeg()
    cur_name = None; f = m = None
    last_index = 0; last_frame = None
    win_cnt = 0; win_start = time.time()   # fps sur fenêtre glissante (~1s), pas cumulé
    interval = 1.0 / FPS; next_t = time.time()
    print(f"hot-input actif {{WIDTH}}x{{HEIGHT}}@{{FPS}} — source initiale {{_hot_cur['shm']!r}}")
    while True:
        if bus_error.is_set():
            bus_error.clear()
            try:
                if m: m.close()
                if f: f.close()
            except Exception: pass
            f = m = None; cur_name = None; last_frame = None
            time.sleep(1)
        with _hot_lock: want = _hot_cur["shm"]
        if want != cur_name:
            try:
                if m: m.close()
                if f: f.close()
            except Exception: pass
            f, m = _open_named(want)
            cur_name = want; last_index = 0; last_frame = None
        elif m is None and want:
            f, m = _open_named(want)   # source pas encore prête : retente
            if m is not None: last_index = 0
        if m is not None:
            try:
                fi, ts = struct.unpack("QQ", m[0:16])
                if fi != last_index and fi != 0:
                    slot = fi % RING_SIZE
                    off  = HEADER_SIZE + slot * FRAME_SIZE
                    last_frame = bytes(memoryview(m)[off:off + FRAME_SIZE])
                    last_index = fi
                    age_us = (time.time_ns() - ts) // 1000
                    if 0 <= age_us < 5_000_000: lat_in.push(age_us / 1000.0)
            except Exception:
                try:
                    if m: m.close()
                    if f: f.close()
                except Exception: pass
                f = m = None; cur_name = None; last_frame = None
        now = time.time()
        if now >= next_t:
            if ffmpeg_out.poll() is not None:
                _refresh_metrics(up=False); time.sleep(0.3); ffmpeg_out = creer_ffmpeg()
            buf = last_frame if last_frame is not None else black
            try:
                ffmpeg_out.stdin.write(buf); ffmpeg_out.stdin.flush()
                win_cnt += 1
                _el = time.time() - win_start
                if _el >= 1.0:
                    _refresh_metrics(fps=round(win_cnt / _el, 1), up=True)
                    win_cnt = 0; win_start = time.time()
            except BrokenPipeError:
                time.sleep(0.3); ffmpeg_out = creer_ffmpeg()
            next_t += interval
            if next_t < now: next_t = now + interval
        time.sleep(0.001)

# Géométrie d'entrée toujours résolue depuis le shm réel (corrige une config périmée,
# respecte un non-16:9 exact — cf. _detect_dims). En mode hot-input (moniteur) les
# dimensions sont imposées (canvas fixe) → pas de détection.
if not HOT_INPUT:
    WIDTH, HEIGHT = _detect_dims()
    FRAME_SIZE = int(WIDTH * HEIGHT * _IN_BPP)
    TOTAL_SIZE = HEADER_SIZE + (FRAME_SIZE * RING_SIZE)
    print(f"dims entrée résolues: {{WIDTH}}x{{HEIGHT}}")

# Signal reçu publié sur /metrics (dims d'entrée résolues : config ou auto-détectées).
metrics["in_width"], metrics["in_height"] = WIDTH, HEIGHT

# Mode moniteur : boucle hot-input dédiée (ne retourne jamais).
if HOT_INPUT:
    _run_hot()

ffmpeg_out  = creer_ffmpeg()
shm_f, shm  = ouvrir_shm()
last_index  = 0
win_cnt     = 0
win_start   = time.time()   # fps sur fenêtre glissante (~1s), pas une moyenne cumulée
# Diagnostic : production vue (delta d'index), frames poussées, frames jetées car trop vieilles.
diag_seen = diag_pushed = diag_stale = 0
diag_win  = time.time()

while True:
    # Reconnexion après Bus error
    if bus_error.is_set():
        bus_error.clear()
        fermer_shm(shm_f, shm)
        time.sleep(2)
        shm_f, shm = ouvrir_shm()
        last_index = 0
        continue

    _eld = time.time() - diag_win
    if _eld >= 1.0:
        metrics["in_fps_seen"]       = round(diag_seen / _eld, 1)
        metrics["pushed_fps"]        = round(diag_pushed / _eld, 1)
        metrics["dropped_stale_fps"] = round(diag_stale / _eld, 1)
        diag_seen = diag_pushed = diag_stale = 0
        diag_win  = time.time()

    try:
        frame_index, ts = struct.unpack("QQ", shm[0:16])

        if ffmpeg_out.poll() is not None:
            print("FFmpeg arrêté, redémarrage...")
            _refresh_metrics(up=False)
            time.sleep(1)
            ffmpeg_out = creer_ffmpeg()

        if frame_index > last_index:
            diag_seen += frame_index - last_index   # frames PRODUITES depuis la dernière lecture
            age_us = (time.time_ns() - ts) // 1000
            if age_us < 80000:
                lat_in.push(age_us / 1000.0)
                slot   = frame_index % RING_SIZE
                offset = HEADER_SIZE + slot * FRAME_SIZE
                frame_bytes = memoryview(shm)[offset:offset + FRAME_SIZE]
                try:
                    ffmpeg_out.stdin.write(frame_bytes)
                    ffmpeg_out.stdin.flush()
                    win_cnt += 1
                    diag_pushed += 1
                    _el = time.time() - win_start
                    if _el >= 1.0:
                        _refresh_metrics(fps=round(win_cnt / _el, 1), up=True)
                        win_cnt = 0; win_start = time.time()
                except BrokenPipeError:
                    print("BrokenPipe, redémarrage FFmpeg...")
                    time.sleep(1)
                    ffmpeg_out = creer_ffmpeg()
            else:
                diag_stale += 1   # frame trop vieille (≥80 ms) → jetée par le filtre de fraîcheur
            last_index = frame_index

        time.sleep(0.00001)

    except Exception as e:
        print(f"Erreur ({{type(e).__name__}}): {{e}}, reconnexion...")
        fermer_shm(shm_f, shm)
        time.sleep(2)
        shm_f, shm = ouvrir_shm()
        last_index = 0
