# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

import mmap, struct, time, subprocess, threading, json, signal, os, math
from collections import deque
import bobimxl   # migration MXL : entrée vidéo (Reader) + audio (AudioReader gapless) — Phases 1/3
from http.server import HTTPServer, BaseHTTPRequestHandler

# ─── Config injectée (contrat plugin) ───────────────────────────────
# Les params sont DÉJÀ normalisés (normalize_worker_udp_params) et les destinations
# WebRTC résolues (deploy._resolve_webrtc_destinations) côté orchestrateur avant rendu.
CONFIG         = {config}
HOSTNAME       = "{hostname}"
PLUGIN_VERSION = "{plugin_version}"

def _as_bool(v):
    # bool("False") == True : on parse explicitement les chaînes de CONFIG.
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on")
    return bool(v)

SHM_NAME     = CONFIG.get("shm_name")
AUDIO_SHM    = CONFIG.get("audio_shm")
VIDEO_CFG    = CONFIG.get("video") or {{}}
AUDIO_CFG    = CONFIG.get("audio") or {{}}
DESTINATIONS = CONFIG.get("destinations") or []
HOT_INPUT    = _as_bool(CONFIG.get("hot_input"))   # mode moniteur : source re-câblable à chaud (dims FIXES)

# ── MODE TRANCHE (chantier latence sous-trame, patch mxl-planar-slices) ──────────────
# slice_mode=true → la boucle vidéo (non-hot) suit le grain de TÊTE via get_slice (réveil à
# chaque commit partiel du producteur) et alimente le pipe ffmpeg PAR BANDES au fil de leur
# arrivée. Un pipe est un FLUX : l'encodeur reçoit ainsi la trame complète au moment où la
# DERNIÈRE bande arrive au lieu de ~+1 période d'attente de grain complet (~18 ms glass-to-
# glass en moins — chemin du monitoring WebRTC). ⚠ Le format rawvideo attendu par ffmpeg est
# PLANAR (yuv420p/yuv422p… : Y complet PUIS U complet PUIS V complet) → on ne peut PAS écrire
# « bande Y+U+V » interleavé : on streame le plan Y par bandes (1/2 des octets en 422, 2/3 en
# 420), puis U et V d'un bloc à la dernière tranche (par convention k tranches ⇔ lignes
# [0, k·slice_height) valides sur les TROIS plans → U/V sont complets quand Y l'est).
# slice_mode absent/False → chemin historique STRICTEMENT identique (octet-identique).
SLICE_MODE  = _as_bool(CONFIG.get("slice_mode", False)) and not HOT_INPUT
SLICE_LINES = max(1, int(CONFIG.get("slice_lines") or 36))   # informatif — le pas vient du grain source

SHM_PATH = f"/dev/shm/{{SHM_NAME}}"
inst = bobimxl.Instance()   # domaine MXL ($MXL_DOMAIN ou /dev/shm/mxl) — flux vidéo d'entrée
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
RING_SIZE   = CONFIG.get("shm_video_ring", 10)
# ── Chroma : IN = layout du shm source (doit matcher le producteur) ; OUT = encode souhaité.
_CHROMA_DIV = {{"420": (2, 2), "422": (2, 1), "444": (1, 1)}}
_PIX_FMT    = {{"420": "yuv420p", "422": "yuv422p", "444": "yuv444p"}}
IN_CHROMA   = str(CONFIG.get("chroma") or "422")
IN_CHROMA   = IN_CHROMA if IN_CHROMA in _CHROMA_DIV else "422"
OUT_CHROMA  = str(VIDEO_CFG.get("chroma") or "422")
OUT_CHROMA  = OUT_CHROMA if OUT_CHROMA in _CHROMA_DIV else "422"
# Profondeur du shm d'ENTRÉE (8/10/12 bits). La SORTIE (encode h264/h265) reste 8 bits.
BIT_DEPTH   = int(CONFIG.get("bit_depth") or 8)
# ── ENTRELACÉ NATIF (modèle SDK MXL) : 1 grain = 1 CHAMP (½ hauteur), cadence CHAMP ──────
# La SOURCE DE VÉRITÉ est le flow_def du flux (reader.format() : `interlaced`, `field_order`,
# `frame_height`, `frame_fps_num`) — PAS `CONFIG["scan"]`, qui peut être absent ou faux selon le
# câblage (bug mesuré : mur 1080i50 → le streamer annonçait 1920×540 et n'encodait qu'UN champ
# sur deux, image écrasée). CONFIG ne sert plus que de valeur d'attente avant détection.
# WIDTH/HEIGHT = dims de TRAME (1920×1080) ; GRAIN_H/GRAIN_SIZE = dims/taille d'un CHAMP.
# Chaîne d'encodage : on TISSE les 2 grains-champs en une trame pleine, puis bwdif la
# DÉSENTRELACE (send_frame = 1 trame de sortie/trame d'entrée → cadence TRAME, 25 fps pour du
# 1080i50, jamais la cadence champ qui doublerait le débit). La sortie est donc PROGRESSIVE :
# c'est obligatoire pour WebRTC (H.264 entrelacé non supporté par les navigateurs) et sans
# risque pour UDP/SRT (le peigne, lui, se verrait sur tout décodeur).
IN_SCAN          = str(CONFIG.get("scan") or "p").strip().lower()
IN_FIELD_ORDER   = str(CONFIG.get("field_order") or "").strip().lower()
IN_INTERLACED    = (IN_SCAN == "i")

def _deint_vf():
    """Filtre de désentrelacement courant (recalculé à chaque création de ffmpeg : le balayage
    n'est connu qu'APRÈS détection du flow_def / re-câblage à chaud)."""
    if not IN_INTERLACED:
        return ""
    return "bwdif=mode=send_frame:parity=" + ("1" if IN_FIELD_ORDER == "bff" else "0")

_IN_DEEP    = BIT_DEPTH >= 10
_IN_DBPS    = 2 if _IN_DEEP else 1                                 # octets/échantillon entrée
_IN_SUF     = (("12le" if BIT_DEPTH >= 12 else "10le") if _IN_DEEP else "")
IN_PIX_FMT  = _PIX_FMT[IN_CHROMA] + _IN_SUF
OUT_PIX_FMT = _PIX_FMT[OUT_CHROMA]
_ICW, _ICH  = _CHROMA_DIV[IN_CHROMA]         # diviseurs chroma de l'ENTRÉE
_IN_BPP     = (1.0 + 2.0 / (_ICW * _ICH)) * _IN_DBPS   # octets/pixel d'entrée (incl. profondeur)
FRAME_SIZE = int(WIDTH * HEIGHT * _IN_BPP)    # TRAME pleine (ce qu'on pousse à ffmpeg)
TOTAL_SIZE = HEADER_SIZE + (FRAME_SIZE * RING_SIZE)
GRAIN_H    = HEIGHT                            # hauteur d'un GRAIN (= ½ trame si entrelacé)
GRAIN_SIZE = FRAME_SIZE                        # taille d'un GRAIN (= ½ trame si entrelacé)

def _recalc_sizes():
    """Recalcule tous les dérivés de (WIDTH, HEIGHT, IN_CHROMA, BIT_DEPTH, IN_INTERLACED).
    WIDTH/HEIGHT sont TOUJOURS des dims de TRAME ; le GRAIN vaut ½ trame en entrelacé."""
    global _IN_DEEP, _IN_DBPS, _IN_SUF, IN_PIX_FMT, _ICW, _ICH, _IN_BPP
    global FRAME_SIZE, TOTAL_SIZE, GRAIN_H, GRAIN_SIZE
    _IN_DEEP    = BIT_DEPTH >= 10
    _IN_DBPS    = 2 if _IN_DEEP else 1
    _IN_SUF     = (("12le" if BIT_DEPTH >= 12 else "10le") if _IN_DEEP else "")
    IN_PIX_FMT  = _PIX_FMT[IN_CHROMA] + _IN_SUF
    _ICW, _ICH  = _CHROMA_DIV[IN_CHROMA]
    _IN_BPP     = (1.0 + 2.0 / (_ICW * _ICH)) * _IN_DBPS
    FRAME_SIZE  = int(WIDTH * HEIGHT * _IN_BPP)
    TOTAL_SIZE  = HEADER_SIZE + (FRAME_SIZE * RING_SIZE)
    GRAIN_H     = (HEIGHT // 2) if IN_INTERLACED else HEIGHT
    GRAIN_SIZE  = int(WIDTH * GRAIN_H * _IN_BPP)

def _field_planes():
    """Plans d'un grain-CHAMP : [(taille_octets, octets_par_ligne)] pour Y, U, V."""
    y_sz = WIDTH * GRAIN_H * _IN_DBPS
    uv_w = WIDTH // _ICW
    uv_sz = uv_w * (GRAIN_H // _ICH) * _IN_DBPS
    return [(y_sz, WIDTH * _IN_DBPS), (uv_sz, uv_w * _IN_DBPS), (uv_sz, uv_w * _IN_DBPS)]

def _weave_fields(first_b, second_b):
    """Tisse 2 grains-champs en une TRAME planar pleine (ordre pipe : Y puis U puis V).
    `first_b` = grain d'index PAIR (1er champ), `second_b` = index impair (2e champ).
    tff → 1er champ = lignes PAIRES ; bff → 1er champ = lignes impaires. (Même convention que
    le recorder et le moteur 2110 — la seule qui fait foi.)"""
    bff = (IN_FIELD_ORDER == "bff")
    out = bytearray(); off = 0
    for (psz, rb) in _field_planes():
        a = _np.frombuffer(first_b,  dtype=_np.uint8, count=psz, offset=off).reshape(-1, rb)
        b = _np.frombuffer(second_b, dtype=_np.uint8, count=psz, offset=off).reshape(-1, rb)
        fr = _np.empty((a.shape[0] * 2, rb), dtype=_np.uint8)
        if bff:
            fr[1::2] = a; fr[0::2] = b
        else:
            fr[0::2] = a; fr[1::2] = b
        out += fr.tobytes(); off += psz
    return bytes(out)

def _apply_fmt(f):
    """Applique un format lu d'un flow_def MXL (reader.format()) aux globales d'ENTRÉE.
    `f["width"]/["height"]` sont les dims de GRAIN → on remonte aux dims de TRAME
    (`frame_width`/`frame_height`) et on retient l'entrelacement + l'ordre de champ."""
    global WIDTH, HEIGHT, IN_CHROMA, BIT_DEPTH, IN_INTERLACED, IN_SCAN, IN_FIELD_ORDER, IN_FPS
    IN_INTERLACED = bool(f.get("interlaced"))
    IN_SCAN       = "i" if IN_INTERLACED else "p"
    IN_FIELD_ORDER = str(f.get("field_order") or ("tff" if IN_INTERLACED else "")).lower()
    w = int(f.get("frame_width") or f.get("width") or WIDTH)
    h = int(f.get("frame_height") or f.get("height") or HEIGHT)
    if w > 0: WIDTH  = w - (w % 2)
    if h > 0: HEIGHT = h - (h % 2)
    ch = str(f.get("chroma") or IN_CHROMA)
    IN_CHROMA = ch if ch in _CHROMA_DIV else IN_CHROMA
    BIT_DEPTH = int(f.get("bit_depth") or BIT_DEPTH)
    if IN_INTERLACED:
        # cadence d'entrée déclarée à ffmpeg = cadence TRAME (on pousse des trames tissées)
        fn = int(f.get("frame_fps_num") or 0); fd = int(f.get("frame_fps_den") or 1) or 1
        if fn > 0: IN_FPS = max(1, int(round(fn / fd)))
    _recalc_sizes()

def _detect_dims():
    """Résout le format d'ENTRÉE depuis le flow_def du flux MXL (source de vérité côté donnée),
    entrelacement compris. Attend que le flux existe."""
    while True:
        try:
            r = bobimxl.Reader(inst, SHM_NAME)
            f = r.format(); r.close()
            if f:
                return f
        except Exception:
            pass
        print(f"detect-dims: attente du flux MXL {{SHM_NAME}}…")
        time.sleep(1)

# Signale au main loop de fermer/rouvrir le shm et relancer ffmpeg (injection format à chaud).
_restart_signal = threading.Event()
VCODEC = {{"h264": "libx264", "h265": "libx265"}}.get(VIDEO_CFG.get("codec", "h264"), "libx264")

# ─── Format audio — MXL : flux continu float32 par-canal (48kHz / 8ch) ──
# Migration MXL Phase 3 : l'entrée audio n'est plus un ring shm s24be mais un FLUX MXL de samples
# float32 (bobimxl.AudioReader). On alimente ffmpeg en `-f f32le` ; le ré-aiguillage de canaux =
# sélection de colonnes numpy. (L'ancien ring shm/s24be a été retiré.)
A_SAMPLE_RATE = 48000
A_CHANNELS    = 8
A_BYTES_PER_SAMPLE = 4   # float32 (was 3 = s24be)
A_BLOCK       = A_SAMPLE_RATE // 1000   # 48 samples = 1 ms (granularité de lecture gapless)
AUDIO_FIFO    = "/tmp/wudp_audio.raw"
AUDIO_ENABLED = bool(AUDIO_CFG.get("enabled")) and bool(AUDIO_SHM)

# Calage A/V : offset MANUEL `av_offset_ms` (réglage, signé : >0 retarde l'audio via adelay, <0
# retarde la vidéo via -itsoffset). Défaut 0 = aucun décalage. UTILE notamment quand la vidéo et
# l'audio ne suivent PAS le même chemin (ex. vidéo qui traverse un traitement — mixer/dve/correcteur —
# qui ajoute de la latence, audio direct) : on rattrape le décalage constant introduit. Aussi pour un
# résiduel d'encodage. À calibrer au banc (clap/flash, ou capture mpegts) sur une sortie qui honore
# les PTS (UDP/SRT/fichier). NB (banc 2026-06-05) : (1) les GROS décalages A/V venaient de la FAMINE
# CPU du conteneur (encode 1080p qui ne tient pas le temps réel → l'audio DÉRIVE) — réglée par le
# profil `resources.cores` du manifeste, pas par un offset ; (2) le calage AUTO par media_ts a été
# retiré (media_ts audio biaisé + inefficace) ; (3) le transport WebRTC réaligne l'A/V tout seul
# (RTCP) → un offset ffmpeg n'a aucun effet sur WHEP (n'agit que sur mpegts/fichier).
_sync_v_mts = [0]
A_CHUNK_NS  = 1000000

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

def _remap(arr, idx):
    """Ré-aiguille les canaux d'un bloc float32 (n, A_CHANNELS) → octets f32le (n, len(idx)).
    idx[k] = canal source (0..7) écrit dans le slot de sortie k. `arr` = vue numpy float32 issue
    de bobimxl.AudioReader (déjà par-canal) → simple sélection de colonnes."""
    return _np.ascontiguousarray(arr[:, idx], dtype=_np.float32).tobytes()

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

metrics = {{"fps": 0.0, "in_width": 0, "in_height": 0, "in_chroma": IN_CHROMA, "in_bit_depth": BIT_DEPTH,
            "out_width": OUT_WIDTH, "out_height": OUT_HEIGHT, "out_fps": OUT_FPS,
            "in_fps_seen": 0.0, "pushed_fps": 0.0, "dropped_stale_fps": 0.0,
            "inputs_latency_ms": {{}}, "out_bitrate_kbps": 0.0, "destinations": [],
            "av_offset_ms": 0, "slice_mode": SLICE_MODE}}

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

def _audio_plan(a_delay_ms=0):
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
    if a_delay_ms > 0:                       # calage A/V manuel : retarder l'audio de av_offset_ms.
        asy += f",adelay=delays={{a_delay_ms}}:all=1"   # APRÈS first_pts=0 (sinon retrimé) → silence inséré
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
    _dv = _deint_vf()
    if _dv:
        parts.append(_dv)                # désentrelacement AVANT scale (sinon peigne ré-échantillonné)
    if OUT_WIDTH and OUT_HEIGHT and (OUT_WIDTH != WIDTH or OUT_HEIGHT != HEIGHT):
        parts.append(f"scale={{OUT_WIDTH}}:{{OUT_HEIGHT}}:flags=bicubic")
    if (not HOT_INPUT) and OUT_FPS and OUT_FPS != IN_FPS:
        parts.append(f"fps={{OUT_FPS}}")
    return ",".join(parts)

def creer_ffmpeg():
    # -r d'entrée = cadence du signal reçu : EFF_FPS en mode moniteur (le feeder cadence
    # lui-même à cette valeur), sinon la cadence native du pipeline (IN_FPS).
    in_rate = EFF_FPS if HOT_INPUT else IN_FPS
    # Calage A/V MANUEL (réglage `av_offset_ms`, signé, ms) appliqué via les PTS : >0 → retarder
    # l'audio (adelay, cf. _audio_plan) ; <0 → retarder la vidéo (-itsoffset sur pipe:0, pas de
    # resampler côté vidéo donc l'itsoffset survit). 0 = aucun décalage (défaut). N'agit que sur les
    # sorties qui honorent les PTS (mpegts/fichier) ; le WebRTC réaligne lui-même l'A/V (RTCP).
    av = int(CONFIG.get("av_offset_ms") or 0) * 1000000 if AUDIO_ENABLED else 0
    a_delay_ms = int(round(av / 1e6)) if av > 0 else 0
    metrics["av_offset_ms"] = int(round(av / 1e6))        # délai appliqué (signé), exposé sur :8080
    vin = ["-thread_queue_size", "512",                   # buffer d'entrée vidéo : ffmpeg draine pipe:0
           "-f", "rawvideo", "-pix_fmt", IN_PIX_FMT,      # indépendamment (sinon bloqué pendant la
           "-s", f"{{WIDTH}}x{{HEIGHT}}", "-r", str(in_rate)]  # config du filtre [1:a])
    if av < 0:                                            # vidéo en avance → la retarder
        vin += ["-itsoffset", f"{{-av / 1e9:.3f}}"]
    vin += ["-i", "pipe:0"]
    cmd = ["ffmpeg"] + vin
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
                "-f", "f32le", "-ar", str(A_SAMPLE_RATE), "-ac", str(OUT_CHANNELS),
                "-i", AUDIO_FIFO]
        filters, amaps, acodecs, ts_sel, webrtc_sel = _audio_plan(a_delay_ms=a_delay_ms)
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
        # Borne la capacité du fifo audio à ~120 ms (par canaux de sortie) : pendant le pic CPU de
        # démarrage de l'encodeur, ffmpeg lit le fifo en retard → l'audio s'y empile → transitoire de
        # retard. Le blocage (write bloquant + fifo borné) crée un BACKPRESSURE qui limite ce que
        # ffmpeg ingère. F_SETPIPE_SZ=1031 (min 4 Ko). NB : f32le = 4 o/sample (1,33× le s24be).
        try:
            import fcntl
            _fsz = max(4096, int(0.12 * OUT_CHANNELS * A_SAMPLE_RATE * A_BYTES_PER_SAMPLE))
            fcntl.fcntl(fifo.fileno(), 1031, _fsz)
        except Exception:
            pass
        # Entrée audio = FLUX MXL de samples float32 (bobimxl.AudioReader), lu GAPLESS : on suit une
        # position `pos` (index sample) qui démarre à head et avance par blocs de 1 ms. Décrochage
        # (samples tombés de l'anneau) → resync sur head. Si pas de samples frais : SILENCE cadencé
        # (jamais starver ffmpeg → la vidéo continue). Latence = (now_tai − lastWriteTime).
        ar = None; pos = None
        last_ar_try = 0.0
        sil_start = time.monotonic(); sil_written = 0
        try:
            while True:
                if bus_error.is_set():
                    break
                if ar is None and (time.monotonic() - last_ar_try) > 1.0:
                    last_ar_try = time.monotonic()
                    try: ar = bobimxl.AudioReader(inst, AUDIO_SHM)
                    except Exception: ar = None
                    pos = None
                wrote_real = False
                if ar is not None:
                    try:
                        head = ar.head_index()
                    except Exception:
                        try: ar.close()
                        except Exception: pass
                        ar = None; head = bobimxl.MXL_UNDEFINED_INDEX
                    if ar is not None and head != bobimxl.MXL_UNDEFINED_INDEX:
                        if pos is None:
                            pos = head
                        with _amap_lock:
                            idx = list(_amap["slot_src"])
                        guard = 0
                        while pos <= head and guard < 512:
                            r = ar.read_from(pos, A_BLOCK)
                            if r is None:
                                if pos < head:      # samples tombés de l'anneau → resync au plus récent
                                    pos = head; continue
                                break               # head pas encore complet → on attend
                            fifo.write(_remap(r, idx))
                            pos += A_BLOCK; wrote_real = True; guard += 1
                        if wrote_real:
                            fifo.flush()            # BrokenPipe → handler externe
                            lat = (bobimxl.now_tai() - ar.last_write_time()) / 1e6
                            if 0 <= lat < 5000: lat_audio.push(lat)
                            sil_start = time.monotonic(); sil_written = 0
                if wrote_real:
                    time.sleep(0.0005)
                else:
                    # Pas de samples frais — flux ABSENT *ou* source muette : silence cadencé
                    # (~temps réel) pour ne JAMAIS starver ffmpeg → la vidéo continue quoi qu'il arrive.
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
            for c in (fifo, ar):
                try:
                    if c: c.close()
                except Exception: pass
            time.sleep(1)

if AUDIO_ENABLED:
    threading.Thread(target=audio_feeder, daemon=True).start()

# ─── Ouverture du shm vidéo ──────────────────────────────────────────
def ouvrir_shm():
    """Ouvre le flux vidéo MXL d'entrée (Reader). Attend qu'il existe."""
    while True:
        try:
            r = bobimxl.Reader(inst, SHM_NAME)
            print(f"flux MXL ouvert : {{SHM_NAME}}")
            return r
        except Exception as e:
            print(f"flux MXL indisponible, attente... ({{e}})")
            time.sleep(1)

def fermer_shm(reader):
    try: reader.close()
    except Exception: pass

def _gc_domain():
    """GC ENTRE close et reopen (motif pyramide / moteur tx_reopen_if_stale) : sans GC un flux
    recréé sous le même nom reste résolvable vers l'ORPHELIN périmé → le reopen retombe dessus
    et la boucle gèle en silence (mesuré côté pyramide : 40 min de gel après recréation)."""
    try: inst.garbage_collect()
    except Exception: pass

def _neutral_tail(a, b, y_sz):
    """MODE TRANCHE — octets NEUTRES (noir) pour TERMINER une trame commencée dans le pipe,
    intervalle [a, b) : 0x10 sur le plan Y, 0x80 sur la chroma. Secours ULTIME quand ni le
    grain courant ni le précédent ne sont lisibles (producteur mort mi-trame) — une trame
    entamée DOIT être finie, sinon l'encodeur se désynchronise sur tout le flux. Exact en
    8 bits ; approximation visuellement acceptable en 10/12 bits (une trame de secours)."""
    ny = max(0, min(b, y_sz) - a)
    return b"\x10" * ny + b"\x80" * max(0, b - a - ny)

# ─── Mode hot-input (moniteur) : change de source sans redéployer ────────────
# La boucle lit une source courante muable (POST :8082/input {{shm}}), rouvre le
# mmap quand elle change, et alimente ffmpeg à cadence FPS (frame noire si pas
# d'image fraîche → le même process ffmpeg et le path WebRTC restent vivants :
# zéro coupure). Les dimensions sont FIXES ; un changement de résolution est géré
# par un redéploiement côté orchestrateur (il choisit hot vs redeploy via la DB).
_hot_lock = threading.Lock()
_hot_cur  = {{"shm": SHM_NAME or ""}}

def _open_named(name):
    """Reader MXL du flux `name`, ou None si absent (mode moniteur : source re-câblable)."""
    try:
        return bobimxl.Reader(inst, name) if name else None
    except Exception:
        return None

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
            if not HOT_INPUT:
                # En mode non-hot, l'orchestrateur injecte le format au câblage (pattern UDC).
                # Mettre à jour tous les dérivés et signaler le redémarrage ffmpeg.
                fmt = body.get("format")
                if fmt and isinstance(fmt, dict):
                    global WIDTH, HEIGHT, BIT_DEPTH, IN_CHROMA
                    global IN_INTERLACED, IN_SCAN, IN_FIELD_ORDER
                    new_chroma = str(fmt.get("chroma") or IN_CHROMA)
                    if new_chroma not in _CHROMA_DIV:
                        new_chroma = IN_CHROMA
                    # Format injecté par l'orchestrateur : `scan`/`field_order` optionnels. Les
                    # dims annoncées sont des dims de TRAME (côté DB) ; le flow_def MXL reste
                    # ré-arbitre à la réouverture du reader (cf. boucle : _apply_fmt(r.format())).
                    _sc = str(fmt.get("scan") or ("i" if IN_INTERLACED else "p")).strip().lower()
                    IN_INTERLACED  = _sc.startswith("i")
                    IN_SCAN        = "i" if IN_INTERLACED else "p"
                    IN_FIELD_ORDER = str(fmt.get("field_order")
                                         or (IN_FIELD_ORDER or ("tff" if IN_INTERLACED else ""))).lower()
                    new_w = int(fmt.get("width") or WIDTH)
                    new_h = int(fmt.get("height") or HEIGHT)
                    if new_w > 0: WIDTH = new_w - (new_w % 2)
                    if new_h > 0: HEIGHT = new_h - (new_h % 2)
                    BIT_DEPTH   = int(fmt.get("bit_depth") or BIT_DEPTH)
                    IN_CHROMA   = new_chroma
                    _recalc_sizes()
                    metrics["in_width"]       = WIDTH
                    metrics["in_height"]      = HEIGHT
                    metrics["in_chroma"]      = IN_CHROMA
                    metrics["in_bit_depth"]   = BIT_DEPTH
                    metrics["in_scan"]        = IN_SCAN
                    metrics["in_field_order"] = IN_FIELD_ORDER
                    _restart_signal.set()
                self._reply(200, {{"ok": True}})
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

def _hot_scan(r):
    """Mode moniteur : lit le BALAYAGE de la source re-câblée dans son flow_def (le canvas
    WIDTH/HEIGHT reste imposé par le déploiement). Renvoie True si le balayage a CHANGÉ
    (→ ffmpeg à relancer : bwdif à activer/désactiver et cadence d'entrée à revoir)."""
    global IN_INTERLACED, IN_SCAN, IN_FIELD_ORDER
    try:
        f = r.format()
    except Exception:
        f = None
    if not f:
        return False
    il = bool(f.get("interlaced"))
    fo = str(f.get("field_order") or ("tff" if il else "")).lower()
    if il == IN_INTERLACED and fo == IN_FIELD_ORDER:
        return False
    IN_INTERLACED = il
    IN_SCAN = "i" if il else "p"
    IN_FIELD_ORDER = fo
    _recalc_sizes()
    metrics["in_scan"] = IN_SCAN
    metrics["in_field_order"] = IN_FIELD_ORDER
    print(f"hot-input: balayage source = {{IN_SCAN}} {{IN_FIELD_ORDER or '-'}} "
          f"(grain {{WIDTH}}x{{GRAIN_H}})")
    return True

def _hot_restart(proc):
    try: proc.stdin.close()
    except Exception: pass
    try: proc.terminate()
    except Exception: pass
    time.sleep(0.2)
    return creer_ffmpeg()

def _run_hot():
    global ffmpeg_out
    black = b"\x10" * (WIDTH * HEIGHT) + b"\x80" * ((WIDTH // _ICW) * (HEIGHT // _ICH) * 2)
    ffmpeg_out = creer_ffmpeg()
    cur_name = None; r = None
    last_index = 0; last_frame = None
    win_cnt = 0; win_start = time.time()   # fps sur fenêtre glissante (~1s), pas cumulé
    # Diagnostics fenêtre glissante (miroir de la boucle normale) : production vue côté SOURCE
    # (delta d'index → in_fps_seen, détecte une source figée) et frames réellement poussées
    # (pushed_fps ≈ FPS de sortie). dropped_stale_fps reste 0 : le mode moniteur répète la
    # dernière frame à cadence fixe, il ne jette JAMAIS pour péremption.
    diag_seen = diag_pushed = 0; diag_win = time.time()
    interval = 1.0 / FPS; next_t = time.time()
    print(f"hot-input actif {{WIDTH}}x{{HEIGHT}}@{{FPS}} — source initiale {{_hot_cur['shm']!r}}")
    while True:
        if bus_error.is_set():
            bus_error.clear()
            try:
                if r: r.close()
            except Exception: pass
            r = None; cur_name = None; last_frame = None
            time.sleep(1)
        with _hot_lock: want = _hot_cur["shm"]
        if want != cur_name:
            try:
                if r: r.close()
            except Exception: pass
            r = _open_named(want)
            cur_name = want; last_index = 0; last_frame = None
            if r is not None and _hot_scan(r):
                ffmpeg_out = _hot_restart(ffmpeg_out)   # balayage changé → bwdif on/off
        elif r is None and want:
            r = _open_named(want)   # source pas encore prête : retente
            if r is not None:
                last_index = 0
                if _hot_scan(r):
                    ffmpeg_out = _hot_restart(ffmpeg_out)
        if r is not None:
            try:
                got = r.get_latest()
                # Garde monitor : ne JAMAIS pousser un grain de taille ≠ GRAIN_SIZE (canvas, ou
                # ½ canvas si la source est ENTRELACÉE : 1 grain = 1 champ) — un re-point (hot)
                # vers une autre résolution sans redéploiement corromprait le flux.
                if got is not None and len(got[2]) == GRAIN_SIZE:
                    fi = got[0]
                    if fi != last_index:
                        # Production source : delta d'index (garde anti-reset → +1 au 1er coup).
                        diag_seen += (fi - last_index) if 0 < last_index < fi else 1
                        if IN_INTERLACED:
                            # Tisser les 2 grains-CHAMPS de la dernière trame complète.
                            ff = (fi - 1) // 2
                            g0 = r.get(2 * ff) if ff >= 0 else None
                            g1 = r.get(2 * ff + 1) if ff >= 0 else None
                            if (g0 is not None and g1 is not None
                                    and len(g0[2]) == GRAIN_SIZE and len(g1[2]) == GRAIN_SIZE):
                                last_frame = _weave_fields(bytes(g0[2]), bytes(g1[2]))
                                last_index = 2 * ff + 1
                        else:
                            last_frame = bytes(got[2])       # grain = trame planar complète
                            last_index = fi
                        age_us = (bobimxl.now_tai() - r.last_write_time()) // 1000
                        if 0 <= age_us < 5_000_000: lat_in.push(age_us / 1000.0)
            except Exception:
                try:
                    if r: r.close()
                except Exception: pass
                r = None; cur_name = None; last_frame = None
        now = time.time()
        if now >= next_t:
            if ffmpeg_out.poll() is not None:
                _refresh_metrics(up=False); time.sleep(0.3); ffmpeg_out = creer_ffmpeg()
            buf = last_frame if last_frame is not None else black
            try:
                ffmpeg_out.stdin.write(buf); ffmpeg_out.stdin.flush()
                win_cnt += 1; diag_pushed += 1
                _el = time.time() - win_start
                if _el >= 1.0:
                    _refresh_metrics(fps=round(win_cnt / _el, 1), up=True)
                    win_cnt = 0; win_start = time.time()
            except BrokenPipeError:
                time.sleep(0.3); ffmpeg_out = creer_ffmpeg()
            # Publication des diagnostics source/push sur ~1s glissante.
            _eld = time.time() - diag_win
            if _eld >= 1.0:
                metrics["in_fps_seen"]       = round(diag_seen / _eld, 1)
                metrics["pushed_fps"]        = round(diag_pushed / _eld, 1)
                metrics["dropped_stale_fps"] = 0.0
                diag_seen = diag_pushed = 0; diag_win = time.time()
            next_t += interval
            if next_t < now: next_t = now + interval
        time.sleep(0.001)

# Géométrie d'entrée toujours résolue depuis le shm réel (corrige une config périmée,
# respecte un non-16:9 exact — cf. _detect_dims). En mode hot-input (moniteur) les
# dimensions sont imposées (canvas fixe) → pas de détection.
if not HOT_INPUT:
    # TOUJOURS résolu depuis le flow_def (et plus seulement quand la config est vide) : le
    # balayage (entrelacé + ordre de champ) n'existe QUE là, et une config de sortie non nulle
    # masquait la détection en faisant passer les dims de SORTIE pour les dims d'ENTRÉE.
    _apply_fmt(_detect_dims())
    print(f"dims entrée résolues: {{WIDTH}}x{{HEIGHT}}{{IN_SCAN}} {{IN_CHROMA}} {{BIT_DEPTH}}bit"
          f" (grain {{WIDTH}}x{{GRAIN_H}} {{IN_FIELD_ORDER or '-'}})")

# Signal reçu publié sur /metrics (dims d'entrée résolues : config ou auto-détectées).
# in_width/in_height = dims de TRAME (1920×1080 pour du 1080i50, JAMAIS la ½ hauteur du champ).
metrics["in_width"], metrics["in_height"] = WIDTH, HEIGHT
metrics["in_chroma"], metrics["in_bit_depth"] = IN_CHROMA, BIT_DEPTH
metrics["in_scan"], metrics["in_field_order"] = IN_SCAN, IN_FIELD_ORDER

# Mode moniteur : boucle hot-input dédiée (ne retourne jamais).
if HOT_INPUT:
    _run_hot()

ffmpeg_out  = creer_ffmpeg()
reader      = ouvrir_shm()
last_index  = 0
win_cnt     = 0
win_start   = time.time()   # fps sur fenêtre glissante (~1s), pas une moyenne cumulée
# Diagnostic : production vue (delta d'index), frames poussées, frames jetées car trop vieilles.
diag_seen = diag_pushed = diag_stale = 0
diag_win  = time.time()

# MODE TRANCHE actif : source PROGRESSIVE uniquement (l'entrelacé garde le chemin historique
# grain-complet — bwdif consomme des trames entières et l'amont committe par champs). Un flux
# amont NON tranché dégénère proprement dans la même boucle : totalSlices=1 → get_slice(h, 1)
# n'aboutit qu'au commit final = attente du grain complet, comportement historique.
_slice_on = SLICE_MODE and IN_SCAN != "i"
if _slice_on:
    print(f"mode tranche actif (slice_lines={{SLICE_LINES}} informatif — le pas vient du grain source)")

while True:
    # Reconnexion après Bus error
    if bus_error.is_set():
        bus_error.clear()
        fermer_shm(reader)
        _gc_domain()          # GC entre close et reopen (flux recréé sous le même nom)
        time.sleep(2)
        reader = ouvrir_shm()
        last_index = 0
        continue

    # Injection de format depuis l'orchestrateur (POST /input {{format:...}})
    if _restart_signal.is_set():
        _restart_signal.clear()
        try: ffmpeg_out.stdin.close()
        except Exception: pass
        try: ffmpeg_out.terminate()
        except Exception: pass
        time.sleep(0.3)
        fermer_shm(reader)
        _gc_domain()          # GC entre close et reopen (flux recréé sous le même nom)
        ffmpeg_out = creer_ffmpeg()
        reader = ouvrir_shm()
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
        if _slice_on:
            # ─── MODE TRANCHE : suivre le grain de TÊTE et écrire le pipe PAR BANDES ───
            # Ordre du pipe rawvideo planar = Y complet PUIS U PUIS V : le plan Y est streamé
            # par bandes au fil des commits partiels du producteur, puis U+V d'un bloc à la
            # dernière tranche (complets à ce moment-là — convention k tranches ⇔ lignes
            # [0, k·slice_height) valides sur les 3 plans). ffmpeg vivant AVANT d'entamer une
            # trame : une trame COMMENCÉE dans le pipe doit TOUJOURS être finie.
            if ffmpeg_out.poll() is not None:
                print("FFmpeg arrêté, redémarrage...")
                _refresh_metrics(up=False)
                time.sleep(1)
                ffmpeg_out = creer_ffmpeg()
            h = reader.head_index()
            if h == bobimxl.MXL_UNDEFINED_INDEX or h <= last_index:
                time.sleep(0.002)
                continue
            # 1ʳᵉ tranche du grain de tête ; tête à peine réclamée ou flux SANS le patch
            # slices → repli get_latest (grain complet, boucle dégénérée sans attente).
            got = reader.get_slice(h, 1, timeout_ns=2_000_000)
            if got is None:
                got = reader.get_latest()
            if got is None or got[0] <= last_index:
                time.sleep(0.002)
                continue
            idx, gi_s, buf = got
            diag_seen += (idx - last_index) if last_index > 0 else 1
            n_buf = len(buf)                          # trame planar complète (= écrit historique)
            y_sz  = WIDTH * HEIGHT * _IN_DBPS         # plan Y complet, en octets
            row   = WIDTH * _IN_DBPS                  # 1 ligne Y
            total = max(1, int(gi_s.totalSlices or 1))
            valid = max(1, int(gi_s.validSlices or 1))
            islh  = max(1, HEIGHT // total)           # lignes Y par tranche (tranches égales)
            # Budget d'attente TOTAL ≈ 1,5 période d'entrée : un producteur en retard ne
            # bloque jamais l'encodeur au-delà d'une demi-trame après le nominal.
            deadl = time.monotonic_ns() + int(1.5e9 / max(1, IN_FPS))
            sent  = 0                                 # octets déjà écrits pour CETTE trame
            try:
                for j in range(1, total + 1):
                    if j > valid:
                        left = deadl - time.monotonic_ns()
                        g = (reader.get_slice(idx, j, timeout_ns=max(1, left))
                             if left > 0 else None)
                        if g is not None:
                            valid = max(j, int(g[1].validSlices or j))
                        else:
                            # TIMEOUT mi-trame → COMPLÉTER, JAMAIS laisser le pipe à moitié
                            # de trame (l'encodeur se désynchroniserait sur tout le flux) :
                            # le reste vient du dernier grain COMPLET (idx-1 — léger tearing
                            # d'UNE image), sinon du noir neutre (secours ultime).
                            gp = reader.get(idx - 1, timeout_ns=2_000_000) if idx > 0 else None
                            src = gp[2] if (gp is not None and len(gp[2]) >= n_buf) else None
                            if src is not None:
                                if y_sz > sent:
                                    ffmpeg_out.stdin.write(src[sent:y_sz])
                                    sent = y_sz
                                ffmpeg_out.stdin.write(src[sent:n_buf])
                            else:
                                ffmpeg_out.stdin.write(_neutral_tail(sent, n_buf, y_sz))
                            sent = n_buf
                            break
                    # Tranche j dispo : delta du plan Y désormais calculable (vue zéro-copie).
                    # À la DERNIÈRE tranche le grain est complet → fin du Y puis U et V d'un bloc.
                    upto = y_sz if j == total else min(y_sz, j * islh * row)
                    if upto > sent:
                        ffmpeg_out.stdin.write(buf[sent:upto])
                        sent = upto
                    if j == total and n_buf > sent:
                        ffmpeg_out.stdin.write(buf[sent:n_buf])
                        sent = n_buf
                ffmpeg_out.stdin.flush()
                _sync_v_mts[0] = reader.last_write_time()   # playhead vidéo (TAI) → suivi audio
                # Latence exposée = âge au COMPLÈTEMENT du grain (lastWriteTime = dernier commit) —
                # les attentes get_slice (suivi du fil ≈ période) n'y entrent pas par construction.
                age_us = (bobimxl.now_tai() - reader.last_write_time()) // 1000
                if 0 <= age_us < 80000:
                    lat_in.push(age_us / 1000.0)
                win_cnt += 1
                diag_pushed += 1
                _el = time.time() - win_start
                if _el >= 1.0:
                    _refresh_metrics(fps=round(win_cnt / _el, 1), up=True)
                    win_cnt = 0; win_start = time.time()
            except BrokenPipeError:
                # Pipe cassé (ffmpeg mort) : trame abandonnée SANS risque de désync — le
                # nouveau process repart sur un pipe vierge, aligné sur une frontière de trame.
                print("BrokenPipe, redémarrage FFmpeg...")
                time.sleep(1)
                ffmpeg_out = creer_ffmpeg()
            last_index = idx
            continue

        if IN_INTERLACED:
            # ─── ENTRELACÉ NATIF : 1 grain = 1 CHAMP → TISSER la paire avant d'encoder ───
            # On apparie les 2 grains-champs de la DERNIÈRE trame COMPLÈTE (index pair = 1er
            # champ, même convention que le recorder / le moteur 2110), on pousse UNE trame
            # pleine à ffmpeg (cadence TRAME), et bwdif désentrelace (cf. _deint_vf).
            # Pousser le grain brut (l'ancien comportement) revenait à encoder UN champ sur
            # deux comme une image de ½ hauteur : 1920×540 écrasé — le défaut signalé.
            if ffmpeg_out.poll() is not None:
                print("FFmpeg arrêté, redémarrage...")
                _refresh_metrics(up=False); time.sleep(1); ffmpeg_out = creer_ffmpeg()
            got = reader.get_latest()
            if got is not None and len(got[2]) == GRAIN_SIZE and got[0] > last_index:
                fi = (got[0] - 1) // 2                 # trame complète = champs 2·fi et 2·fi+1
                if fi >= 0 and (2 * fi + 1) > last_index:
                    g0 = reader.get(2 * fi); g1 = reader.get(2 * fi + 1)
                    if (g0 is not None and g1 is not None
                            and len(g0[2]) == GRAIN_SIZE and len(g1[2]) == GRAIN_SIZE):
                        diag_seen += 1
                        age_us = (bobimxl.now_tai() - reader.last_write_time()) // 1000
                        if age_us < 80000:
                            lat_in.push(age_us / 1000.0)
                            try:
                                ffmpeg_out.stdin.write(_weave_fields(bytes(g0[2]), bytes(g1[2])))
                                ffmpeg_out.stdin.flush()
                                _sync_v_mts[0] = reader.last_write_time()
                                win_cnt += 1; diag_pushed += 1
                                _el = time.time() - win_start
                                if _el >= 1.0:
                                    _refresh_metrics(fps=round(win_cnt / _el, 1), up=True)
                                    win_cnt = 0; win_start = time.time()
                            except BrokenPipeError:
                                print("BrokenPipe, redémarrage FFmpeg...")
                                time.sleep(1); ffmpeg_out = creer_ffmpeg()
                        else:
                            diag_stale += 1
                        last_index = 2 * fi + 1
            time.sleep(0.0005)
            continue

        got = reader.get_latest()

        if ffmpeg_out.poll() is not None:
            print("FFmpeg arrêté, redémarrage...")
            _refresh_metrics(up=False)
            time.sleep(1)
            ffmpeg_out = creer_ffmpeg()

        if got is not None and got[0] > last_index:
            frame_index = got[0]
            diag_seen += frame_index - last_index   # frames PRODUITES depuis la dernière lecture
            age_us = (bobimxl.now_tai() - reader.last_write_time()) // 1000
            if age_us < 80000:
                lat_in.push(age_us / 1000.0)
                try:
                    ffmpeg_out.stdin.write(got[2])    # grain = trame planar (vue numpy, zéro-copie)
                    ffmpeg_out.stdin.flush()
                    _sync_v_mts[0] = reader.last_write_time()   # playhead vidéo (TAI) → suivi audio
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
        fermer_shm(reader)
        _gc_domain()          # GC entre close et reopen (flux recréé sous le même nom)
        time.sleep(2)
        reader = ouvrir_shm()
        last_index = 0
