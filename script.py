# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

import mmap, struct, time, subprocess, threading, json, signal, os, math
from collections import deque
import bobimxl   # migration MXL : entrée vidéo (Reader) + audio (AudioReader gapless) — Phases 1/3


def _mxl_lib_state():
    """Variante libmxl réellement chargée (baseline / x86-64-v3) — diagnostic seul, ne doit
    JAMAIS faire échouer /state."""
    try:
        return bobimxl.lib_info()
    except Exception:
        return None
from http.server import HTTPServer, BaseHTTPRequestHandler

# ─── Config injectée (contrat plugin) ───────────────────────────────
# Les params sont DÉJÀ normalisés (normalize_worker_udp_params) et les destinations
# WebRTC résolues (deploy._resolve_webrtc_destinations) côté orchestrateur avant rendu.
CONFIG         = {config}
HOSTNAME       = "{hostname}"
PLUGIN_VERSION = "{plugin_version}"

# ─── Niveau de log ─────────────────────────────────────────────────────────
# `log_level` (config_schema du plugin, défaut « info ») filtre les impressions du script.
# Le critère n'est PAS « verbeux vs silencieux » mais ÉVÉNEMENT vs MÉTRIQUE :
#   debug   — le lance-flammes : par trame, par bande, décisions internes
#   info    — ÉVÉNEMENTS rares et signifiants  ← DÉFAUT (toujours visible) : démarrage/
#             arrêt, session ouverte/fermée, changement de format, reconnexion, repli sur
#             un chemin dégradé, entrée qui apparaît/disparaît, rebascule.
#   warning — anomalies et replis subis
#   error   — échecs
# RÈGLE 1 : après une panne, le journal PAR DÉFAUT doit permettre de RECONSTITUER
#   l'histoire. Élever le niveau après coup ne récupère RIEN : ce qui n'a pas été écrit
#   est perdu. On ne coupe donc pas l'information, on coupe la redondance.
# RÈGLE 2 : une MÉTRIQUE PÉRIODIQUE (fps, compteurs) ne se journalise PAS — elle est déjà
#   publiée sur :8080 et échantillonnée par l'orchestrateur. La journaliser duplique la
#   mesure ET consomme la fenêtre de rétention (journal Docker non roté : le bruit purge
#   les lignes utiles anciennes). Au mieux `debug`.
# RÈGLE 3 : un événement qui peut partir EN RAFALE s'AGRÈGE sur une fenêtre et sort en UNE
#   ligne périodique (« N frames lentes sur la dernière minute, pire … ») — le signal
#   reste, le spam disparaît.
# Réglable à chaud, sans redéployer, quand le plugin expose l'endpoint de contrôle :
# POST :8082/log_level {{"level": "debug"}} (exposé aux macros via param_tree/actions).
_LOG_ORDER = {{"debug": 10, "info": 20, "warning": 30, "error": 40}}
LOG_LEVEL = str(CONFIG.get("log_level") or "info").strip().lower()
if LOG_LEVEL not in _LOG_ORDER:
    LOG_LEVEL = "info"
_LOG_MIN = _LOG_ORDER[LOG_LEVEL]


def log(msg, niveau="info"):
    """Impression gatée par le niveau de log courant (défaut du message : « info »)."""
    if _LOG_ORDER.get(niveau, 20) >= _LOG_MIN:
        print(msg, flush=True)


def set_log_level(niveau):
    """Change le niveau à chaud. Renvoie True si le niveau est reconnu."""
    global LOG_LEVEL, _LOG_MIN
    lv = str(niveau or "").strip().lower()
    if lv not in _LOG_ORDER:
        return False
    LOG_LEVEL, _LOG_MIN = lv, _LOG_ORDER[lv]
    return True



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
# Instance servant les LECTEURS VIDÉO. Renouvelable (cf. _renouveler_instance_video) pour
# obtenir un cache vierge sans jamais balayer le domaine ; l'AudioReader, lui, garde `inst`.
_inst_video = [inst]
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
# (ex-shm_video_ring/RING_SIZE/HEADER_SIZE/TOTAL_SIZE : dimensionnaient un ancien shm maison ;
# l'entrée vidéo passe par bobimxl (MXL) — profondeur gérée par domaine, cf. réglage de nœud
# mxl_history_ms. Retiré 2026-08-09, constante morte.)
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


def _out_pix_fmt(out_chroma, vcodec, webrtc):
    """Chroma RÉELLEMENT encodé. Deux cas où honorer le réglage produirait un flux illisible :
    - une branche WebRTC : les navigateurs ne décodent que le H.264 4:2:0 ; un 4:2:2 ou 4:4:4
      part en profil High 4:2:2 / High 4:4:4 Predictive, que la plupart refusent ou saccadent ;
    - `h264_nvenc` ne sait pas le 4:2:2 : ffmpeg choisit alors SEUL le yuv444p (constaté sur le
      moniteur le 2026-10-11 : « High 4:4:4 Predictive ») au lieu de refuser.
    Dans les deux cas on rabat sur yuv420p ; le 4:4:4 en NVENC hors WebRTC reste honoré."""
    if webrtc:
        return "yuv420p"
    if vcodec == "h264_nvenc" and out_chroma == "422":
        return "yuv420p"
    return _PIX_FMT.get(out_chroma, "yuv422p")
_ICW, _ICH  = _CHROMA_DIV[IN_CHROMA]         # diviseurs chroma de l'ENTRÉE
_IN_BPP     = (1.0 + 2.0 / (_ICW * _ICH)) * _IN_DBPS   # octets/pixel d'entrée (incl. profondeur)
FRAME_SIZE = int(WIDTH * HEIGHT * _IN_BPP)    # TRAME pleine (ce qu'on pousse à ffmpeg)
GRAIN_H    = HEIGHT                            # hauteur d'un GRAIN (= ½ trame si entrelacé)
GRAIN_SIZE = FRAME_SIZE                        # taille d'un GRAIN (= ½ trame si entrelacé)

def _recalc_sizes():
    """Recalcule tous les dérivés de (WIDTH, HEIGHT, IN_CHROMA, BIT_DEPTH, IN_INTERLACED).
    WIDTH/HEIGHT sont TOUJOURS des dims de TRAME ; le GRAIN vaut ½ trame en entrelacé."""
    global _IN_DEEP, _IN_DBPS, _IN_SUF, IN_PIX_FMT, _ICW, _ICH, _IN_BPP
    global FRAME_SIZE, GRAIN_H, GRAIN_SIZE
    _IN_DEEP    = BIT_DEPTH >= 10
    _IN_DBPS    = 2 if _IN_DEEP else 1
    _IN_SUF     = (("12le" if BIT_DEPTH >= 12 else "10le") if _IN_DEEP else "")
    IN_PIX_FMT  = _PIX_FMT[IN_CHROMA] + _IN_SUF
    _ICW, _ICH  = _CHROMA_DIV[IN_CHROMA]
    _IN_BPP     = (1.0 + 2.0 / (_ICW * _ICH)) * _IN_DBPS
    FRAME_SIZE  = int(WIDTH * HEIGHT * _IN_BPP)
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
    # CADENCE D'ENTRÉE — lue dans TOUS les cas, progressif compris.
    #
    # ⚠ Cette lecture était conditionnée à `IN_INTERLACED`. En progressif, `IN_FPS` gardait donc
    # son défaut de 25 quelle que soit la source. Conséquence pour une source 50p : ffmpeg était
    # lancé avec `-r 25` (cf. `creer_ffmpeg`), et comme `OUT_FPS (50) != IN_FPS (25)`, un filtre
    # `fps=50` s'ajoutait par-dessus — qui DUPLIQUE. Le streamer poussait bien 50 images
    # DISTINCTES, ffmpeg les horodatait à 25 et en refabriquait le double : une image neuve sur
    # deux était jetée. Mesuré le 2026-08-02 : 50,0 % de trames identiques à la précédente en
    # sortie, sur x264 comme sur NVENC.
    #
    # Le défaut était MUET : `:8080 fps`, `in_fps_seen`, `pushed_fps`, `grain_rate` du flowDef et
    # la cadence du .ts encodé annonçaient tous 50, et avaient tous raison séparément — aucun ne
    # mesure le CONTENU NEUF. Il n'a été vu qu'à l'œil, sur une mire dont la barre se figeait.
    #
    # En entrelacé la cadence déclarée reste celle de la TRAME (on pousse des trames tissées),
    # d'où la préférence donnée à `frame_fps_*` ; en progressif grain = trame, les deux coïncident.
    fn = int(f.get("frame_fps_num") or f.get("fps_num") or 0)
    fd = int(f.get("frame_fps_den") or f.get("fps_den") or 1) or 1
    if fn > 0:
        IN_FPS = max(1, int(round(fn / fd)))
    _recalc_sizes()

# Anti-rafale des attentes (règle 3 « Niveau de log ») : ces boucles tournaient à 1 ligne/s
# tant que la source manquait. On ne journalise que l'ENTRÉE dans l'état d'attente et le
# RÉTABLISSEMENT — l'état courant reste lisible sur :8080.
_wait_warned = [False]
_open_warned = [False]


def _detect_dims():
    """Résout le format d'ENTRÉE depuis le flow_def du flux MXL (source de vérité côté donnée),
    entrelacement compris. Attend que le flux existe."""
    while True:
        try:
            r = bobimxl.Reader(_inst_video[0], SHM_NAME)
            f = r.format(); r.close()
            if f:
                if _wait_warned[0]:
                    _wait_warned[0] = False
                    log(f"detect-dims: flux MXL {{SHM_NAME}} disponible", "info")
                return f
        except Exception:
            pass
        if not _wait_warned[0]:      # une seule ligne à l'ENTRÉE dans l'attente (pas 1/s)
            _wait_warned[0] = True
            log(f"detect-dims: attente du flux MXL {{SHM_NAME}}…", "warning")
        time.sleep(1)

# Signale au main loop de fermer/rouvrir le shm et relancer ffmpeg (injection format à chaud).
_restart_signal = threading.Event()
# ─── Choix de l'encodeur : logiciel (x264/x265) ou matériel (NVENC) ──────────
# `video.encoder` vaut "cpu" (logiciel imposé), "nvenc" (matériel EXIGÉ) ou "auto" (matériel s'il
# est utilisable, logiciel sinon — et on le DIT). Le mode matériel n'est possible que si le
# conteneur a reçu une carte ET la capacité pilote `video` : `--gpus` seul n'accorde que
# compute/utility, `libnvidia-encode` n'est alors PAS injectée et ffmpeg échoue sur un
# « Invalid argument (-22) » qui ne nomme pas la cause. On teste donc la présence de la
# bibliothèque plutôt que d'attendre l'échec.
_ENC_DEMANDE = str(VIDEO_CFG.get("encoder", "cpu")).strip().lower() or "cpu"


def _nvenc_dispo():
    """La pile d'encodage matériel est-elle réellement utilisable dans CE conteneur ?"""
    import glob as _glob
    return bool(_glob.glob("/usr/lib/x86_64-linux-gnu/libnvidia-encode.so*")
                or _glob.glob("/usr/lib/*/libnvidia-encode.so*"))


_NVENC_OK = _nvenc_dispo()
if _ENC_DEMANDE == "nvenc" and not _NVENC_OK:
    # Échec BRUYANT et immédiat : l'exploitant a EXIGÉ le matériel. Repartir en x264 en silence
    # ferait mentir le calcul de capacité — on croirait économiser un cœur par flux en les
    # consommant. Le message nomme la cause exacte, pas le symptôme ffmpeg.
    log("encodeur NVENC exigé mais indisponible dans ce conteneur : aucune libnvidia-encode. "
        "Le nœud n'a pas de GPU alloué, ou la capacité pilote `video` manque "
        "(NVIDIA_DRIVER_CAPABILITIES). Régler video.encoder sur `auto` pour tolérer le repli.",
        "error")
ENCODER = ("nvenc" if (_ENC_DEMANDE == "nvenc"
                       or (_ENC_DEMANDE == "auto" and _NVENC_OK)) else "cpu")
if _ENC_DEMANDE == "auto":
    log("encodeur auto → %s" % ("NVENC (matériel)" if _NVENC_OK else "x264/x265 (logiciel, "
                                "aucun GPU utilisable dans ce conteneur)"), "info")

_VC_LOGICIEL = {{"h264": "libx264", "h265": "libx265"}}
_VC_NVENC = {{"h264": "h264_nvenc", "h265": "hevc_nvenc"}}
VCODEC = (_VC_NVENC if ENCODER == "nvenc" else _VC_LOGICIEL).get(
    VIDEO_CFG.get("codec", "h264"), "libx264")

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
# retiré (media_ts audio biaisé + inefficace) ; (3) ⛔ « le transport WebRTC réaligne l'A/V tout
# seul » était FAUX : la passerelle date les paquets à l'arrivée, un décalage de pts se retrouve
# tel quel dans le navigateur (mesuré 2026-10-11, cf. WEBRTC_AV_COMP_MS).
# Réglage déclaré au manifeste (config_schema, ±2000 ms) depuis 0.19.6. Avant, la liste blanche de
# app/scripts.normalize_worker_udp_params le jetait : il n'atteignait jamais le script (2026-10-11).
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
            # ENCODEUR RÉELLEMENT ACTIF, dans les métriques de base — pas seulement dans `/state`.
            # C'est ici qu'on regarde quand un flux est muet, et `in_fps_seen` élevé face à
            # `pushed_fps` nul dit déjà « la lecture va, l'encodage meurt » : l'encodeur doit être
            # sur la MÊME page, sinon il faut aller le chercher ailleurs pour nommer la cause.
            # `encoder_replie` = on a DÉCLASSÉ le matériel en cours de route (cf.
            # _replier_si_encodeur_refuse) — un flux qui tourne à un coût CPU non budgété.
            "encoder": ENCODER, "encoder_demande": _ENC_DEMANDE, "encoder_replie": False,
            # ORIGINE DES DEUX LIGNES DE TEMPS. Les deux entrées de ffmpeg sont BRUTES
            # (rawvideo, f32le) : aucune ne porte d'horodatage, leurs pts viennent du seul NOMBRE
            # d'éléments écrits. La trame 0 et l'échantillon 0 sont donc appariés quoi qu'il
            # arrive, et l'écart entre les instants où les deux flux COMMENCENT À COULER part tel
            # quel dans le flux émis, constant, pour tous les consommateurs qui honorent les pts.
            # Négatif = l'audio a commencé AVANT la vidéo (l'audio sortira en retard d'autant).
            "av_origine_ms": None,
            # CE QU'ON ÉCRIT, des deux côtés, converti en millisecondes de média. ffmpeg dérive
            # les pts de nos deux entrées BRUTES du seul NOMBRE d'éléments écrits : ce compteur
            # est donc, à la milliseconde près, le calage A/V que NOUS lui donnons. Le comparer à
            # l'écart réellement émis dit de quel côté se trouve le défaut — chez nous, ou dans
            # ffmpeg. Positif = on a écrit plus d'audio que de vidéo (l'audio sortira en avance).
            "av_ecriture_ms": None,
            # Les deux compteurs BRUTS, en ms de média : `av_ecriture_ms` est leur différence, et
            # une différence ne dit pas QUI bouge. Pendant le transitoire de démarrage, c'est la
            # seule façon de voir si l'audio s'emballe ou si la vidéo rattrape.
            "av_ecrit_video_ms": None, "av_ecrit_audio_ms": None,
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
    _ecriture_maj()
    metrics["destinations"] = _dests_summary(up)

# Flag pour signaler une Bus error
bus_error = threading.Event()

def handle_sigbus(signum, frame):
    log("SIGBUS reçu — SHM invalidé, reconnexion...", "warning")
    bus_error.set()

signal.signal(signal.SIGBUS, handle_sigbus)

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        payload = dict(metrics)
        # Bouches-trous amont ÉCARTÉS (cf. bobimxl.MXL_GRAIN_FLAG_INVALID) : seul témoin du
        # comblement d'un trou par le producteur — cadence, index et statut de lecture restent
        # tous parfaits pendant l'épisode. Calculé à la requête : aucun coût par trame.
        payload["invalid_grains"] = bobimxl.invalid_grains_report()
        # Version RÉELLEMENT en cours (contrat :8080) : l'orchestrateur la compare à la base après un
        # redéploiement et à chaque tour de surveillance — un processus resté sur l'ancienne version
        # ne se voit nulle part ailleurs (Z8, 29/10).
        payload["plugin_version"] = PLUGIN_VERSION
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

# ─── REPLI MATÉRIEL → LOGICIEL À L'EXÉCUTION ────────────────────────────────────────────────
#
# `_nvenc_dispo()` teste la PRÉSENCE de `libnvidia-encode` — nécessaire, pas suffisant. Il attrape
# « pas de GPU / capacité pilote `video` absente », décidés au démarrage. Il ne peut RIEN contre
# une carte qui devient inutilisable ENSUITE : le fichier reste là, les `/dev/nvidia*` aussi, et
# seul l'accès cgroup au périphérique a disparu.
#
# ★ VU EN PRODUCTION (Horace, 2026-08-28) : un `systemctl daemon-reload` sur le nœud a révoqué le
# GPU du conteneur de monitoring déjà lancé (mode de panne documenté : les périphériques sont
# injectés à la CRÉATION, un reload réapplique les règles cgroup et retire l'accès aux conteneurs
# en marche). `libnvidia-encode` toujours présente ⇒ `_NVENC_OK` vrai ⇒ ffmpeg lancé en
# `h264_nvenc` ⇒ « Invalid argument (-22) » ⇒ relancé ⇒ re-échoue. **Pendant des JOURS** :
# `pushed_fps` 0,7 pour `in_fps_seen` 50,8, et aucune alerte — le conteneur était « running ».
#
# Le contrat de `auto` était donc à moitié tenu : il promet de retomber sur le logiciel « et de le
# DIRE », mais ne couvrait que l'absence PRÉVUE, pas la panne IMPRÉVUE. Un repli qui ne couvre que
# ce qu'on avait anticipé n'est pas un repli.
#
# Règle : trois relances rapprochées d'affilée = l'encodeur ne démarre pas. En `auto` on DÉCLASSE
# en logiciel et on le crie ; en `nvenc` EXPLICITE on ne déclasse jamais — l'exploitant a exigé le
# matériel, et un repli muet ferait mentir le calcul de capacité (on croirait économiser des cœurs
# en les consommant). On ne teste PAS le périphérique par une sonde : ouvrir la carte pour la
# sonder, c'est la détenir (piège déjà rencontré) — l'échec répété est un signal suffisant.
_ff_relance = {{"t": 0.0, "courtes": 0}}
_REPLI_RELANCES = 3      # relances rapprochées consécutives avant de déclasser
_REPLI_FENETRE_S = 8.0   # « rapprochée » = ffmpeg n'a pas tenu ce temps


def _replier_si_encodeur_refuse():
    """Déclasse NVENC → logiciel quand ffmpeg meurt en boucle, en mode `auto` seulement."""
    global ENCODER, VCODEC
    _t = time.monotonic()
    _ff_relance["courtes"] = (_ff_relance["courtes"] + 1
                              if _ff_relance["t"] and (_t - _ff_relance["t"]) < _REPLI_FENETRE_S
                              else 0)
    _ff_relance["t"] = _t
    if (ENCODER != "nvenc" or _ENC_DEMANDE != "auto"
            or _ff_relance["courtes"] < _REPLI_RELANCES):
        return
    ENCODER = "cpu"
    VCODEC = _VC_LOGICIEL.get(VIDEO_CFG.get("codec", "h264"), "libx264")
    _ff_relance["courtes"] = 0
    metrics["encoder"] = ENCODER
    metrics["encoder_replie"] = True
    log("encodeur MATÉRIEL en échec répété (%d relances en moins de %.0f s) — repli sur %s. "
        "libnvidia-encode est présente : la carte a très probablement été révoquée sous le "
        "conteneur (un `systemctl daemon-reload` sur le nœud le fait). Le flux repart en "
        "LOGICIEL, donc à un coût CPU qui n'était pas budgété — recréer le conteneur rend "
        "l'accès au GPU."
        % (_REPLI_RELANCES, _REPLI_FENETRE_S, VCODEC), "error")


def _fifo_path(gen):
    return "%s.%d" % (AUDIO_FIFO, gen)


def _new_generation():
    """Nouvel encodeur = nouvelle ligne de temps ET nouveau fifo audio. Un fifo PAR encodeur : sur
    un fifo partagé, le feeder encore branché sur l'ancien descripteur écrivait dans le nouvel
    ffmpeg (Linux ne casse le tube que s'il n'a plus AUCUN lecteur) les ≤120 ms tamponnés de
    l'ancienne ligne de temps. Posé AVANT le Popen : le feeder ne doit jamais voir un ffmpeg neuf
    sous l'ancienne génération."""
    old = _av["gen"]
    _av["anchor_ns"] = None; _av["skip_ns"] = 0; _av["hint_ns"] = None
    if AUDIO_ENABLED:
        try:
            if not os.path.exists(_fifo_path(old + 1)):
                os.mkfifo(_fifo_path(old + 1))
        except Exception as e:
            log(f"audio fifo non créé : {{e}}", "warning")
        try: os.unlink(_fifo_path(old))
        except Exception: pass
    _av["gen"] = old + 1


def creer_ffmpeg():
    _replier_si_encodeur_refuse()
    _new_generation()
    # -r d'entrée = cadence du signal reçu : EFF_FPS en mode moniteur (le feeder cadence
    # lui-même à cette valeur), sinon la cadence native du pipeline (IN_FPS).
    in_rate = EFF_FPS if HOT_INPUT else IN_FPS
    _ecrit_rate[0] = float(in_rate or 25)
    # Calage A/V MANUEL (réglage `av_offset_ms`, signé, ms) appliqué via les PTS : >0 → retarder
    # l'audio (adelay, cf. _audio_plan) ; <0 → retarder la vidéo (-itsoffset sur pipe:0, pas de
    # resampler côté vidéo donc l'itsoffset survit). 0 = aucun décalage (défaut). N'agit que sur les
    # sorties qui honorent les PTS (mpegts/fichier) et sur le WebRTC, qui ne réaligne RIEN : la
    # passerelle date les paquets à l'arrivée (mesuré 2026-10-11, cf. l'en-tête du calage plus haut).
    av = int(CONFIG.get("av_offset_ms") or 0) * 1000000 if AUDIO_ENABLED else 0
    a_delay_ms = int(round(av / 1e6)) if av > 0 else 0
    metrics["av_offset_ms"] = int(round(av / 1e6))        # délai appliqué (signé), exposé sur :8080
    # Calage A/V : les deux entrées sont BRUTES, ffmpeg en dérive les pts du seul NOMBRE d'éléments
    # lus. Le calage est donc ce qu'on écrit à chaque position — cf. ANCRE A/V (feeder ancré sur la
    # trame 0, vidéo moniteur cadencée sur t0 + k/fps). Historique : de 2026-08-25 à 2026-10-11 le
    # calage variait d'un démarrage à l'autre (+58/+73/+82 ms en UDP, −430…−500 ms au moniteur) ;
    # mesuré après l'ancre : pts à 0 ± 20 ms (une image à 25p) sur 8 démarrages.
    # `-thread_queue_size` : ffmpeg draine pipe:0 indépendamment (sinon bloqué pendant la
    # configuration du filtre [1:a]).
    vin = ["-thread_queue_size", "512",
           "-f", "rawvideo", "-pix_fmt", IN_PIX_FMT,
           "-s", f"{{WIDTH}}x{{HEIGHT}}", "-r", str(in_rate)]
    if av < 0:                                            # vidéo en avance → la retarder
        vin += ["-itsoffset", f"{{-av / 1e9:.3f}}"]
    vin += ["-i", "pipe:0"]
    cmd = ["ffmpeg"] + vin
    ts_sel = webrtc_sel = None
    vchain = _video_filter()
    _opix = _out_pix_fmt(OUT_CHROMA, VCODEC, _has_webrtc())
    metrics["out_pix_fmt"] = _opix
    if ENCODER == "nvenc":
        # ⚠ Les options de x264 ne sont PAS celles de NVENC : `-preset` prend p1…p7 (et non
        # `ultrafast`), `-tune zerolatency` n'existe pas (c'est `ll`/`ull`), et le débit constant
        # se demande par `-rc cbr`. Passer les options logicielles à NVENC fait échouer ffmpeg.
        vopts = ["-c:v", VCODEC,
                 "-preset", str(VIDEO_CFG.get("nvenc_preset", "p1")),
                 "-tune", str(VIDEO_CFG.get("nvenc_tune", "ull")),
                 "-rc", "cbr", "-b:v", str(VIDEO_CFG.get("bitrate", "4M")),
                 "-delay", "0", "-zerolatency", "1",
                 "-pix_fmt", _opix,                     # chroma de SORTIE (cf. _out_pix_fmt)
                 "-g", str(GOP), "-keyint_min", str(GOP), "-no-scenecut", "1"]
    else:
        vopts = ["-c:v", VCODEC,
                 "-preset", str(VIDEO_CFG.get("preset", "ultrafast")),
                 "-tune", "zerolatency",
                 "-b:v", str(VIDEO_CFG.get("bitrate", "4M")),
                 "-pix_fmt", _opix,                     # chroma de SORTIE (cf. _out_pix_fmt)
                 "-g", str(GOP), "-keyint_min", str(GOP), "-sc_threshold", "0"]
    # Colorimétrie : flags optionnels (vides => laissés à l'auto ffmpeg).
    for _flag, _key in (("-color_primaries", "color_primaries"),
                        ("-color_trc", "color_trc"),
                        ("-colorspace", "colorspace")):
        _val = str(VIDEO_CFG.get(_key) or "").strip()
        if _val:
            vopts += [_flag, _val]
    if VCODEC in ("libx265", "hevc_nvenc"):
        vopts += ["-tag:v", "hvc1"]      # HEVC : le tag vaut pour les deux encodeurs

    if AUDIO_ENABLED:
        cmd += ["-thread_queue_size", "512",
                "-f", "f32le", "-ar", str(A_SAMPLE_RATE), "-ac", str(OUT_CHANNELS),
                "-i", _fifo_path(_av["gen"])]
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

    if _ENC_DEMANDE == "nvenc" and not _NVENC_OK:
        # REFUS, pas crash-loop. Lancer ffmpeg avec un encodeur absent le ferait échouer toutes
        # les secondes sur « Invalid argument (-22) », l'agent le relancerait sans fin, et le
        # journal se remplirait d'un message qui ne nomme pas la cause. On ne le lance pas : le
        # conteneur reste debout, `/state` porte la raison, et l'exploitant la lit.
        raise RuntimeError(
            "encodage NVENC EXIGÉ mais aucune pile matérielle dans ce conteneur "
            "(libnvidia-encode absente). Cause probable : aucun GPU alloué sur ce nœud, ou "
            "capacité pilote `video` manquante. Basculer video.encoder sur `auto` pour "
            "tolérer le repli logiciel.")

    cmd += ["-flush_packets", "1"]
    branches = _build_outputs(ts_sel, webrtc_sel)
    if branches:
        cmd += ["-f", "tee", "|".join(branches)]
    else:
        cmd += ["-f", "null", "-"]
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    # Nouvel encodeur = nouvelle origine pour les deux lignes de temps.
    _origine["video"] = _origine["audio"] = None
    _ecrit["video"] = _ecrit["audio"] = 0
    metrics["av_origine_ms"] = None
    metrics["av_ecriture_ms"] = metrics["av_ecrit_video_ms"] = metrics["av_ecrit_audio_ms"] = None
    proc.stdin = _StdinTemoin(proc.stdin)
    threading.Thread(target=_drain_stderr, args=(proc,), daemon=True).start()
    return proc

# ─── Origine des deux lignes de temps (mesure du calage A/V) ─────────
# On date la PREMIÈRE écriture de chaque essence vers ffmpeg. Leur écart EST le calage A/V du
# flux émis (cf. metrics["av_origine_ms"]) : c'est la seule grandeur qui explique un décalage
# CONSTANT, là où une différence de fraîcheur de lecture (`inputs_latency_ms`) expliquerait un
# décalage variable. Posé à UN endroit par essence — pour la vidéo, en enveloppant stdin plutôt
# qu'en touchant les huit points d'écriture (entière, tranche, weave, secours) : un chemin
# oublié n'aurait faussé la mesure que sur ce chemin-là.
_origine = {{"video": None, "audio": None}}
_ecrit = {{"video": 0, "audio": 0}}      # trames et échantillons écrits vers ffmpeg
_ecrit_rate = [25.0]                    # cadence DÉCLARÉE à ffmpeg (-r), posée par creer_ffmpeg

# ─── ANCRE A/V : les deux entrées de ffmpeg sur UNE ligne de temps, celle du bus (TAI) ─────────
# ffmpeg date des entrées BRUTES par leur seul compte : trame k ↔ k/fps, échantillon n ↔ n/48000.
# Le calage du flux émis est donc entièrement décidé par CE QU'ON ÉCRIT à la position k ou n. Le
# feeder d'avant écrivait « ce qui est frais quand ffmpeg lit » et comblait les creux par un silence
# cadencé sur une horloge À LUI : deux lignes de temps sans rapport. Mesuré le 2026-10-11 sur le
# moniteur : son EN AVANCE de 430-500 ms, bips espacés de 0,91 à 1,07 s pour 1,000 s à la source —
# et, au démarrage d'un encodeur NVENC (≈1 s d'init pendant laquelle le fifo est plein), une dette
# de silence ≥ l'anneau audio (~0,3 s) qui ne se résorbait JAMAIS : écrire B ms de silence prend
# B ms quand ffmpeg consomme en temps réel, la position de lecture tombait hors de l'anneau à chaque
# passage, on se recalait sur la tête, rien n'y était encore complet → re-silence. 100 % de silence
# sur 7 redéploiements sur 7, son intact sur le bus.
# Règle désormais : l'échantillon n écrit est l'échantillon du bus d'index A0 + n, où A0 est
# l'instant TAI du CONTENU de la trame 0 de CET encodeur. Un bloc absent du bus (source coupée,
# anneau dépassé) est remplacé par du silence À SA PLACE — la ligne de temps ne glisse jamais.
# `gen` change à chaque ffmpeg : un feeder resté branché sur le fifo d'un encodeur précédent
# (le nouveau rouvre le même fifo avant que l'ancien descripteur ne casse) se ré-ancre.
_av = {{"gen": 0, "anchor_ns": None, "skip_ns": 0, "hint_ns": None}}
A_LATE = A_SAMPLE_RATE // 5        # 200 ms : source absente/figée depuis cet âge → silence à sa place
A_BATCH = 40                       # blocs max par passage du feeder (rattrapage sans affamer la boucle)
A_OFFGRID = A_SAMPLE_RATE         # tête à plus d'1 s de la grille TAI → source hors grille, rebasée


def _tai_samples(ns):
    """Instant TAI (ns) → index d'échantillon à 48 kHz (même base que l'index d'un flux audio MXL)."""
    return int(ns) * A_SAMPLE_RATE // 1_000_000_000


def _anchor_from_hint(hint_ns, now_ns, video_age_ms=None):
    """Instant TAI du CONTENU de la trame 0. `hint_ns` = instant du grain poussé (index_time du
    flux, ou lastWriteTime) ; écarté s'il est à plus d'1 s de l'horloge (writer à index libre, flux
    sans cadence lisible). Repli : maintenant moins l'âge moyen mesuré de la vidéo."""
    if hint_ns is not None and abs(int(hint_ns) - int(now_ns)) < 1_000_000_000:
        return int(hint_ns)
    return int(now_ns) - int((video_age_ms or 0) * 1_000_000)


def _audio_step(pos, head, now_s, src_off, block, late, alive=True):
    """Décision du feeder pour le bloc [pos, pos+block) :
    - "read"    : le bus l'a (tête au-delà) → le lire (silence à sa place s'il est sorti de l'anneau) ;
    - "silence" : absent, source MORTE ou absente (pas de lecteur, tête figée), et déjà en retard
                  de `late` sur l'horloge (+ décalage de la source) → comblé À CETTE POSITION ;
    - "wait"    : pas encore écrit → attendre. Une source VIVANTE n'est jamais comblée d'avance :
                  si elle est en retard, on l'attend (latence en plus, calage intact) — combler
                  avant elle, c'était jeter ensuite le vrai son qui arrive.
    `head` None = pas de lecteur / tête indéfinie."""
    if head is not None and pos + block <= head:
        return "read"
    if (head is None or not alive) and pos + block <= now_s + src_off - late:
        return "silence"
    return "wait"


def _src_offset_update(st, head, now_s, t, tol, settle_s, sr):
    """Décalage de la ligne d'index de la source par rapport à la grille TAI (en échantillons).
    L'index audio MXL est normativement l'instant TAI ; un writer à compteur libre en est loin, et
    le suivre sur la grille ferait attendre (ou combler) pour toujours. On n'ADOPTE un nouveau
    décalage que s'il est STABLE pendant `settle_s` : la tête d'un flux à
    peine créé vaut ~0 un instant (vu le 2026-10-11 sur une réplique RDMA neuve : −1,8e12
    échantillons), et adopter cette valeur-là laissait le feeder à des heures de la tête, en
    silence pour toujours. `st` = {{"off": décalage adopté, "cand": candidat ou None}}."""
    off = int(head) - int(now_s)
    want = off if abs(off) > tol else 0
    if abs(want - st["off"]) <= tol:
        st["cand"] = None
        return st["off"]
    c = st.get("cand")
    if c is None or abs(off - c[0]) > sr // 40:          # candidat neuf (ou instable : ±25 ms)
        st["cand"] = (off, t)
        return st["off"]
    # Stable à ±25 ms sur `settle_s` : une tête FIGÉE ne l'est jamais (l'horloge avance, l'écart
    # dérive de 100 ms par 100 ms), une tête transitoire non plus.
    if t - c[1] >= settle_s:
        st["off"] = want
        st["cand"] = None
    return st["off"]


# ─── Compensation du transport WebRTC ──────────────────────────────────────────────────────────
# ⚠ Ne vaut QUE pour une passerelle qui date à l'arrivée. Depuis services/webrtc_gateway 0.3.0
# (MediaMTX 1.21.1, `useAbsoluteTimestamp`), la passerelle garde la date NTP des SR de ffmpeg :
# la vue navigateur = calage des pts, la compensation tombe à 0. Ce qu'elle applique RÉELLEMENT est
# injecté par le hook (`gateway_absolute_timestamp` sur chaque destination WebRTC, depuis le réglage
# `webrtc_absolute_timestamp`, lui-même écrit d'après le :8080 de la passerelle). Mesuré 2026-10-12
# (avsync, client RTSP/RTCP, 4 redémarrages) : compensation 0 → −27…+13 ms en NVENC comme en x264 ;
# compensation 90 gardée sur la passerelle horodatée → +13…+74 ms.
# Historique — la passerelle (MediaMTX 1.9) n'utilise PAS les pts ni les SR RTCP de ffmpeg : elle date chaque
# paquet reçu à son HEURE D'ARRIVÉE, et c'est cette date qui part dans les SR vers le navigateur —
# lequel cale l'image et le son dessus. Le calage vu par le navigateur = calage des pts + (retard
# d'émission de la vidéo − celui de l'audio). Mesuré le 2026-10-11 (NVENC p4/ull, 1080p25 8 Mb/s,
# client RTSP qui lit les SR de la passerelle comme le ferait un navigateur, avsync) : pts à 0/−20 ms
# mais SR à −70…−117 ms (son en avance), 5 démarrages. Décaler le son de N ms dans les pts décale
# la vue navigateur d'autant (vérifié : +100 → −80 devient +20). D'où cette compensation, appliquée
# à l'ancre audio QUAND LA SORTIE EST WEBRTC SEULE (le moniteur) : une sortie mpegts (UDP/SRT)
# honore les pts, elle serait décalée d'autant — on ne sert pas deux maîtres avec un seul encode.
WEBRTC_AV_COMP_MS = 90


def _webrtc_comp_ms(has_ts, has_webrtc, gateway_abs=False):
    if gateway_abs:              # la passerelle transmet les dates de l'encodeur : rien à corriger
        return 0
    return WEBRTC_AV_COMP_MS if (has_webrtc and not has_ts) else 0


def _gateway_abs_ts():
    """Vrai si TOUTES les destinations WebRTC actives passent par une passerelle à horodatage absolu
    (une seule passerelle qui date à l'arrivée suffit à garder l'ancienne compensation)."""
    w = [d for d in DESTINATIONS if d.get("type") == "webrtc" and d.get("enabled") and d.get("ingest_url")]
    return bool(w) and all(d.get("gateway_absolute_timestamp") for d in w)


def _video_due(elapsed_s, fps, pushed, max_behind):
    """Mode moniteur : trames à pousser MAINTENANT pour que la trame k parte à t0 + k/fps.
    Renvoie (à_pousser, à_sauter). La trame k doit représenter l'instant t0 + k/fps, sinon la vidéo
    glisse par rapport à l'audio (ancré sur le bus) : un retard (init de l'encodeur, pipe plein) se
    RATTRAPE en répétant la dernière image. Au-delà de `max_behind` trames de retard on SAUTE des
    créneaux plutôt que de rafaler — et l'audio saute la même durée (cf. _av["skip_ns"])."""
    due = int(elapsed_s * fps) + 1
    behind = due - pushed
    if behind <= 0:
        return 0, 0
    skip = behind - 1 if behind > max_behind else 0
    return behind - skip, skip

# ⚠ PLAFONNER L'AVANCE DE L'AUDIO A ÉTÉ ESSAYÉ ET NE MARCHE PAS (2026-08-25). L'idée était de
# tenir les deux compteurs en phase, puisque ffmpeg apparie positionnellement l'échantillon 0 avec
# la trame 0. Mesuré : le plafond DÉPLACE le calage d'environ 130 ms (les écarts passent de +58…+82
# à −41…−70 ms) mais n'enlève RIEN à la variabilité d'un démarrage à l'autre — et trois plafonds
# différents (100, 250, 400 ms) donnent le MÊME résidu de 240 ms, parce que le garde-fou qui doit
# relâcher l'invariant quand la vidéo est à l'arrêt s'ouvre pendant le démarrage, c'est-à-dire
# pendant la seule phase qui compte. Ne pas le refaire tel quel : la variabilité résiduelle
# (±20 ms) se joue ailleurs, dans le démarrage de ffmpeg lui-même. Cf. TODO.md.
def _ecriture_maj():
    r = _ecrit_rate[0] or 25.0
    v_ms = _ecrit["video"] * 1000.0 / r
    a_ms = _ecrit["audio"] / (A_SAMPLE_RATE / 1000.0)
    metrics["av_ecrit_video_ms"] = round(v_ms, 1)
    metrics["av_ecrit_audio_ms"] = round(a_ms, 1)
    metrics["av_ecriture_ms"] = round(a_ms - v_ms, 1)

def _origine_note(essence):
    if _origine[essence] is not None:
        return
    _origine[essence] = bobimxl.now_tai()
    v, a = _origine["video"], _origine["audio"]
    if v is not None and a is not None:
        metrics["av_origine_ms"] = round((a - v) / 1e6, 1)
        log(f"calage A/V : l'audio commence à couler {{(a - v) / 1e6:+.1f}} ms après la vidéo "
            f"(négatif = avant ; cet écart part tel quel dans le flux émis)", "info")

class _StdinTemoin:
    """Enveloppe le stdin de ffmpeg pour dater la PREMIÈRE écriture vidéo."""
    __slots__ = ("_f",)
    def __init__(self, f): self._f = f
    def write(self, b):
        if _av["anchor_ns"] is None:
            # AVANT l'écriture : la 1ʳᵉ peut bloquer (ffmpeg en init) et le feeder attend l'ancre.
            _av["anchor_ns"] = _anchor_from_hint(_av["hint_ns"], bobimxl.now_tai(), lat_in.avg())
        n = self._f.write(b)
        _origine_note("video")
        # Une trame = GRAIN_SIZE octets ; les écritures partielles (mode tranche, secours) sont
        # comptées au prorata, sinon le compte dériverait sur ces chemins-là seulement.
        # ⚠ LIMITE CONNUE : en mode tranche, une trame complétée par TIMEOUT écrit des bandes de
        # secours (reste du grain précédent, remplissage neutre) dont le total peut DÉPASSER une
        # trame — le compte sur-estime alors la vidéo. Ces timeouts sont fréquents au DÉMARRAGE et
        # absents en régime établi : le compteur est exact une fois la chaîne installée (mesuré
        # ×0,995 sur 10 s) mais NE DOIT PAS servir à interpréter le transitoire de démarrage.
        _ecrit["video"] += (len(b) / float(GRAIN_SIZE)) if GRAIN_SIZE else 0
        return n
    def flush(self):  return self._f.flush()
    def close(self):  return self._f.close()
    def fileno(self): return self._f.fileno()

# ─── Thread lecteur audio → fifo (2e entrée ffmpeg) ──────────────────
# Le fifo est ouvert dès que ffmpeg l'ouvre, et alimenté sur la ligne de temps du bus (cf. ANCRE
# A/V) : l'échantillon n écrit est celui d'index A0 + n. Un bloc pas encore écrit sur le bus est
# ATTENDU tant qu'il a moins de A_LATE ; au-delà (source absente, muette par coupure, figée), il est
# remplacé par du silence À SA POSITION — ffmpeg n'est jamais affamé, la vidéo ne bloque jamais, et
# la ligne de temps audio ne glisse pas d'un échantillon.
SILENCE = bytes(A_OUT_CHUNK)   # 1 ms de silence à la largeur de SORTIE (OUT_CHANNELS)

# Témoin du feeder, publié sur :8080 (`audio_feed`). Sans lui, un flux 100 % silence et un flux
# sain affichaient les MÊMES compteurs (av_ecrit_audio_ms avance dans les deux cas) : la panne
# n'était visible qu'à l'oreille. `silence_ms` qui croît pendant que le bus a du son = feeder décroché.
_afeed = {{"real_ms": 0, "silence_ms": 0, "gap_ms": 0, "rebased_ms": 0, "reader": False,
           "lag_ms": None, "gen": 0}}
metrics["audio_feed"] = _afeed if AUDIO_ENABLED else None   # même objet : à jour sans recopie


def _fifo_open_writer(gen):
    """Ouvre en écriture le fifo de l'encodeur `gen`, sans bloquer si ce dernier ne l'ouvre jamais
    (ffmpeg mort avant d'atteindre son entrée audio) : O_NONBLOCK échoue en ENXIO tant qu'aucun
    lecteur ne l'a ouvert — un ffmpeg BLOQUÉ dans son open() compte déjà comme lecteur. Rend un fd
    BLOQUANT (la contre-pression de ffmpeg est voulue), ou None si la génération a changé."""
    import fcntl
    path = _fifo_path(gen)
    while _av["gen"] == gen and not bus_error.is_set():
        try:
            fd = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
        except OSError:
            time.sleep(0.01); continue
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl & ~os.O_NONBLOCK)
        # Capacité bornée à ~120 ms : au-delà, l'audio tamponné dans le tube est de la latence.
        try:
            fcntl.fcntl(fd, 1031, max(4096, int(0.12 * OUT_CHANNELS * A_SAMPLE_RATE
                                                * A_BYTES_PER_SAMPLE)))   # F_SETPIPE_SZ
        except Exception:
            pass
        return fd
    return None


def _fifo_write(fd, b):
    mv = memoryview(b)
    while mv:
        n = os.write(fd, mv)
        mv = mv[n:]


def audio_feeder():
    while True:
        gen = _av["gen"]
        fd = _fifo_open_writer(gen) if gen else None
        if fd is None:
            time.sleep(0.05); continue
        log("audio fifo connecté à ffmpeg (encodeur n°%d)" % gen, "info")
        _afeed["gen"] = gen
        _afeed["webrtc_comp_ms"] = _webrtc_comp_ms(_has_ts(), _has_webrtc(), _gateway_abs_ts())
        ar = None; last_ar_try = 0.0; last_stale_chk = 0.0
        try:
            # Ancre : posée par la 1ʳᵉ écriture vidéo de CET encodeur (avant qu'elle ne bloque).
            t_wait = time.monotonic()
            while _av["anchor_ns"] is None and _av["gen"] == gen and time.monotonic() - t_wait < 5.0:
                time.sleep(0.002)
            if _av["gen"] != gen:
                continue
            anchor = _av["anchor_ns"] if _av["anchor_ns"] is not None else bobimxl.now_tai()
            pos = _tai_samples(anchor) - _webrtc_comp_ms(_has_ts(), _has_webrtc(), _gateway_abs_ts()) * (A_SAMPLE_RATE // 1000)
            skip_seen = _av["skip_ns"]
            off_st = {{"off": 0, "cand": None}}   # décalage source/grille ADOPTÉ (cf. _src_offset_update)
            head_prev = None; head_t = 0.0; bad_reads = 0; reopen_n = 0
            while _av["gen"] == gen:
                if bus_error.is_set():
                    break
                sk = _av["skip_ns"]
                if sk != skip_seen:        # la vidéo a sauté des créneaux → l'audio saute autant
                    pos += _tai_samples(sk - skip_seen); skip_seen = sk
                if ar is None and (time.monotonic() - last_ar_try) > 1.0:
                    last_ar_try = time.monotonic()
                    try: ar = bobimxl.AudioReader(inst, AUDIO_SHM)
                    except Exception: ar = None
                    _afeed["reader"] = ar is not None
                head = None
                if ar is not None:
                    try:
                        h = ar.head_index()
                        head = None if h == bobimxl.MXL_UNDEFINED_INDEX else int(h)
                    except Exception:
                        try: ar.close()
                        except Exception: pass
                        ar = None; _afeed["reader"] = False
                    if ar is not None and time.monotonic() - last_stale_chk >= 1.0:
                        last_stale_chk = time.monotonic()
                        if ar.reopen_if_head_stale(on_reopen=lambda s_, n: log(
                                f"flux audio MXL {{AUDIO_SHM}} : lecteur PÉRIMÉ (head figé depuis "
                                f"{{s_:.1f}} s), reconnexion (tentative {{n}})"
                                + (" → Instance DÉDIÉE" if n >= 2 else ""), "warning")):
                            # ⚠ un flux réellement MUET a un head qui AVANCE (le writer comble en
                            # silence) : un head figé n'est pas du silence, c'est un décrochage.
                            head = None
                tm = time.monotonic()
                if head is not None and head != head_prev:
                    head_prev = head; head_t = tm
                alive = head is not None and (tm - head_t) < A_LATE / A_SAMPLE_RATE
                now_s = _tai_samples(bobimxl.now_tai())
                if head is not None and alive:
                    old = off_st["off"]
                    new = _src_offset_update(off_st, head, now_s, tm, A_OFFGRID, 0.5, A_SAMPLE_RATE)
                    if new != old:
                        pos += new - old
                        _afeed["rebased_ms"] = round(new / (A_SAMPLE_RATE / 1000.0), 1)
                        log(f"flux audio {{AUDIO_SHM}} : ligne d'index à {{new / A_SAMPLE_RATE:+.3f}} s de "
                            f"la grille TAI — suivi sur sa propre ligne", "warning")
                src_off = off_st["off"]
                if head is not None:
                    _afeed["head_ahead_ms"] = round((head - pos) / (A_SAMPLE_RATE / 1000.0), 1)
                    _afeed["head_vs_clock_ms"] = round((head - now_s) / (A_SAMPLE_RATE / 1000.0), 1)
                with _amap_lock:
                    idx = list(_amap["slot_src"])
                n = 0; real = 0
                while n < A_BATCH:
                    act = _audio_step(pos, head, now_s, src_off, A_BLOCK, A_LATE, alive)
                    if act == "wait":
                        break
                    r = ar.read_from(pos, A_BLOCK) if (act == "read" and ar is not None) else None
                    if r is not None:
                        _fifo_write(fd, _remap(r, idx)); real += 1; _afeed["real_ms"] += 1
                        bad_reads = 0; reopen_n = 0
                    else:
                        # "read" sans données = bloc sorti de l'anneau (encodeur en retard) ; sinon
                        # source absente. Dans les deux cas : silence À CETTE position.
                        _fifo_write(fd, SILENCE)
                        _afeed["gap_ms" if act == "read" else "silence_ms"] += 1
                        if act == "read" and head - pos < A_SAMPLE_RATE // 10:
                            bad_reads += 1     # bloc RÉCENT illisible : le lecteur, pas l'anneau
                    _origine_note("audio")
                    _ecrit["audio"] += A_BLOCK
                    pos += A_BLOCK; n += 1
                if bad_reads >= 200 and ar is not None:
                    # Tête qui avance, blocs de moins de 100 ms illisibles pendant 200 ms : lecteur
                    # accroché à une génération morte dont la tête suit l'horloge (réplique RDMA
                    # détruite puis recréée). La garde « tête figée » ne le voit pas.
                    bad_reads = 0; reopen_n += 1
                    try:
                        # 2ᵉ essai : Instance DÉDIÉE (le cache de la nôtre peut retenir l'orphelin)
                        nre = ar.reopen(dedicated=reopen_n >= 2)
                        log(f"flux audio MXL {{AUDIO_SHM}} : tête vivante mais blocs récents illisibles "
                            f"— lecteur rouvert (tentative {{nre}})", "warning")
                    except Exception:
                        try: ar.close()
                        except Exception: pass
                        ar = None; _afeed["reader"] = False
                if n:
                    _ecriture_maj()
                if real:
                    # Âge du contenu donné à l'encodeur (position écrite vs horloge, ms). La
                    # fraîcheur audio ne se lit pas sur lastWriteTime : les writers audio ne le
                    # bumpent jamais (cf. mxl-audio-no-lastwritetime).
                    lat = (now_s + src_off - pos) / (A_SAMPLE_RATE / 1000.0)
                    if 0 <= lat < 5000: lat_audio.push(lat)
                    _afeed["lag_ms"] = round(lat, 1)
                if not n:
                    time.sleep(0.001)
        except (BrokenPipeError, OSError, ValueError):
            log("audio fifo cassé (ffmpeg redémarré ?), reconnexion...", "warning")
        except Exception as e:
            log(f"audio feeder erreur : {{type(e).__name__}}: {{e}}", "warning")
        finally:
            for c in (ar,):
                try:
                    if c: c.close()
                except Exception: pass
            try: os.close(fd)
            except Exception: pass
            _afeed["reader"] = False
            time.sleep(0.05)


if AUDIO_ENABLED:
    threading.Thread(target=audio_feeder, daemon=True).start()

# ─── Ouverture du shm vidéo ──────────────────────────────────────────
def ouvrir_shm():
    """Ouvre le flux vidéo MXL d'entrée (Reader). Attend qu'il existe."""
    while True:
        try:
            r = bobimxl.Reader(_inst_video[0], SHM_NAME)
            log(f"flux MXL ouvert : {{SHM_NAME}}", "info")   # source qui apparaît = événement
            _open_warned[0] = False
            return r
        except Exception as e:
            if not _open_warned[0]:  # une seule ligne par ÉPISODE d'indisponibilité (pas 1/s)
                _open_warned[0] = True
                log(f"flux MXL indisponible, attente... ({{e}})", "warning")
            time.sleep(1)

def fermer_shm(reader):
    try: reader.close()
    except Exception: pass

def _renouveler_instance_video():
    """Repart sur une Instance NEUVE pour la lecture vidéo — cache vierge, résolution sur disque.

    ★★ REMPLACE un `inst.garbage_collect()` (2026-10-24). Il fallait bien faire oublier
    l'ORPHELIN entre close et reopen — sans quoi un flux recréé sous le même nom reste résolvable
    vers la génération périmée et la boucle gèle en silence (40 min mesurées côté pyramide). Mais
    le GC de libmxl est **global au DOMAINE** : il ne visait pas notre orphelin, il balayait aussi
    les flux VIVANTS des autres processus. Dégâts mesurés : quatre flux du moteur 2110 détruits
    sous leur producteur le 2026-08-19 ; et le 2026-10-24 sur r620-2, une réplique RDMA recréée
    sous son récepteur, qui a continué d'écrire dans un inode délié — réplique vide pour toujours,
    et ~850 ré-établissements par jour pour tenter de réparer ça. Or c'est très exactement un
    `streamer` (l'encodeur de monitoring) qui tournait sur ce nœud.

    Une Instance neuve obtient le même oubli SANS toucher au domaine. On ne ferme que les
    Instances qu'on a soi-même créées : jamais `inst`, dont l'AudioReader se sert dans un autre
    fil — la fermer sous lui invaliderait son handle."""
    ancienne = _inst_video[0]
    try:
        _inst_video[0] = bobimxl.Instance()
    except Exception as e:                      # échec de création : on garde l'ancienne
        log(f"renouvellement d'Instance MXL impossible ({{e}}) — on garde la précédente", "warning")
        return
    if ancienne is not inst:
        try: ancienne.close()
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
        return bobimxl.Reader(_inst_video[0], name) if name else None
    except Exception:
        return None

class ControlHandler(BaseHTTPRequestHandler):
    """Contrôle à chaud sur :8082.
    - POST /input    {{shm}}             → re-câble la source vidéo (mode moniteur HOT_INPUT)
    - POST /audiomap {{tracks:[…]}}      → ré-aiguille les canaux audio (forme figée) sans restart
    - POST /log_level {{level}}           → verbosité du journal à chaud (debug|info|warning|error)
    - GET  /audiomap                     → mapping courant + forme
    - GET  /state                        → état (source, audio, niveau de log courant)
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
        elif self.path == "/log_level":
            # Verbosité À CHAUD (pas de redéploiement) : instruction d'incident. Le niveau
            # PERSISTANT reste le champ `log_level` du config_schema ; celui-ci est volatil.
            ok = set_log_level(body.get("level") or body.get("log_level"))
            self._reply(200 if ok else 400, {{"ok": ok, "log_level": LOG_LEVEL}})
        else:
            self._reply(404, {{"error": "not found"}})

    def do_GET(self):
        if self.path == "/audiomap":
            with _amap_lock: src = list(_amap["slot_src"])
            self._reply(200, {{"slot_src": src, "widths": AUDIO_WIDTHS,
                              "channels": OUT_CHANNELS, "enabled": AUDIO_ENABLED}})
        elif self.path == "/state":
            with _hot_lock: shm = _hot_cur["shm"]
            with _amap_lock: src = list(_amap["slot_src"])
            self._reply(200, {{"shm": shm, "width": WIDTH, "height": HEIGHT,
                              "audio_enabled": AUDIO_ENABLED, "slot_src": src,
                              "hot_input": HOT_INPUT,
                              "log_level": LOG_LEVEL,   # lisible en condition de macro
                              # Encodeur RÉELLEMENT actif, pas celui demandé : en mode `auto` les
                              # deux diffèrent dès qu'aucun GPU n'est utilisable. Sans cette
                              # distinction, un repli passerait inaperçu et le calcul de capacité
                              # continuerait de compter un cœur économisé qui ne l'est pas.
                              "encoder": ENCODER, "encoder_demande": _ENC_DEMANDE,
                              "nvenc_dispo": _NVENC_OK, "vcodec": VCODEC,
                              "plugin_version": PLUGIN_VERSION,
                              "mxl_lib": _mxl_lib_state()}})
        else:
            with _hot_lock: shm = _hot_cur["shm"]
            self._reply(200, {{"shm": shm, "width": WIDTH, "height": HEIGHT}})
    def log_message(self, *a): pass

# Le serveur de contrôle tourne TOUJOURS : même sans audio ni mode moniteur, il porte
# /log_level (bascule de verbosité pendant un incident) et /state. Il ne coûte qu'un thread
# oisif — auparavant il était conditionnel (AUDIO_ENABLED or HOT_INPUT) et un streamer
# vidéo-seul était injoignable, donc muet aux macros.
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
    log(f"hot-input: balayage source = {{IN_SCAN}} {{IN_FIELD_ORDER or '-'}} "
        f"(grain {{WIDTH}}x{{GRAIN_H}})", "info")
    return True

def _grain_tai(r, index):
    """Instant TAI du CONTENU d'un grain : sa coordonnée sur la grille du flux, sinon l'instant
    de sa dernière écriture. Sert d'ancre à l'audio (cf. _anchor_from_hint)."""
    try:
        t = r.index_time_ns(index)
        if t:
            return t
    except Exception:
        pass
    try:
        return r.last_write_time() or None
    except Exception:
        return None


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
    # Cadence ANCRÉE (cf. _video_due) : la trame k part à v_t0 + k/FPS, par encodeur (`v_gen`).
    v_gen = None; v_t0 = None; v_pushed = 0; last_tai = None
    log(f"hot-input actif {{WIDTH}}x{{HEIGHT}}@{{FPS}} — source initiale {{_hot_cur['shm']!r}}", "info")
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
                                last_tai = _grain_tai(r, 2 * ff)
                        else:
                            last_frame = bytes(got[2])       # grain = trame planar complète
                            last_index = fi
                            last_tai = _grain_tai(r, fi)
                        age_us = (bobimxl.now_tai() - r.last_write_time()) // 1000
                        if 0 <= age_us < 5_000_000: lat_in.push(age_us / 1000.0)
            except Exception:
                try:
                    if r: r.close()
                except Exception: pass
                r = None; cur_name = None; last_frame = None
        now = time.time()
        if v_gen != _av["gen"]:
            v_gen = _av["gen"]; v_t0 = None; v_pushed = 0     # encodeur neuf → trame 0
        if v_t0 is None or now >= v_t0 + v_pushed / FPS:
            if ffmpeg_out.poll() is not None:
                _refresh_metrics(up=False); time.sleep(0.3); ffmpeg_out = creer_ffmpeg()
                continue
            if v_t0 is None:
                v_t0 = now
            todo, skip = _video_due(now - v_t0, FPS, v_pushed, 2 * FPS)
            if skip:
                v_pushed += skip
                _av["skip_ns"] += int(skip * 1_000_000_000 // FPS)
                log(f"hot-input : encodeur en retard de {{skip}} trames — créneaux sautés "
                    f"(l'audio saute la même durée, le calage est conservé)", "warning")
            buf = last_frame if last_frame is not None else black
            _av["hint_ns"] = last_tai if last_frame is not None else None
            try:
                # Rattrapage : la MÊME image répétée, pour que la trame k reste à v_t0 + k/FPS.
                for _ in range(todo):
                    ffmpeg_out.stdin.write(buf)
                    v_pushed += 1; win_cnt += 1; diag_pushed += 1
                ffmpeg_out.stdin.flush()
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
        time.sleep(0.001)

# Géométrie d'entrée toujours résolue depuis le shm réel (corrige une config périmée,
# respecte un non-16:9 exact — cf. _detect_dims). En mode hot-input (moniteur) les
# dimensions sont imposées (canvas fixe) → pas de détection.
if not HOT_INPUT:
    # TOUJOURS résolu depuis le flow_def (et plus seulement quand la config est vide) : le
    # balayage (entrelacé + ordre de champ) n'existe QUE là, et une config de sortie non nulle
    # masquait la détection en faisant passer les dims de SORTIE pour les dims d'ENTRÉE.
    _apply_fmt(_detect_dims())
    log(f"dims entrée résolues: {{WIDTH}}x{{HEIGHT}}{{IN_SCAN}} {{IN_CHROMA}} {{BIT_DEPTH}}bit"
        f" (grain {{WIDTH}}x{{GRAIN_H}} {{IN_FIELD_ORDER or '-'}})", "info")

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
    log(f"mode tranche actif (slice_lines={{SLICE_LINES}} informatif — le pas vient du grain source)", "info")

_last_stale_chk = time.monotonic()   # cadence du contrôle de péremption du Reader (cf. plus bas)

while True:
    # Reconnexion après Bus error
    if bus_error.is_set():
        bus_error.clear()
        fermer_shm(reader)
        _renouveler_instance_video()   # cache vierge entre close et reopen (JAMAIS un GC global)
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
        _renouveler_instance_video()   # cache vierge entre close et reopen (JAMAIS un GC global)
        ffmpeg_out = creer_ffmpeg()
        reader = ouvrir_shm()
        last_index = 0
        continue

    # DÉCROCHAGE DE GÉNÉRATION (parade partagée bobimxl) : le producteur amont peut DÉTRUIRE puis
    # RECRÉER son flux sous le MÊME nom (changement de source d'un slot RX, redéploiement…). Notre
    # Reader reste alors collé à la génération MORTE, dont les grains restent LISIBLES : aucun
    # SIGBUS, aucune exception — les deux reprises ci-dessus ne partent JAMAIS et la boucle tourne
    # à vide pour toujours (got[0] figé sous last_index). Seul `lastWriteTime` le voit. ~2 Hz.
    if time.monotonic() - _last_stale_chk >= 0.5:
        _last_stale_chk = time.monotonic()
        if reader.reopen_if_stale(on_reopen=lambda age, n: log(
                f"flux MXL {{SHM_NAME}} : lecteur PÉRIMÉ (aucune écriture depuis {{age / 1000:.1f}} s), "
                f"reconnexion (tentative {{n}})" + (" → Instance DÉDIÉE" if n >= 2 else ""),
                "warning")):
            last_index = 0      # génération neuve → les index repartent, ne pas les filtrer

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
                log("FFmpeg arrêté, redémarrage...", "warning")
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
                log("BrokenPipe, redémarrage FFmpeg...", "warning")
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
                log("FFmpeg arrêté, redémarrage...", "warning")
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
                                log("BrokenPipe, redémarrage FFmpeg...", "warning")
                                time.sleep(1); ffmpeg_out = creer_ffmpeg()
                        else:
                            diag_stale += 1
                        last_index = 2 * fi + 1
            time.sleep(0.0005)
            continue

        got = reader.get_latest()

        if ffmpeg_out.poll() is not None:
            log("FFmpeg arrêté, redémarrage...", "warning")
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
                    log("BrokenPipe, redémarrage FFmpeg...", "warning")
                    time.sleep(1)
                    ffmpeg_out = creer_ffmpeg()
            else:
                diag_stale += 1   # frame trop vieille (≥80 ms) → jetée par le filtre de fraîcheur
            last_index = frame_index

        time.sleep(0.00001)

    except Exception as e:
        log(f"Erreur ({{type(e).__name__}}): {{e}}, reconnexion...", "warning")
        fermer_shm(reader)
        _renouveler_instance_video()   # cache vierge entre close et reopen (JAMAIS un GC global)
        time.sleep(2)
        reader = ouvrir_shm()
        last_index = 0
