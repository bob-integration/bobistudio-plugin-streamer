#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.
#
# BANC DU CALAGE A/V DU STREAMER — à lancer À LA MAIN, aucune flotte touchée :
#     ./venv/bin/python plugins/streamer/av_origine_bench.py <port> <durée> [avant|apres] \
#                                                            [retard_vidéo_ms] [filtre_asy]
#     puis mesurer :  ./venv/bin/python <votre mesure_av.py> /tmp/banc_av_<version>.ts
#
# Fait tourner le script du streamer TEL QUEL, hors conteneur, avec un `bobimxl` factice servant
# une vidéo et un audio SYNCHRONES PAR CONSTRUCTION (mêmes instants de repère, dérivés de la même
# horloge). Le streamer encode vers UDP local ; on capture et on compare les pts du flash et de
# la salve avec `signalstats` et `silencedetect`. Tout écart non nul vient du streamer.
#
# ⚠ CE QU'IL NE SAIT PAS ENCORE FAIRE (2026-08-25). Il mesure un écart de ~200 ms qui CROÎT au
# fil du run (+200 → +220 → +240 → +390), alors que le défaut réel mesuré en production est
# CONSTANT à +99,1 ms. Sa source audio factice n'est donc pas fidèle : très probablement
# l'interaction entre `read_from`/`head_index` et la branche « silence cadencé » du feeder, qui
# sur-écrit. **Rendre le faux AudioReader fidèle AVANT de se servir de ce banc pour trancher.**
# Ce qu'il a déjà servi à établir, en revanche, tient : la trace de démarrage montre que ffmpeg
# n'ouvre le fifo audio QU'APRÈS la première trame écrite sur pipe:0 (il attend des données sur
# sa première entrée), donc les deux origines ne peuvent pas se croiser — l'hypothèse « le fifo
# coule avant la vidéo » est RÉFUTÉE.
import io, os, sys, time, types, threading, subprocess
import numpy as np

PORT = sys.argv[1] if len(sys.argv) > 1 else "19300"
DUREE = float(sys.argv[2]) if len(sys.argv) > 2 else 30.0
# « avant » rejoue la version du script telle qu'elle est dans git HEAD : c'est ce qui permet de
# comparer AVANT/APRÈS sur le même banc, au lieu de croire un raisonnement.
VERSION = sys.argv[3] if len(sys.argv) > 3 else "apres"
# La vidéo n'est PAS disponible dès le lancement : le lecteur MXL doit s'attacher et attendre son
# premier grain. C'est précisément ce retard qui partait dans le flux, alors reproduisons-le.
RETARD_VIDEO_MS = float(sys.argv[4]) if len(sys.argv) > 4 else 300.0
# Remplace le filtre de resampling audio, pour éprouver son rôle dans le calage.
ASY = sys.argv[5] if len(sys.argv) > 5 else None
W, H, FPS = 640, 360, 50
SR, CH = 48000, 8
GRAIN = W * H * 2                     # yuv422p 8 bits
BEAT_S = 2.0                          # un repère toutes les 2 s
FLASH_MS, BIP_MS = 200, 200   # repères LONGS : la détection ne doit jamais être le facteur limitant

PLUG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "script.py")
T0 = [None]                           # origine commune aux deux essences (posée au 1er accès)

def _t():
    if T0[0] is None:
        T0[0] = time.monotonic()
    return time.monotonic() - T0[0]

_NOIR = np.full(GRAIN, 16, dtype=np.uint8); _NOIR[W*H:] = 128
_FLASH = np.full(GRAIN, 235, dtype=np.uint8); _FLASH[W*H:] = 128

def _trame(idx):
    """Trame d'index idx : flash sur les premières FLASH_MS d'un battement."""
    ms = (idx * 1000 // FPS) % int(BEAT_S * 1000)
    return (_FLASH if ms < FLASH_MS else _NOIR).tobytes()

class FauxReader:
    def __init__(self, inst, nom): self.nom = nom
    def format(self):
        return {"width": W, "height": H, "frame_width": W, "frame_height": H,
                "chroma": "422", "bit_depth": 8, "interlaced": False,
                "frame_fps_num": FPS, "frame_fps_den": 1}
    def _idx(self): return int(_t() * FPS)
    def _pret(self): return _t() * 1000.0 >= RETARD_VIDEO_MS
    def get(self, idx, timeout_ns=None):
        if idx < 0 or not self._pret(): return None
        return (idx, types.SimpleNamespace(validSlices=1, totalSlices=1), _trame(idx))
    def get_latest(self):
        if not self._pret(): return None
        return self.get(self._idx())
    def get_slice(self, idx, j, timeout_ns=None):
        return (idx, types.SimpleNamespace(validSlices=1, totalSlices=1), _trame(idx))
    def last_write_time(self): return faux.now_tai()
    def reopen_if_head_stale(self, on_reopen=None): return False
    def reopen_if_stale(self, on_reopen=None): return False
    def close(self): pass

class FauxAudioReader:
    def __init__(self, inst, nom): self.nom = nom
    def head_index(self): return int(_t() * SR)
    def read_from(self, pos, n):
        if pos + n > self.head_index(): return None
        i = np.arange(pos, pos + n)
        ms = (i * 1000 // SR) % int(BEAT_S * 1000)
        w = np.where(ms < BIP_MS,
                     0.5 * np.sin(2 * np.pi * 1000.0 * i / SR), 0.0).astype(np.float32)
        return np.repeat(w[:, None], CH, axis=1)
    def last_write_time(self): return faux.now_tai()
    def reopen_if_head_stale(self, on_reopen=None): return False
    def close(self): pass

faux = types.ModuleType("bobimxl")
faux.Instance = lambda *a, **k: types.SimpleNamespace(garbage_collect=lambda: None)
faux.Reader = FauxReader
faux.AudioReader = FauxAudioReader
faux.now_tai = lambda: time.clock_gettime_ns(time.CLOCK_REALTIME)
faux.MXL_UNDEFINED_INDEX = -1
faux.lib_info = lambda: {"variante": "banc"}
sys.modules["bobimxl"] = faux

cfg = {"shm_name": "banc_video", "audio_shm": "banc_audio", "log_level": "info",
       "video": {"fps": FPS, "bitrate": "3M", "preset": "ultrafast", "encoder": "cpu", "gop": 50},
       "audio": {"enabled": True, "shm": "banc_audio", "bitrate": "128k",
                 "tracks": [{"channels": [0, 1]}]},
       "destinations": [{"type": "udp", "host": "127.0.0.1", "port": int(PORT)}]}
if VERSION == "avant":
    PLUG = "/tmp/streamer_avant.py"
    io.open(PLUG, "w", encoding="utf-8").write(subprocess.run(
        ["git", "-C", os.path.dirname(os.path.abspath(__file__)), "show", "HEAD:script.py"],
        capture_output=True, text=True).stdout)
src = io.open(PLUG, encoding="utf-8").read().format(
    config=repr(cfg), hostname="banc-streamer", plugin_version="banc")
if ASY:
    src = src.replace('asy = "aresample=async=1:min_hard_comp=0.100:first_pts=0"',
                      'asy = "%s"' % ASY)
# ── traces de banc (diagnostic du calage) : posées dans le SOURCE, pas dans le plugin ──
_TR = 'import time as _t_; print("[banc] %s t=%.3f" % ("{}", _t_.monotonic()), flush=True)'
src = src.replace("    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE",
                  "    " + _TR.format("ffmpeg spawn") + "\n    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE")
src = src.replace("        if not video_demarree.is_set():\n            video_demarree.set()",
                  "        if not video_demarree.is_set():\n            " + _TR.format("1re trame ecrite")
                  + "\n            video_demarree.set()")
src = src.replace('            fifo = open(AUDIO_FIFO, "wb")',
                  "            " + _TR.format("feeder: avant open fifo") + '\n            fifo = open(AUDIO_FIFO, "wb")')
src = src.replace("        _t_att = time.monotonic()",
                  "        " + _TR.format("feeder: fifo ouvert, attente trame") + "\n        _t_att = time.monotonic()")

cap = subprocess.Popen(
    ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-analyzeduration", "10M",
     "-probesize", "10M", "-i", "udp://0.0.0.0:%s?fifo_size=1000000&overrun_nonfatal=1" % PORT,
     "-t", str(int(DUREE - 6)), "-c", "copy", "-f", "mpegts", "banc_av_%s.ts" % VERSION],
    cwd="/tmp")
threading.Thread(target=lambda: (time.sleep(DUREE), os._exit(0)), daemon=True).start()
# Le script pose un handler SIGBUS : fil PRINCIPAL obligatoire. Sa boucle finale ne rend jamais
# la main — on la laisse tourner, le minuteur ci-dessus arrête le banc.
exec(compile(src, "script.py", "exec"), {"__name__": "banc"})
