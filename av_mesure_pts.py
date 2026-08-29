#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.
#
# Écart A/V d'un mpegts, mesuré sur les PTS et sans aucun instrument maison :
#     ./venv/bin/python plugins/streamer/av_mesure_pts.py <fichier.ts>
#
# Le flux doit porter un repère A/V simultané (flash plein écran + salve audio) — c'est le cas
# de tout flux issu d'`avsync`. `signalstats` donne le pts des trames claires, `silencedetect`
# celui des salves, `-copyts` garde les deux sur la ligne de temps de la SOURCE.
# C'est la mesure qui a établi, le 2026-08-23, que le flux émis par le streamer porte +99,1 ms
# constant — chiffre qui ne doit rien à la sonde ni au plugin.
"""Écart des pts entre le flash vidéo et la salve audio dans un mpegts capturé."""
import re, subprocess, sys, statistics as st
f = sys.argv[1]
lu = subprocess.run(["ffmpeg","-copyts","-hide_banner","-loglevel","error","-i",f,"-map","0:v:0",
     "-vf","signalstats,metadata=print:key=lavfi.signalstats.YAVG:file=-","-f","null","-"],
     capture_output=True, text=True).stdout
si = subprocess.run(["ffmpeg","-copyts","-hide_banner","-nostats","-i",f,"-map","0:a:0",
     "-af","silencedetect=noise=-40dB:d=0.05","-f","null","-"],
     capture_output=True, text=True).stderr
pts, ys, cur = [], [], None
for l in lu.splitlines():
    m = re.search(r"pts_time:([0-9.]+)", l)
    if m: cur = float(m.group(1)); continue
    m = re.search(r"YAVG=([0-9.]+)", l)
    if m and cur is not None: pts.append(cur); ys.append(float(m.group(1))); cur = None
if not ys: sys.exit("aucune trame analysée")
# Seuil RELATIF à la dynamique observée, pas absolu : la mire d'avsync n'a pas un fond noir
# (bandeaux, habillage, horloge) et un seuil « fond + 40 » n'y voyait qu'un flash sur trente.
fond = st.median(ys)
seuil = fond + 0.4 * (max(ys) - fond)
fl = [t for t, y in zip(pts, ys) if y > seuil]
fl = [t for i, t in enumerate(fl) if i == 0 or t - fl[i-1] > 0.4]
bips = [float(m) for m in re.findall(r"silence_end: ([0-9.]+)", si)]
ec = []
for t in fl:
    if not bips: break
    b = min(bips, key=lambda x: abs(x - t))
    if abs(b - t) < 0.4: ec.append((b - t) * 1000)
print("  %d flashs, %d salves, %d appariés" % (len(fl), len(bips), len(ec)))
if ec:
    ec.sort()
    print("  ÉCART A/V : médiane %+.1f ms   (étendue %+.1f … %+.1f)"
          % (ec[len(ec)//2], ec[0], ec[-1]))
    print("  détail :", " ".join("%+.0f" % v for v in ec))
