# streamer — encodeur multi-destinations de Bobi.Studio

*[English version](README.en.md)*

Lit une source vidéo sur le bus [MXL](https://github.com/dmf-mxl/mxl), l'encode **une seule
fois** (H.264 ou H.265) et la diffuse simultanément vers plusieurs destinations : **UDP**,
**SRT**, **WebRTC**.

Composant de [Bobi.Studio](https://github.com/bob-integration/bobistudio).

---

## Le principe

Un seul encodage ffmpeg, réparti vers les destinations par le muxer `tee`. Ajouter une
destination ne coûte donc pas un encodeur de plus — ce qui compte quand on sert le même
programme à un diffuseur, à une régie distante et à une prévisualisation navigateur.

Chaque branche porte `onfail=ignore` : **une destination morte n'emporte pas les autres**.
C'est le comportement qu'on veut sur un lien SRT qui tombe pendant que l'UDP local continue.

**Audio multi-pistes** : l'entrée est un flux 8 canaux, découpé en pistes mono ou stéréo.
Le codec suit la destination — **AAC** pour UDP et SRT (toutes les pistes), **Opus** pour
WebRTC (la première seulement, la spécification ne prévoyant pas mieux). Le routage se fait
par `tee select=`, sans ré-encoder la vidéo.

L'alimentation audio écrit du **silence** quand aucune trame fraîche n'arrive, plutôt que de
bloquer : un défaut côté audio ne doit pas arrêter la vidéo.

---

## Ce qu'il faut savoir

**Le calage audio/vidéo se décide au démarrage de ffmpeg**, et il varie d'un lancement à
l'autre. Le réglage `av_offset_ms` corrige un décalage constant, pas cette variation. Deux
programmes de mesure sont fournis pour l'observer plutôt que de l'estimer :

```bash
python3 tools/av_mesure_pts.py       # les pts vidéo et audio tels qu'ils sortent
python3 tools/av_origine_bench.py    # l'origine des pts sur plusieurs démarrages
```

**La résolution peut être déduite.** Si `video.width` et `video.height` valent `0`, l'encodeur
calcule les dimensions depuis la taille du segment partagé — utile pour prévisualiser une
source dont on ignore le format.

**WebRTC passe par une passerelle.** Le plugin pousse en WHIP ou RTSP vers un MediaMTX
déployé à part ; il ne sert pas le WebRTC lui-même.

---

## L'utiliser

Ce dépôt est un **plugin** de Bobi.Studio, monté dans `plugins/streamer/`. Il se configure
depuis la page **Streams** de l'orchestrateur — encodage d'un côté, liste des destinations de
l'autre — et se câble à une source depuis la page Câbles.

Il ne s'utilise pas seul : sa configuration et son câblage vivent dans l'orchestrateur.
`help.md` est l'article d'aide rendu dans le produit.

---

## Licence

GPL-3.0-or-later — voir [LICENSE](LICENSE). Copyright © 2026 BOBI SAS, France.
