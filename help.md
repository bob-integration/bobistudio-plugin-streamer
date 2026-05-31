# Streamer (encodeur multi-destinations)

Lit une source shm, l'encode une seule fois (H.264 ou H.265) et diffuse vers plusieurs destinations simultanément : UDP, SRT, WebRTC. Un seul encode ffmpeg, fan-out via le muxer `tee`.

## Configurer via la page Streams

La page **Streams** est l'éditeur riche du streamer. Elle permet de :

- Choisir le codec (H.264 recommandé pour WebRTC, H.265 pour meilleure compression)
- Régler débit, preset ffmpeg, GOP
- Définir le **format de sortie** (résolution + fps) — toujours appliqué, l'entrée est auto-adaptée
- Ajouter/supprimer des destinations (UDP / SRT / WebRTC)

## Destinations

| Type | Usage |
|------|-------|
| **UDP** | MPEG-TS sur UDP, lecture via `ffplay udp://<ip>:<port>` |
| **SRT** | Transport fiable sur réseau lossy (caller → listener) |
| **WebRTC** | Push vers la passerelle MediaMTX, preview navigateur |

## Audio

La source audio se câble via la page **Câbles** (port d'entrée audio séparé). Les pistes sont configurables dans la section Audio de la page Streams. UDP/SRT portent toutes les pistes AAC, WebRTC porte uniquement la 1ère piste (Opus).

## Mode source

| Mode | Comportement |
|------|-------------|
| **Adaptation auto** | Détecte la résolution d'entrée et l'adapte — recâbler déclenche un redéploiement |
| **Bascule sans coupure** | Format figé, changement de source de même résolution sans coupure |

## Lien client

Dès qu'une destination WebRTC est active, un **lien client** peut être généré : page publique brandée lisible dans n'importe quel navigateur, sans compte.

## Notes

- Une destination morte (`onfail=ignore`) ne coupe pas les autres
- La preview WebRTC ne fonctionne qu'avec H.264 (les navigateurs ne décodent pas H.265 en WebRTC)
