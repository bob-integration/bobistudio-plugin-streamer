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

## Encodage matériel (NVENC)

Le champ **Encodeur vidéo** choisit entre logiciel (x264/x265, défaut), matériel NVENC
(GPU NVIDIA du nœud) ou automatique :

| Valeur | Comportement |
|--------|-------------|
| **Logiciel** | x264/x265, aucun GPU requis |
| **Matériel — exigé** | Refuse de démarrer si aucun GPU/pilote NVENC utilisable plutôt que de retomber en silence sur le logiciel |
| **Automatique** | Matériel si disponible, sinon logiciel — le repli est annoncé dans l'état du conteneur (`encoder` ≠ `encoder_demande`) |

NVENC décharge le processeur d'environ un cœur par flux 1080p50. **Preset NVENC** (p1 rapide
→ p7 qualité maximale) et **Profil de latence NVENC** (ultra faible latence / faible latence
/ qualité, équivalent du « zerolatency » x264) n'ont d'effet qu'en encodage matériel.
L'encodeur réellement actif et la disponibilité de la pile matérielle sont publiés sur
`:8080` (`encoder`, `nvenc_dispo`).

## Calage audio/vidéo (`av_offset_ms`)

Décalage manuel de l'audio par rapport à la vidéo, en millisecondes, signé : positif retarde
l'audio, négatif l'avance. Utile quand la chaîne en amont introduit un décalage connu
(traitement audio externe, latence d'un encodeur source) que le streamer doit compenser
avant diffusion. Le délai effectivement appliqué est publié sur `:8080`
(`av_offset_ms`). Sans effet si l'audio est désactivé.

## Audio

La source audio se câble via la page **Câbles** (port d'entrée audio séparé). Les pistes sont configurables dans la section Audio de la page Streams. UDP/SRT portent toutes les pistes AAC, WebRTC porte uniquement la 1ère piste (Opus).

## Mode source

| Mode | Comportement |
|------|-------------|
| **Adaptation auto** | Détecte la résolution d'entrée et l'adapte — recâbler déclenche un redéploiement |
| **Bascule sans coupure** | Format figé, changement de source de même résolution sans coupure |

## Lien client

Dès qu'une destination WebRTC est active, un **lien client** peut être généré : page publique brandée lisible dans n'importe quel navigateur, sans compte.

## Mode tranche (latence réduite)

Sur une source publiée en mode tranche MXL (moteur 2110, traitements en tranche…), le streamer peut alimenter l'encodeur **au fil de l'arrivée des bandes** au lieu d'attendre l'image complète : la latence bout-en-bout (ex. monitoring WebRTC) baisse d'environ une image (~18 ms en 50p).

- **Opt-in** : paramètre `slice_mode` (désactivé par défaut — comportement inchangé). Le réglage est conservé par l'éditeur Streams.
- Source entrelacée ou flux amont non tranché → comportement classique automatique.
- L'encodage, l'audio et les destinations ne changent pas.

## Notes

- Une destination morte (`onfail=ignore`) ne coupe pas les autres
- La preview WebRTC ne fonctionne qu'avec H.264 (les navigateurs ne décodent pas H.265 en WebRTC)
