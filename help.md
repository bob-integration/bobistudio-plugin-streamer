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

⚠ **Non réglable aujourd'hui.** Le script sait appliquer un décalage manuel de l'audio par rapport
à la vidéo (millisecondes, signé : positif retarde l'audio, négatif retarde la vidéo), mais aucune
page ne le propose et la normalisation des paramètres faite par l'orchestrateur ne le conserve pas :
il vaut donc toujours 0. Le délai effectivement appliqué est publié sur `:8080` (`av_offset_ms`) —
c'est là qu'on vérifie qu'il vaut bien 0.

## Audio

La source audio se câble via la page **Câbles** (port d'entrée audio séparé). Les pistes sont configurables dans la section Audio de la page Streams. UDP/SRT portent toutes les pistes AAC, WebRTC porte uniquement la 1ère piste (Opus).

## Mode source

| Mode | Comportement |
|------|-------------|
| **Adaptation auto** | Détecte la résolution d'entrée et l'adapte — recâbler déclenche un redéploiement |
| **Bascule sans coupure** | Format figé, changement de source de même résolution sans coupure |

## Moniteur (panneau Monitoring)

Le moniteur de chaque utilisateur est un streamer qui pousse une seule destination WebRTC. Son réglage est fait pour être **vu et entendu dans un navigateur** :

- **Son calé sur l'image** : l'audio est placé sur la même ligne de temps que la vidéo, quelle que soit la source ; il ne reste plus de silence après un changement de source ni d'avance de près d'une demi-seconde. Le temps de transport de la passerelle WebRTC est compensé (90 ms, appliqué quand WebRTC est la seule destination) ; mesuré dans le navigateur : écart de quelques dizaines de ms.
- **Image 4:2:0** : la sortie est en yuv420p (le 4:4:4 n'est pas lu de façon fiable par les navigateurs), y compris en encodage matériel.
- **Témoin du son** : le port :8080 publie `audio_feed` (échantillons réellement écrits, silence comblé, source vivante ou non). Un flux muet par absence de source se distingue ainsi d'un flux muet par panne.

## Lien client

Dès qu'une destination WebRTC est active, un **lien client** peut être généré : page publique brandée lisible dans n'importe quel navigateur, sans compte.

## Mode tranche (latence réduite)

Sur une source publiée en mode tranche MXL (moteur 2110, traitements en tranche…), le streamer peut alimenter l'encodeur **au fil de l'arrivée des bandes** au lieu d'attendre l'image complète : la latence bout-en-bout (ex. monitoring WebRTC) baisse d'environ une image (~18 ms en 50p).

- **Interrupteur global** : **Réglages → MXL**, « Trancher toute la chaîne » (éteint par défaut). Pas de réglage par conteneur dans l'interface ; la bascule prend effet au (re)déploiement. Une valeur posée sur le conteneur est conservée par l'éditeur Streams.
- Source entrelacée ou flux amont non tranché → comportement classique automatique.
- L'encodage, l'audio et les destinations ne changent pas.

## Notes

- Une destination morte (`onfail=ignore`) ne coupe pas les autres
- La preview WebRTC ne fonctionne qu'avec H.264 (les navigateurs ne décodent pas H.265 en WebRTC)
