# CCIOI GPU Client Deployment

This repo contains a Dockerized GPU client (`gpu_client.py`) for CCIOI. It connects to the bridge, executes video generation jobs, stitches segments, and uploads results back to the server.

## Prerequisites

- NVIDIA driver + nvidia-container-runtime
- Docker + Docker Compose v2
- Model weights available under `./ckpts` (mounted into the container)

## Quick Start

```bash
cp .env.example .env
# edit .env with your GPU_ID / BRIDGE_WS / SERVER_BASE and stitch settings

docker compose up -d --build
```

## Mounts and Outputs

- `./ckpts` → `/app/ckpts` (model weights)
- `./outputs` → `/app/outputs` (generated videos + stitched outputs)
- `./configs` → `/app/configs`
- `./assets` → `/app/assets`
- `./prompts` → `/app/prompts`

## Configuration

Key runtime settings are controlled via `.env` (or environment variables):

- `GPU_ID`, `BRIDGE_WS`, `SERVER_BASE`
- `OSS_ACCESS_KEY_ID`, `OSS_ACCESS_KEY_SECRET`, `OSS_BUCKET`, `OSS_ENDPOINT`, `OSS_PREFIX`
- `SEGMENT_MAX_FRAMES_768PX` (segmentation for 768px jobs)
- `STITCH_CROSSFADE_SEC`, `STITCH_ENABLE_INTERP`, `STITCH_INTERP_FPS`

## Stop

```bash
docker compose down
```
