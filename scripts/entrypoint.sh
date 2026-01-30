#!/usr/bin/env bash
set -euo pipefail

MODEL_DOWNLOAD="${MODEL_DOWNLOAD:-hf}"
CKPT_DIR="${CKPT_DIR:-/app/ckpts}"
HF_TOKEN="${HF_TOKEN:-}"

need_download=0
if [[ ! -d "${CKPT_DIR}" ]]; then
  mkdir -p "${CKPT_DIR}"
  need_download=1
elif [[ -z "$(ls -A "${CKPT_DIR}")" ]]; then
  need_download=1
fi

if [[ "${MODEL_DOWNLOAD}" != "none" && "${need_download}" -eq 1 ]]; then
  echo "⬇️ Downloading model weights to ${CKPT_DIR} (source=${MODEL_DOWNLOAD})"
  if [[ "${MODEL_DOWNLOAD}" == "hf" ]]; then
    if [[ -n "${HF_TOKEN}" ]]; then
      export HF_TOKEN
    fi
    huggingface-cli download hpcai-tech/Open-Sora-v2 --local-dir "${CKPT_DIR}"
  elif [[ "${MODEL_DOWNLOAD}" == "modelscope" ]]; then
    modelscope download hpcai-tech/Open-Sora-v2 --local_dir "${CKPT_DIR}"
  else
    echo "❌ Unknown MODEL_DOWNLOAD=${MODEL_DOWNLOAD}. Use hf|modelscope|none."
    exit 1
  fi
else
  echo "✅ Skip download (MODEL_DOWNLOAD=${MODEL_DOWNLOAD}, CKPT_DIR=${CKPT_DIR})"
fi

exec python3 -u /app/gpu_client.py
