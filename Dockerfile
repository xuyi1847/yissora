FROM nvidia/cuda:12.1.1-cudnn8-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    git \
    ffmpeg \
    libgl1 \
    libglib2.0-0 \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN grep -vE '^(torch|torchvision)==?' /app/requirements.txt > /tmp/requirements.txt

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --index-url https://download.pytorch.org/whl/cu121 \
      torch==2.4.0 torchvision==0.19.0 && \
    python3 -m pip install -r /tmp/requirements.txt && \
    python3 -m pip install websockets requests

COPY . /app
ENV PYTHONPATH=/app

CMD ["python3", "-u", "gpu_client.py"]
