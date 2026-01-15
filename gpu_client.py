import asyncio
import json
import os
import shlex
import subprocess
import time
import threading
from typing import Optional

import websockets
import requests
import math
# =========================================================
# 基础配置
# =========================================================
GPU_ID = "gpu-01"

# GPU 机器主动连公网 Bridge
BRIDGE_WS = "wss://www.ccioi.com/ws/gpu"
SERVER_BASE = "https://www.ccioi.com/api"
# Open-Sora 固定输出路径（与你当前保持一致）
LOCAL_VIDEO_PATH = "/data/Open-Sora/outputs/videodemo5/video_256px/prompt_0000.mp4"

# OSS 配置（只负责上传，不负责权限）
OSSUTIL_BIN = "/data/ossutil64"
OSS_BUCKET = "yisvideo"
OSS_ENDPOINT = "oss-cn-shanghai.aliyuncs.com"

# 拼接配置
STITCH_CROSSFADE_SEC = 0.5
STITCH_ENABLE_INTERP = False
STITCH_INTERP_FPS = 32


# =========================================================
# 子进程：流式执行 + 日志回传
# =========================================================
async def stream_process_and_send_logs(ws, task_id, command, prefix=""):
    print(f"⚙️ EXEC: {command}")

    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    loop = asyncio.get_running_loop()

    def reader():
        for line in proc.stdout:
            line = line.rstrip()
            asyncio.run_coroutine_threadsafe(
                ws.send(json.dumps({
                    "type": "TASK_LOG",
                    "task_id": task_id,
                    "stream": "stdout",
                    "line": f"{prefix}{line}"
                })),
                loop
            )

    t = threading.Thread(target=reader, daemon=True)
    t.start()

    return await loop.run_in_executor(None, proc.wait)

# =========================================================
# HTTP 上传到 Server（关键）
# =========================================================
def upload_video_to_server(
    task_id: str,
    user_id: str,
    prompt: Optional[str],
    video_path: str,
):
    url = f"{SERVER_BASE}/gpu/upload"

    with open(video_path, "rb") as f:
        files = {
            "file": ("video.mp4", f, "video/mp4")
        }
        data = {
            "task_id": task_id,
            "user_id": user_id,
            "prompt": prompt or "",
        }

        resp = requests.post(url, data=data, files=files, timeout=600)
        resp.raise_for_status()
        return resp.json()

import re
from pathlib import Path

def parse_save_dir(torch_command: str) -> Optional[str]:
    """
    从 torchrun 命令中解析 --save-dir 的值
    支持：--save-dir outputs/xxx
         --save-dir "outputs/xxx"
    """
    m = re.search(r'--save-dir\s+(".*?"|\'.*?\'|\S+)', torch_command)
    if not m:
        return None
    val = m.group(1).strip()
    if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
        val = val[1:-1]
    return val

def pick_best_mp4(save_dir: str) -> Optional[str]:
    """
    在 save_dir 下递归找 mp4，并挑一个“最可能是最终输出”的：
    - 目录优先：video_768px > video_512px > video_256px > 其他
    - 然后：mtime 最新
    - 然后：size 最大
    """
    p = Path(save_dir)
    if not p.exists() or not p.is_dir():
        return None

    mp4s = list(p.rglob("*.mp4"))
    if not mp4s:
        return None

    def res_rank(path: Path) -> int:
        s = str(path)
        if "video_768px" in s:
            return 3
        if "video_512px" in s:
            return 2
        if "video_256px" in s:
            return 1
        return 0

    # 评分：先分辨率目录，再 mtime，再 size
    mp4s.sort(
        key=lambda x: (
            res_rank(x),
            x.stat().st_mtime,
            x.stat().st_size,
        ),
        reverse=True,
    )
    return str(mp4s[0])


def _parse_flag_value(tokens: list[str], flags: list[str]) -> Optional[str]:
    for i, tok in enumerate(tokens):
        if tok in flags and i + 1 < len(tokens):
            return tokens[i + 1]
        for f in flags:
            if tok.startswith(f + "="):
                return tok.split("=", 1)[1]
    return None


def _set_flag(tokens: list[str], flag: str, value: str) -> None:
    for i, tok in enumerate(tokens):
        if tok == flag and i + 1 < len(tokens):
            tokens[i + 1] = value
            return
        if tok.startswith(flag + "="):
            tokens[i] = f"{flag}={value}"
            return
    tokens.extend([flag, value])


def _find_config_path(tokens: list[str]) -> Optional[str]:
    for i, tok in enumerate(tokens):
        if tok.endswith("scripts/diffusion/inference.py"):
            for j in range(i + 1, len(tokens)):
                if not tokens[j].startswith("-"):
                    return tokens[j]
    return None


def _parse_config_defaults(config_path: str) -> dict:
    visited = set()

    def _parse_file(path: str) -> dict:
        if not path or path in visited or (not os.path.exists(path)):
            return {}
        visited.add(path)
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception:
            return {}

        base_values = {}
        m = re.search(r"_base_\\s*=\\s*\\[(.*?)\\]", content, re.S)
        if m:
            base_items = re.findall(r'["\\\'](.*?)["\\\']', m.group(1))
            for item in base_items:
                base_path = item
                if not os.path.isabs(base_path):
                    base_path = os.path.join(os.path.dirname(path), base_path)
                base_values.update(_parse_file(base_path))

        values = dict(base_values)
        m_num = re.search(r"num_frames\\s*=\\s*(\\d+)", content)
        if m_num:
            values["num_frames"] = int(m_num.group(1))
        m_fps = re.search(r"fps_save\\s*=\\s*(\\d+)", content)
        if m_fps:
            values["fps_save"] = int(m_fps.group(1))
        return values

    return _parse_file(config_path)


def _align_frames(frames: int) -> int:
    if frames <= 1:
        return 1
    k = max(0, (frames - 1) // 4)
    return k * 4 + 1


def _get_video_duration_seconds(video_path: str) -> Optional[float]:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=nw=1:nk=1",
        video_path,
    ]
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        return float(result.stdout.strip())
    except Exception:
        return None


def _plan_v2v_segments(torch_command: str) -> Optional[dict]:
    tokens = shlex.split(torch_command)
    num_frames_str = _parse_flag_value(tokens, ["--sampling_option.num_frames", "--sampling-option.num-frames"])
    fps_save_str = _parse_flag_value(tokens, ["--fps_save", "--fps-save"])

    config_path = _find_config_path(tokens)
    if config_path and not os.path.isabs(config_path):
        config_path = os.path.join(os.getcwd(), config_path)
    cfg_defaults = _parse_config_defaults(config_path) if config_path else {}

    num_frames = int(num_frames_str) if num_frames_str else cfg_defaults.get("num_frames")
    fps_save = int(fps_save_str) if fps_save_str else cfg_defaults.get("fps_save")

    if not num_frames or not fps_save:
        return None

    if num_frames <= fps_save * 5:
        return None

    max_frames = _align_frames(int(fps_save * 5))
    segments = max(2, math.ceil(num_frames / max_frames))
    duration_seconds = num_frames / float(fps_save)

    return {
        "num_frames": num_frames,
        "fps_save": fps_save,
        "max_frames": max_frames,
        "segments": segments,
        "duration_seconds": duration_seconds,
    }


async def _run_segmented_v2v(
    ws,
    task_id: str,
    torch_command: str,
    save_dir: str,
):
    tokens = shlex.split(torch_command)
    plan = _plan_v2v_segments(torch_command)
    if not plan:
        rc = await stream_process_and_send_logs(
            ws=ws,
            task_id=task_id,
            command=torch_command
        )
        if rc != 0:
            return rc, None, "torchrun failed"
        video_path = pick_best_mp4(save_dir)
        if not video_path:
            return 1, None, f"output video not found under save_dir: {save_dir}"
        return 0, video_path, None

    segment_paths = []
    for idx in range(plan["segments"]):
        seg_tokens = list(tokens)
        seg_save_dir = os.path.join(save_dir, "segments", task_id, f"seg_{idx+1:02d}")
        os.makedirs(seg_save_dir, exist_ok=True)
        _set_flag(seg_tokens, "--save-dir", seg_save_dir)
        _set_flag(seg_tokens, "--sampling_option.num_frames", str(plan["max_frames"]))

        if idx > 0:
            _set_flag(seg_tokens, "--cond_type", "v2v_tail")
            _set_flag(seg_tokens, "--ref", segment_paths[-1])

        seg_command = " ".join(shlex.quote(t) for t in seg_tokens)
        rc = await stream_process_and_send_logs(
            ws=ws,
            task_id=task_id,
            command=seg_command,
            prefix=f"[seg {idx+1}/{plan['segments']}] "
        )
        if rc != 0:
            return rc, None, "torchrun failed"

        seg_video = pick_best_mp4(seg_save_dir)
        if not seg_video:
            return 1, None, f"output video not found under save_dir: {seg_save_dir}"
        segment_paths.append(seg_video)

    # 拼接视频
    concat_dir = os.path.join(save_dir, "segments", task_id)
    stitched_path = os.path.join(concat_dir, "stitched.mp4")
    if len(segment_paths) == 1:
        return 0, segment_paths[0], None

    durations = []
    for p in segment_paths:
        d = _get_video_duration_seconds(p)
        durations.append(d)

    if any(d is None for d in durations):
        # Fallback: approximate by fps and frames
        approx = plan["max_frames"] / float(plan["fps_save"])
        durations = [approx] * len(segment_paths)

    fade = STITCH_CROSSFADE_SEC
    inputs = " ".join(f"-i {shlex.quote(p)}" for p in segment_paths)
    filter_parts = []
    for idx in range(len(segment_paths)):
        filter_parts.append(f"[{idx}:v]setpts=PTS-STARTPTS[v{idx}]")

    offset = max(0.0, durations[0] - fade)
    filter_parts.append(
        f"[v0][v1]xfade=transition=fade:duration={fade}:offset={offset}[x1]"
    )
    acc = durations[0]
    for i in range(2, len(segment_paths)):
        acc += durations[i - 1]
        offset = max(0.0, acc - fade * i)
        filter_parts.append(
            f"[x{i-1}][v{i}]xfade=transition=fade:duration={fade}:offset={offset}[x{i}]"
        )

    last_tag = "[x1]" if len(segment_paths) == 2 else f"[x{len(segment_paths)-1}]"

    filter_complex = ";".join(filter_parts)
    if STITCH_ENABLE_INTERP:
        filter_complex += f";{last_tag}minterpolate=fps={STITCH_INTERP_FPS}[vout]"
        map_tag = "[vout]"
    else:
        map_tag = last_tag

    ffmpeg_cmd = (
        f"ffmpeg -y {inputs} -filter_complex {shlex.quote(filter_complex)} "
        f"-map {map_tag} -r {plan['fps_save']} -t {plan['duration_seconds']:.3f} "
        f"{shlex.quote(stitched_path)}"
    )
    rc = await stream_process_and_send_logs(
        ws=ws,
        task_id=task_id,
        command=ffmpeg_cmd,
        prefix="[stitch] "
    )
    if rc != 0 or (not os.path.exists(stitched_path)):
        return 1, None, "ffmpeg stitch failed"

    return 0, stitched_path, None


# =========================================================
# GPU 主循环（断线自动重连）
# =========================================================
async def run_gpu_client():
    while True:
        try:
            async with websockets.connect(
                BRIDGE_WS,
                ping_interval=10,
                ping_timeout=10,
            ) as ws:
                # ---------- 注册 ----------
                await ws.send(json.dumps({
                    "gpu_id": GPU_ID
                }))
                print(f"🔥 GPU registered: {GPU_ID}")

                # ---------- 心跳 ----------
                async def heartbeat():
                    while True:
                        await ws.send(json.dumps({
                            "type": "heartbeat",
                            "ts": time.time()
                        }))
                        await asyncio.sleep(5)

                heartbeat_task = asyncio.create_task(heartbeat())

                try:
                    while True:
                        msg = json.loads(await ws.recv())

                        if msg.get("type") != "exec_command":
                            continue

                        # =================================================
                        # 接收 Bridge 下发任务
                        # =================================================
                        task_id = msg["task_id"]
                        torch_command = msg["command"]

                        # ✅ 关键：原样接收，不解析
                        user_id = msg.get("user_id")
                        prompt = msg.get("prompt")

                        # =================================================
                        # 1️⃣ 执行 torchrun（日志流式回传）
                        # =================================================
                        save_dir = parse_save_dir(torch_command)
                        if not save_dir:
                            await ws.send(json.dumps({
                                "type": "task_finished",
                                "task_id": task_id,
                                "user_id": user_id,
                                "prompt": prompt,
                                "status": "failed",
                                "error": "missing --save-dir in torch command"
                            }))
                            continue

                        rc, video_path, err = await _run_segmented_v2v(
                            ws=ws,
                            task_id=task_id,
                            torch_command=torch_command,
                            save_dir=save_dir,
                        )

                        if rc != 0 or not video_path:
                            await ws.send(json.dumps({
                                "type": "task_finished",
                                "task_id": task_id,
                                "user_id": user_id,
                                "prompt": prompt,
                                "status": "failed",
                                "error": err or "torchrun failed",
                                "returncode": rc
                            }))
                            continue

                        # =================================================
                        # 2️⃣ 查找输出视频（从 --save-dir 目录里找最新 mp4）
                        # =================================================
                        if not video_path or (not os.path.exists(video_path)):
                            await ws.send(json.dumps({
                                "type": "task_finished",
                                "task_id": task_id,
                                "user_id": user_id,
                                "prompt": prompt,
                                "status": "failed",
                                "error": f"output video not found under save_dir: {save_dir}"
                            }))
                            continue

                        # =================================================
                        # 3️⃣ 上传 OSS
                        # =================================================
                        # =================================================
                        # 3️⃣ HTTP 上传给 Server
                        # =================================================
                        try:
                            result = upload_video_to_server(
                                task_id=task_id,
                                user_id=user_id,
                                prompt=prompt,
                                video_path=video_path,
                            )

                            public_url = result.get("public_url")
                            
                            print(f"✅ [{task_id}] Done → {public_url}")
                            await ws.send(
                                json.dumps(
                                    {
                                       "type": "task_finished",
                                        "task_id": task_id,
                                        "user_id": user_id,
                                        "prompt": prompt,
                                        "status": "success",
                                        "returncode": 0,
                                        "output": {
                                            "local_path": "",
                                            "oss_path": "",
                                            "public_url": public_url
                                        }
                                    }
                                )
                            )

                            print(f"✅ [{task_id}] Uploaded → {public_url}")

                        except Exception as e:
                            await ws.send(
                                json.dumps(
                                    {
                                        "type": "task_finished",
                                        "task_id": task_id,
                                        "user_id": user_id,
                                        "prompt": prompt,
                                        "status": "failed",
                                        "error": f"upload failed: {e}",
                                    }
                                )
                            )

                finally:
                    heartbeat_task.cancel()
                    print("🧹 Cleanup heartbeat task")

        except Exception as e:
            print(f"🔌 WS disconnected / error: {e} → retry in 3s")
            await asyncio.sleep(3)


# =========================================================
# 入口
# =========================================================
if __name__ == "__main__":
    asyncio.run(run_gpu_client())
