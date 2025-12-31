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
# =========================================================
# 基础配置
# =========================================================
GPU_ID = "gpu-01"

# GPU 机器主动连公网 Bridge
BRIDGE_WS = "wss://www.ccioi.com/ws/gpu"
SERVER_BASE = "https://www.ccioi.com"
# Open-Sora 固定输出路径（与你当前保持一致）
LOCAL_VIDEO_PATH = "/data/Open-Sora/outputs/videodemo5/video_256px/prompt_0000.mp4"

# OSS 配置（只负责上传，不负责权限）
OSSUTIL_BIN = "/data/ossutil64"
OSS_BUCKET = "yisvideo"
OSS_ENDPOINT = "oss-cn-shanghai.aliyuncs.com"


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
                        rc = await stream_process_and_send_logs(
                            ws=ws,
                            task_id=task_id,
                            command=torch_command
                        )

                        if rc != 0:
                            await ws.send(json.dumps({
                                "type": "task_finished",
                                "task_id": task_id,
                                "user_id": user_id,
                                "prompt": prompt,
                                "status": "failed",
                                "error": "torchrun failed",
                                "returncode": rc
                            }))
                            continue

                        # =================================================
                        # 2️⃣ 查找输出视频（从 --save-dir 目录里找最新 mp4）
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

                        video_path = pick_best_mp4(save_dir)

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

                            await ws.send(
                                json.dumps(
                                    {
                                        "type": "task_finished",
                                        "task_id": task_id,
                                        "user_id": user_id,
                                        "prompt": prompt,
                                        "status": "success",
                                        "returncode": 0,
                                        "public_url": public_url,
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

                        # =================================================
                        # 4️⃣ 成功回传（Bridge 会做 history / 计费）
                        # =================================================
                        await ws.send(json.dumps({
                            "type": "task_finished",
                            "task_id": task_id,
                            "user_id": user_id,
                            "prompt": prompt,
                            "status": "success",
                            "returncode": 0,
                            "output": {
                                "local_path": LOCAL_VIDEO_PATH,
                                "oss_path": oss_dest,
                                "public_url": public_url
                            }
                        }))

                        print(f"✅ [{task_id}] Done → {public_url}")

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
