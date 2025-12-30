import asyncio
import json
import os
import shlex
import subprocess
import time
import threading
from typing import Optional

import websockets

# =========================================================
# 基础配置
# =========================================================
GPU_ID = "gpu-01"

# GPU 机器主动连公网 Bridge
BRIDGE_WS = "wss://www.ccioi.com/ws/gpu"

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
                        # 2️⃣ 校验输出文件
                        # =================================================
                        if not os.path.exists(LOCAL_VIDEO_PATH):
                            await ws.send(json.dumps({
                                "type": "task_finished",
                                "task_id": task_id,
                                "user_id": user_id,
                                "prompt": prompt,
                                "status": "failed",
                                "error": f"output video not found: {LOCAL_VIDEO_PATH}"
                            }))
                            continue

                        # =================================================
                        # 3️⃣ 上传 OSS
                        # =================================================
                        oss_object_path = f"videos/{task_id}.mp4"
                        oss_dest = f"oss://{OSS_BUCKET}/{oss_object_path}"
                        public_url = f"https://{OSS_BUCKET}.{OSS_ENDPOINT}/{oss_object_path}"

                        oss_cmd = (
                            f"{OSSUTIL_BIN} cp "
                            f"{shlex.quote(LOCAL_VIDEO_PATH)} "
                            f"{oss_dest} -f"
                        )

                        oss_rc = await stream_process_and_send_logs(
                            ws=ws,
                            task_id=task_id,
                            command=oss_cmd,
                            prefix="[OSS] "
                        )

                        if oss_rc != 0:
                            await ws.send(json.dumps({
                                "type": "task_finished",
                                "task_id": task_id,
                                "user_id": user_id,
                                "prompt": prompt,
                                "status": "failed",
                                "error": "OSS upload failed",
                                "returncode": oss_rc
                            }))
                            continue

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
