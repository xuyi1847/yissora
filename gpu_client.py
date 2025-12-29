import asyncio
import json
import subprocess
import time
import websockets
import shlex
import os

# =========================================================
# 基本配置
# =========================================================
GPU_ID = "gpu-01"

BRIDGE_WS = "ws://115.191.1.112:8000/ws/gpu"

# torchrun 固定输出路径（按你当前 Open-Sora）
LOCAL_VIDEO_PATH = "/data/Open-Sora/outputs/videodemo5/video_256px/prompt_0000.mp4"

# OSS 配置
OSSUTIL_BIN = "/data/ossutil64"
OSS_BUCKET = "yisvideo"
OSS_ENDPOINT = "oss-cn-shanghai.aliyuncs.com"


# =========================================================
# 工具函数
# =========================================================
def run_command(command: str) -> int:
    print("⚙️ EXEC:", command)
    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    for line in proc.stdout:
        print("[GPU]", line.rstrip())

    return proc.wait()


# =========================================================
# GPU 主循环
# =========================================================
async def gpu_loop():
    async with websockets.connect(BRIDGE_WS) as ws:
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

        asyncio.create_task(heartbeat())

        # ---------- 任务循环 ----------
        while True:
            msg = json.loads(await ws.recv())

            if msg.get("type") != "exec_command":
                continue

            task_id = msg["task_id"]
            torch_command = msg["command"]

            print(f"🚀 [{task_id}] Start task")

            # ========== 1. 执行 torchrun ==========
            rc = run_command(torch_command)
            if rc != 0:
                await ws.send(json.dumps({
                    "type": "task_finished",
                    "task_id": task_id,
                    "status": "failed",
                    "error": "torchrun failed",
                    "returncode": rc
                }))
                continue

            # ========== 2. 生成 OSS 路径 ==========
            oss_object_path = f"videos/{task_id}.mp4"
            oss_dest = f"oss://{OSS_BUCKET}/{oss_object_path}"
            public_url = f"https://{OSS_BUCKET}.{OSS_ENDPOINT}/{oss_object_path}"

            if not os.path.exists(LOCAL_VIDEO_PATH):
                await ws.send(json.dumps({
                    "type": "task_finished",
                    "task_id": task_id,
                    "status": "failed",
                    "error": "output video not found"
                }))
                continue

            # ========== 3. 上传 OSS ==========
            oss_cmd = (
                f"{OSSUTIL_BIN} cp "
                f"{shlex.quote(LOCAL_VIDEO_PATH)} "
                f"{oss_dest} -f"
            )

            rc = run_command(oss_cmd)
            if rc != 0:
                await ws.send(json.dumps({
                    "type": "task_finished",
                    "task_id": task_id,
                    "status": "failed",
                    "error": "OSS upload failed",
                    "returncode": rc
                }))
                continue

            # ========== 4. 回传成功 ==========
            await ws.send(json.dumps({
                "type": "task_finished",
                "task_id": task_id,
                "status": "success",
                "output": {
                    "oss_path": oss_dest,
                    "public_url": public_url
                }
            }))

            print(f"✅ [{task_id}] Done → {public_url}")


if __name__ == "__main__":
    asyncio.run(gpu_loop())
