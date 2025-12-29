import asyncio
import json
import subprocess
import time
import websockets
import shlex
import os
from websockets.exceptions import ConnectionClosed

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
# Heartbeat（关闭内置 ping 后，使用自定义心跳）
# =========================================================
async def heartbeat(ws):
    try:
        while True:
            await ws.send(json.dumps({
                "type": "heartbeat",
                "ts": time.time()
            }))
            await asyncio.sleep(5)
    except Exception:
        # WS 关闭 / 异常时，安静退出
        print("🫀 Heartbeat stopped")


# =========================================================
# GPU 主循环
# =========================================================
async def gpu_loop():
    while True:  # 为将来自动重连预留
        try:
            async with websockets.connect(
                BRIDGE_WS,
                ping_interval=None,   # ⭐ 关键：关闭内置 ping
                ping_timeout=None
            ) as ws:

                # ---------- 注册 ----------
                await ws.send(json.dumps({
                    "gpu_id": GPU_ID
                }))
                print(f"🔥 GPU registered: {GPU_ID}")

                hb_task = asyncio.create_task(heartbeat(ws))

                try:
                    while True:
                        try:
                            raw = await ws.recv()
                        except ConnectionClosed:
                            print("🔌 WS closed by server")
                            break

                        msg = json.loads(raw)

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

                finally:
                    hb_task.cancel()
                    print("🧹 Cleanup heartbeat task")

        except Exception as e:
            # 连接失败 / 网络抖动 / bridge 重启
            print("⚠️ GPU client error:", e)
            print("⏳ Retry in 5 seconds...")
            await asyncio.sleep(5)


# =========================================================
# Entry
# =========================================================
if __name__ == "__main__":
    asyncio.run(gpu_loop())
