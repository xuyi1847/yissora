import asyncio
import json
import os
import shlex
import subprocess
import time
from typing import Optional

import websockets

# =========================================================
# 配置
# =========================================================
GPU_ID = "gpu-01"

# 公网中转地址（GPU 内网主动连出去）
BRIDGE_WS = "ws://115.191.1.112:8000/ws/gpu"

# Open-Sora 输出文件（按你当前固定路径）
LOCAL_VIDEO_PATH = "/data/Open-Sora/outputs/videodemo5/video_256px/prompt_0000.mp4"

# OSS 配置
OSSUTIL_BIN = "/data/ossutil64"
OSS_BUCKET = "yisvideo"
OSS_ENDPOINT = "oss-cn-shanghai.aliyuncs.com"


# =========================================================
# 子进程流式执行并回传日志
# =========================================================
async def stream_process_and_send_logs(
    ws,
    task_id: str,
    command: str,
    prefix: str = ""
) -> int:
    """
    运行 command，逐行读取 stdout(含stderr合并)，并通过 ws 发送 TASK_LOG
    返回 returncode
    """
    print(f"⚙️ EXEC: {command}")

    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    assert proc.stdout is not None

    for line in proc.stdout:
        line = line.rstrip()
        local_line = f"{prefix}{line}" if prefix else line
        print(f"[GPU] {local_line}")

        # 实时推送日志到中转
        await ws.send(json.dumps({
            "type": "TASK_LOG",
            "task_id": task_id,
            "stream": "stdout",
            "line": local_line
        }))

    return proc.wait()


# =========================================================
# 主循环（断线重连）
# =========================================================
async def run_gpu_client():
    while True:
        try:
            async with websockets.connect(BRIDGE_WS, ping_interval=None) as ws:
                # ---------- 注册 ----------
                await ws.send(json.dumps({"gpu_id": GPU_ID}))
                print(f"🔥 GPU registered: {GPU_ID}")

                # ---------- 心跳 ----------
                heartbeat_task: Optional[asyncio.Task] = None

                async def heartbeat():
                    while True:
                        await ws.send(json.dumps({
                            "type": "heartbeat",
                            "ts": time.time()
                        }))
                        await asyncio.sleep(5)

                heartbeat_task = asyncio.create_task(heartbeat())

                try:
                    # ---------- 等待任务 ----------
                    while True:
                        msg = json.loads(await ws.recv())

                        if msg.get("type") != "exec_command":
                            continue

                        task_id = msg["task_id"]
                        torch_command = msg["command"]

                        # 1) torchrun 任务日志流
                        rc = await stream_process_and_send_logs(
                            ws=ws,
                            task_id=task_id,
                            command=torch_command,
                            prefix=""
                        )

                        if rc != 0:
                            fail_payload = {
                                "type": "task_finished",
                                "task_id": task_id,
                                "status": "failed",
                                "error": "torchrun failed",
                                "returncode": rc
                            }
                            print("📤 Sending task_finished (failed):")
                            print(json.dumps(fail_payload, ensure_ascii=False, indent=2))
                            await ws.send(json.dumps(fail_payload))
                            continue

                        # 2) 检查输出文件存在
                        if not os.path.exists(LOCAL_VIDEO_PATH):
                            fail_payload = {
                                "type": "task_finished",
                                "task_id": task_id,
                                "status": "failed",
                                "error": f"output video not found: {LOCAL_VIDEO_PATH}"
                            }
                            print("📤 Sending task_finished (failed):")
                            print(json.dumps(fail_payload, ensure_ascii=False, indent=2))
                            await ws.send(json.dumps(fail_payload))
                            continue

                        # 3) 动态 OSS 路径 & URL
                        oss_object_path = f"videos/{task_id}.mp4"
                        oss_dest = f"oss://{OSS_BUCKET}/{oss_object_path}"
                        public_url = f"https://{OSS_BUCKET}.{OSS_ENDPOINT}/{oss_object_path}"

                        # 4) ossutil 上传日志流
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
                            fail_payload = {
                                "type": "task_finished",
                                "task_id": task_id,
                                "status": "failed",
                                "error": "OSS upload failed",
                                "returncode": oss_rc
                            }
                            print("📤 Sending task_finished (failed):")
                            print(json.dumps(fail_payload, ensure_ascii=False, indent=2))
                            await ws.send(json.dumps(fail_payload))
                            continue

                        # 5) 成功回传
                        ok_payload = {
                            "type": "task_finished",
                            "task_id": task_id,
                            "status": "success",
                            "returncode": 0,
                            "output": {
                                "local_path": LOCAL_VIDEO_PATH,
                                "oss_path": oss_dest,
                                "public_url": public_url
                            }
                        }

                        print("📤 Sending task_finished (success):")
                        print(json.dumps(ok_payload, ensure_ascii=False, indent=2))
                        await ws.send(json.dumps(ok_payload))

                        print(f"✅ [{task_id}] Done → {public_url}")

                finally:
                    if heartbeat_task:
                        heartbeat_task.cancel()
                        print("🧹 Cleanup heartbeat task")

        except Exception as e:
            print(f"🔌 WS error/disconnected, retry in 3s. error={e}")
            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(run_gpu_client())
