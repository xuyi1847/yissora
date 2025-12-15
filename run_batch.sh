#!/bin/bash

BASE_DIR="/data/Open-Sora"
PROMPT_DIR="${BASE_DIR}/prompts"
OUT_DIR="${BASE_DIR}/outputs"
OSS_PATH="oss://yisvideo/videos"
SCRIPT="${BASE_DIR}/scripts/diffusion/inference.py"
CONFIG="${BASE_DIR}/configs/diffusion/inference/t2i2v_256px.py"

mkdir -p "$PROMPT_DIR"
mkdir -p "$OUT_DIR"

echo "=============================="
echo "  批量视频生成任务开始"
echo "=============================="

i=1
for prompt_file in ${PROMPT_DIR}/*.txt; do
    [[ ! -e "$prompt_file" ]] && echo "没有找到 prompts/*.txt" && exit 1

    echo "=============================="
    echo " 开始生成视频 ${i} ..."
    echo " 提示词文件: $prompt_file"
    echo "=============================="

    VIDEO_DIR="${OUT_DIR}/video${i}"
    mkdir -p "$VIDEO_DIR"

    torchrun --nproc_per_node 2 --standalone \
        $SCRIPT \
        $CONFIG \
        --save-dir "$VIDEO_DIR" \
        --prompt "$(cat $prompt_file)" \

    LOCAL_MP4=$(find "$VIDEO_DIR" -name "*.mp4" | head -1)

    if [[ ! -f "$LOCAL_MP4" ]]; then
        echo "❌ 错误: 没有生成视频文件，跳过上传"
        continue
    fi

    OSS_FILE="video${i}.mp4"
    /data/ossutil64 cp "$LOCAL_MP4" "$OSS_PATH/$OSS_FILE" -f

    echo "视频 ${i} 上传完成: https://yisvideo.oss-cn-shanghai.aliyuncs.com/videos/${OSS_FILE}"
    echo

    i=$((i+1))
done

echo "=============================="
echo " 全部生成完成！视频链接如下："
echo "=============================="

for idx in {1..6}; do
    echo "https://yisvideo.oss-cn-shanghai.aliyuncs.com/videos/video${idx}.mp4"
done

