#!/bin/bash

BASE_DIR="/data/Open-Sora"
PROMPT_DIR="${BASE_DIR}/prompts"
OUT_DIR="${BASE_DIR}/outputs"
OSS_PATH="oss://yisvideo/videos"

SCRIPT="${BASE_DIR}/scripts/diffusion/inference.py"
CONFIG="${BASE_DIR}/configs/diffusion/inference/t2i2v_768px.py"
FIRST_REF="${BASE_DIR}/assets/FIRST_REF.png"
mkdir -p "$PROMPT_DIR"
mkdir -p "$OUT_DIR"

echo "=============================="
echo "  批量 i2v 连续视频生成开始"
echo "  第一段不使用 ref"
echo "=============================="

REF_IMAGE=""
i=1

for prompt_file in ${PROMPT_DIR}/*.txt; do
    [[ ! -e "$prompt_file" ]] && echo "❌ 没有找到 prompts/*.txt" && exit 1

    echo "=============================="
    echo " 开始生成第 ${i} 段视频"
    echo " Prompt 文件: $prompt_file"
    [[ -n "$REF_IMAGE" ]] && echo " Ref 图片: $REF_IMAGE" || echo " Ref 图片: （无，第一段）"
    echo "=============================="

    VIDEO_DIR="${OUT_DIR}/video${i}"
    mkdir -p "$VIDEO_DIR"
    # ---------- 0. 如果视频已存在则跳过 ----------
    EXISTING_MP4=$(find "$VIDEO_DIR" -name "*.mp4" | head -1)

    if [[ -f "$EXISTING_MP4" ]]; then
        echo "⏭️  第 ${i} 段视频已存在，跳过生成："
        echo "     $EXISTING_MP4"
        REF_IMAGE="${VIDEO_DIR}/last_frame.png"
        i=$((i+1))
        continue
    fi
    # ---------- 1. 生成视频 ----------
    if [[ $i -eq 1 ]]; then
        # 第一段：不带 ref
        torchrun --nproc_per_node 2 --standalone \
            "$SCRIPT" \
            "$CONFIG" \
            --save-dir "$VIDEO_DIR" \
            --num_frames 96 \
            --prompt "$(cat "$prompt_file")" \
            --offload True
    else
        # 后续段：使用上一段 last frame 作为 ref
        torchrun --nproc_per_node 2 --standalone \
            "$SCRIPT" \
            "$CONFIG" \
            --cond_type i2v_head \
            --save-dir "$VIDEO_DIR" \
            --num_frames 96 \
            --prompt "$(cat "$prompt_file")" \
            --ref "$REF_IMAGE" \
            --offload True
    fi

    # ---------- 2. 找到生成的视频 ----------
	LOCAL_MP4=$(find "$VIDEO_DIR" -name "*.mp4" | head -1)

	if [[ ! -f "$LOCAL_MP4" ]]; then
	    echo "❌ 错误：没有生成视频文件，终止流程"
	    break
	fi

	# ---------- 3. 截取最后一帧（ffmpeg 4.x 兼容版） ----------
	LAST_FRAME="${VIDEO_DIR}/last_frame.png"

	/usr/bin/ffmpeg -y \
	    -i "$LOCAL_MP4" \
	    -vf reverse \
	    -frames:v 1 \
	    "$LAST_FRAME"

	if [[ ! -f "$LAST_FRAME" ]] || [[ ! -s "$LAST_FRAME" ]]; then
	    echo "❌ 错误：未能生成有效的 last_frame.png，终止流程"
	    break
	fi

	echo "✅ 已生成最后一帧: $LAST_FRAME"

    # ---------- 4. 上传视频 ----------
    OSS_FILE="video${i}.mp4"
    /data/ossutil64 cp "$LOCAL_MP4" "$OSS_PATH/$OSS_FILE" -f

    echo "📤 视频 ${i} 上传完成:"
    echo "https://yisvideo.oss-cn-shanghai.aliyuncs.com/videos/${OSS_FILE}"
    echo

    # ---------- 5. 更新 ref，用于下一轮 ----------
    REF_IMAGE="$LAST_FRAME"
    i=$((i+1))
done

echo "=============================="
echo " 连续视频生成完成"
echo "=============================="

for ((idx=1; idx<i; idx++)); do
    echo "https://yisvideo.oss-cn-shanghai.aliyuncs.com/videos/video${idx}.mp4"
done

