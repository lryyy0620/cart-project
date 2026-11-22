# 不打包安装，直接把这个仓库的 src 放进 PYTHONPATH。
# 这样镜像构建不依赖任何构建后端，任何机器上都能一把过。
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

WORKDIR /app

RUN pip install --no-cache-dir "fastapi>=0.115" "uvicorn>=0.30" "httpx>=0.27"

COPY src ./src
COPY scripts ./scripts

EXPOSE 8000

# --factory：由环境变量 REPLICA_NAME / CLUSTER / PEERS / DATA_DIR 决定自己是谁
CMD ["uvicorn", "cart.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
