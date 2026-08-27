# ── AutoBatch Pipeline 多阶段构建 ──────────────────────────────
# Stage 1: builder  — 安装 Python 依赖到独立目录
# Stage 2: runtime  — 仅拷贝必要文件 + 依赖，非 root 运行
# 设计参见 docs/evolution.md §6.x（Docker 化部署）
#
# 基础镜像 python:3.12-slim：与 CI 测试矩阵覆盖的 3.10/3.11/3.12 对齐
# （此前基于 python:3.13.1-slim，该版本组合从未被任何 CI 腿验证，
#   属交付盲区——2026-08 审查后对齐到已验证的 3.12）。
# 注意：镜像内无 JDK，`engine.backend="spark"` 在容器内不可用；
# 容器化运行仅支持 python / polars 计算路径（见 readme「Docker」一节）。
# ────────────────────────────────────────────────────────────────

# ── Stage 1: builder ────────────────────────────────────────────
FROM python:3.12-slim AS builder
WORKDIR /build
COPY requirements.txt .
# 装到 /install（便于 runtime 阶段精确拷贝，不含 pip/setuptools 等工具）
RUN pip install --no-cache-dir --target=/install -r requirements.txt

# ── Stage 2: runtime ────────────────────────────────────────────
FROM python:3.12-slim AS runtime
WORKDIR /app

# 拷贝 builder 阶段安装的依赖
COPY --from=builder /install /app/vendor
ENV PYTHONPATH="/app/vendor"
ENV PATH="/app/vendor/bin:${PATH}"

# 创建非 root 用户（UID/GID 1000，与 K8s/securityContext 约定一致）
RUN useradd -m -u 1000 autobatch

# 拷贝应用代码（仅 src + config + 入口，排除 tests/benchmarks/docs 等）
COPY --chown=autobatch:autobatch src/ ./src/
COPY --chown=autobatch:autobatch config/ ./config/
COPY --chown=autobatch:autobatch main.py ./
COPY --chown=autobatch:autobatch requirements.txt ./

# 创建运行时目录并赋属主
RUN mkdir -p run data/raw state && chown -R autobatch:autobatch run data state

USER autobatch

# 健康检查：入口模块可导入 + 配置可解析（与 docker-compose.yml 的 healthcheck 语义一致）
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import json, src.pipeline; json.load(open('config/pipeline.json', encoding='utf-8-sig'))"

CMD ["python", "-m", "src.pipeline", "--config", "config/pipeline.json"]
