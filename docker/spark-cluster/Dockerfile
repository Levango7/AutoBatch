# ── AutoBatch Spark Standalone Cluster ──────────────────────────
# Base: Eclipse Temurin JRE 17（Master/Worker 运行时仅需 JRE）
# Spark: 4.2.0（清华镜像加速） + Hadoop-AWS 3.5.0 + AWS SDK v2 Bundle
# Python3 + PySpark（Worker Python 需要）
#
# Phase 5 可选：Iceberg Spark extensions（用 ARG ENABLE_ICEBERG=true 开启）
#   - Iceberg 官方 JAR 最高支持 Spark 4.1，不支持 Spark 4.2
#   - 启用 Iceberg 时需把 SPARK_VERSION 改为 4.1.0 + ICEBERG_VERSION=1.11.0
#   - JAR: iceberg-spark-runtime-4.1_2.13-1.11.0.jar + iceberg-aws-bundle-1.11.0.jar
#   - 缺省 ENABLE_ICEBERG=false，不影响现有 cluster 测试（Spark 4.2.0 保持）
#
# 供应链校验（二轮审查 H-5，任务79）：
#   - 所有外部下载（Spark tarball + 5 类 JAR）均钉扎官方校验和
#     （downloads.apache.org / archive.apache.org / repo1.maven.org）
#   - 下载后立即校验，失败即 exit 1（fail-fast，绝不产出带病镜像）
#   - 校验和不依赖下载镜像源：JAR 可从阿里云镜像下载，但校验值取自
#     repo1.maven.org 官方，防止"镜像源与下载源同时被污染"
#   - 各校验值来源与获取日期见同目录 CHECKSUMS.md
# ────────────────────────────────────────────────────────────────

# 基座钉扎 noble（Ubuntu 24.04，默认 python3=3.12）：浮动标签 17-jre 已漂移
# 到 Ubuntu 26.04（默认 python3=3.14），而项目 Driver 端 Python 钉扎 3.12
# （CI 矩阵 / 主 Dockerfile python:3.12-slim）。PySpark 要求 Driver 与
# Worker 的 Python 次版本一致（否则 PYTHON_VERSION_MISMATCH，2026-08-28
# 实测集群 Python 任务全灭），故基座必须钉扎以保持 worker python3=3.12。
# 下方 apt 安装后另有构建期断言，基座再漂移时 fail-fast。
FROM eclipse-temurin:17-jre-noble

# ── 元数据 ──────────────────────────────────────────────────────
LABEL maintainer="AutoBatch DevOps"
LABEL description="Spark Standalone cluster node (Master/Worker)"

# ── 构建参数 ────────────────────────────────────────────────────
ARG SPARK_VERSION=4.2.0
ARG HADOOP_AWS_VERSION=3.5.0
ARG AWS_SDK_V2_BUNDLE_VERSION=2.35.4
# ── 供应链校验和（钉扎官方值，版本与校验和必须同步变更） ────────
# 来源：repo1.maven.org 官方 .sha1（获取日期与 URL 见 CHECKSUMS.md）
# 若升级版本 ARG 而忘记更新校验和 ARG，构建会在校验步骤 fail-fast
ARG HADOOP_AWS_SHA1=9e594525d264c0db653c7f68da98b245f7d61ea5
ARG AWS_SDK_V2_BUNDLE_SHA1=7252265e3970b214708e68a8b74a8fa8c875af1e
# Phase 5: Iceberg Spark extensions（可选，缺省关闭）
# 启用方式：docker build --build-arg ENABLE_ICEBERG=true --build-arg SPARK_VERSION=4.1.0 .
ARG ENABLE_ICEBERG=false
ARG ICEBERG_VERSION=1.11.0
ARG ICEBERG_SPARK_MAJOR=4.1
ARG ICEBERG_SCALA_VERSION=2.13
# Iceberg JAR 官方 SHA1（对应 ICEBERG_VERSION=1.11.0 + 4.1_2.13 缺省组合）
ARG ICEBERG_SPARK_RUNTIME_SHA1=f9b1e4a18797c58ae406104ef5c75d1b64a373c5
ARG ICEBERG_AWS_BUNDLE_SHA1=afe5b22dd5bc2d2c0639049b06a92dae2545cac7

# ── 系统依赖：curl + tini + python3 + pip ──────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl ca-certificates tini procps python3 python3-pip python3-venv socat && \
    rm -rf /var/lib/apt/lists/*

# ── 构建期防护：worker python3 必须是 3.12（与 Driver 端 Python 对齐） ──
# PySpark 要求 Driver/Worker Python 次版本一致，否则 worker 端抛
# PYTHON_VERSION_MISMATCH 拒绝执行任何 Python 任务。基座钉扎 noble 时
# 默认 python3=3.12；若未来基座升级再次漂移（如 26.04 → 3.14），
# 此处立即终止构建，绝不产出带病镜像（与供应链校验同一 fail-fast 原则）。
RUN python3 --version | grep -q "Python 3\.12\." || { \
        echo ">>> [FATAL] worker python3 不是 3.12（实际 $(python3 --version)），将与 Driver 端 Python 错配！"; \
        exit 1; \
    }

# ── Spark 下载（含版本回退 + 国内镜像） ─────────────────────────
# 优先 ${SPARK_VERSION}（ARG，缺省 4.2.0），若不可用则回退 3.5.5
# 使用清华镜像加速，回退到 Apache 官方
# 注意：ENV 引用 ARG 必须在 ARG 声明之后（已在 line 20 声明）
ENV SPARK_VERSION_PRIMARY=${SPARK_VERSION}
ENV SPARK_VERSION_FALLBACK=3.5.5

RUN set -eux; \
    SPARK_VER=${SPARK_VERSION_PRIMARY}; \
    SPARK_TGZ="spark-${SPARK_VER}-bin-hadoop3.tgz"; \
    \
    # 镜像列表（按优先级）
    MIRROR_1="https://mirrors.tuna.tsinghua.edu.cn/apache/spark/spark-${SPARK_VER}/${SPARK_TGZ}"; \
    MIRROR_2="https://mirrors.aliyun.com/apache/spark/spark-${SPARK_VER}/${SPARK_TGZ}"; \
    MIRROR_3="https://archive.apache.org/dist/spark/spark-${SPARK_VER}/${SPARK_TGZ}"; \
    \
    DOWNLOADED=0; \
    for URL in "${MIRROR_1}" "${MIRROR_2}" "${MIRROR_3}"; do \
        echo ">>> 尝试下载 Spark ${SPARK_VER} from ${URL} ..."; \
        if curl -fsSL --retry 2 --retry-delay 3 --max-time 600 -o /tmp/${SPARK_TGZ} "${URL}"; then \
            DOWNLOADED=1; \
            echo ">>> Spark ${SPARK_VER} 下载成功 (${URL})"; \
            break; \
        else \
            echo ">>> 下载失败，尝试下一个源 ..."; \
        fi; \
    done; \
    \
    # 如果主版本全部失败，回退到备选版本
    if [ "${DOWNLOADED}" = "0" ]; then \
        SPARK_VER=${SPARK_VERSION_FALLBACK}; \
        SPARK_TGZ="spark-${SPARK_VER}-bin-hadoop3.tgz"; \
        MIRROR_1="https://mirrors.tuna.tsinghua.edu.cn/apache/spark/spark-${SPARK_VER}/${SPARK_TGZ}"; \
        MIRROR_2="https://mirrors.aliyun.com/apache/spark/spark-${SPARK_VER}/${SPARK_TGZ}"; \
        MIRROR_3="https://archive.apache.org/dist/spark/spark-${SPARK_VER}/${SPARK_TGZ}"; \
        for URL in "${MIRROR_1}" "${MIRROR_2}" "${MIRROR_3}"; do \
            echo ">>> 回退: 尝试下载 Spark ${SPARK_VER} from ${URL} ..."; \
            if curl -fsSL --retry 2 --retry-delay 3 --max-time 600 -o /tmp/${SPARK_TGZ} "${URL}"; then \
                DOWNLOADED=1; \
                echo ">>> Spark ${SPARK_VER} 下载成功 (${URL})"; \
                break; \
            else \
                echo ">>> 下载失败，尝试下一个源 ..."; \
            fi; \
        done; \
    fi; \
    \
    if [ "${DOWNLOADED}" = "0" ]; then \
        echo ">>> [FATAL] 所有下载源均失败！"; \
        exit 1; \
    fi; \
    \
    # ── 供应链校验（任务79）：版本 → 官方钉扎 SHA512 ─────────────
    # 无论实际下载自哪个镜像源，一律对照官方钉扎值校验，失败即 exit 1。
    # 校验和来源（获取日期 2026-08-27，详见 CHECKSUMS.md）：
    #   4.2.0 ← https://downloads.apache.org/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz.sha512
    #   4.1.0 ← https://archive.apache.org/dist/spark/spark-4.1.0/spark-4.1.0-bin-hadoop3.tgz.sha512
    #   3.5.5 ← https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz.sha512
    # 未在清单中的版本（如升级 SPARK_VERSION 忘更新）直接拒绝构建。
    case "${SPARK_VER}" in \
        4.2.0) SPARK_TGZ_SHA512="3a6559e8546ff387db8fe7a04b8fe4008853b467b972c2e343df8e14a81450534777719fc5d4998dd96519a86fdf9de140cee753c61e2e94db044d5f9555ddc4" ;; \
        4.1.0) SPARK_TGZ_SHA512="fff7f929d98779b096a2d2395b1b9db1ce277660f3852dd45e1457c373013f2669074315252181a3cea291d8f3a726c70f7f9b247e723edbf8080f40888edde1" ;; \
        3.5.5) SPARK_TGZ_SHA512="ec5ff678136b1ff981e396d1f7b5dfbf399439c5cb853917e8c954723194857607494a89b7e205fce988ec48b1590b5caeae3b18e1b5db1370c0522b256ff376" ;; \
        *) \
            echo ">>> [FATAL] Spark ${SPARK_VER} 无钉扎 SHA512，供应链校验不可跳过！"; \
            echo ">>> 请从 downloads.apache.org / archive.apache.org 获取该版本官方 .sha512 并更新本文件"; \
            exit 1; \
            ;; \
    esac; \
    echo ">>> 校验 Spark ${SPARK_VER} SHA512 ..."; \
    echo "${SPARK_TGZ_SHA512}  /tmp/${SPARK_TGZ}" | sha512sum -c - || { \
        echo ">>> [FATAL] Spark ${SPARK_VER} SHA512 校验失败（文件可能被篡改）！"; \
        exit 1; \
    }; \
    \
    tar -xzf /tmp/${SPARK_TGZ} -C /opt/ && \
    ln -s /opt/spark-${SPARK_VER}-bin-hadoop3 /opt/spark && \
    rm -f /tmp/${SPARK_TGZ} && \
    echo ">>> Spark ${SPARK_VER} 安装完成" && \
    echo "${SPARK_VER}" > /opt/spark/.version

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# ── Hadoop-AWS 3.5.0 + AWS SDK v2 Bundle（构建时包含） ──────────
# hadoop-aws 3.5.0 需要 AWS SDK v2（bundle），不能用 v1 的 aws-java-sdk-bundle
# JAR 放入 /opt/spark/jars/，Spark 启动时自动加载到 classpath
# 构建时下载，不需要运行时分发（避免 530MB 传输超时/OOM）

ENV MAVEN_BASE=https://repo1.maven.org/maven2
ENV MAVEN_MIRROR=https://maven.aliyun.com/repository/central

RUN set -eux; \
    echo ">>> 下载 hadoop-aws ${HADOOP_AWS_VERSION} ..."; \
    HADOOP_AWS_JAR="hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    DOWNLOADED=0; \
    for BASE_URL in "${MAVEN_MIRROR}" "${MAVEN_BASE}"; do \
        URL="${BASE_URL}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/${HADOOP_AWS_JAR}"; \
        echo ">>> 尝试 ${URL} ..."; \
        if curl -fsSL --retry 2 --retry-delay 3 --max-time 120 -o /tmp/${HADOOP_AWS_JAR} "${URL}"; then \
            DOWNLOADED=1; \
            echo ">>> hadoop-aws ${HADOOP_AWS_VERSION} 下载成功"; \
            break; \
        fi; \
    done; \
    if [ "${DOWNLOADED}" = "0" ]; then \
        echo ">>> [FATAL] hadoop-aws 下载失败！"; \
        exit 1; \
    fi; \
    # 供应链校验（任务79）：对照 repo1.maven.org 官方钉扎 SHA1，失败即 exit 1
    echo ">>> 校验 hadoop-aws SHA1 ..."; \
    echo "${HADOOP_AWS_SHA1}  /tmp/${HADOOP_AWS_JAR}" | sha1sum -c - || { \
        echo ">>> [FATAL] hadoop-aws SHA1 校验失败（文件可能被篡改）！"; \
        exit 1; \
    }; \
    cp /tmp/${HADOOP_AWS_JAR} ${SPARK_HOME}/jars/ && \
    rm -f /tmp/${HADOOP_AWS_JAR} && \
    echo ">>> hadoop-aws 安装完成"

RUN set -eux; \
    echo ">>> 下载 aws-sdk-v2-bundle ${AWS_SDK_V2_BUNDLE_VERSION} ..."; \
    AWS_SDK_JAR="bundle-${AWS_SDK_V2_BUNDLE_VERSION}.jar"; \
    DOWNLOADED=0; \
    for BASE_URL in "${MAVEN_MIRROR}" "${MAVEN_BASE}"; do \
        URL="${BASE_URL}/software/amazon/awssdk/bundle/${AWS_SDK_V2_BUNDLE_VERSION}/${AWS_SDK_JAR}"; \
        echo ">>> 尝试 ${URL} ..."; \
        if curl -fsSL --retry 2 --retry-delay 3 --max-time 600 -o /tmp/${AWS_SDK_JAR} "${URL}"; then \
            DOWNLOADED=1; \
            echo ">>> aws-sdk-v2-bundle ${AWS_SDK_V2_BUNDLE_VERSION} 下载成功"; \
            break; \
        fi; \
    done; \
    if [ "${DOWNLOADED}" = "0" ]; then \
        echo ">>> [FATAL] aws-sdk-v2-bundle 下载失败！"; \
        exit 1; \
    fi; \
    # 供应链校验（任务79）：对照 repo1.maven.org 官方钉扎 SHA1，失败即 exit 1
    echo ">>> 校验 aws-sdk-v2-bundle SHA1 ..."; \
    echo "${AWS_SDK_V2_BUNDLE_SHA1}  /tmp/${AWS_SDK_JAR}" | sha1sum -c - || { \
        echo ">>> [FATAL] aws-sdk-v2-bundle SHA1 校验失败（文件可能被篡改）！"; \
        exit 1; \
    }; \
    cp /tmp/${AWS_SDK_JAR} ${SPARK_HOME}/jars/ && \
    rm -f /tmp/${AWS_SDK_JAR} && \
    echo ">>> aws-sdk-v2-bundle 安装完成"

# ── Analytics Accelerator S3（hadoop-aws 3.5.0 传递依赖） ──────
ARG ANALYTICS_ACCELERATOR_VERSION=1.3.1
# 供应链校验：repo1.maven.org 官方 SHA1（见 CHECKSUMS.md）
ARG ANALYTICS_ACCELERATOR_SHA1=6c9bd0f6c440c9a78e82d272f5f0252d942419f6
RUN set -eux; \
    echo ">>> 下载 analyticsaccelerator-s3 ${ANALYTICS_ACCELERATOR_VERSION} ..."; \
    JAR_NAME="analyticsaccelerator-s3-${ANALYTICS_ACCELERATOR_VERSION}.jar"; \
    DOWNLOADED=0; \
    for BASE_URL in "${MAVEN_MIRROR}" "${MAVEN_BASE}"; do \
        URL="${BASE_URL}/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/${ANALYTICS_ACCELERATOR_VERSION}/${JAR_NAME}"; \
        echo ">>> 尝试 ${URL} ..."; \
        if curl -fsSL --retry 2 --retry-delay 3 --max-time 60 -o /tmp/${JAR_NAME} "${URL}"; then \
            DOWNLOADED=1; \
            echo ">>> analyticsaccelerator-s3 下载成功"; \
            break; \
        fi; \
    done; \
    if [ "${DOWNLOADED}" = "0" ]; then \
        echo ">>> [FATAL] analyticsaccelerator-s3 下载失败！"; \
        exit 1; \
    fi; \
    # 供应链校验（任务79）：对照 repo1.maven.org 官方钉扎 SHA1，失败即 exit 1
    echo ">>> 校验 analyticsaccelerator-s3 SHA1 ..."; \
    echo "${ANALYTICS_ACCELERATOR_SHA1}  /tmp/${JAR_NAME}" | sha1sum -c - || { \
        echo ">>> [FATAL] analyticsaccelerator-s3 SHA1 校验失败（文件可能被篡改）！"; \
        exit 1; \
    }; \
    cp /tmp/${JAR_NAME} ${SPARK_HOME}/jars/ && \
    rm -f /tmp/${JAR_NAME} && \
    echo ">>> analyticsaccelerator-s3 安装完成"

# ── PySpark 安装（Worker Python 需要） ──────────────────────────
# 容器内 python3 用于 Worker 端执行 Python 任务（UDF 等）
# 注意：移除 `|| true`，让 PySpark 安装失败立即暴露构建问题
# （此前 `|| true` 会掩盖 pip 安装失败，导致 Worker 启动后才报错）
# 版本取 /opt/spark/.version（下载阶段写入的**实际**安装版本）：
# 主版本下载失败回退 3.5.5 时，若仍按 ${SPARK_VERSION} 装 pyspark 4.2.0，
# Worker 端 Python 与集群 JVM 版本错配必然运行时报错。
RUN PYSPARK_VER=$(cat /opt/spark/.version) && \
    echo ">>> installing pyspark==${PYSPARK_VER}" && \
    python3 -m pip install --break-system-packages "pyspark==${PYSPARK_VER}" || \
    (pip3 install --break-system-packages "pyspark==${PYSPARK_VER}" || exit 1)

# ── Phase 5: Iceberg Spark extensions（可选） ──────────────────
# 当 ENABLE_ICEBERG=true 时下载 iceberg-spark-runtime + iceberg-aws-bundle JAR
# 放入 ${SPARK_HOME}/jars/，使 spark.read.table("catalog.ns.tbl") / df.writeTo(...)
# 能路由到 Iceberg 表.
#
# 关键约束：Iceberg 官方 JAR 最高支持 Spark 4.1，不支持 Spark 4.2.
# 启用 Iceberg 时必须同时把 SPARK_VERSION 改为 4.1.0（构建参数）.
# 缺省 ENABLE_ICEBERG=false，跳过此步骤，不影响现有 cluster 测试.
#
# JAR 来源：
#   iceberg-spark-runtime-${ICEBERG_SPARK_MAJOR}_${ICEBERG_SCALA_VERSION}-${ICEBERG_VERSION}.jar
#   iceberg-aws-bundle-${ICEBERG_VERSION}.jar
#
# ── 构建期防护（任务71） ────────────────────────────────────────
# 下方 ENABLE_ICEBERG 块下载的是 Iceberg 4.1 runtime JAR，必须配合 SPARK_VERSION=4.1.0
# （ARG 缺省 4.2.0），否则运行期不兼容。此前仅靠注释警告，这里改为显式断言：
# ENABLE_ICEBERG=true 且 SPARK_VERSION!=4.1.0 时立即终止构建，避免产出带病镜像。
# ARG 作用域：SPARK_VERSION / ENABLE_ICEBERG 均已在上方声明（单 stage，作用域有效）。
RUN if [ "${ENABLE_ICEBERG}" = "true" ] && [ "${SPARK_VERSION}" != "4.1.0" ]; then \
        echo ">>> [FATAL] ENABLE_ICEBERG=true requires SPARK_VERSION=4.1.0 (got ${SPARK_VERSION})." && \
        echo ">>> Pass --build-arg SPARK_VERSION=4.1.0 --build-arg ENABLE_ICEBERG=true together." && \
        exit 1; \
    fi

RUN if [ "${ENABLE_ICEBERG}" = "true" ]; then \
        set -eux; \
        echo ">>> Phase 5: 安装 Iceberg Spark extensions ${ICEBERG_VERSION} (Spark ${ICEBERG_SPARK_MAJOR} Scala ${ICEBERG_SCALA_VERSION})"; \
        ICEBERG_SPARK_JAR="iceberg-spark-runtime-${ICEBERG_SPARK_MAJOR}_${ICEBERG_SCALA_VERSION}-${ICEBERG_VERSION}.jar"; \
        DOWNLOADED=0; \
        for BASE_URL in "${MAVEN_MIRROR}" "${MAVEN_BASE}"; do \
            URL="${BASE_URL}/org/apache/iceberg/iceberg-spark-runtime-${ICEBERG_SPARK_MAJOR}_${ICEBERG_SCALA_VERSION}/${ICEBERG_VERSION}/${ICEBERG_SPARK_JAR}"; \
            echo ">>> 尝试 ${URL} ..."; \
            if curl -fsSL --retry 2 --retry-delay 3 --max-time 120 -o /tmp/${ICEBERG_SPARK_JAR} "${URL}"; then \
                DOWNLOADED=1; \
                echo ">>> iceberg-spark-runtime 下载成功"; \
                break; \
            fi; \
        done; \
        if [ "${DOWNLOADED}" = "0" ]; then \
            echo ">>> [FATAL] iceberg-spark-runtime 下载失败！"; \
            exit 1; \
        fi; \
        # 供应链校验（任务79）：对照 repo1.maven.org 官方钉扎 SHA1，失败即 exit 1
        # 注意：钉扎值对应缺省组合 ICEBERG_VERSION=1.11.0 + 4.1_2.13，
        # 若覆盖这些 ARG，必须同步更新 ICEBERG_SPARK_RUNTIME_SHA1
        echo ">>> 校验 iceberg-spark-runtime SHA1 ..."; \
        echo "${ICEBERG_SPARK_RUNTIME_SHA1}  /tmp/${ICEBERG_SPARK_JAR}" | sha1sum -c - || { \
            echo ">>> [FATAL] iceberg-spark-runtime SHA1 校验失败（文件可能被篡改）！"; \
            exit 1; \
        }; \
        cp /tmp/${ICEBERG_SPARK_JAR} ${SPARK_HOME}/jars/ && \
        rm -f /tmp/${ICEBERG_SPARK_JAR} && \
        echo ">>> iceberg-spark-runtime 安装完成"; \
        \
        ICEBERG_AWS_JAR="iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
        DOWNLOADED=0; \
        for BASE_URL in "${MAVEN_MIRROR}" "${MAVEN_BASE}"; do \
            URL="${BASE_URL}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/${ICEBERG_AWS_JAR}"; \
            echo ">>> 尝试 ${URL} ..."; \
            if curl -fsSL --retry 2 --retry-delay 3 --max-time 120 -o /tmp/${ICEBERG_AWS_JAR} "${URL}"; then \
                DOWNLOADED=1; \
                echo ">>> iceberg-aws-bundle 下载成功"; \
                break; \
            fi; \
        done; \
        if [ "${DOWNLOADED}" = "0" ]; then \
            echo ">>> [FATAL] iceberg-aws-bundle 下载失败！"; \
            exit 1; \
        fi; \
        # 供应链校验（任务79）：对照 repo1.maven.org 官方钉扎 SHA1，失败即 exit 1
        # 注意：钉扎值对应缺省 ICEBERG_VERSION=1.11.0，覆盖时须同步更新 SHA1
        echo ">>> 校验 iceberg-aws-bundle SHA1 ..."; \
        echo "${ICEBERG_AWS_BUNDLE_SHA1}  /tmp/${ICEBERG_AWS_JAR}" | sha1sum -c - || { \
            echo ">>> [FATAL] iceberg-aws-bundle SHA1 校验失败（文件可能被篡改）！"; \
            exit 1; \
        }; \
        cp /tmp/${ICEBERG_AWS_JAR} ${SPARK_HOME}/jars/ && \
        rm -f /tmp/${ICEBERG_AWS_JAR} && \
        echo ">>> iceberg-aws-bundle 安装完成"; \
        echo "${ICEBERG_VERSION}" > ${SPARK_HOME}/.iceberg_version; \
    else \
        echo ">>> Phase 5: ENABLE_ICEBERG=false，跳过 Iceberg JAR 安装（保持 Spark ${SPARK_VERSION} 兼容）"; \
    fi

# ── 非 root 用户（UID/GID 自适应，与 K8s/securityContext 约定一致） ─
# Spark Master/Worker 日志写到 ${SPARK_HOME}/logs/，需对 spark 用户可写。
# 基础镜像（Ubuntu）可能已有 UID 1000 的用户（如 ubuntu）：此时 spark 复用
# 默认 UID 而非固定 1000，避免 useradd 冲突导致构建失败。
#
# 关键修复（2026-08-26）：chown -R /opt/spark 走符号链接时，在 containerd
# snapshotter 下出现过 logs/work 子目录属主未生效（root:root）的问题——
# 运行时 spark-daemon.sh 对日志目录 touch 探测失败后尝试 chown 又因非 root
# 失败，导致 Master/Worker "failed to launch"。因此这里显式 readlink -f
# 解析真实路径后再 mkdir+chown，并以 spark 身份做写入自验证（失败即中止
# 构建，避免带病镜像）。
RUN set -eux; \
    if ! id -u spark >/dev/null 2>&1; then \
        if id -u 1000 >/dev/null 2>&1; then \
            useradd -m spark; \
        else \
            useradd -m -u 1000 spark; \
        fi; \
    fi; \
    SPARK_REAL_HOME=$(readlink -f /opt/spark); \
    echo ">>> Spark real home: ${SPARK_REAL_HOME}"; \
    mkdir -p "${SPARK_REAL_HOME}/logs" "${SPARK_REAL_HOME}/work"; \
    chown -R spark:spark "${SPARK_REAL_HOME}"; \
    chown -h spark:spark /opt/spark; \
    su -s /bin/bash spark -c "touch '${SPARK_REAL_HOME}/logs/.rwtest' && rm '${SPARK_REAL_HOME}/logs/.rwtest' && touch '${SPARK_REAL_HOME}/work/.rwtest' && rm '${SPARK_REAL_HOME}/work/.rwtest'"; \
    ls -ld "${SPARK_REAL_HOME}" "${SPARK_REAL_HOME}/logs" "${SPARK_REAL_HOME}/work"

# ── 入口脚本 ────────────────────────────────────────────────────
COPY --chown=spark:spark entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

USER spark

# ── 端口 ────────────────────────────────────────────────────────
# 7077 = Spark Master RPC
# 8080 = Master Web UI
# 8081 = Worker Web UI
EXPOSE 7077 8080 8081

# ── 入口点（tini 作为 init 系统） ──────────────────────────────
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/entrypoint.sh"]
