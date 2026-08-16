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
# ────────────────────────────────────────────────────────────────

FROM eclipse-temurin:17-jre

# ── 元数据 ──────────────────────────────────────────────────────
LABEL maintainer="AutoBatch DevOps"
LABEL description="Spark Standalone cluster node (Master/Worker)"

# ── 构建参数 ────────────────────────────────────────────────────
ARG SPARK_VERSION=4.2.0
ARG HADOOP_AWS_VERSION=3.5.0
ARG AWS_SDK_V2_BUNDLE_VERSION=2.35.4
# Phase 5: Iceberg Spark extensions（可选，缺省关闭）
# 启用方式：docker build --build-arg ENABLE_ICEBERG=true --build-arg SPARK_VERSION=4.1.0 .
ARG ENABLE_ICEBERG=false
ARG ICEBERG_VERSION=1.11.0
ARG ICEBERG_SPARK_MAJOR=4.1
ARG ICEBERG_SCALA_VERSION=2.13

# ── 系统依赖：curl + tini + python3 + pip ──────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl ca-certificates tini procps python3 python3-pip python3-venv socat && \
    rm -rf /var/lib/apt/lists/*

# ── Spark 下载（含版本回退 + 国内镜像） ─────────────────────────
# 优先 Spark 4.2.0，若不可用则回退 3.5.5
# 使用清华镜像加速，回退到 Apache 官方
ENV SPARK_VERSION_PRIMARY=4.2.0
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
    cp /tmp/${AWS_SDK_JAR} ${SPARK_HOME}/jars/ && \
    rm -f /tmp/${AWS_SDK_JAR} && \
    echo ">>> aws-sdk-v2-bundle 安装完成"

# ── Analytics Accelerator S3（hadoop-aws 3.5.0 传递依赖） ──────
ARG ANALYTICS_ACCELERATOR_VERSION=1.3.1
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
    cp /tmp/${JAR_NAME} ${SPARK_HOME}/jars/ && \
    rm -f /tmp/${JAR_NAME} && \
    echo ">>> analyticsaccelerator-s3 安装完成"

# ── PySpark 安装（Worker Python 需要） ──────────────────────────
# 容器内 python3 用于 Worker 端执行 Python 任务（UDF 等）
RUN python3 -m pip install --break-system-packages pyspark==${SPARK_VERSION} || \
    pip3 install pyspark==${SPARK_VERSION} || true

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
        cp /tmp/${ICEBERG_AWS_JAR} ${SPARK_HOME}/jars/ && \
        rm -f /tmp/${ICEBERG_AWS_JAR} && \
        echo ">>> iceberg-aws-bundle 安装完成"; \
        echo "${ICEBERG_VERSION}" > ${SPARK_HOME}/.iceberg_version; \
    else \
        echo ">>> Phase 5: ENABLE_ICEBERG=false，跳过 Iceberg JAR 安装（保持 Spark ${SPARK_VERSION} 兼容）"; \
    fi

# ── 入口脚本 ────────────────────────────────────────────────────
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

# ── 端口 ────────────────────────────────────────────────────────
# 7077 = Spark Master RPC
# 8080 = Master Web UI
# 8081 = Worker Web UI
EXPOSE 7077 8080 8081

# ── 入口点（tini 作为 init 系统） ──────────────────────────────
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/entrypoint.sh"]
