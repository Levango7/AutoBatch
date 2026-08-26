# docker/spark-cluster 供应链校验和清单（CHECKSUMS.md）

> 任务 #79（二轮审查 H-5：供应链风险）。本文件记录 `Dockerfile` 中所有外部下载文件的
> 钉扎校验和及其官方来源，供审计与版本升级时更新。
>
> 原则：
> - 下载源（清华/阿里云镜像）仅为加速，**校验值一律取自官方源**，
>   防止镜像源与下载源同时被污染。
> - 任一文件校验失败，构建立即 `exit 1`（fail-fast）。
> - 升级版本 ARG 时必须同步更新对应 SHA 值（Dockerfile 内 ARG / case 映射），
>   否则构建会在校验步骤失败——这是有意的 fail-safe。

获取日期：2026-08-27（通过官方 .sha512 / .sha1 端点在线获取）

## Spark tarball（SHA512，构建期 `sha512sum -c`）

| 文件 | 版本 | 钉扎 SHA512 | 校验和来源 URL | 备注 |
|---|---|---|---|---|
| spark-4.2.0-bin-hadoop3.tgz | 4.2.0 | `3a6559e8546ff387db8fe7a04b8fe4008853b467b972c2e343df8e14a81450534777719fc5d4998dd96519a86fdf9de140cee753c61e2e94db044d5f9555ddc4` | https://downloads.apache.org/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz.sha512 | 当前缺省版本 |
| spark-4.1.0-bin-hadoop3.tgz | 4.1.0 | `fff7f929d98779b096a2d2395b1b9db1ce277660f3852dd45e1457c373013f2669074315252181a3cea291d8f3a726c70f7f9b247e723edbf8080f40888edde1` | https://archive.apache.org/dist/spark/spark-4.1.0/spark-4.1.0-bin-hadoop3.tgz.sha512 | ENABLE_ICEBERG=true 所需；downloads.apache.org 同路径值与之相同（双源交叉验证一致） |
| spark-3.5.5-bin-hadoop3.tgz | 3.5.5 | `ec5ff678136b1ff981e396d1f7b5dfbf399439c5cb853917e8c954723194857607494a89b7e205fce988ec48b1590b5caeae3b18e1b5db1370c0522b256ff376` | https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz.sha512 | 主版本下载全失败时的回退版本 |

下载源（Dockerfile 保持不变，按优先级）：
1. https://mirrors.tuna.tsinghua.edu.cn/apache/spark/spark-<版本>/<文件>
2. https://mirrors.aliyun.com/apache/spark/spark-<版本>/<文件>
3. https://archive.apache.org/dist/spark/spark-<版本>/<文件>

## Maven Central JAR（SHA1，构建期 `sha1sum -c`）

| 文件 | 版本 | 钉扎 SHA1 | 校验和来源 URL（repo1.maven.org 官方） | Dockerfile ARG |
|---|---|---|---|---|
| hadoop-aws-3.5.0.jar | 3.5.0 | `9e594525d264c0db653c7f68da98b245f7d61ea5` | https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar.sha1 | HADOOP_AWS_SHA1 |
| bundle-2.35.4.jar（AWS SDK v2 Bundle） | 2.35.4 | `7252265e3970b214708e68a8b74a8fa8c875af1e` | https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar.sha1 | AWS_SDK_V2_BUNDLE_SHA1 |
| analyticsaccelerator-s3-1.3.1.jar | 1.3.1 | `6c9bd0f6c440c9a78e82d272f5f0252d942419f6` | https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar.sha1 | ANALYTICS_ACCELERATOR_SHA1 |
| iceberg-spark-runtime-4.1_2.13-1.11.0.jar | 1.11.0 | `f9b1e4a18797c58ae406104ef5c75d1b64a373c5` | https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar.sha1 | ICEBERG_SPARK_RUNTIME_SHA1 |
| iceberg-aws-bundle-1.11.0.jar | 1.11.0 | `afe5b22dd5bc2d2c0639049b06a92dae2545cac7` | https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar.sha1 | ICEBERG_AWS_BUNDLE_SHA1 |

JAR 下载源（Dockerfile 保持不变，按优先级）：
1. https://maven.aliyun.com/repository/central/<路径>（内网加速）
2. https://repo1.maven.org/maven2/<路径>（官方兜底）

Iceberg 两个 JAR 仅在 `--build-arg ENABLE_ICEBERG=true` 时下载；
钉扎 SHA1 对应缺省组合 `ICEBERG_VERSION=1.11.0` + `4.1_2.13`，
覆盖这些 ARG 时必须同步更新 SHA1。

## 范围外说明（已知残留项）

- `pip install pyspark==<版本>`：PyPI 安装未启用 hash-pinning
  （需 `--require-hashes` 且覆盖 pyspark 全部依赖的哈希，维护成本高），
  本次任务范围（Spark tarball + 5 类 JAR）之外，列入后续改进项。
- 基础镜像 `eclipse-temurin:17-jre`：依赖 Docker Hub 内容寻址 digest，
  如需更强保障可在 FROM 行钉扎 `@sha256:<digest>`（本次不改，避免影响现有构建）。