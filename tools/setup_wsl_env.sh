#!/bin/bash
set -euo pipefail
echo '=== AutoBatch WSL 测试环境安装（轻量版）==='

# 1. 系统依赖（Java 已存在则跳过）
sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends curl wget ca-certificates

# 2. Java（已在系统中）
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
echo "JAVA_HOME=$JAVA_HOME" | sudo tee /etc/profile.d/autobatch-java.sh > /dev/null

# 3. 通过 pip 安装 pyspark（自带 native libs + spark-submit）
echo "安装 pyspark（包含 Spark 运行时 + native libraries）..."
pip3 install --break-system-packages pyspark==4.2.0 pyarrow minio pyiceberg polars pytest pytest-cov

# 4. 探测 pip 安装的 spark 路径
SPARK_PYTHON_LIB=$(python3 -c "import pyspark; import os; print(os.path.dirname(pyspark.__file__))")
export SPARK_HOME="$SPARK_PYTHON_LIB"
echo "SPARK_HOME=$SPARK_HOME" | sudo tee /etc/profile.d/autobatch-spark.sh > /dev/null

# 5. Hadoop native libs 在 pyspark 包内
HADOOP_NATIVE_PATH="$SPARK_PYTHON_LIB/jars"
# pyspark 的 native libs 在 PYSPARK_GATEWAY_PORT 相关目录
NATIVE_LIB_DIR=$(find "$SPARK_PYTHON_LIB" -name "libhadoop.so*" -type f 2>/dev/null | head -1 | xargs dirname 2>/dev/null || echo "")
if [ -z "$NATIVE_LIB_DIR" ]; then
    # 尝试 spark 自带的 native
    NATIVE_LIB_DIR="$SPARK_PYTHON_LIB/python/pyspark/libs/native"
fi

# 6. 写完整环境配置
cat > /etc/profile.d/autobatch-env.sh << ENVEOF
export JAVA_HOME=${JAVA_HOME}
export SPARK_HOME=${SPARK_HOME}
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export PATH="\${JAVA_HOME}/bin:\${SPARK_HOME}/bin:\${PATH}"
export PYTHONPATH="\${SPARK_HOME}/python:\${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:\${PYTHONPATH}"
ENVEOF
chmod +x /etc/profile.d/autobatch-env.sh

# 7. 验证
echo ''
echo '=== 环境验证 ==='
source /etc/profile.d/autobatch-env.sh
echo "SPARK_HOME=$SPARK_HOME"
echo "JAVA_HOME=$JAVA_HOME"
echo "Python: $(python3 --version)"
echo "Java: $(java -version 2>&1 | head -1)"
echo "pyspark: $(python3 -c 'import pyspark; print(pyspark.__version__)' 2>&1)"
echo "spark-submit: $(which spark-submit 2>/dev/null || echo NOT_IN_PATH)"
echo "libhadoop: $(find ${SPARK_HOME} -name 'libhadoop.so*' 2>/dev/null | head -3 || echo NOT_FOUND)"
echo ''
echo '=== 安装完成 ==='
echo 'Run tests:'
echo '  cd /mnt/f/Nexus/Workflow/AutoBatch'
echo '  source /etc/profile.d/autobatch-env.sh'
echo '  python3 -m pytest tests/ -m "not cluster and not benchmark" -q'
