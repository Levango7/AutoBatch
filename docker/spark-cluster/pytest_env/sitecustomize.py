"""Spark 集群测试专用的 localhost IPv4 优先 sitecustomize.

背景（2026-08-26）：
    本机 OpsMesh 服务以 [::]:8080（IPv6，V6ONLY=1）监听，而 Docker Desktop
    的端口代理在绑定宿主机 8080 时走双栈路径与之冲突（其他端口正常），因此
    docker-compose.yml 中 Spark Master Web UI 只能显式绑定 127.0.0.1:8080。
    Windows 上 getaddrinfo("localhost") 返回 ::1 优先，导致测试代码中
    urlopen("http://localhost:8080/json/") 命中 OpsMesh 的 404 而非 Spark
    Master REST API。

作用：
    本文件通过 PYTHONPATH 注入测试进程，做三类修正（缺一不可）：
      1) patch socket.getaddrinfo —— 将 "localhost" 的解析结果重排为
         AF_INET(127.0.0.1) 优先；
      2) patch socket.create_connection —— 直接把 localhost 改写为
         "127.0.0.1" 再发起连接。这一层是关键：Python 3.14 的
         http/client.py 在 HTTPConnection.__init__ 中以
         ``self._create_connection = socket.create_connection`` 捕获了
         函数引用，而 CPython 3.14 的 create_connection 内部实现（含
         Happy Eyeballs 路径）并不总是经过第 1 层的模块级 getaddrinfo，
         因此必须替换函数本身才能让 http.client / urllib.request 生效。
      3) 引导 SPARK_HOME / JAVA_HOME 指向 F: 盘根下的无空格无括号 junction
         （F:\\spark_home -> pip 版 pyspark 包；F:\\jdk17 -> Amazon Corretto
         17）。本机 Python 与 JDK 安装在 "F:\\Program Files (x86)\\..."
         下，PySpark 自带的 bin\\*.cmd 批处理脚本在展开 %JAVA_HOME% /
         %~dp0 时会被 "(x86)" 的右括号截断 if/for 块，报
         "\\Python\\Python was unexpected at this time." /
         "\\Amazon was unexpected at this time." 并以 JAVA_GATEWAY_EXITED
         收场。junction 路径不含空格/括号，cmd 解析安全。
         注入规则：junction 不存在则不动；现有值为空或含空格/括号等
         cmd 不安全字符时才覆盖；外部已设置安全路径则尊重不覆盖。
      4) 引导 SPARK_CONF_DIR 指向本目录下 spark_conf/（内含
         spark-defaults.conf 禁用 SparkUI）。本机 Hyper-V 保留 tcp
         4002-4101，SparkUI 默认 4040 起重试 16 次全部落入保留区，
         SparkContext 构造抛 BindException 连带 JVM gateway 崩溃；测试
         无 UI 需求故禁用。仅 setdefault，不覆盖外部配置。
      5) 引导 PYSPARK_DRIVER_PYTHON 指向 F:\\Py314\\python.exe（无括号
         junction）。Driver 端解释器路径同样被 bin\\*.cmd 展开，带括号
         的真实安装路径会导致同款批处理崩溃。
    仅影响带此 PYTHONPATH 启动的 Python 进程，不改系统全局行为。

用法：
    $env:PYTHONPATH = "<项目>/docker/spark-cluster/pytest_env"
    F:\\Py314\\python.exe -m pytest tests/test_engine_spark_cluster.py ...

注意：RPC(15077)/MinIO(9000) 端口无 IPv6 占用者，::1 连接失败会自动回退
    127.0.0.1，本修正对它们只是减少一次无效连接尝试。
"""

import os
import socket

# ---- 第 0 层：SPARK_HOME / JAVA_HOME 引导（必须在任何 pyspark import 前，
# ---- pyspark.find_spark_home._find_spark_home() 优先读 SPARK_HOME 环境变量）。
_SPARK_JUNCTION = r"F:\spark_home"
_JAVA_JUNCTION = r"F:\jdk17"


def _cmd_unsafe(p: str) -> bool:
    """路径是否含 cmd 批处理不安全字符（空格/括号/& 等，未加引号展开必炸）."""
    return any(c in p for c in ' ()&^%!')


def _bootstrap_env(key: str, junction: str) -> None:
    """用无空格无括号的 junction 路径引导 SPARK_HOME/JAVA_HOME.

    规则：junction 不存在 → 不动；现有值为空或不安全（含空格/括号，
    如 "F:\\Program Files (x86)\\..." 会截断 PySpark bin\\*.cmd 的
    if/for 块）→ 覆盖；否则（外部已显式设置安全路径）→ 尊重不覆盖.
    """
    if not os.path.isdir(junction):
        return
    current = os.environ.get(key, "")
    if current and not _cmd_unsafe(current):
        return
    os.environ[key] = junction


_bootstrap_env("SPARK_HOME", _SPARK_JUNCTION)
_bootstrap_env("JAVA_HOME", _JAVA_JUNCTION)

# 第 3b 层：PYSPARK_DRIVER_PYTHON 引导。Driver 端解释器同样会被 PySpark 的
# bin\*.cmd 展开（"/Python/Python was unexpected at this time." 同款崩溃），
# 本机 F:\\Py314 是指向真实安装目录的无括号 junction，作为缺省值注入；
# 外部显式设置的安全值不受影响。
_PY314_JUNCTION = r"F:\Py314"
if os.path.isdir(_PY314_JUNCTION):
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", os.path.join(_PY314_JUNCTION, "python.exe"))

# SPARK_CONF_DIR：相对本文件定位（避免硬编码项目绝对路径），仅 setdefault
_CONF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spark_conf")
if os.path.isdir(_CONF_DIR):
    os.environ.setdefault("SPARK_CONF_DIR", _CONF_DIR)

_orig_getaddrinfo = socket.getaddrinfo


def _localhost_ipv4_first(host, port, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
    res = _orig_getaddrinfo(host, port, *args, **kwargs)
    try:
        if isinstance(host, str) and host.rstrip(".").lower() in ("localhost",):
            res = sorted(res, key=lambda r: 0 if r[0] == socket.AF_INET else 1)
    except Exception:  # noqa: BLE001 - 排序失败时退回原始结果，绝不影响调用方
        return _orig_getaddrinfo(host, port, *args, **kwargs)
    return res


socket.getaddrinfo = _localhost_ipv4_first

# ---- 第 2 层：patch socket.create_connection，http.client 在实例化时捕获的
# ---- 就是这个 patched 版本（见文件头说明），确保 urlopen("localhost:8080")
# ---- 直达 127.0.0.1 上的 Spark Master 而非 [::1]:8080 上的 OpsMesh。
_orig_create_connection = socket.create_connection
_GLOBAL_DEFAULT_TIMEOUT = getattr(socket, "_GLOBAL_DEFAULT_TIMEOUT", None)


def _create_connection_ipv4(address, timeout=_GLOBAL_DEFAULT_TIMEOUT, source_address=None):  # noqa: ANN001, ANN002
    """将 localhost 显式改写为 127.0.0.1 后再建立 TCP 连接."""
    try:
        host, port = address
        if isinstance(host, str) and host.rstrip(".").lower() in ("localhost",):
            address = ("127.0.0.1", port)
    except Exception:  # noqa: BLE001 - 解析失败时保持原参数，绝不影响调用方
        pass
    return _orig_create_connection(address, timeout, source_address)


socket.create_connection = _create_connection_ipv4