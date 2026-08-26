"""引擎后端分发工具——消除各 stage run() 中重复的 if/elif 三分支 dispatch.

各 stage（clean/compute/output）的 ``run()`` 在分发到 ``_run_python`` /
``_run_polars`` / ``_run_spark`` 时存在相同的 ``if ctx.engine_backend == "spark"``
/ ``"polars"`` / python 三分支重复。本模块提供一个轻量分发函数
``dispatch_by_engine``，把三分支收敛为一处。

设计原则：
- **不过度工程化**——只是一个分发函数，不引入抽象基类 / Protocol / 注册表。
- **行为 100% 不变**——分支顺序与原 if/elif 链一致（spark → polars → python 兜底）。
- **不强制 backend 取值**——未知 backend 走 python 路径（与原 ``else: return _run_python``
  兜底语义一致）。

参见 docs/evolution.md §4.3.1.2 / §4.3.2.2。
"""

from __future__ import annotations

from typing import Any, Callable

# 支持的引擎后端（仅作文档/校验参考，dispatch_by_engine 不强制校验——
# 未知 backend 走 python 兜底，与原 if/elif 链语义一致）。
ENGINES: tuple[str, ...] = ("python", "polars", "spark")


def dispatch_by_engine(
    backend: str,
    python_fn: Callable[..., Any],
    polars_fn: Callable[..., Any],
    spark_fn: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """按引擎后端分发到对应实现函数.

    分支顺序与原各 stage ``run()`` 中的 if/elif 链一致：
    ``spark`` → ``spark_fn``、``polars`` → ``polars_fn``、其他（含 ``"python"``
    与任何未知值）→ ``python_fn`` 兜底。

    Args:
        backend: 引擎后端名称（``"python"`` / ``"polars"`` / ``"spark"``）。
            未知值走 ``python_fn`` 兜底，与原 ``else: return _run_python(...)`` 语义一致。
        python_fn: Python 引擎实现函数。
        polars_fn: Polars 引擎实现函数。
        spark_fn: Spark 引擎实现函数。
        *args: 透传给所选实现函数的位置参数。
        **kwargs: 透传给所选实现函数的关键字参数。

    Returns:
        所选引擎实现函数的返回值。

    Examples:
        替代原 ``run()`` 中的::

            if ctx.engine_backend == "spark":
                return _run_spark(ctx, log, cl_dir)
            if ctx.engine_backend == "polars":
                return _run_polars(ctx, log, cl_dir)
            return _run_python(ctx, log, cl_dir)

        写为::

            return dispatch_by_engine(
                ctx.engine_backend, _run_python, _run_polars, _run_spark, ctx, log, cl_dir
            )
    """
    if backend == "spark":
        return spark_fn(*args, **kwargs)
    if backend == "polars":
        return polars_fn(*args, **kwargs)
    return python_fn(*args, **kwargs)
