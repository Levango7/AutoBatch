"""src/stages/_dispatch.py 单元测试.

覆盖：
- dispatch_by_engine 三个分支（"python" / "polars" / "spark"）各路由到对应实现
- 每次分发只调用被选中的实现，其余实现不被触碰
- 返回值原样透传自被选中的实现
- *args / **kwargs 透传给被选中实现（与各 stage run() 的调用形态一致）
- 未知 / 空串 / None / 大小写不符的 backend 均兜底到 python_fn
  （与原各 stage ``else: return _run_python(...)`` 的语义一致）
- 被选中实现抛出的异常原样传播，且其余实现不被调用
- ENGINES 常量与文档声明的支持后端一致

分发函数本身不执行任何真实 stage 逻辑：所有用例以可调用桩对象替代
_run_python / _run_polars / _run_spark，只验证「路由正确」，不验证
「结果正确」（后者由各 stage 的既有测试覆盖）。
"""

from __future__ import annotations

from typing import Any

import pytest

from src.stages._dispatch import ENGINES, dispatch_by_engine


class _Spy:
    """记录调用的可调用对象，替代各引擎内部实现函数."""

    def __init__(self, name: str, result: Any = None, exc: Exception | None = None) -> None:
        self.name = name
        self.result = f"{name}-result" if result is None else result
        self.exc = exc
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((args, kwargs))
        if self.exc is not None:
            raise self.exc
        return self.result

    @property
    def called(self) -> bool:
        return len(self.calls) > 0


def _make_spies() -> dict[str, _Spy]:
    """构造 python / polars / spark 三个桩实现."""
    return {name: _Spy(name) for name in ("python", "polars", "spark")}


def _dispatch(backend: Any, spies: dict[str, _Spy], *args: Any, **kwargs: Any) -> Any:
    """以三桩调用 dispatch_by_engine，与 src/stages/*.run() 的用法一致."""
    return dispatch_by_engine(
        backend, spies["python"], spies["polars"], spies["spark"], *args, **kwargs
    )


# ----------------------------------------------------------------------
# 三个 backend 分支路由
# ----------------------------------------------------------------------
@pytest.mark.parametrize("backend", ["python", "polars", "spark"])
def test_dispatch_routes_to_matching_backend(backend: str) -> None:
    """backend 与实现一一对应：路由到同名桩并返回其结果."""
    spies = _make_spies()
    result = _dispatch(backend, spies)
    assert spies[backend].called
    assert result == f"{backend}-result"


@pytest.mark.parametrize("backend", ["python", "polars", "spark"])
def test_dispatch_calls_only_selected_backend(backend: str) -> None:
    """每次分发只调用被选中的实现一次，其余实现零调用."""
    spies = _make_spies()
    _dispatch(backend, spies)
    for name, spy in spies.items():
        if name == backend:
            assert len(spy.calls) == 1
        else:
            assert not spy.called


def test_dispatch_returns_selected_fn_result_unchanged() -> None:
    """返回值原样透传：polars 桩返回复杂结构时不包装、不改写."""
    spies = _make_spies()
    payload = {"rows_in": 3, "rows_out": 3, "lineage": ["orders_clean.csv"]}
    spies["polars"].result = payload
    result = _dispatch("polars", spies)
    assert result is payload  # 同一对象，非拷贝


# ----------------------------------------------------------------------
# 参数透传
# ----------------------------------------------------------------------
def test_dispatch_passes_positional_and_keyword_args() -> None:
    """*args / **kwargs 原样透传给被选中实现."""
    spies = _make_spies()
    _dispatch("spark", spies, "ctx", "log", cl_dir="run/b/03_clean", rows=10)
    args, kwargs = spies["spark"].calls[0]
    assert args == ("ctx", "log")
    assert kwargs == {"cl_dir": "run/b/03_clean", "rows": 10}


def test_dispatch_stage_style_invocation() -> None:
    """按各 stage run() 的真实调用形态分发：(ctx, log, cl_dir) 位置参数到位."""
    spies = _make_spies()
    fake_ctx = object()
    fake_log = object()
    cl_dir = "run/fixed-batch/03_clean"
    result = _dispatch("polars", spies, fake_ctx, fake_log, cl_dir)
    assert result == "polars-result"
    assert spies["polars"].calls[0] == (((fake_ctx, fake_log, cl_dir), {}))


# ----------------------------------------------------------------------
# unknown backend 兜底（未知值 → python 路径）
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "backend",
    ["flink", "duckdb", "", "SPARK", "Polars", " spark", "python3"],
    ids=[
        "flink",
        "duckdb",
        "empty",
        "uppercase-spark",
        "titlecase-polars",
        "leading-space",
        "python3",
    ],
)
def test_dispatch_unknown_backend_falls_back_to_python(backend: str) -> None:
    """未知 / 大小写不符的 backend 一律走 python_fn 兜底（不抛异常）."""
    spies = _make_spies()
    result = _dispatch(backend, spies)
    assert result == "python-result"
    assert len(spies["python"].calls) == 1
    assert not spies["polars"].called
    assert not spies["spark"].called


def test_dispatch_none_backend_falls_back_to_python() -> None:
    """backend=None 同样走 python_fn 兜底（代码无强制校验，不抛 TypeError）."""
    spies = _make_spies()
    result = _dispatch(None, spies)
    assert result == "python-result"
    assert spies["python"].called
    assert not spies["polars"].called
    assert not spies["spark"].called


# ----------------------------------------------------------------------
# 异常传播与常量
# ----------------------------------------------------------------------
def test_dispatch_propagates_exception_from_selected_fn() -> None:
    """被选中实现抛出的异常原样上抛，其余实现不被调用."""
    err = RuntimeError("spark engine boom")
    spies = _make_spies()
    spies["spark"].exc = err
    with pytest.raises(RuntimeError, match="spark engine boom"):
        _dispatch("spark", spies)
    assert not spies["python"].called
    assert not spies["polars"].called


def test_engines_constant_lists_supported_backends() -> None:
    """ENGINES 常量与文档声明的三个支持后端一致."""
    assert ENGINES == ("python", "polars", "spark")
    assert set(ENGINES) == {"python", "polars", "spark"}
