"""tools/quality_collect.py 单元测试.

覆盖：
- 调用拼装：main(argv) 组装 ``[sys.executable, -m, pytest, *argv]`` 交给
  subprocess.run，argv 顺序原样透传；空 argv 组装裸 pytest 命令
- subprocess.run 的关键字参数（capture_output / text / encoding / errors）
- 退出码语义：main 返回值 == pytest 退出码（pipefail：测试失败即步骤失败）
- 日志落盘：stdout+stderr 合并以 UTF-8 写入 CWD 相对的 benchmarks/quality_pytest.log，
  父目录不存在时自动创建
- 失败注解：FAILED / ERROR 开头行转 ``::error::``（两流合并过滤，最多 30 条，
  单行截断 180 字符）
- 无 FAILED/ERROR 行时转储输出尾部 40 行窗口内的非空行（``::error::TAIL|``，
  单行截断 170 字符）
- stdout / stderr 为 None 的防御行为（成功与失败两条路径）
- __main__ 入口冒烟：子进程直跑脚本（--version 透传给 pytest，不执行任何测试）

tools/ 目录不是 Python 包（无 __init__.py），本文件通过 importlib 动态加载
被测脚本。除最后的 __main__ 冒烟用例外，subprocess.run 全部被 monkeypatch
替换为预置的 CompletedProcess，不真实执行任何测试套件，不依赖
Spark / MinIO / S3。qc.LOG_PATH 是 CWD 相对路径，所有用例均用
monkeypatch.chdir 把 CWD 切到 tmp_path，防止污染项目内 benchmarks/ 目录。
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "tools" / "quality_collect.py"


def _load_quality_collect():
    """tools/ 无 __init__.py，用 importlib 按文件路径加载被测模块."""
    spec = importlib.util.spec_from_file_location("_autobatch_quality_collect", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


qc = _load_quality_collect()


class _RunCapture:
    """替代 subprocess.run：记录组装出的 cmd / kwargs，返回预置 CompletedProcess."""

    def __init__(self) -> None:
        self.cmd: list[str] | None = None
        self.kwargs: dict[str, Any] = {}
        self.result: subprocess.CompletedProcess = subprocess.CompletedProcess(
            [], 0, stdout="", stderr=""
        )

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def _fake_run(cmd: Any, *args: Any, **kwargs: Any) -> subprocess.CompletedProcess:
            self.cmd = list(cmd)
            self.kwargs = dict(kwargs)
            return self.result

        monkeypatch.setattr(qc.subprocess, "run", _fake_run)

    def set_result(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.result = subprocess.CompletedProcess(self.cmd or [], returncode, stdout, stderr)


@pytest.fixture()
def workdir(tmp_path, monkeypatch):
    """把 CWD 切到 tmp_path：qc.LOG_PATH 为相对路径，防止写进项目 benchmarks/."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture()
def run_capture(monkeypatch):
    """安装 subprocess.run 桩，供用例预置 pytest 进程的退出码与输出."""
    cap = _RunCapture()
    cap.install(monkeypatch)
    return cap


# ----------------------------------------------------------------------
# 调用拼装
# ----------------------------------------------------------------------
def test_cmd_assembled_from_argv_and_executable(workdir, run_capture) -> None:
    """main 组装 [sys.executable, -m, pytest, *argv]，argv 顺序原样透传."""
    run_capture.set_result(0)
    rc = qc.main(["tests/test_x.py", "-q", "--tb=short"])
    assert rc == 0
    assert run_capture.cmd == [
        sys.executable,
        "-m",
        "pytest",
        "tests/test_x.py",
        "-q",
        "--tb=short",
    ]


def test_empty_argv_builds_bare_pytest_cmd(workdir, run_capture) -> None:
    """argv 为空时组装裸 pytest 命令（quality.yml 全量回归的形态）."""
    run_capture.set_result(0)
    rc = qc.main([])
    assert rc == 0
    assert run_capture.cmd == [sys.executable, "-m", "pytest"]


def test_subprocess_run_invocation_kwargs(workdir, run_capture) -> None:
    """capture_output / text / encoding / errors 四个关键字参数齐备."""
    run_capture.set_result(0)
    qc.main([])
    assert run_capture.kwargs["capture_output"] is True
    assert run_capture.kwargs["text"] is True
    assert run_capture.kwargs["encoding"] == "utf-8"
    assert run_capture.kwargs["errors"] == "replace"


# ----------------------------------------------------------------------
# 成功路径：日志落盘 + START/DONE 标记
# ----------------------------------------------------------------------
def test_success_writes_merged_log_and_markers(workdir, run_capture, capsys) -> None:
    """rc=0：返回 0；stdout+stderr 合并落盘；打 START/DONE；无 ::error::."""
    run_capture.set_result(0, stdout="3 passed in 0.1s\n", stderr="some warning\n")
    rc = qc.main(["-q"])
    out = capsys.readouterr().out

    assert rc == 0
    log_path = workdir / qc.LOG_PATH
    assert log_path.is_file()
    assert log_path.read_text(encoding="utf-8") == "3 passed in 0.1s\nsome warning\n"
    # 父目录自动创建（benchmarks/ 事先不存在）
    assert (workdir / "benchmarks").is_dir()
    # 排障生命线标记：首行 START、末行 DONE
    assert "[qcollect] START" in out
    assert "[qcollect] DONE" in out
    assert "::error::" not in out


def test_nonzero_returncode_passed_through(workdir, run_capture) -> None:
    """main 原样返回 pytest 退出码（pipefail 语义：非零即步骤失败）."""
    run_capture.set_result(1, stdout="FAILED tests/test_a.py::test_b\n")
    assert qc.main([]) == 1


def test_interrupt_returncode_passed_through(workdir, run_capture) -> None:
    """退出码 2（中断 / 用法错误）同样原样透传."""
    run_capture.set_result(2, stdout="usage: pytest ...\n")
    assert qc.main([]) == 2


# ----------------------------------------------------------------------
# 失败注解：FAILED / ERROR 行 → ::error::
# ----------------------------------------------------------------------
def test_failed_and_error_lines_annotated_from_both_streams(workdir, run_capture, capsys) -> None:
    """stdout 的 FAILED 行与 stderr 的 ERROR 行都被转成 ::error:: 注解."""
    stdout = (
        "=== short test summary info ===\n"
        "FAILED tests/test_a.py::test_one - assert 1 == 2\n"
        "PASSED tests/test_c.py::test_three\n"
    )
    stderr = "ERROR tests/test_b.py::test_two - fixture exploded\n"
    run_capture.set_result(1, stdout=stdout, stderr=stderr)
    rc = qc.main([])
    out = capsys.readouterr().out

    assert rc == 1
    assert "::error::FAILED tests/test_a.py::test_one - assert 1 == 2" in out
    assert "::error::ERROR tests/test_b.py::test_two - fixture exploded" in out
    # 非 FAILED / ERROR 开头行不注解；也不走 TAIL 转储
    assert "::error::PASSED" not in out
    assert "dumping tail" not in out


def test_annotations_capped_at_thirty(workdir, run_capture, capsys) -> None:
    """FAILED 行超过 30 条时只注解前 30 条."""
    lines = [f"FAILED tests/test_many.py::test_{i:02d} - boom" for i in range(35)]
    run_capture.set_result(1, stdout="\n".join(lines) + "\n")
    qc.main([])
    out = capsys.readouterr().out

    errors = [x for x in out.splitlines() if x.startswith("::error::")]
    assert len(errors) == 30
    assert "::error::FAILED tests/test_many.py::test_00 - boom" in out
    assert "::error::FAILED tests/test_many.py::test_29 - boom" in out
    assert "test_30" not in out
    assert "test_34" not in out


def test_annotation_line_truncated_to_180_chars(workdir, run_capture, capsys) -> None:
    """单条注解正文截断到 180 字符（x[:180]）."""
    long_line = "FAILED tests/test_long.py::test_x - " + "x" * 300
    run_capture.set_result(1, stdout=long_line + "\n")
    qc.main([])
    out = capsys.readouterr().out

    annotated = [x for x in out.splitlines() if x.startswith("::error::FAILED")]
    assert len(annotated) == 1
    assert annotated[0] == "::error::" + long_line[:180]


# ----------------------------------------------------------------------
# 崩溃在摘要前：无 FAILED / ERROR 行 → TAIL 转储
# ----------------------------------------------------------------------
def test_no_failed_lines_dumps_tail(workdir, run_capture, capsys) -> None:
    """无 FAILED/ERROR 行时转储输出尾部 40 行窗口内的非空行（跳过空行）."""
    body = [f"line-{i:02d}" for i in range(45)]
    body[10] = ""  # 落在尾部窗口内的空行，应被跳过
    run_capture.set_result(2, stdout="\n".join(body))
    rc = qc.main([])
    out = capsys.readouterr().out

    assert rc == 2
    assert "::error::no FAILED lines — dumping tail" in out
    tail_lines = [x for x in out.splitlines() if x.startswith("::error::TAIL|")]
    # 尾部 40 行窗口 = line-05..line-44，其中 1 行空行被跳过 → 39 条 TAIL
    assert len(tail_lines) == 39
    assert "::error::TAIL|line-05" in out
    assert "::error::TAIL|line-44" in out
    assert "::error::TAIL|line-04" not in out  # 窗口之前的行不转储


def test_tail_line_truncated_to_170_chars(workdir, run_capture, capsys) -> None:
    """TAIL 行截断到 170 字符（x[:170]）."""
    long_line = "segfault detail " + "z" * 300
    run_capture.set_result(3, stdout=long_line)
    qc.main([])
    out = capsys.readouterr().out

    tail_lines = [x for x in out.splitlines() if x.startswith("::error::TAIL|")]
    assert tail_lines == ["::error::TAIL|" + long_line[:170]]


# ----------------------------------------------------------------------
# 防御行为：stdout / stderr 为 None
# ----------------------------------------------------------------------
def test_none_streams_treated_as_empty_on_success(workdir, run_capture, capsys) -> None:
    """stream 为 None 时按空串处理（(proc.stdout or "")），成功路径不抛异常."""
    run_capture.result = subprocess.CompletedProcess(["pytest"], 0, None, None)
    rc = qc.main([])
    out = capsys.readouterr().out

    assert rc == 0
    log_path = workdir / qc.LOG_PATH
    assert log_path.read_text(encoding="utf-8") == ""
    assert "(0 chars)" in out
    assert "::error::" not in out


def test_none_streams_on_failure_dumps_empty_tail(workdir, run_capture, capsys) -> None:
    """失败且无任何输出：打 "no FAILED lines" 注解但无 TAIL 行可转储."""
    run_capture.result = subprocess.CompletedProcess(["pytest"], 4, None, None)
    rc = qc.main([])
    out = capsys.readouterr().out

    assert rc == 4
    assert "::error::no FAILED lines — dumping tail" in out
    assert "::error::TAIL|" not in out


# ----------------------------------------------------------------------
# __main__ 入口冒烟（唯一真实子进程用例：--version 不执行任何测试）
# ----------------------------------------------------------------------
def test_script_entry_point_smoke(tmp_path) -> None:
    """子进程直跑脚本：--version 透传给 pytest，exit code / 标记 / 日志齐备."""
    proc = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--version"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0
    assert "[qcollect] START" in proc.stdout
    assert "[qcollect] DONE" in proc.stdout
    log = tmp_path / "benchmarks" / "quality_pytest.log"
    assert log.is_file()
    assert "pytest" in log.read_text(encoding="utf-8")
