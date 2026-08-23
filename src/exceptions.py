"""AutoBatch 自定义异常类.

集中定义 pipeline 与 stages 抛出的领域异常，便于调用方按异常类型分支处理
（例如重试逻辑区分可重试异常 vs 不可重试异常）.

目前提供：
    StageExecutionError — 单个 stage 在重试耗尽后仍失败时抛出，
        携带 stage_name / batch_id / attempt / original_error / traceback_str
        等上下文，便于日志追踪与上层聚合诊断.
    StageTimeoutError   — stage 执行超过配置的 timeout_seconds 时抛出.
        是 StageExecutionError 的子类，调用方既可按超时专门处理，也可统一
        当作 stage 失败处理.

设计原则：
    - 异常携带结构化上下文（不依赖日志即可拿到 batch_id / attempt 等）.
    - __str__ 返回人类可读摘要，便于 print / 简单日志输出.
    - 不破坏既有异常体系：所有异常均继承自 Exception，可被 ``except Exception`` 捕获.
"""

from __future__ import annotations


class StageExecutionError(Exception):
    """Stage 执行失败，包含上下文信息.

    在 ``pipeline._run_stage_with_retry`` 中，当 stage 重试耗尽（attempt ==
    max_retries）后仍失败时抛出.调用方（run_pipeline）捕获后记录 failed
    状态并终止本轮批次.

    Attributes:
        stage_name:      失败的 stage 名（ingest/validate/clean/compute/output）.
        batch_id:        批次 ID，关联日志与产物.
        attempt:         最后一次失败的 attempt 序号（0-based，即第 attempt+1 次尝试）.
        original_error:  stage 抛出的原始异常实例（保留 cause 链）.
        traceback_str:   原始异常的 traceback 字符串，便于日志记录.
    """

    def __init__(
        self,
        stage_name: str,
        batch_id: str,
        attempt: int,
        original_error: BaseException,
        traceback_str: str,
    ):
        self.stage_name = stage_name
        self.batch_id = batch_id
        self.attempt = attempt
        self.original_error = original_error
        self.traceback_str = traceback_str
        super().__init__(
            f"Stage '{stage_name}' failed after {attempt + 1} attempts for batch '{batch_id}': {original_error}"
        )
        # 保留异常链，使 traceback.print_chain 等工具可追溯原始异常
        self.__cause__ = original_error


class StageTimeoutError(StageExecutionError):
    """Stage 执行超时.

    在 ``pipeline._run_stage_with_retry`` 中，当 stage 执行耗时超过
    ``error_handling.stage_timeouts[stage_name]`` 时抛出.继承自
    StageExecutionError，调用方既可按超时专门处理，也可统一当作 stage 失败.

    额外属性：
        timeout_seconds: 配置的超时阈值（秒）.
        elapsed_seconds: 实际耗时（秒，近似）.
    """

    def __init__(
        self,
        stage_name: str,
        batch_id: str,
        attempt: int,
        timeout_seconds: float,
        elapsed_seconds: float,
    ):
        # 用一个普通 TimeoutError 作为 original_error，保持字段一致
        original = TimeoutError(
            f"Stage '{stage_name}' exceeded timeout: {round(elapsed_seconds, 3)}s > {timeout_seconds}s"
        )
        super().__init__(
            stage_name=stage_name,
            batch_id=batch_id,
            attempt=attempt,
            original_error=original,
            traceback_str="",
        )
        self.timeout_seconds = timeout_seconds
        self.elapsed_seconds = elapsed_seconds


__all__ = ["StageExecutionError", "StageTimeoutError"]
