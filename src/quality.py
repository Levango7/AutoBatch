"""Data quality rule engine: row checks, quarantine reasons, DQ report."""

from __future__ import annotations

import re
import statistics
from typing import Any, Optional

from .helpers import as_float, date_parse


def _col_to_str_series(df: Any, col: str) -> Any:
    """把 df[col] 转为字符串 Series，temporal 类型按 ISO 格式化.

    polars read_csv(try_parse_dates=True) 会把日期列解析为 Date/Datetime，
    cast(pl.Utf8) 对 Datetime 输出 "2026-07-27 14:26:00.000000"（带微秒），
    不匹配 format regex。本函数对 temporal 类型用 dt.strftime() 转成 ISO 字符串，
    与原 CSV 字符串表示一致；非 temporal 类型直接 cast(pl.Utf8)。

    Args:
        df: polars.DataFrame.
        col: 列名.

    Returns:
        polars.Expr 或 Series（字符串类型），供 .to_series().to_list() 使用。
    """
    import polars as pl

    dtype = df.schema.get(col)
    if dtype is not None and isinstance(dtype, (pl.Datetime, pl.Date, pl.Time, pl.Duration)):
        # Datetime → "YYYY-MM-DDTHH:MM:SS"，Date → "YYYY-MM-DD"
        if isinstance(dtype, pl.Datetime):
            return df.select(pl.col(col).dt.strftime("%Y-%m-%dT%H:%M:%S"))
        if isinstance(dtype, pl.Date):
            return df.select(pl.col(col).dt.strftime("%Y-%m-%d"))
        # Time / Duration 等少见类型回退到 cast
        return df.select(pl.col(col).cast(pl.Utf8))
    return df.select(pl.col(col).cast(pl.Utf8))


class RuleEngine:
    """Run configured rules over rows; classify good/bad, flag outliers."""

    def __init__(
        self,
        dataset: str,
        rules: dict[str, Any],
        ref_data: Optional[dict[str, list[dict[str, str]]]] = None,
    ):
        self.dataset = dataset
        self.rules = rules
        self.ref_data = ref_data or {}

    def _ref_keys(self, table: str, column: str):
        rows = self.ref_data.get(table, [])
        return {r.get(column) for r in rows if r.get(column) not in (None, "")}

    def check(
        self, rows: Optional[list[dict[str, str]]] = None, df: Any = None, spark: Any = None
    ) -> tuple[Any, Any, list[dict[str, Any]], set]:
        """Return (good, bad, rule_stats, outlier_indices).

        向后兼容：仅提供 ``rows``（List[Dict]）时走 Python 逐行路径，
        返回 (List[Dict], List[Dict], List[Dict], set)。

        Polars 分支：提供 ``df``（polars.DataFrame）且 ``spark`` 为 None 时
        走向量化路径，返回 (polars.DataFrame, polars.DataFrame, List[Dict], set)；
        bad DataFrame 额外带 ``_reasons``/``_line`` 列。

        Spark 分支：同时提供 ``df``（SparkDataFrame）与 ``spark``（SparkSession）
        时走分布式路径，返回 (SparkDataFrame, SparkDataFrame, List[Dict], set)；
        bad DataFrame 额外带 ``_reasons``/``_line`` 列。参见 docs/evolution.md
        §4.3.2.4 / §4.4.2.1。

        DQ Score 口径、quality_summary 格式、mode 标记在三条路径下一致；
        ``engine.backend="python"`` 时行为与 Phase 1 完全相同。
        """
        if df is not None:
            if spark is not None:
                return self.check_spark(df, spark)
            return self.check_polars(df)
        assert rows is not None
        return self.check_python(rows)

    @staticmethod
    def _build_stats(counters: dict[str, dict[str, int]]) -> list[dict[str, Any]]:
        """从 counters 构造 stats list（三引擎共用）.

        每条 stat：{rule, checked, passed, failed, pass_rate}，
        pass_rate = passed/checked（checked=0 时记 1.0）.
        """
        return [
            {
                "rule": rule,
                "checked": c["checked"],
                "passed": c["passed"],
                "failed": c["checked"] - c["passed"],
                "pass_rate": round(c["passed"] / c["checked"], 4) if c["checked"] else 1.0,
            }
            for rule, c in counters.items()
        ]

    def check_python(
        self, rows: list[dict[str, str]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], set]:
        """Python 逐行校验路径（原 check 逻辑，向后兼容）。"""
        rconf = self.rules
        good: list[dict[str, Any]] = []
        bad: list[dict[str, Any]] = []
        stats: list[dict[str, Any]] = []
        outlier_indices: set = set()
        if not rconf:
            return rows, bad, stats, outlier_indices

        uniq_cols = (rconf.get("uniqueness") or {}).get("columns", [])
        seen: dict[str, set] = {c: set() for c in uniq_cols}
        dup_ids = set()
        for i, row in enumerate(rows):
            for c in uniq_cols:
                v = row.get(c)
                if v is None or v == "":
                    continue
                if v in seen[c]:
                    dup_ids.add(i)
                else:
                    seen[c].add(v)

        required = (rconf.get("completeness") or {}).get("required_columns", [])
        ranges = rconf.get("range", [])
        allowed = rconf.get("allowed_values", {})
        formats = rconf.get("format", {})
        dconf = rconf.get("date_valid") or {}
        date_cols = dconf.get("columns", [])
        date_min = date_parse(dconf.get("min")) if dconf.get("min") else None
        date_max = date_parse(dconf.get("max")) if dconf.get("max") else None
        refer = rconf.get("referential", {})
        oc = rconf.get("outlier") or {}
        bounds = self._outlier_bounds(rows, oc)

        # Precompute referential key sets once (outside the row loop).
        # Previously _ref_keys was called per row, rebuilding the same set
        # O(rows x ref_table_size) times. Now it is O(ref_table_size) once.
        refer_keys: dict[str, set] = {
            col: self._ref_keys(*target.split(".")) for col, target in refer.items()
        }

        counters: dict[str, dict[str, int]] = {}

        def bump(rule: str, ok: bool) -> None:
            c = counters.setdefault(rule, {"checked": 0, "passed": 0})
            c["checked"] += 1
            if ok:
                c["passed"] += 1

        for i, row in enumerate(rows):
            reasons = self._python_row_reasons(
                i,
                row,
                required,
                uniq_cols,
                dup_ids,
                ranges,
                allowed,
                formats,
                date_cols,
                date_min,
                date_max,
                refer,
                refer_keys,
                bounds,
                oc,
                counters,
                bump,
                outlier_indices,
            )
            if reasons:
                row_out: dict[str, Any] = dict(row)
                row_out["_reasons"] = ";".join(reasons)
                row_out["_line"] = i + 2
                bad.append(row_out)
            else:
                good.append(row)

        stats = self._build_stats(counters)
        return good, bad, stats, outlier_indices

    @staticmethod
    def _python_row_reasons(
        i: int,
        row: dict[str, str],
        required: list[str],
        uniq_cols: list[str],
        dup_ids: set,
        ranges: list,
        allowed: dict,
        formats: dict,
        date_cols: list,
        date_min,
        date_max,
        refer: dict,
        refer_keys: dict,
        bounds,
        oc: dict,
        counters: dict,
        bump,
        outlier_indices: set,
    ) -> list[str]:
        """Python 路径单行规则校验：bump counters，返回该行 reasons 列表.

        抽出 check_python 主循环的单行逻辑，降低 check_python 行数.
        """
        reasons: list[str] = []
        for col in required:
            v = row.get(col)
            missing = v is None or str(v).strip() == ""
            bump("completeness", not missing)
            if missing:
                reasons.append("missing_required:" + col)
        for c in uniq_cols:
            is_dup = i in dup_ids
            bump("uniqueness", not is_dup)
            if is_dup:
                reasons.append("duplicate_key:" + c)
        for rng in ranges:
            col = rng["column"]
            fv = as_float(row.get(col))
            lo = rng.get("min", float("-inf"))
            hi = rng.get("max", float("inf"))
            ok = fv is not None and lo <= fv <= hi
            bump("range", ok)
            if not ok:
                reasons.append("range_violation:" + col)
        for col, vals in allowed.items():
            v = row.get(col)
            ok = v is None or str(v).strip() == "" or v in vals
            bump("allowed_values", ok)
            if not ok:
                reasons.append("invalid_value:" + col)
        for col, pat in formats.items():
            v = row.get(col)
            ok = v is None or str(v).strip() == "" or re.match(pat, str(v)) is not None
            bump("format", ok)
            if not ok:
                reasons.append("format_violation:" + col)
        for col in date_cols:
            d = date_parse(row.get(col))
            ok = d is not None
            if ok and date_min is not None and d is not None and d < date_min:
                ok = False
            if ok and date_max is not None and d is not None and d > date_max:
                ok = False
            bump("date_valid", ok)
            if not ok:
                reasons.append("invalid_date:" + col)
        for col in refer:
            v = row.get(col)
            ok = v is None or str(v).strip() == "" or v in refer_keys[col]
            bump("referential", ok)
            if not ok:
                reasons.append("orphan_reference:" + col)
        if bounds is not None and oc.get("column"):
            fv = as_float(row.get(oc["column"]))
            is_out = fv is not None and (fv < bounds[0] or fv > bounds[1])
            bump("outlier", not is_out)
            if is_out:
                outlier_indices.add(i)
        return reasons

    def check_polars(self, df: Any) -> tuple[Any, Any, list[dict[str, Any]], set]:
        """Polars 向量化校验分支.

        返回 ``(good_df, bad_df, stats, outlier_indices)``，good_df/bad_df 为
        polars.DataFrame；bad_df 额外带 ``_reasons``/``_line`` 列。

        - completeness/uniqueness/range/allowed_values/referential: polars 表达式向量化
        - format/date_valid: python 逐行算 mask（polars regex/多格式 date 解析支持有限）
        - outlier: polars quantile 算 bounds，向量化算 mask，只标记不拒收
        - bad 行 reason 文本: 基于已计算的 mask 生成，顺序与 python 路径一致

        DQ Score 口径与 python 路径一致（规则检查项简单平均通过率）。
        参见 docs/evolution.md §4.3.1.5 / §4.4.1。

        本方法为调度入口，子逻辑拆为 ``_polars_collect_masks`` /
        ``_polars_apply_masks`` / ``_polars_outlier`` /
        ``_polars_counters`` / ``_polars_good_bad`` 五个子方法.
        """
        import polars as pl  # noqa: F401

        rconf = self.rules
        n = df.height
        if not rconf:
            return df, df.head(0), [], set()

        # 1. 各规则 fail mask（polars Expr 或预计算 list[bool]）
        expr_masks, precomputed, reason_specs, rule_masks = self._polars_collect_masks(df, rconf)

        # 2. 把 mask 加到 df，合并 bad_mask
        dfm, bad_mask, all_mask_cols = self._polars_apply_masks(df, expr_masks, precomputed, n)

        # 3. outlier 检测（只标记不拒收）
        outlier_indices, outlier_count = self._polars_outlier(df, rconf)

        # 4. counters（与 python 路径对齐：每行每个子规则单独计数）
        counters = self._polars_counters(dfm, rule_masks, n, rconf, outlier_count)

        # 5. good / bad + reason 文本
        good_df, bad_df = self._polars_good_bad(df, dfm, bad_mask, reason_specs)

        stats = self._build_stats(counters)
        return good_df, bad_df, stats, outlier_indices

    def _polars_collect_masks(
        self, df: Any, rconf: dict[str, Any]
    ) -> tuple[
        list[tuple[str, Any]], dict[str, list[bool]], list[tuple[str, str]], dict[str, list[str]]
    ]:
        """Polars 路径：构建各规则 fail mask.

        遍历 completeness/uniqueness/range/allowed_values/referential/format/date_valid
        七类规则，为每个子规则生成一个 mask（polars Expr 或预计算 list[bool]）.

        Returns:
            (expr_masks, precomputed, reason_specs, rule_masks)
            - expr_masks:   [(col_name, pl.Expr)] 加到 df 的表达式 mask
            - precomputed:  {col_name: list[bool]} 预计算的 list mask
            - reason_specs: [(reason_text, mask_col_name)] bad 行 reason 文本生成用
            - rule_masks:   {rule_name: [mask_col_name]} counters 计算用
        """
        import polars as pl

        expr_masks: list[tuple[str, Any]] = []
        precomputed: dict[str, list[bool]] = {}
        reason_specs: list[tuple[str, str]] = []
        rule_masks: dict[str, list[str]] = {}

        def _next_mask_name() -> str:
            return f"__m{len(expr_masks) + len(precomputed)}"

        def add_expr_mask(rule: str, reason: str, expr: Any) -> None:
            col = _next_mask_name()
            expr_masks.append((col, expr))
            reason_specs.append((reason, col))
            rule_masks.setdefault(rule, []).append(col)

        def add_list_mask(rule: str, reason: str, mask_list: list[bool]) -> None:
            col = _next_mask_name()
            precomputed[col] = mask_list
            reason_specs.append((reason, col))
            rule_masks.setdefault(rule, []).append(col)

        # completeness: null 或 strip 后为空
        for col in (rconf.get("completeness") or {}).get("required_columns", []):
            expr = pl.col(col).is_null() | (pl.col(col).cast(pl.Utf8).str.strip_chars() == "")
            add_expr_mask("completeness", "missing_required:" + col, expr)

        # uniqueness: is_duplicated & ~is_first_distinct（只标记重复的后续行，与 python 路径一致）
        # 注：polars 1.43+ 移除了 Expr.is_first()，改用 is_first_distinct()。
        # null/空串 key 豁免：is_duplicated 把 null 归为同一组，第 2 条及以后的
        # null 行会被误判重复——与 python 路径（跳过 None/"" key）不一致。
        # 统一为 python 语义：null/空串 key 不参与 uniqueness 判定，其缺失由
        # completeness 规则负责。
        for c in (rconf.get("uniqueness") or {}).get("columns", []):
            cs = pl.col(c).cast(pl.Utf8)
            null_key = cs.is_null() | (cs.str.strip_chars() == "")
            expr = ~null_key & cs.is_duplicated() & ~cs.is_first_distinct()
            add_expr_mask("uniqueness", "duplicate_key:" + c, expr)

        # range: null 或超出 [min, max]（str.replace_all 去千位分隔符，与 as_float 一致）
        for rng in rconf.get("range", []):
            col = rng["column"]
            lo = rng.get("min", float("-inf"))
            hi = rng.get("max", float("inf"))
            cf = pl.col(col).cast(pl.Utf8).str.replace_all(",", "").cast(pl.Float64, strict=False)
            add_expr_mask("range", "range_violation:" + col, (cf < lo) | (cf > hi) | cf.is_null())

        # allowed_values: 非空且不在允许列表
        for col, vals in (rconf.get("allowed_values", {}) or {}).items():
            vs = pl.col(col).cast(pl.Utf8)
            add_expr_mask(
                "allowed_values",
                "invalid_value:" + col,
                (~vs.is_in(list(vals))) & (~vs.is_null()) & (vs.str.strip_chars() != ""),
            )

        # referential: anti join 一次找出孤儿值，再 is_in 标记
        for col, target in (rconf.get("referential", {}) or {}).items():
            ref_table, ref_col = target.split(".")
            ref_rows = self.ref_data.get(ref_table, [])
            vs = pl.col(col).cast(pl.Utf8)
            non_empty = (~vs.is_null()) & (vs.str.strip_chars() != "")
            if ref_rows:
                ref_keys = [
                    str(r.get(ref_col)) for r in ref_rows if r.get(ref_col) not in (None, "")
                ]
                ref_keys_df = pl.DataFrame({"__k": ref_keys}).unique()
                keys_df = df.select(vs.alias("__k"))
                orphan_keys = (
                    keys_df.join(ref_keys_df, on="__k", how="anti").get_column("__k").to_list()
                )
                expr = vs.is_in(orphan_keys) & non_empty
            else:
                expr = non_empty
            add_expr_mask("referential", "orphan_reference:" + col, expr)

        # format (python 逐行算 mask; polars regex 支持有限)
        # 注：polars read_csv(try_parse_dates=True) 会把日期列解析为 Date/Datetime，
        # cast(pl.Utf8) 对 Datetime 输出 "2026-07-27 14:26:00.000000"（带微秒），
        # 不匹配 format regex。用 _col_to_str_series 把 temporal 类型按 ISO 格式
        # 转回字符串，与原 CSV 字符串表示一致（参见 docs/evolution.md §4.3.1.5）。
        for col, pat in (rconf.get("format", {}) or {}).items():
            pat_re = re.compile(pat)
            vals = _col_to_str_series(df, col).fill_null("").to_series().to_list()
            mask_list = [
                not (str(v).strip() == "" or pat_re.match(str(v)) is not None) for v in vals
            ]
            add_list_mask("format", "format_violation:" + col, mask_list)

        # date_valid (python 逐行算 mask; 多格式解析 polars 不擅长)
        dconf = rconf.get("date_valid") or {}
        date_min = date_parse(dconf.get("min")) if dconf.get("min") else None
        date_max = date_parse(dconf.get("max")) if dconf.get("max") else None
        for col in dconf.get("columns", []):
            vals = _col_to_str_series(df, col).fill_null("").to_series().to_list()
            mask_list = []
            for v in vals:
                d = date_parse(v)
                ok = d is not None
                if ok and date_min is not None and d is not None and d < date_min:
                    ok = False
                if ok and date_max is not None and d is not None and d > date_max:
                    ok = False
                mask_list.append(not ok)
            add_list_mask("date_valid", "invalid_date:" + col, mask_list)

        return expr_masks, precomputed, reason_specs, rule_masks

    @staticmethod
    def _polars_apply_masks(
        df: Any,
        expr_masks: list[tuple[str, Any]],
        precomputed: dict[str, list[bool]],
        n: int,
    ) -> tuple[Any, Any, list[str]]:
        """Polars 路径：把 mask 加到 df，合并 bad_mask.

        Returns:
            (dfm, bad_mask, all_mask_cols)
            - dfm:           加了所有 mask 列的 DataFrame
            - bad_mask:      polars.Series[bool]，True 表示该行至少违反一条规则
            - all_mask_cols: 所有 mask 列名（counters/good_bad 用）
        """
        import polars as pl

        # 把所有 mask 加到 df
        if expr_masks:
            dfm = df.with_columns(*[e.alias(c) for c, e in expr_masks])
        else:
            dfm = df
        for cname, mask_list in precomputed.items():
            dfm = dfm.with_columns(pl.Series(cname, mask_list))

        # 合并 bad_mask
        all_mask_cols = [c for c, _ in expr_masks] + list(precomputed.keys())
        if all_mask_cols:
            bad_mask_expr = pl.lit(False)
            for c in all_mask_cols:
                bad_mask_expr = bad_mask_expr | pl.col(c)
            bad_mask = dfm.select(bad_mask_expr.alias("__bad")).to_series()
        else:
            bad_mask = pl.Series([False] * n)
        return dfm, bad_mask, all_mask_cols

    @staticmethod
    def _polars_outlier(df: Any, rconf: dict[str, Any]) -> tuple[set, int]:
        """Polars 路径：outlier 检测（polars quantile，只标记不拒收）.

        Returns:
            (outlier_indices, outlier_count)
        """
        import polars as pl

        oc = rconf.get("outlier") or {}
        oc_col = oc.get("column")
        outlier_indices: set = set()
        outlier_count = 0
        if oc_col and oc.get("action") == "flag" and oc_col in df.columns:
            cf_s = df.select(
                pl.col(oc_col).cast(pl.Utf8).str.replace_all(",", "").cast(pl.Float64, strict=False)
            ).to_series()
            valid = cf_s.drop_nulls()
            bounds = None
            if valid.len() >= 100:
                factor = float(oc.get("factor", 1.5))
                if oc.get("method", "iqr") == "zscore":
                    mean = valid.mean()
                    sd = valid.std(ddof=0)  # 总体标准差，与 statistics.pstdev 一致
                    if sd and sd > 0:
                        bounds = (mean - factor * sd, mean + factor * sd)
                else:
                    q1 = valid.quantile(0.25, interpolation="linear")
                    q3 = valid.quantile(0.75, interpolation="linear")
                    iqr = q3 - q1
                    bounds = (q1 - factor * iqr, q3 + factor * iqr)
            if bounds is not None:
                out_mask = (cf_s < bounds[0]) | (cf_s > bounds[1])
                outlier_count = int(out_mask.sum())
                for i, b in enumerate(out_mask.to_list()):
                    if b:
                        outlier_indices.add(i)
        return outlier_indices, outlier_count

    @staticmethod
    def _polars_counters(
        dfm: Any,
        rule_masks: dict[str, list[str]],
        n: int,
        rconf: dict[str, Any],
        outlier_count: int,
    ) -> dict[str, dict[str, int]]:
        """Polars 路径：counters 计算（与 python 路径对齐：每行每个子规则单独计数）.

        checked = n * len(mcols)，fail = sum(每个 mask_col 的 True 数).
        不能用 OR 合并后算 sum，否则同时违反多个子规则的行只算一次.
        注：空 DataFrame（n=0）时 select(sum).item() 返回 None，需兜底为 0.
        """
        import polars as pl

        oc = rconf.get("outlier") or {}
        oc_col = oc.get("column")
        counters: dict[str, dict[str, int]] = {}
        for rule, mcols in rule_masks.items():
            if mcols:
                fail = 0
                for c in mcols:
                    s = dfm.select(pl.col(c).sum()).item()
                    fail += int(s) if s is not None else 0
                checked = n * len(mcols)
            else:
                fail = 0
                checked = n
            counters[rule] = {"checked": checked, "passed": checked - fail}
        if oc_col and oc.get("action") == "flag":
            counters["outlier"] = {"checked": n, "passed": n - outlier_count}
        return counters

    @staticmethod
    def _polars_good_bad(
        df: Any,
        dfm: Any,
        bad_mask: Any,
        reason_specs: list[tuple[str, str]],
    ) -> tuple[Any, Any]:
        """Polars 路径：构造 good_df / bad_df + bad 行 reason 文本.

        bad 行 reason 文本基于已计算的 mask（顺序与 python 路径一致）.
        """
        import polars as pl

        good_df = df.filter(~bad_mask)
        bad_df = df.filter(bad_mask)

        if bad_df.height > 0:
            bad_idx = [i for i, b in enumerate(bad_mask.to_list()) if b]
            reason_mask_cols = list(dict.fromkeys(mc for _, mc in reason_specs))
            reason_values = {
                mc: dfm.select(pl.col(mc)).to_series().to_list() for mc in reason_mask_cols
            }
            bad_rows = bad_df.to_dicts()
            enriched = []
            for row_idx, row in zip(bad_idx, bad_rows):
                reasons = [rt for rt, mc in reason_specs if reason_values[mc][row_idx]]
                row_out = dict(row)
                row_out["_reasons"] = ";".join(reasons)
                row_out["_line"] = row_idx + 2
                enriched.append(row_out)
            bad_df = pl.DataFrame(enriched) if enriched else bad_df.head(0)
        return good_df, bad_df

    def check_spark(self, df: Any, spark: Any) -> tuple[Any, Any, list[dict[str, Any]], set]:
        """Spark 分布式校验分支.

        返回 ``(good_df, bad_df, stats, outlier_indices)``，good_df/bad_df 为
        SparkDataFrame；bad_df 额外带 ``_reasons``/``_line`` 列。

        - completeness/uniqueness/range/allowed_values: Spark SQL 表达式向量化
        - referential: broadcast join 小参考键表物化孤儿标记列（不 collect 孤儿键，
          内存 O(|ref|) 与孤儿数量无关；per-row 布尔语义与旧 isin 实现一致）
        - format (regex): F.col.rlike（Java regex，与 python re 在常见模式下等价）
        - date_valid: F.coalesce(F.to_date(...), ...) 多格式解析
        - outlier: collect 到 driver 算 bounds（与 polars 路径一致），只标记不拒收
        - bad 行 reason 文本: 用 F.when + F.concat_ws 在 executor 端生成，避免 collect

        DQ Score 口径与 python/polars 路径一致（规则检查项简单平均通过率）。
        参见 docs/evolution.md §4.3.2.4 / §4.4.2.1。

        注意：
        - Spark DataFrame 是 lazy 的，count()/collect()/write() 触发执行
        - F.col(c).cast("string") 把任意类型转为字符串，与 python 路径 row.get(c) 一致
        - F.trim(vs) == "" 判断空白字符串，与 python str(v).strip() == "" 一致
        - uniqueness 用窗口函数 row_number > 1 标记重复的后续行（与 polars
          is_duplicated & ~is_first_distinct 语义一致）；null/空串 key 不参与
          判定（对齐 python 路径），窗口 orderBy 用物化的稳定行号 _row_idx
        - referential 孤儿标记由 _spark_referential_markers 广播参考键表实现；
          旧实现 anti-join collect 孤儿键再 isin，孤儿键量大时 driver/序列化爆炸
        - format 用 F.when(vs.isNull() | trim==""，False).otherwise(~rlike)，
          null/空字符串视为通过（与 python/polars 一致）；rlike 前缀补 ^ 与
          python re.match 的前缀锚定对齐
        - _line = _row_idx + 2（_row_idx 为物化稳定行号；单文件 CSV 读入时
          对应原文件行序，多分区下为确定性的行标识）

        本方法为调度入口，子逻辑拆为 ``_spark_indexed`` /
        ``_spark_collect_masks`` / ``_spark_apply_masks`` / ``_spark_outlier`` /
        ``_spark_counters`` / ``_spark_good_bad`` 六个子方法.
        """
        rconf = self.rules
        if not rconf:
            return df, df.limit(0), [], set()

        # 0. 生成并物化稳定行号列 _row_idx（uniqueness 窗口与 _line 都依赖它；
        # 参见 _spark_indexed 注释：monotonically_increasing_id 非确定性不可用）
        df_indexed, n = self._spark_indexed(df)

        # 0.5 referential 孤儿标记物化（broadcast join 小参考键表）。与 clean 阶段
        # is_anomaly 的修复同理：旧实现把孤儿键 collect 回 driver 后 isin——孤儿键
        # 恰是质量校验的目标产出，量大时 driver 内存 + 每 task 序列化双重爆炸。
        df_indexed, orphan_markers, orphan_drop_cols = self._spark_referential_markers(
            df_indexed, rconf, spark
        )

        # 1. 各规则 fail mask（Spark Expr）
        expr_masks, reason_specs, rule_masks = self._spark_collect_masks(
            df_indexed, rconf, spark, orphan_markers
        )

        # 2. 把 mask 加到 df，合并 bad_mask
        dfm, all_mask_cols = self._spark_apply_masks(df_indexed, expr_masks)

        # 3. outlier 检测（collect 到 driver 算 bounds，只标记不拒收）
        outlier_indices, outlier_count = self._spark_outlier(df_indexed, rconf)

        # 4. counters（与 python/polars 路径对齐：每行每个子规则单独计数）
        counters = self._spark_counters(dfm, rule_masks, n, rconf, outlier_count)

        # 5. good / bad + reason 文本（df 原样传入仅作 bad 为空时的 schema 兜底）
        good_df, bad_df = self._spark_good_bad(
            df, dfm, all_mask_cols, reason_specs, extra_drop=orphan_drop_cols
        )

        stats = self._build_stats(counters)
        return good_df, bad_df, stats, outlier_indices

    def _spark_referential_markers(
        self, df: Any, rconf: dict[str, Any], spark: Any
    ) -> tuple[Any, dict[str, str], list[str]]:
        """为每条 referential 规则物化孤儿标记列（broadcast join，不 collect 孤儿键）.

        旧实现：keys_df anti-join collect 孤儿键到 driver → ``vs.isin(list)`` 构建表
        达式。悬空外键是质量校验的目标场景，孤儿键可达数万～百万级——isin 的巨型
        表达式树会 driver 内存 + 每 task 序列化双重爆炸（clean.py 同款问题 2026-08
        已改 broadcast join 修复）。现改为：broadcast **参考键表**（维度表，量级有界，
        先 set() 去重保证 1:1 命中不改变行数），left join 未命中且非空 → 孤儿标记列
        ``__orphan_rk<i>``。内存 O(|ref|)，与孤儿数量无关，per-row 布尔语义与旧
        isin 实现完全一致。

        Returns:
            (df_with_markers, orphan_markers, orphan_drop_cols)
            - orphan_markers:   {col: 标记列名}——_spark_collect_masks 据此引用标记列
            - orphan_drop_cols: 本方法引入的全部辅助列（__ref_rk<i> / __orphan_rk<i>），
                                good/bad 输出前需剔除
        参考表缺失或无有效键时不引入 join（_spark_collect_masks 回退 non_empty 表达式，
        语义与旧实现一致：所有非空值都是孤儿）。
        """
        from pyspark.sql import functions as F

        orphan_markers: dict[str, str] = {}
        orphan_drop_cols: list[str] = []
        for i, (col, target) in enumerate((rconf.get("referential", {}) or {}).items()):
            ref_table, ref_col = target.split(".")
            ref_rows = self.ref_data.get(ref_table, [])
            if not ref_rows:
                continue
            ref_keys = [str(r.get(ref_col)) for r in ref_rows if r.get(ref_col) not in (None, "")]
            if not ref_keys:
                continue
            ref_name = f"__ref_rk{i}"
            marker_name = f"__orphan_rk{i}"
            ref_df = spark.createDataFrame(
                [(k,) for k in set(ref_keys)], "k string"
            ).withColumnRenamed("k", ref_name)
            vs = F.col(col).cast("string")
            non_empty = ~vs.isNull() & (F.trim(vs) != "")
            df = df.join(F.broadcast(ref_df), vs == F.col(ref_name), "left")
            df = df.withColumn(marker_name, F.col(ref_name).isNull() & non_empty)
            orphan_markers[col] = marker_name
            orphan_drop_cols.extend([ref_name, marker_name])
        return df, orphan_markers, orphan_drop_cols

    @staticmethod
    def _spark_indexed(df: Any) -> tuple[Any, int]:
        """生成并物化稳定行号列 ``_row_idx``，返回 ``(indexed_df, row_count)``.

        为什么不能用 ``F.monotonically_increasing_id()``：它是非确定性表达式，
        每次 action（counters 聚合、good/bad 过滤、坏行写出）都会重新求值执行
        计划并重新生成 ID——同一组重复行被标记为 rn>1 的成员可能前后不一致，
        导致统计计数与实际隔离行不自洽。

        因此用 ``rdd.zipWithIndex()`` 在数据本身上派生行号，并立即
        ``cache()`` + ``count()`` 物化，所有下游 action 共享同一份物理行号。

        注：``zipWithIndex`` 后 ``rdd.toDF()`` 的结构是两列——``_1`` 为原行
        Row 折叠成的 struct 列，``_2`` 为 Long 索引列；须把 ``_1.<col>`` 逐一
        展开还原为原始列，并把 ``_2`` 重命名为 ``_row_idx``。

        Windows + Python 3.14 下 ``rdd.toDF()`` 无 schema 参数时会调用
        ``rdd.first()`` 做 schema 推断，而 ``RDD.first()/take()`` 在 PySpark
        4.2.0 的 Python 3.14 worker 中存在 pickle 反序列化崩溃（Connection
        reset by peer）。通过显式传入 ``struct<_1:原schema, _2:long>`` 绕开
        推断路径，避免触发崩溃。

        空表短路：``rdd.zipWithIndex().toDF()`` 在空 RDD 上做 schema 推断会调
        ``rdd.first()`` 抛 ``ValueError: RDD is empty``（增量批次中某表零新增
        行时 validate 即拿到空 DataFrame）。空表直接补 null ``_row_idx`` 列返
        回行计数 0，下游 lazy 检查自然产出零违规/空 good・bad，与
        python/polars 空表语义一致（checked=0, pass_rate=1.0）。
        """
        from pyspark.sql import functions as F

        # 2026-08-29 审查清理：此处曾残留一行超长 import
        # （StructType...DecimalType 全量类型），实际仅下方 StructType/
        # StructField/LongType 被使用（且与下方重复导入），已删除。

        # isEmpty() 底层调 take(1)，Python 3.14 + PySpark 4.2.0 下同样会
        # 触发 worker pickle 崩溃（Connection reset by peer）。改用 count()==0
        # 判断——count() 走 Spark 原生计数路径，不依赖 take()。
        if df.rdd.count() == 0:
            return df.withColumn("_row_idx", F.lit(None).cast("long")), 0

        # 显式构造 zipWithIndex 输出的 struct schema，避免 toDF() 无参时调
        # rdd.first() 做 schema 推断（Python 3.14 + PySpark 4.2.0 下该路径
        # 会因 worker pickle 崩溃导致 Connection reset by peer）。
        from pyspark.sql.types import LongType, StructField, StructType

        inner = StructType(df.schema.fields)
        zw_schema = StructType(
            [
                StructField("_1", inner, nullable=False),
                StructField("_2", LongType(), nullable=False),
            ]
        )
        indexed = (
            df.rdd.zipWithIndex()
            .toDF(zw_schema)
            .select(
                *[F.col("_1." + c).alias(c) for c in df.columns],
                F.col("_2").alias("_row_idx"),
            )
        )
        indexed = indexed.cache()
        n = indexed.count()  # 立即物化：后续所有 action 读同一份缓存
        return indexed, n

    def _spark_collect_masks(
        self,
        df: Any,
        rconf: dict[str, Any],
        spark: Any,
        orphan_markers: Optional[dict[str, str]] = None,
    ) -> tuple[list[tuple[str, Any]], list[tuple[str, str]], dict[str, list[str]]]:
        """Spark 路径：构建各规则 fail mask.

        遍历 completeness/uniqueness/range/allowed_values/referential/format/date_valid
        七类规则，为每个子规则生成一个 Spark Expr mask.

        referential：孤儿标记列由 ``_spark_referential_markers`` 预先以 broadcast join
        物化（orphan_markers[col] 给出标记列名），此处仅引用；参考表缺失/无有效键时
        回退 non_empty 表达式（所有非空值都是孤儿，语义与历史实现一致）.

        Returns:
            (expr_masks, reason_specs, rule_masks)
            - expr_masks:   [(col_name, F.Expr)] 加到 df 的表达式 mask
            - reason_specs: [(reason_text, mask_col_name)] bad 行 reason 文本生成用
            - rule_masks:   {rule_name: [mask_col_name]} counters 计算用
        """
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        expr_masks: list[tuple[str, Any]] = []
        reason_specs: list[tuple[str, str]] = []
        rule_masks: dict[str, list[str]] = {}

        def _next_mask_name() -> str:
            return f"__m{len(expr_masks)}"

        def add_expr_mask(rule: str, reason: str, expr: Any) -> None:
            col = _next_mask_name()
            expr_masks.append((col, expr))
            reason_specs.append((reason, col))
            rule_masks.setdefault(rule, []).append(col)

        # completeness: null 或 strip 后为空
        for col in (rconf.get("completeness") or {}).get("required_columns", []):
            vs = F.col(col).cast("string")
            expr = vs.isNull() | (F.trim(vs) == "")
            add_expr_mask("completeness", "missing_required:" + col, expr)

        # uniqueness: 窗口函数 row_number > 1（标记重复的后续行，与 polars
        # is_duplicated & ~is_first_distinct 语义一致）。
        # - orderBy 用 _spark_indexed 物化的稳定行号 _row_idx：保证 counters
        #   与 good/bad 过滤等所有 action 对"保留哪一条重复行"的判断一致。
        # - null/空串 key 豁免：partitionBy 把 null 归为同组会误判后续 null 行
        #   重复——统一为 python 语义（跳过 None/"" key，缺失由 completeness
        #   负责），这里对 null key 强制 rn=1 使其不被标记。
        for c in (rconf.get("uniqueness") or {}).get("columns", []):
            vs = F.col(c).cast("string")
            null_key = vs.isNull() | (F.trim(vs) == "")
            w = Window.partitionBy(c).orderBy(F.col("_row_idx"))
            rn = F.when(null_key, F.lit(1)).otherwise(F.row_number().over(w))
            add_expr_mask("uniqueness", "duplicate_key:" + c, rn > 1)

        # range: null 或超出 [min, max]（cast double 与 as_float 一致）
        for rng in rconf.get("range", []):
            col = rng["column"]
            lo = rng.get("min", float("-inf"))
            hi = rng.get("max", float("inf"))
            cf = F.col(col).cast("double")
            add_expr_mask("range", "range_violation:" + col, ~cf.between(lo, hi) | cf.isNull())

        # allowed_values: 非空且不在允许列表
        for col, vals in (rconf.get("allowed_values", {}) or {}).items():
            vs = F.col(col).cast("string")
            add_expr_mask(
                "allowed_values",
                "invalid_value:" + col,
                ~vs.isin(list(vals)) & ~vs.isNull() & (F.trim(vs) != ""),
            )

        # referential: 孤儿标记列已由 _spark_referential_markers 以 broadcast join
        # 物化（内存 O(|ref|)，与孤儿数量无关）；标记缺失时回退 non_empty（参考表
        # 缺失/无有效键 → 所有非空值都是孤儿，语义不变）
        for col, _target in (rconf.get("referential", {}) or {}).items():
            vs = F.col(col).cast("string")
            non_empty = ~vs.isNull() & (F.trim(vs) != "")
            marker = (orphan_markers or {}).get(col)
            if marker is not None:
                expr = F.col(marker)
            else:
                expr = non_empty
            add_expr_mask("referential", "orphan_reference:" + col, expr)

        # format (regex): F.col.rlike（Java regex）
        # null 或空字符串视为通过（与 python/polars 一致）
        # 前缀锚定对齐：python 路径用 re.match（前缀锚定），而 rlike 等价
        # re.search（无锚定）——同一模式 "ORD-\d{8}" 下 "XORD-12345678"
        # python 判失败、旧 spark 判通过。统一为锚定语义：前面补 ^
        # （模式本身以 ^ 开头时保持幂等）。
        # 注：Java regex 与 python re 在常见模式（ORD-\d{8}、ISO 日期）下等价；
        # 复杂回溯/命名分组等差异在本项目配置中不会触发。
        for col, pat in (rconf.get("format", {}) or {}).items():
            vs = F.col(col).cast("string")
            anchored = pat if pat.startswith("^") else "^" + pat
            expr = F.when(vs.isNull() | (F.trim(vs) == ""), False).otherwise(~vs.rlike(anchored))
            add_expr_mask("format", "format_violation:" + col, expr)

        # date_valid: F.try_to_timestamp 完整时间戳解析 + 秒级边界比较
        # 与 python date_parse 的三种格式（%Y-%m-%d、%Y-%m-%dT%H:%M:%S、
        # %Y-%m-%d %H:%M:%S）一致。解析失败返回 null。
        # 边界比较必须用完整 timestamp：旧实现 to_date 截断到日粒度，
        # max="2099-12-31" 时同日 23:59:59 的行被 spark 放行，而 python 路径
        # 按 datetime 精确比较（> 2099-12-31 00:00:00）会拦截——三引擎分歧。
        # 统一为 python 语义：配置值也按 timestamp 解析（纯日期配置取当日
        # 00:00:00，与 date_parse("2099-12-31") → datetime 零点一致）。
        # 用 try_to_timestamp 而非 to_timestamp：Spark 4.x 默认 ANSI 开启，
        # to_timestamp 对不匹配格式的输入直接抛 CANNOT_PARSE_TIMESTAMP，
        # coalesce 无法落到下一格式；try_ 族解析失败返回 null，与 python
        # date_parse 解析失败返回 None 的语义一致（ANSI 开/关均成立）。
        dconf = rconf.get("date_valid") or {}
        date_min = dconf.get("min")
        date_max = dconf.get("max")
        for col in dconf.get("columns", []):
            vs = F.col(col).cast("string")
            # 注：try_to_timestamp 的 format 参数为 ColumnOrName，裸字符串会被
            # 解析为列名（UNRESOLVED_COLUMN），必须用 F.lit 包裹成字面量
            parsed = F.coalesce(
                F.try_to_timestamp(vs, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
                F.try_to_timestamp(vs, F.lit("yyyy-MM-dd HH:mm:ss")),
                F.try_to_timestamp(vs, F.lit("yyyy-MM-dd")),
            )
            expr = parsed.isNull()
            if date_min:
                expr = expr | (parsed < F.lit(str(date_min)).cast("timestamp"))
            if date_max:
                expr = expr | (parsed > F.lit(str(date_max)).cast("timestamp"))
            add_expr_mask("date_valid", "invalid_date:" + col, expr)

        return expr_masks, reason_specs, rule_masks

    @staticmethod
    def _spark_apply_masks(df: Any, expr_masks: list[tuple[str, Any]]) -> tuple[Any, list[str]]:
        """Spark 路径：把 mask 加到 df，合并 bad_mask.

        Returns:
            (dfm, all_mask_cols)
            - dfm:           加了所有 mask 列 + __bad 列的 DataFrame
            - all_mask_cols: 所有 mask 列名（counters/good_bad 用）
        """
        from pyspark.sql import functions as F

        # 把所有 mask 加到 df
        dfm = df
        for col, expr in expr_masks:
            dfm = dfm.withColumn(col, expr)

        # 合并 bad_mask
        all_mask_cols = [c for c, _ in expr_masks]
        if all_mask_cols:
            bad_mask_expr = F.lit(False)
            for c in all_mask_cols:
                bad_mask_expr = bad_mask_expr | F.col(c)
            dfm = dfm.withColumn("__bad", bad_mask_expr)
        else:
            dfm = dfm.withColumn("__bad", F.lit(False))
        return dfm, all_mask_cols

    @staticmethod
    def _spark_outlier(df: Any, rconf: dict[str, Any]) -> tuple[set, int]:
        """Spark 路径：outlier 检测（分布式分位数/矩统计，只标记不拒收）.

        口径说明（与 python/polars 路径对齐项）：
        - 样本门槛：有效样本 < 100 时跳过检测，与 python _outlier_bounds /
          polars _polars_outlier 的 ``len >= 100`` 门槛一致（旧 Spark 实现
          缺失该门槛，小样本批次会单独计算 bounds，与另两引擎分歧——
          2026-08 审查 B2）。
        - method=zscore：mean + stddev_pop（总体标准差，对齐 python
          statistics.pstdev / polars std(ddof=0)）；旧 Spark 实现无视
          method 配置恒走 iqr，zscore 配置被静默改写（2026-08 审查 B2）。
        - 非法值处理：try_cast 解析失败 → null 排除统计，对齐 python
          as_float / polars cast(strict=False)。

        仍保留的轻微分歧（有意设计）：iqr 分位数用 approxQuantile(ε=0.001)
        而非精确线性插值——分位数估计误差在数据秩上最大 ±0.1%，映射到值域
        后 Q1/Q3 边界可能与精确值相差极小，边界附近（±误差带）的样本点在
        两个引擎间可能被不同地标记/放行，outlier_count 在小样本边界场景下
        可能与 python/polars 有 ±1～2 行级别的差异。不对齐精确口径的原因：
        精确 IQR 需要把整列 collect 到 driver，千万行级直接 OOM
        （2026-08 亿行基准实测）；outlier 仅 flag 不拒收、比例约 0.002，
        近似误差对下游聚合无实质影响。

        Returns:
            (outlier_indices, outlier_count)
        """
        from pyspark.sql import functions as F

        oc = rconf.get("outlier") or {}
        oc_col = oc.get("column")
        outlier_indices: set = set()
        outlier_count = 0
        if oc_col and oc.get("action") == "flag" and oc_col in df.columns:
            factor = float(oc.get("factor", 1.5))
            # total_amount 等派生列在 Spark 路径为 StringType：先落 double
            # 派生列再求统计量（approxQuantile/mean 不收字符串列）。
            # try_cast 而非 cast：ANSI 模式下非法值 cast 抛异常，try_cast
            # 保证"解析失败 → null → 排除统计"的 python/polars 语义。
            dfn = df.withColumn("_oc_num", F.col(oc_col).try_cast("double"))
            # 样本门槛对齐 python/polars：有效（非 null）样本 < 100 跳过.
            n_valid = dfn.where(F.col("_oc_num").isNotNull()).count()
            if n_valid < 100:
                return outlier_indices, outlier_count
            if oc.get("method", "iqr") == "zscore":
                # zscore 口径：分布式 mean/stddev_pop 聚合，driver 仅收 2 标量.
                row = dfn.agg(F.mean("_oc_num"), F.stddev_pop("_oc_num")).collect()[0]
                mean, sd = row[0], row[1]
                if mean is None or sd is None or sd == 0:
                    return outlier_indices, outlier_count
                lo, hi = mean - factor * sd, mean + factor * sd
            else:
                # bounds/count 用分布式 approxQuantile(+count) 求 IQR 界与越界
                # 行数（driver 仅收 2 个分位数标量 + 1 个计数）。旧实现把单列
                # 值**全表 collect 到 driver** 再算精确 IQR，千万行级直接 OOM
                # （2026-08 亿行基准实测）。ε=0.001 的分位近似在大表上的标记
                # 数差异可忽略；Spark 路径的 indices 仅作"存在越界行"信号，
                # 消费方 stages/validate.py 据此自行做越界 id 收集。
                qs = dfn.approxQuantile("_oc_num", [0.25, 0.75], 0.001)
                if not qs or any(q is None for q in qs):
                    return outlier_indices, outlier_count
                iqr = qs[1] - qs[0]
                lo, hi = qs[0] - factor * iqr, qs[1] + factor * iqr
            outlier_count = dfn.where((F.col("_oc_num") < lo) | (F.col("_oc_num") > hi)).count()
            if outlier_count > 0:
                outlier_indices.add(0)
        return outlier_indices, outlier_count

    @staticmethod
    def _spark_counters(
        dfm: Any,
        rule_masks: dict[str, list[str]],
        n: int,
        rconf: dict[str, Any],
        outlier_count: int,
    ) -> dict[str, dict[str, int]]:
        """Spark 路径：counters 计算（与 python/polars 路径对齐：每行每个子规则单独计数）.

        checked = n * len(mcols)，fail = sum(每个 mask_col 的 True 数).
        用 F.sum(cast int) 算每个 mask_col 的 True 数（boolean cast int: true=1, false=0）.

        优化：所有 mask_col 的 sum 合并为单次 agg() 调用（原实现逐列 agg().collect()，
        17 列 orders 表在 Python 3.14 + PySpark 4.2.0 下耗时 ~57s，合并后 ~5s）。
        """
        from pyspark.sql import functions as F

        oc = rconf.get("outlier") or {}
        oc_col = oc.get("column")
        counters: dict[str, dict[str, int]] = {}

        # 单次 agg 聚合全部 mask 列，避免 N 次独立 Spark action
        all_mcols: list[str] = []
        for mcols in rule_masks.values():
            all_mcols.extend(mcols)
        col_sums: dict[str, int] = {}
        if all_mcols:
            agg_exprs = [F.sum(F.col(c).cast("int")).alias(c) for c in all_mcols]
            row = dfm.agg(*agg_exprs).collect()[0]
            col_sums = {c: int(row[c]) if row[c] is not None else 0 for c in all_mcols}

        for rule, mcols in rule_masks.items():
            if mcols:
                fail = sum(col_sums.get(c, 0) for c in mcols)
                checked = n * len(mcols)
            else:
                fail = 0
                checked = n
            counters[rule] = {"checked": checked, "passed": checked - fail}
        if oc_col and oc.get("action") == "flag":
            counters["outlier"] = {"checked": n, "passed": n - outlier_count}
        return counters

    @staticmethod
    def _spark_good_bad(
        df: Any,
        dfm: Any,
        all_mask_cols: list[str],
        reason_specs: list[tuple[str, str]],
        extra_drop: Optional[list[str]] = None,
    ) -> tuple[Any, Any]:
        """Spark 路径：构造 good_df / bad_df + bad 行 reason 文本.

        bad 行 reason 文本用 F.when + F.concat 在 executor 端生成，避免 collect.
        每个 reason_part = rt + ";" 或 ""，concat 拼接后 regexp_replace 去末尾分号，
        顺序与 reason_specs 一致.

        extra_drop：调用方（check_spark）引入的辅助列（referential broadcast join
        的 __ref_rk<i> / __orphan_rk<i> 标记列），随 mask 列一并剔除.

        注：dfm 由 ``_spark_indexed`` 结果派生，带物化列 ``_row_idx``——
        good_df/bad_df 输出前必须剔除（下游按原始 schema 消费）；``_line``
        直接取 ``_row_idx + 2``（确定性，单文件 CSV 场景对应原文件行号；
        旧实现用 monotonically_increasing_id，非确定性且不对应行序）。
        """
        from pyspark.sql import functions as F

        drop_cols = all_mask_cols + ["__bad", "_row_idx"] + list(extra_drop or [])
        good_df = dfm.filter(~F.col("__bad")).drop(*drop_cols)
        bad_df_with_masks = dfm.filter(F.col("__bad"))

        bad_count = bad_df_with_masks.count()
        if bad_count > 0:
            reason_parts = []
            for rt, mc in reason_specs:
                reason_parts.append(F.when(F.col(mc), F.lit(rt + ";")).otherwise(F.lit("")))
            if reason_parts:
                reasons_expr = F.concat(*reason_parts)
                reasons_expr = F.regexp_replace(reasons_expr, r";$", "")
            else:
                reasons_expr = F.lit("")
            bad_df = (
                bad_df_with_masks.withColumn("_reasons", reasons_expr)
                .withColumn("_line", F.col("_row_idx") + 2)
                .drop(*drop_cols)
            )
        else:
            bad_df = df.limit(0)
        return good_df, bad_df

    def _outlier_bounds(self, rows: list[dict[str, str]], cfg: dict[str, Any]):
        col = cfg.get("column", "")
        if not col or cfg.get("action") != "flag":
            return None
        vals = [as_float(r.get(col)) for r in rows]
        vals = [v for v in vals if v is not None]
        if len(vals) < 100:
            return None
        factor = float(cfg.get("factor", 1.5))
        if cfg.get("method", "iqr") == "zscore":
            mean = statistics.mean(vals)
            sd = statistics.pstdev(vals)
            if sd == 0:
                return None
            return (mean - factor * sd, mean + factor * sd)
        q1, _, q3 = statistics.quantiles(vals, n=4)
        iqr = q3 - q1
        return (q1 - factor * iqr, q3 + factor * iqr)


def quality_summary(
    stats_by_dataset: dict[str, list[dict[str, Any]]], quarantined: dict[str, int]
) -> dict[str, Any]:
    all_rules = []
    for ds, stats in stats_by_dataset.items():
        for s in stats:
            all_rules.append(dict(s, dataset=ds))
    total_checked = sum(r["checked"] for r in all_rules)
    total_passed = sum(r["passed"] for r in all_rules)
    score = round(total_passed / total_checked, 4) if total_checked else 1.0
    return {
        "dq_score": score,
        "rules_total": len(all_rules),
        "checks_total": total_checked,
        "checks_passed": total_passed,
        "checks_failed": total_checked - total_passed,
        "quarantined_rows": dict(quarantined),
        "rules": all_rules,
    }


def render_markdown_report(summary: dict[str, Any]) -> str:
    lines = []
    lines.append("# 数据质量报告")
    lines.append("")
    lines.append(
        "- DQ Score（全部规则检查项简单平均通过率）: **{:.2%}**".format(summary["dq_score"])
    )
    lines.append(
        "- 规则检查项: {} 项（通过 {} / 失败 {}）".format(
            summary["checks_total"], summary["checks_passed"], summary["checks_failed"]
        )
    )
    lines.append(
        "- 隔离行数: {}".format(
            ", ".join(f"{k}={v}" for k, v in summary["quarantined_rows"].items()) or "无"
        )
    )
    lines.append("")
    lines.append("## 规则明细")
    lines.append("")
    lines.append("| 数据集 | 规则 | 检查数 | 通过 | 失败 | 通过率 |")
    lines.append("|---|---|---|---|---|---|")
    for r in summary["rules"]:
        lines.append(
            "| {} | {} | {} | {} | {} | {:.1%} |".format(
                r["dataset"], r["rule"], r["checked"], r["passed"], r["failed"], r["pass_rate"]
            )
        )
    lines.append("")
    lines.append("> DQ Score 口径：全部规则检查项的简单平均通过率；outlier 规则仅标记不拒收；")
    lines.append("> 唯一性/引用完整性为 100% 硬阈值；阈值均可在 config/pipeline.json 调整。")
    return "\n".join(lines)
