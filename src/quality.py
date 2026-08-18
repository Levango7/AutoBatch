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

    def __init__(self, dataset: str, rules: dict[str, Any],
                 ref_data: Optional[dict[str, list[dict[str, str]]]] = None):
        self.dataset = dataset
        self.rules = rules
        self.ref_data = ref_data or {}

    def _ref_keys(self, table: str, column: str):
        rows = self.ref_data.get(table, [])
        return {r.get(column) for r in rows if r.get(column) not in (None, "")}

    def check(self, rows: Optional[list[dict[str, str]]] = None,
              df: Any = None,
              spark: Any = None) -> tuple[Any, Any, list[dict[str, Any]], set]:
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
        return [{
            "rule": rule,
            "checked": c["checked"],
            "passed": c["passed"],
            "failed": c["checked"] - c["passed"],
            "pass_rate": round(c["passed"] / c["checked"], 4) if c["checked"] else 1.0,
        } for rule, c in counters.items()]

    def check_python(self, rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], set]:
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
            col: self._ref_keys(*target.split("."))
            for col, target in refer.items()
        }

        counters: dict[str, dict[str, int]] = {}

        def bump(rule: str, ok: bool) -> None:
            c = counters.setdefault(rule, {"checked": 0, "passed": 0})
            c["checked"] += 1
            if ok:
                c["passed"] += 1

        for i, row in enumerate(rows):
            reasons = self._python_row_reasons(
                i, row, required, uniq_cols, dup_ids, ranges, allowed,
                formats, date_cols, date_min, date_max, refer, refer_keys,
                bounds, oc, counters, bump, outlier_indices,
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
        i: int, row: dict[str, str], required: list[str], uniq_cols: list[str],
        dup_ids: set, ranges: list, allowed: dict, formats: dict,
        date_cols: list, date_min, date_max, refer: dict, refer_keys: dict,
        bounds, oc: dict, counters: dict, bump, outlier_indices: set,
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
        expr_masks, precomputed, reason_specs, rule_masks = (
            self._polars_collect_masks(df, rconf)
        )

        # 2. 把 mask 加到 df，合并 bad_mask
        dfm, bad_mask, all_mask_cols = self._polars_apply_masks(
            df, expr_masks, precomputed, n
        )

        # 3. outlier 检测（只标记不拒收）
        outlier_indices, outlier_count = self._polars_outlier(df, rconf)

        # 4. counters（与 python 路径对齐：每行每个子规则单独计数）
        counters = self._polars_counters(
            dfm, rule_masks, n, rconf, outlier_count
        )

        # 5. good / bad + reason 文本
        good_df, bad_df = self._polars_good_bad(
            df, dfm, bad_mask, reason_specs
        )

        stats = self._build_stats(counters)
        return good_df, bad_df, stats, outlier_indices

    def _polars_collect_masks(
        self, df: Any, rconf: dict[str, Any]
    ) -> tuple[list[tuple[str, Any]], dict[str, list[bool]], list[tuple[str, str]], dict[str, list[str]]]:
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
        for c in (rconf.get("uniqueness") or {}).get("columns", []):
            expr = pl.col(c).is_duplicated() & ~pl.col(c).is_first_distinct()
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
            add_expr_mask("allowed_values", "invalid_value:" + col,
                          (~vs.is_in(list(vals))) & (~vs.is_null()) & (vs.str.strip_chars() != ""))

        # referential: anti join 一次找出孤儿值，再 is_in 标记
        for col, target in (rconf.get("referential", {}) or {}).items():
            ref_table, ref_col = target.split(".")
            ref_rows = self.ref_data.get(ref_table, [])
            vs = pl.col(col).cast(pl.Utf8)
            non_empty = (~vs.is_null()) & (vs.str.strip_chars() != "")
            if ref_rows:
                ref_keys = [str(r.get(ref_col)) for r in ref_rows
                            if r.get(ref_col) not in (None, "")]
                ref_keys_df = pl.DataFrame({"__k": ref_keys}).unique()
                keys_df = df.select(vs.alias("__k"))
                orphan_keys = keys_df.join(ref_keys_df, on="__k", how="anti").get_column("__k").to_list()
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
            mask_list = [not (str(v).strip() == "" or pat_re.match(str(v)) is not None) for v in vals]
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
        df: Any, expr_masks: list[tuple[str, Any]],
        precomputed: dict[str, list[bool]], n: int,
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
    def _polars_outlier(
        df: Any, rconf: dict[str, Any]
    ) -> tuple[set, int]:
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
            cf_s = df.select(pl.col(oc_col).cast(pl.Utf8).str.replace_all(",", "")
                             .cast(pl.Float64, strict=False)).to_series()
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
        dfm: Any, rule_masks: dict[str, list[str]], n: int,
        rconf: dict[str, Any], outlier_count: int,
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
        df: Any, dfm: Any, bad_mask: Any,
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
            reason_values = {mc: dfm.select(pl.col(mc)).to_series().to_list()
                             for mc in reason_mask_cols}
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
        - referential: orders.join(ref_df, key, 'left_anti') 找外键不匹配的行
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
          is_duplicated & ~is_first_distinct 语义一致）
        - referential 用 left_anti join 一次找出孤儿 key 集合，再 isin 标记
        - format 用 F.when(vs.isNull() | trim==""，False).otherwise(~rlike)，
          null/空字符串视为通过（与 python/polars 一致）
        - _line 用 monotonically_increasing_id + 2（Spark 无文件行号概念，
          仅作唯一标识，不对应原文件行号）

        本方法为调度入口，子逻辑拆为 ``_spark_collect_masks`` /
        ``_spark_apply_masks`` / ``_spark_outlier`` /
        ``_spark_counters`` / ``_spark_good_bad`` 五个子方法.
        """
        rconf = self.rules
        n = df.count()  # 触发 action 获取行数
        if not rconf:
            return df, df.limit(0), [], set()

        # 1. 各规则 fail mask（Spark Expr）
        expr_masks, reason_specs, rule_masks = self._spark_collect_masks(
            df, rconf, spark
        )

        # 2. 把 mask 加到 df，合并 bad_mask
        dfm, all_mask_cols = self._spark_apply_masks(df, expr_masks)

        # 3. outlier 检测（collect 到 driver 算 bounds，只标记不拒收）
        outlier_indices, outlier_count = self._spark_outlier(df, rconf)

        # 4. counters（与 python/polars 路径对齐：每行每个子规则单独计数）
        counters = self._spark_counters(dfm, rule_masks, n, rconf, outlier_count)

        # 5. good / bad + reason 文本
        good_df, bad_df = self._spark_good_bad(
            df, dfm, all_mask_cols, reason_specs
        )

        stats = self._build_stats(counters)
        return good_df, bad_df, stats, outlier_indices

    def _spark_collect_masks(
        self, df: Any, rconf: dict[str, Any], spark: Any
    ) -> tuple[list[tuple[str, Any]], list[tuple[str, str]], dict[str, list[str]]]:
        """Spark 路径：构建各规则 fail mask.

        遍历 completeness/uniqueness/range/allowed_values/referential/format/date_valid
        七类规则，为每个子规则生成一个 Spark Expr mask.

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
        # is_duplicated & ~is_first_distinct 语义一致）。用 monotonically_increasing_id
        # 作为 orderBy 保证 partition 内行顺序确定。
        for c in (rconf.get("uniqueness") or {}).get("columns", []):
            w = Window.partitionBy(c).orderBy(F.monotonically_increasing_id())
            expr = (F.row_number().over(w) > 1)
            add_expr_mask("uniqueness", "duplicate_key:" + c, expr)

        # range: null 或超出 [min, max]（cast double 与 as_float 一致）
        for rng in rconf.get("range", []):
            col = rng["column"]
            lo = rng.get("min", float("-inf"))
            hi = rng.get("max", float("inf"))
            cf = F.col(col).cast("double")
            add_expr_mask("range", "range_violation:" + col,
                          ~cf.between(lo, hi) | cf.isNull())

        # allowed_values: 非空且不在允许列表
        for col, vals in (rconf.get("allowed_values", {}) or {}).items():
            vs = F.col(col).cast("string")
            add_expr_mask("allowed_values", "invalid_value:" + col,
                          ~vs.isin(list(vals)) & ~vs.isNull() & (F.trim(vs) != ""))

        # referential: left_anti join 一次找出孤儿 key 集合，再 isin 标记
        # ref_data 始终为 List[Dict] 格式（validate.py 用 load_csv 读），
        # 这里用 spark.createDataFrame 转成 SparkDataFrame 再 join。
        for col, target in (rconf.get("referential", {}) or {}).items():
            ref_table, ref_col = target.split(".")
            ref_rows = self.ref_data.get(ref_table, [])
            vs = F.col(col).cast("string")
            non_empty = ~vs.isNull() & (F.trim(vs) != "")
            if ref_rows:
                ref_keys = [str(r.get(ref_col)) for r in ref_rows
                            if r.get(ref_col) not in (None, "")]
                if ref_keys:
                    # 用 left_anti join 找孤儿 key（分布式，避免 collect 整张 ref 表）
                    ref_keys_df = spark.createDataFrame(
                        [(k,) for k in set(ref_keys)], "k string"
                    ).withColumnRenamed("k", "__k")
                    keys_df = df.select(vs.alias("__k")).distinct()
                    orphan_rows = keys_df.join(
                        ref_keys_df, "__k", "left_anti"
                    ).collect()
                    orphan_key_list = [row["__k"] for row in orphan_rows
                                       if row["__k"] is not None]
                    expr = vs.isin(orphan_key_list) & non_empty
                else:
                    # ref 表为空：所有非空值都是孤儿
                    expr = non_empty
            else:
                expr = non_empty
            add_expr_mask("referential", "orphan_reference:" + col, expr)

        # format (regex): F.col.rlike（Java regex）
        # null 或空字符串视为通过（与 python/polars 一致）
        # 注：Java regex 与 python re 在常见模式（ORD-\d{8}、ISO 日期）下等价；
        # 复杂回溯/命名分组等差异在本项目配置中不会触发。
        for col, pat in (rconf.get("format", {}) or {}).items():
            vs = F.col(col).cast("string")
            expr = F.when(vs.isNull() | (F.trim(vs) == ""), False).otherwise(~vs.rlike(pat))
            add_expr_mask("format", "format_violation:" + col, expr)

        # date_valid: F.coalesce(F.to_date(...), ...) 多格式解析
        # 与 python date_parse 的三种格式（%Y-%m-%d、%Y-%m-%dT%H:%M:%S、
        # %Y-%m-%d %H:%M:%S）一致。to_date 解析失败返回 null。
        dconf = rconf.get("date_valid") or {}
        date_min = dconf.get("min")
        date_max = dconf.get("max")
        for col in dconf.get("columns", []):
            vs = F.col(col).cast("string")
            parsed = F.coalesce(
                F.to_date(vs, "yyyy-MM-dd"),
                F.to_date(vs, "yyyy-MM-dd'T'HH:mm:ss"),
                F.to_date(vs, "yyyy-MM-dd HH:mm:ss"),
            )
            expr = parsed.isNull()
            if date_min:
                expr = expr | (parsed < F.lit(date_min).cast("date"))
            if date_max:
                expr = expr | (parsed > F.lit(date_max).cast("date"))
            add_expr_mask("date_valid", "invalid_date:" + col, expr)

        return expr_masks, reason_specs, rule_masks

    @staticmethod
    def _spark_apply_masks(
        df: Any, expr_masks: list[tuple[str, Any]]
    ) -> tuple[Any, list[str]]:
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
    def _spark_outlier(
        df: Any, rconf: dict[str, Any]
    ) -> tuple[set, int]:
        """Spark 路径：outlier 检测（collect 到 driver 算 bounds，只标记不拒收）.

        注：对大数据集 collect 到 driver 有性能瓶颈；Spark 原生方式是用
        df.stat.approxQuantile 算 Q1/Q3。这里为了与 python/polars 路径
        bounds 计算完全一致（statistics.quantiles 用线性插值），选择 collect。
        outlier 比例通常 0.002，collect 的数据量小，可接受.

        Returns:
            (outlier_indices, outlier_count)
        """
        from pyspark.sql import functions as F
        oc = rconf.get("outlier") or {}
        oc_col = oc.get("column")
        outlier_indices: set = set()
        outlier_count = 0
        if oc_col and oc.get("action") == "flag" and oc_col in df.columns:
            cf = F.col(oc_col).cast("double")
            vals_rows = df.select(cf.alias("v")).collect()
            vals = [row["v"] for row in vals_rows if row["v"] is not None]
            bounds = None
            if len(vals) >= 100:
                factor = float(oc.get("factor", 1.5))
                if oc.get("method", "iqr") == "zscore":
                    mean = statistics.mean(vals)
                    sd = statistics.pstdev(vals)
                    if sd > 0:
                        bounds = (mean - factor * sd, mean + factor * sd)
                else:
                    q1, _, q3 = statistics.quantiles(vals, n=4)
                    iqr = q3 - q1
                    bounds = (q1 - factor * iqr, q3 + factor * iqr)
            if bounds is not None:
                # 用 collect 后的顺序算 outlier 行索引（与 polars 路径一致）
                for i, row in enumerate(vals_rows):
                    v = row["v"]
                    if v is not None and (v < bounds[0] or v > bounds[1]):
                        outlier_indices.add(i)
                        outlier_count += 1
        return outlier_indices, outlier_count

    @staticmethod
    def _spark_counters(
        dfm: Any, rule_masks: dict[str, list[str]], n: int,
        rconf: dict[str, Any], outlier_count: int,
    ) -> dict[str, dict[str, int]]:
        """Spark 路径：counters 计算（与 python/polars 路径对齐：每行每个子规则单独计数）.

        checked = n * len(mcols)，fail = sum(每个 mask_col 的 True 数).
        用 F.sum(cast int) 算每个 mask_col 的 True 数（boolean cast int: true=1, false=0）.
        """
        from pyspark.sql import functions as F
        oc = rconf.get("outlier") or {}
        oc_col = oc.get("column")
        counters: dict[str, dict[str, int]] = {}
        for rule, mcols in rule_masks.items():
            if mcols:
                fail = 0
                for c in mcols:
                    s = dfm.agg(F.sum(F.col(c).cast("int")).alias("s")).collect()[0]["s"]
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
    def _spark_good_bad(
        df: Any, dfm: Any, all_mask_cols: list[str],
        reason_specs: list[tuple[str, str]],
    ) -> tuple[Any, Any]:
        """Spark 路径：构造 good_df / bad_df + bad 行 reason 文本.

        bad 行 reason 文本用 F.when + F.concat 在 executor 端生成，避免 collect.
        每个 reason_part = rt + ";" 或 ""，concat 拼接后 regexp_replace 去末尾分号，
        顺序与 reason_specs 一致.
        """
        from pyspark.sql import functions as F
        good_df = dfm.filter(~F.col("__bad")).drop(*(all_mask_cols + ["__bad"]))
        bad_df_with_masks = dfm.filter(F.col("__bad"))

        bad_count = bad_df_with_masks.count()
        if bad_count > 0:
            reason_parts = []
            for rt, mc in reason_specs:
                reason_parts.append(
                    F.when(F.col(mc), F.lit(rt + ";")).otherwise(F.lit(""))
                )
            if reason_parts:
                reasons_expr = F.concat(*reason_parts)
                reasons_expr = F.regexp_replace(reasons_expr, r";$", "")
            else:
                reasons_expr = F.lit("")
            # _line 用 monotonically_increasing_id + 2（Spark 无文件行号概念，
            # 仅作唯一标识，不对应原文件行号；与 python/polars 的 i+2 语义近似）
            bad_df = (bad_df_with_masks
                      .withColumn("_reasons", reasons_expr)
                      .withColumn("_line", F.monotonically_increasing_id() + 2)
                      .drop(*(all_mask_cols + ["__bad"])))
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


def quality_summary(stats_by_dataset: dict[str, list[dict[str, Any]]],
                    quarantined: dict[str, int]) -> dict[str, Any]:
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
    lines.append("- DQ Score（全部规则检查项简单平均通过率）: **{:.2%}**".format(summary["dq_score"]))
    lines.append("- 规则检查项: {} 项（通过 {} / 失败 {}）".format(
        summary["checks_total"], summary["checks_passed"], summary["checks_failed"]))
    lines.append("- 隔离行数: {}".format(
        ", ".join(f"{k}={v}" for k, v in summary["quarantined_rows"].items()) or "无"))
    lines.append("")
    lines.append("## 规则明细")
    lines.append("")
    lines.append("| 数据集 | 规则 | 检查数 | 通过 | 失败 | 通过率 |")
    lines.append("|---|---|---|---|---|---|")
    for r in summary["rules"]:
        lines.append("| {} | {} | {} | {} | {} | {:.1%} |".format(
            r["dataset"], r["rule"], r["checked"], r["passed"], r["failed"], r["pass_rate"]))
    lines.append("")
    lines.append("> DQ Score 口径：全部规则检查项的简单平均通过率；outlier 规则仅标记不拒收；")
    lines.append("> 唯一性/引用完整性为 100% 硬阈值；阈值均可在 config/pipeline.json 调整。")
    return "\n".join(lines)
