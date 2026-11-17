"""检测引擎：有效修订视图、PSI 分布漂移、状态总变差距离、规则汇总。

一次检测任务：
1. 固定入库序号上限 ``seq_cap``，后续所有规则/基线/报告均限于该数据范围；
2. 每个测量身份只取该范围内的最高修订版；
3. 数据只读取一次、规则只编译一次，全部窗口与分组复用；
4. 窗口按 UTC 十分钟整点、左闭右开，按窗口时间顺序推进。
"""
from __future__ import annotations

import math
from collections import deque
from typing import Any, Iterable

from .common import WINDOW_MINUTES, iso, window_start_for
from .rules import CompiledRule, evaluate_with_path

QUANTILES = (0.25, 0.5, 0.75)
MISSING_STATUS = "__MISSING__"

# 元组字段顺序（与 rules.FIELD_INDEX 对齐）
TUPLE_FIELDS = (
    "farm_id", "collector_id", "local_measure_id", "revision", "measured_at",
    "turbine_id", "firmware_version", "vibration", "bearing_temp",
    "operating_status",
)


# ---------------------------------------------------------------- 数据读取

def load_effective(conn, seq_cap: int, farm_id: str) -> list[dict]:
    """读取 seq_cap 范围内各测量身份的有效修订版（单次 SQL）。"""
    sql = """
    WITH ranked AS (
        SELECT farm_id, collector_id, local_measure_id, revision,
               measured_at, measured_epoch, turbine_id, firmware_version,
               vibration, bearing_temp, operating_status,
               ROW_NUMBER() OVER (
                   PARTITION BY farm_id, collector_id, local_measure_id
                   ORDER BY revision DESC, ingest_seq DESC, id DESC
               ) AS rn
        FROM measurements
        WHERE ingest_seq <= ? AND farm_id = ?
    )
    SELECT * FROM ranked WHERE rn = 1
    """
    return [dict(r) for r in conn.execute(sql, (seq_cap, farm_id)).fetchall()]


def to_tuple(rec: dict) -> tuple:
    return tuple(rec[f] for f in TUPLE_FIELDS)


# ---------------------------------------------------------------- 分箱与 PSI

def quantile_cuts(samples: list[float]) -> list[float]:
    """分位点取排序后第 ceil(p*n) 个样本（1 基），重复切点合并。"""
    ordered = sorted(samples)
    n = len(ordered)
    cuts: list[float] = []
    for p in QUANTILES:
        idx = math.ceil(p * n)  # 1 基下标
        cut = ordered[idx - 1]
        if not cuts or cut != cuts[-1]:
            cuts.append(cut)
    return cuts


def assign_bins(values: Iterable[float], cuts: list[float]) -> list[int]:
    """切点归左侧：x <= cut 归入该切点左侧的箱。"""
    out = []
    for x in values:
        placed = False
        for i, c in enumerate(cuts):
            if x <= c:
                out.append(i)
                placed = True
                break
        if not placed:
            out.append(len(cuts))  # 最后一个箱 (last_cut, +inf)
    return out


def psi_result(current: list[float], baseline: list[float],
               threshold: float, min_samples: int) -> dict:
    """单侧样本不足 30 判定 undetermined，绝不伪造零分。"""
    n_cur, n_base = len(current), len(baseline)
    result: dict[str, Any] = {
        "current_samples": n_cur,
        "baseline_samples": n_base,
        "threshold": threshold,
    }
    if current:
        result["current_range"] = [min(current), max(current)]
    if baseline:
        result["baseline_range"] = [min(baseline), max(baseline)]
    if n_cur < min_samples or n_base < min_samples:
        result.update(status="undetermined", psi=None,
                      reason=f"样本不足(当前 {n_cur}/基线 {n_base}，各需 ≥{min_samples})")
        return result

    cuts = quantile_cuts(baseline)  # 切点来自基线
    n_bins = len(cuts) + 1
    cur_counts = [0] * n_bins
    base_counts = [0] * n_bins
    for i in assign_bins(current, cuts):
        cur_counts[i] += 1
    for i in assign_bins(baseline, cuts):
        base_counts[i] += 1

    # 每箱计数加 0.5 后分别归一化
    cur_smooth = [c + 0.5 for c in cur_counts]
    base_smooth = [c + 0.5 for c in base_counts]
    cur_total = sum(cur_smooth)
    base_total = sum(base_smooth)
    cur_prop = [c / cur_total for c in cur_smooth]
    base_prop = [c / base_total for c in base_smooth]

    psi = sum(
        (cp - bp) * math.log(cp / bp) for cp, bp in zip(cur_prop, base_prop)
    )
    result.update(
        cuts=cuts,
        bins=_bin_descriptors(cuts),
        current_counts=cur_counts,
        baseline_counts=base_counts,
        current_proportions=cur_prop,
        baseline_proportions=base_prop,
        psi=psi,
        status="fail" if psi > threshold else "pass",
    )
    return result


def _bin_descriptors(cuts: list[float]) -> list[dict]:
    edges = [float("-inf")] + cuts + [float("inf")]
    bins = []
    for i in range(len(edges) - 1):
        bins.append({"lower": edges[i], "upper": edges[i + 1],
                     "upper_closed": i < len(cuts)})
    return bins


# ---------------------------------------------------------------- 总变差距离

def tvd_result(current: list[str | None], baseline: list[str | None],
               threshold: float, min_samples: int) -> dict:
    n_cur, n_base = len(current), len(baseline)
    result: dict[str, Any] = {
        "current_samples": n_cur,
        "baseline_samples": n_base,
        "threshold": threshold,
    }
    if n_cur < min_samples or n_base < min_samples:
        result.update(status="undetermined", distance=None,
                      reason=f"样本不足(当前 {n_cur}/基线 {n_base}，各需 ≥{min_samples})")
        return result

    def norm(rows):
        counts: dict[str, float] = {}
        for s in rows:
            key = MISSING_STATUS if s is None else s
            counts[key] = counts.get(key, 0) + 1
        total = sum(counts.values())
        return {k: v / total for k, v in counts.items()}, counts

    cur_p, cur_counts = norm(current)
    base_p, base_counts = norm(baseline)
    categories = sorted(set(cur_p) | set(base_p))
    distance = 0.5 * sum(abs(cur_p.get(c, 0.0) - base_p.get(c, 0.0))
                         for c in categories)
    result.update(
        categories=categories,
        current_counts={c: cur_counts.get(c, 0) for c in categories},
        baseline_counts={c: base_counts.get(c, 0) for c in categories},
        current_proportions={c: cur_p.get(c, 0.0) for c in categories},
        baseline_proportions={c: base_p.get(c, 0.0) for c in categories},
        distance=distance,
        status="fail" if distance > threshold else "pass",
    )
    return result


# ---------------------------------------------------------------- 窗口组织

def group_key_of(rec: dict, mode: str) -> str:
    if mode == "farm":
        return rec["farm_id"]
    return f"{rec['farm_id']}|{rec['firmware_version']}"


def window_of(epoch: float, minutes: int = WINDOW_MINUTES) -> tuple[str, str, float]:
    start = window_start_for(epoch, minutes)
    from datetime import timedelta
    end = start + timedelta(minutes=minutes)
    return iso(start), iso(end), start.timestamp()


def organize(records: list[dict], group_mode: str,
             minutes: int = WINDOW_MINUTES
             ) -> dict[str, dict[float, list[dict]]]:
    """{group_key: {window_start_epoch: [records]}}，窗口键有序。"""
    out: dict[str, dict[float, list[dict]]] = {}
    for rec in records:
        gk = group_key_of(rec, group_mode)
        ws, _, wepoch = window_of(rec["measured_epoch"], minutes)
        out.setdefault(gk, {}).setdefault(wepoch, []).append(rec)
    return out


# ---------------------------------------------------------------- 规则评估

def evaluate_rules(window_recs: list[dict], rules: list[CompiledRule],
                   max_samples: int) -> dict[str, dict]:
    """对一个窗口 × 分组做规则汇总，复用编译闭包，逐记录只扫一遍。"""
    summaries: dict[str, dict] = {}
    for rule in rules:
        summaries[rule.rule_id] = {
            "rule_id": rule.rule_id, "rule_name": rule.name,
            "denominator": 0, "violations": 0, "samples": [],
        }

    for rec in window_recs:
        tup = to_tuple(rec)
        for rule in rules:
            if rule.when is not None and not rule.when(tup):
                continue  # 适用条件不满足，不进分母
            s = summaries[rule.rule_id]
            s["denominator"] += 1
            if not rule.expr(tup):
                s["violations"] += 1
                if len(s["samples"]) < max_samples:
                    _, path = evaluate_with_path(rule.raw_expr, tup)
                    s["samples"].append({
                        "measurement": _sample_measurement(rec),
                        "failure_path": path,
                    })
    return summaries


def _sample_measurement(rec: dict) -> dict:
    return {k: rec[k] for k in (
        "farm_id", "collector_id", "local_measure_id", "revision",
        "measured_at", "turbine_id", "firmware_version",
        "vibration", "bearing_temp", "operating_status",
    )}


# ---------------------------------------------------------------- 逐窗口推进

def run_windows(grouped: dict[str, dict[float, list[dict]]],
                rules: list[CompiledRule], items: dict[str, dict],
                cfg_thresholds: dict, min_samples: int, baseline_windows: int,
                max_violation_samples: int, minutes: int = WINDOW_MINUTES):
    """按窗口时间顺序产出 (window_start, window_end, group_key, item, status,
    score, threshold, detail, violation_samples)。

    ``items``: {item_name: {"threshold": float}}，规则项名 rule:<id>。
    基线 = 同分组此前连续 baseline_windows 个窗口的样本合并。
    """
    history: dict[str, deque] = {}
    group_names = sorted(grouped)
    all_window_epochs = sorted({w for g in grouped.values() for w in g})

    for w_epoch in all_window_epochs:
        from datetime import datetime, timedelta, timezone
        w_start = datetime.fromtimestamp(w_epoch, tz=timezone.utc)
        w_end = w_start + timedelta(minutes=minutes)
        ws_iso, we_iso = iso(w_start), iso(w_end)

        for gk in group_names:
            windows = grouped[gk]
            current = windows.get(w_epoch)
            if not current:
                # 该分组此窗口无当前样本，仍把空窗口压入历史，保持“连续窗口”语义
                history.setdefault(gk, deque(maxlen=baseline_windows)).append([])
                continue
            hist = history.setdefault(gk, deque(maxlen=baseline_windows))
            base_recs = [r for past in hist for r in past]
            hist.append(current)

            for item_name, spec in items.items():
                threshold = spec.get("threshold")
                if threshold is None:
                    threshold = cfg_thresholds.get(
                        item_name, cfg_thresholds.get("__rule_default__", 0.05))
                if item_name == "vibration_psi":
                    detail = psi_result(
                        [r["vibration"] for r in current if r["vibration"] is not None],
                        [r["vibration"] for r in base_recs if r["vibration"] is not None],
                        threshold, min_samples,
                    )
                    yield ws_iso, we_iso, gk, item_name, detail.pop("status"), \
                        detail.get("psi"), threshold, detail, []
                elif item_name == "temperature_psi":
                    detail = psi_result(
                        [r["bearing_temp"] for r in current if r["bearing_temp"] is not None],
                        [r["bearing_temp"] for r in base_recs if r["bearing_temp"] is not None],
                        threshold, min_samples,
                    )
                    yield ws_iso, we_iso, gk, item_name, detail.pop("status"), \
                        detail.get("psi"), threshold, detail, []
                elif item_name == "status_tvd":
                    detail = tvd_result(
                        [r["operating_status"] for r in current],
                        [r["operating_status"] for r in base_recs],
                        threshold, min_samples,
                    )
                    yield ws_iso, we_iso, gk, item_name, detail.pop("status"), \
                        detail.get("distance"), threshold, detail, []
                elif item_name.startswith("rule:"):
                    rule_id = item_name.split(":", 1)[1]
                    rule = next(r for r in rules if r.rule_id == rule_id)
                    s = evaluate_rules(current, [rule], max_violation_samples)[rule_id]
                    samples = s.pop("samples")
                    denom = s["denominator"]
                    if denom == 0:
                        detail = {**s, "violation_rate": None,
                                  "reason": "分母为零（无适用记录），无法判定"}
                        yield ws_iso, we_iso, gk, item_name, "undetermined", \
                            None, threshold, detail, samples
                    else:
                        rate = s["violations"] / denom
                        detail = {**s, "violation_rate": rate}
                        yield ws_iso, we_iso, gk, item_name, \
                            ("fail" if rate > threshold else "pass"), \
                            rate, threshold, detail, samples
                else:  # pragma: no cover - 提交时已校验
                    raise ValueError(f"未知检测项: {item_name}")
