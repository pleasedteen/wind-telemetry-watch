"""纯算法单元测试：PSI 切点/分箱/平滑、TVD、窗口划分、规则编译。"""
from __future__ import annotations

import math

import pytest

from windwatch.common import (parse_instant, window_start_for,
                              validate_measurement)
from windwatch.detection import (MISSING_STATUS, assign_bins,
                                 quantile_cuts, psi_result, tvd_result)
from windwatch.rules import (MAX_DEPTH, RuleValidationError, compile_rules,
                             evaluate_with_path)


# ------------------------------------------------------------ PSI

def test_quantile_uses_ceil_index_one_based():
    # n=100，p=0.25/0.5/0.75 -> 第 25/50/75 个样本
    assert quantile_cuts(list(range(1, 101))) == [25, 50, 75]


def test_quantile_duplicate_cuts_merged():
    # 大量重复值导致切点重合，必须合并
    samples = [1.0] * 90 + [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0]
    cuts = quantile_cuts(samples)
    assert len(cuts) == len(set(cuts))


def test_bins_cover_whole_line_and_cut_belongs_to_left():
    cuts = [2.0, 5.0]
    assert assign_bins([-100.0, 2.0, 4.9, 5.0, 999.0], cuts) == [0, 0, 1, 1, 2]


def test_psi_identical_distributions_is_small():
    rng = deterministic(1)
    base = [rng() for _ in range(200)]
    cur = [rng() for _ in range(200)]
    r = psi_result(cur, base, 0.25, 30)
    assert r["status"] == "pass"
    assert r["psi"] < 0.1
    # 加 0.5 平滑后比例和为 1
    assert math.isclose(sum(r["current_proportions"]), 1.0)
    assert math.isclose(sum(r["baseline_proportions"]), 1.0)


def test_psi_shifted_distribution_fails():
    rng = deterministic(2)
    base = [rng.gauss(0, 1) for _ in range(200)]
    cur = [rng.gauss(4, 1) for _ in range(200)]
    r = psi_result(cur, base, 0.25, 30)
    assert r["status"] == "fail" and r["psi"] > 1.0


def test_psi_undetermined_never_fakes_zero():
    r = psi_result([1.0, 2.0], list(range(100)), 0.25, 30)
    assert r["status"] == "undetermined"
    assert r["psi"] is None
    assert "样本不足" in r["reason"]


def test_psi_missing_values_excluded():
    rng = deterministic(3)
    base = [rng.gauss(0, 1) for _ in range(100)]
    cur_clean = [rng.gauss(0, 1) for _ in range(100)]
    # 混入 None 不参与分布，样本数仍为 100
    cur = cur_clean + [None] * 50
    vals = [v for v in cur if v is not None]
    r = psi_result(vals, base, 0.25, 30)
    assert r["current_samples"] == 100


# ------------------------------------------------------------ TVD

def test_tvd_known_value():
    # 当前 90/10，基线 60/40 -> 0.5*(|.9-.6|+|.1-.4|)=0.3
    r = tvd_result(["a"] * 90 + ["b"] * 10,
                   ["a"] * 60 + ["b"] * 40, 0.2, 30)
    assert r["status"] == "fail"
    assert math.isclose(r["distance"], 0.3)


def test_tvd_union_categories_and_missing_class():
    r = tvd_result(["a"] * 60 + [None] * 40,
                   ["a"] * 60 + ["b"] * 40, 0.9, 30)
    assert set(r["categories"]) == {"a", "b", MISSING_STATUS}
    assert MISSING_STATUS in r["current_proportions"]


def test_tvd_undetermined_below_min_samples():
    r = tvd_result(["a"] * 5, ["a"] * 100, 0.2, 30)
    assert r["status"] == "undetermined" and r["distance"] is None


# ------------------------------------------------------------ 时间窗口

def test_window_aligned_to_ten_minutes_utc():
    dt = parse_instant("2026-11-10T00:07:33+00:00")
    ws = window_start_for(dt.timestamp())
    assert ws.isoformat() == "2026-11-10T00:00:00+00:00"


def test_window_timezone_normalized_to_utc():
    dt = parse_instant("2026-11-10T08:15:00+08:00")
    assert dt.isoformat() == "2026-11-10T00:15:00+00:00"


def test_naive_datetime_rejected():
    with pytest.raises(ValueError):
        parse_instant("2026-11-10T00:00:00")


# ------------------------------------------------------------ 行校验

def _base_row():
    return {
        "farm_id": "F1", "collector_id": "C1", "local_measure_id": "M1",
        "revision": 1, "measured_at": "2026-11-10T00:05:00+00:00",
        "turbine_id": "T1", "firmware_version": "3.1.7",
        "vibration": 2.1, "bearing_temp": 55.0, "operating_status": "generating",
    }


def test_validate_accepts_null_numeric_and_status():
    row = _base_row()
    row.update(vibration=None, operating_status=None)
    norm, reason = validate_measurement(row)
    assert reason is None and norm["vibration"] is None
    assert norm["operating_status"] is None


@pytest.mark.parametrize("mutate,needle", [
    (lambda r: r.update(unknown=1), "未知字段"),
    (lambda r: r.pop("farm_id"), "必填字段缺失"),
    (lambda r: r.update(revision=-1), "revision"),
    (lambda r: r.update(vibration="x"), "数值"),
    (lambda r: r.update(measured_at="2026-11-10 00:00:00"), "时区"),
    (lambda r: r.update(bearing_temp=float("inf")), "有限数值"),
])
def test_validate_rejects_bad_rows(mutate, needle):
    row = _base_row()
    mutate(row)
    _, reason = validate_measurement(row)
    assert needle in reason


# ------------------------------------------------------------ 规则引擎

def test_compile_and_evaluate_all_node_types():
    rules = compile_rules({
        "name": "t",
        "rules": [{
            "id": "r1", "name": "r1",
            "when": {"op": "compare", "left": {"field": "operating_status"},
                     "cmp": "==", "right": {"value": "generating"}},
            "expr": {"op": "all", "args": [
                {"op": "range", "field": "vibration", "min": 0.0, "max": 6.0},
                {"op": "any", "args": [
                    {"op": "field_exists", "field": "bearing_temp"}]},
                {"op": "not", "arg": {
                    "op": "in_set", "field": "firmware_version",
                    "values": ["bad"]}},
                {"op": "compare",
                 "left": {"field": "bearing_temp"},
                 "cmp": ">", "right": {"field": "vibration"}},
            ]},
        }],
    })
    from windwatch.detection import to_tuple
    rec = to_tuple({
        "farm_id": "F1", "collector_id": "C1", "local_measure_id": "M1",
        "revision": 1, "measured_at": "t", "turbine_id": "T1",
        "firmware_version": "3.1.7", "vibration": 2.0,
        "bearing_temp": 55.0, "operating_status": "generating",
    })
    rule = rules[0]
    assert rule.when(rec) is True
    assert rule.expr(rec) is True


def test_rule_rejects_unknown_field_and_bad_types_and_depth():
    bad_cases = [
        {"rules": [{"id": "x", "name": "x",
                    "expr": {"op": "range", "field": "ghost", "min": 0,
                             "max": 1}}]},
        {"rules": [{"id": "x", "name": "x",
                    "expr": {"op": "in_set", "field": "vibration",
                             "values": [1, "a"]}}]},
        {"rules": [{"id": "x", "name": "x",
                    "expr": {"op": "compare",
                             "left": {"field": "vibration"},
                             "cmp": "??", "right": {"value": 1}}}]},
    ]
    for body in bad_cases:
        with pytest.raises(RuleValidationError):
            compile_rules(body)

    deep = {"op": "field_exists", "field": "vibration"}
    for _ in range(MAX_DEPTH):  # 9 层
        deep = {"op": "not", "arg": deep}
    with pytest.raises(RuleValidationError):
        compile_rules({"rules": [{"id": "x", "name": "x", "expr": deep}]})


def test_failure_path_locates_leaf():
    tree = {"op": "all", "args": [
        {"op": "range", "field": "vibration", "min": 0.0, "max": 1.0},
        {"op": "range", "field": "bearing_temp", "min": 0.0, "max": 100.0},
    ]}
    from windwatch.detection import to_tuple
    rec = to_tuple({
        "farm_id": "F1", "collector_id": "C1", "local_measure_id": "M1",
        "revision": 1, "measured_at": "t", "turbine_id": "T1",
        "firmware_version": "f", "vibration": 5.0, "bearing_temp": 55.0,
        "operating_status": "generating",
    })
    ok, path = evaluate_with_path(tree, rec)
    assert ok is False and "range(vibration)" in path


# ------------------------------------------------------------ 辅助

class deterministic:
    """带 gauss 的确定性随机源，避免依赖 global random。"""

    def __init__(self, seed: int):
        import random
        self.rng = random.Random(seed)

    def __call__(self):
        return self.rng.random()

    def gauss(self, mu, sigma):
        return self.rng.gauss(mu, sigma)
