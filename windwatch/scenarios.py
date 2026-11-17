"""固定种子的回放数据与中等规模数据生成器（纯本地数据）。

回放覆盖：正常运行、固件导致的振动偏移、状态类别变化、补传修订、
无效行、样本不足、通知接收后断连。
所有随机数来自 random.Random(seed)，结果可复现。
"""
from __future__ import annotations

import random
from datetime import datetime, timezone

SEED = 20261117
# 固定基准窗口：2026-11-10 00:00 UTC，保证每次回放的窗口完全一致
BASE_EPOCH = datetime(2026, 11, 10, 0, 0, tzinfo=timezone.utc).timestamp()
WINDOW_SECS = 600

STATUSES = ("generating", "standby", "curtailed", "fault")
GOOD_FIRMWARE = "3.1.7"
BAD_FIRMWARE = "3.1.9"


def _ts(window_idx: int, rng: random.Random) -> str:
    epoch = BASE_EPOCH + window_idx * WINDOW_SECS + rng.uniform(0, 590)
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def _measure(farm, collector, local_id, revision, window_idx, rng,
             firmware=GOOD_FIRMWARE, vibration_shift=0.0,
             force_status=None, turbine=None, missing_temp=False,
             missing_vib=False):
    status = force_status or rng.choices(
        STATUSES, weights=(0.8, 0.08, 0.08, 0.04))[0]
    return {
        "farm_id": farm,
        "collector_id": collector,
        "local_measure_id": f"M{local_id:05d}",
        "revision": revision,
        "measured_at": _ts(window_idx, rng),
        "turbine_id": turbine or f"T{(local_id % 6) + 1:02d}",
        "firmware_version": firmware,
        "vibration": None if missing_vib else round(
            rng.gauss(2.0, 0.3) + vibration_shift, 4),
        "bearing_temp": None if missing_temp else round(
            rng.gauss(55.0, 4.0), 3),
        "operating_status": status,
    }


# ---------------------------------------------------------------- 场景定义

def list_scenarios() -> list[str]:
    return [
        "normal", "firmware_vibration_shift", "status_category_change",
        "revision_backfill", "invalid_rows", "insufficient_samples",
        "duplicate_replay", "notify_disconnect",
    ]


def build_replay_batches(scenario: str, seed: int = SEED) -> list[dict]:
    if scenario not in list_scenarios():
        raise KeyError(f"未知场景: {scenario}")
    fn = {
        "normal": _s_normal,
        "firmware_vibration_shift": _s_firmware_shift,
        "status_category_change": _s_status_change,
        "revision_backfill": _s_backfill,
        "invalid_rows": _s_invalid,
        "insufficient_samples": _s_insufficient,
        "duplicate_replay": _s_duplicate,
        "notify_disconnect": _s_disconnect,
    }[scenario]
    return fn(random.Random(seed))


def _s_normal(rng):
    rows = []
    lid = 10000
    for w in range(12):
        for _ in range(36):
            lid += 1
            rows.append(_measure("F1", "C1", lid, 1, w, rng))
    return [{"batch_id": "normal-b1", "measurements": rows}]


def _s_firmware_shift(rng):
    """F1 的 C2 采集器在窗口 7 之后升级坏固件 3.1.9，振动整体上移。"""
    good, bad = [], []
    lid = 20000
    for w in range(7):
        for _ in range(36):
            lid += 1
            good.append(_measure("F1", "C2", lid, 1, w, rng, GOOD_FIRMWARE))
    for w in range(7, 12):
        for _ in range(36):
            lid += 1
            bad.append(_measure("F1", "C2", lid, 1, w, rng,
                                BAD_FIRMWARE, vibration_shift=2.5))
    return [
        {"batch_id": "fw-b1-good", "measurements": good},
        {"batch_id": "fw-b2-bad", "measurements": bad},
    ]


def _s_status_change(rng):
    """窗口 8 起运行状态集合变化（新增 maintenance、fault 消失）。"""
    before, after = [], []
    lid = 30000
    for w in range(8):
        for _ in range(36):
            lid += 1
            before.append(_measure("F2", "C1", lid, 1, w, rng))
    for w in range(8, 12):
        for _ in range(36):
            lid += 1
            after.append(_measure(
                "F2", "C1", lid, 1, w, rng,
                force_status=rng.choices(
                    ("generating", "standby", "maintenance"),
                    weights=(0.5, 0.2, 0.3))[0]))
    return [
        {"batch_id": "st-b1", "measurements": before},
        {"batch_id": "st-b2", "measurements": after},
    ]


def _s_backfill(rng):
    """30 个测量身份、每个跨 4 个窗口各一条；先收修订版 1，再补传修订版 2。

    两批使用完全相同的测量身份（风场+采集器+本地测量号+测量时刻），
    仅修订号与振动值不同，模拟维修后对历史测量的补传。
    """
    first, revised = [], []
    base_lid = 40000
    for window in range(4):
        for k in range(30):
            local_id = f"M{base_lid + k + 1:05d}"  # 30 个稳定身份
            m = _measure("F1", "C3", base_lid + k + 1, 1, window, rng)
            m["local_measure_id"] = local_id
            first.append(m)
    rng2 = random.Random(SEED + 1)
    for m in first:
        revised.append({
            **m,
            "revision": 2,
            "vibration": round(rng2.gauss(2.1, 0.25), 4),
            "measured_at": m["measured_at"],  # 同一测量时刻的修订版
        })
    return [
        {"batch_id": "bf-b1-original", "measurements": first},
        {"batch_id": "bf-b2-revision", "measurements": revised},
    ]


def _s_invalid(rng):
    good = [_measure("F1", "C4", 50000 + i, 1, 0, rng) for i in range(20)]
    bad = [
        {"farm_id": "F1", "collector_id": "C4", "local_measure_id": "X1",
         "revision": 1, "measured_at": "2026-11-10T00:05:00+00:00",
         "turbine_id": "T01", "firmware_version": GOOD_FIRMWARE,
         "vibration": "not-a-number", "bearing_temp": 50.0,
         "operating_status": "generating"},  # 错误类型
        {"farm_id": "F1", "collector_id": "C4", "local_measure_id": "X2",
         "revision": -2, "measured_at": "2026-11-10T00:06:00+00:00",
         "turbine_id": "T01", "firmware_version": GOOD_FIRMWARE,
         "vibration": 2.0, "bearing_temp": 50.0,
         "operating_status": "generating"},  # 负修订号
        {"farm_id": "F1", "collector_id": "C4",
         "revision": 1, "measured_at": "2026-11-10T00:07:00+00:00",
         "turbine_id": "T01", "firmware_version": GOOD_FIRMWARE,
         "vibration": 2.0, "bearing_temp": 50.0,
         "operating_status": "generating"},  # 缺必填
        {"farm_id": "F1", "collector_id": "C4", "local_measure_id": "X4",
         "revision": 1, "measured_at": "2026-11-10 00:08:00",
         "turbine_id": "T01", "firmware_version": GOOD_FIRMWARE,
         "vibration": 2.0, "bearing_temp": 50.0,
         "operating_status": "generating"},  # 时间无时区
        {"farm_id": "F1", "collector_id": "C4", "local_measure_id": "X5",
         "revision": 1, "measured_at": "2026-11-10T00:09:00+00:00",
         "turbine_id": "T01", "firmware_version": GOOD_FIRMWARE,
         "vibration": 2.0, "bearing_temp": 50.0,
         "operating_status": "generating", "extra_field": 123},  # 未知字段
    ]
    return [{"batch_id": "inv-b1", "measurements": good + bad}]


def _s_insufficient(rng):
    """新风场 F9 每个窗口只有个位数样本，检测应判 undetermined。"""
    rows = []
    lid = 60000
    for w in range(10):
        for _ in range(5):
            lid += 1
            rows.append(_measure("F9", "C1", lid, 1, w, rng))
    return [{"batch_id": "low-b1", "measurements": rows}]


def _s_duplicate(rng):
    rows = [_measure("F1", "C5", 70000 + i, 1, 0, rng) for i in range(25)]
    return [
        {"batch_id": "dup-b1", "measurements": rows},
        # 同一个回放包原封不动重新提交
        {"batch_id": "dup-b1-replay", "measurements": [dict(r) for r in rows]},
    ]


def _s_disconnect(rng):
    """数据本身正常；断连由 verify 流程通过通知接收端的开关模拟，
    这里返回带标记的说明批次。"""
    rows = [_measure("F1", "C6", 80000 + i, 1, w % 10, rng)
            for i, w in enumerate(range(40))]
    return [{
        "batch_id": "drop-b1",
        "measurements": rows,
        "notification_hint": "verify 会先开启接收端断连开关再触发检测",
    }]


# ---------------------------------------------------------------- 中等规模

def medium_dataset(n: int = 100_000, n_rules: int = 20, seed: int = SEED):
    """生成约 n 条测量（拆 20 批）与 n_rules 条规则。

    3 个风场、多采集器/固件；F3 的一小部分采集器带固件振动偏移，
    使检测既有 pass 也有 fail，贴近真实巡检负载。
    """
    rng = random.Random(seed)
    farms = ("F1", "F2", "F3")
    firmwares = ("3.1.7", "3.1.8", "3.1.9")
    n_batches = 20
    per_batch = max(1, n // n_batches)
    batches: list[dict] = []
    lid = 0
    for b in range(n_batches):
        rows = []
        # 最后一批补齐余数，保证总量恰好为 n
        take = per_batch if b < n_batches - 1 else n - per_batch * (n_batches - 1)
        for _ in range(take):
            lid += 1
            farm = farms[lid % 3]
            collector = f"C{(lid % 9) + 1}"
            fw = firmwares[lid % 3]
            window_idx = (lid // 60) % 160  # 约 160 个窗口
            shift = 2.2 if (farm == "F3" and fw == "3.1.9"
                            and collector in ("C7", "C8")) else 0.0
            rows.append(_measure(
                farm, collector, lid, 1, window_idx, rng,
                firmware=fw, vibration_shift=shift,
                missing_temp=(lid % 97 == 0),
                missing_vib=(lid % 101 == 0),
            ))
        batches.append({"batch_id": f"medium-b{b + 1:02d}", "measurements": rows})

    rules = _build_rules(n_rules)
    return batches, rules


def _build_rules(n_rules: int) -> dict:
    base = [
        {"id": "vib_range", "name": "振动值范围",
         "expr": {"op": "range", "field": "vibration", "min": 0.0, "max": 8.0}},
        {"id": "temp_range", "name": "轴承温度范围",
         "when": {"op": "field_exists", "field": "bearing_temp"},
         "expr": {"op": "range", "field": "bearing_temp",
                  "min": 20.0, "max": 90.0}},
        {"id": "status_set", "name": "运行状态枚举",
         "expr": {"op": "in_set", "field": "operating_status",
                  "values": list(STATUSES) + ["maintenance"]}},
        {"id": "temp_vs_vib", "name": "发电态温度高于振动基准",
         "when": {"op": "compare", "left": {"field": "operating_status"},
                  "cmp": "==", "right": {"value": "generating"}},
         "expr": {"op": "compare",
                  "left": {"field": "bearing_temp"},
                  "cmp": ">", "right": {"field": "vibration"}}},
        {"id": "fw_known", "name": "固件版本集合",
         "expr": {"op": "in_set", "field": "firmware_version",
                  "values": ["3.1.7", "3.1.8", "3.1.9"]}},
    ]
    # 组合规则与更多范围规则凑满 n_rules
    combo = [
        {"id": "combo_all", "name": "振动与温度同时在范围",
         "expr": {"op": "all", "args": [
             {"op": "range", "field": "vibration", "min": 0.0, "max": 8.0},
             {"op": "range", "field": "bearing_temp", "min": 20.0, "max": 90.0}]}},
        {"id": "combo_any", "name": "传感器至少一个有值",
         "expr": {"op": "any", "args": [
             {"op": "field_exists", "field": "vibration"},
             {"op": "field_exists", "field": "bearing_temp"}]}},
        {"id": "combo_not", "name": "不能是 fault 状态",
         "expr": {"op": "not", "arg": {
             "op": "compare", "left": {"field": "operating_status"},
             "cmp": "==", "right": {"value": "fault"}}}},
    ]
    rules = list(base)
    i = 0
    while len(rules) < n_rules:
        lo = 0.2 * i
        rule = {
            "id": f"vib_band_{i:02d}",
            "name": f"振动分档检查 {i}",
            "expr": {"op": "range", "field": "vibration",
                     "min": lo, "max": 8.0 + 0.1 * i},
        }
        if rule["id"] not in {r["id"] for r in rules}:
            rules.append(rule)
        i += 1
        if len(rules) < n_rules and i % 3 == 0:
            for c in combo:
                if len(rules) < n_rules and c["id"] not in {
                        r["id"] for r in rules}:
                    rules.append(c)
    return {"name": f"medium-{n_rules}-rules", "rules": rules[:n_rules]}
