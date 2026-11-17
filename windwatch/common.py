"""公共工具：UTC 时间窗口、测量行校验、常量。"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any

WINDOW_MINUTES = 10

# 允许出现在测量行 / 规则表达式中的字段
MEASUREMENT_FIELDS = (
    "farm_id",
    "collector_id",
    "local_measure_id",
    "revision",
    "measured_at",
    "turbine_id",
    "firmware_version",
    "vibration",
    "bearing_temp",
    "operating_status",
)
REQUIRED_FIELDS = (
    "farm_id",
    "collector_id",
    "local_measure_id",
    "revision",
    "measured_at",
    "turbine_id",
    "firmware_version",
)
# 数值字段允许 None（缺失不参与分布）；operating_status 也允许 None
NUMERIC_FIELDS = ("vibration", "bearing_temp")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_instant(value: Any) -> datetime:
    """解析带时区的测量时间，统一转为 UTC。"""
    if not isinstance(value, str):
        raise ValueError("measured_at 必须是 ISO-8601 字符串")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise ValueError("measured_at 必须带时区信息")
    return dt.astimezone(timezone.utc)


def window_start_for(epoch: float, minutes: int = WINDOW_MINUTES) -> datetime:
    """UTC 十分钟整点窗口起点（左闭右开）。"""
    bucket = math.floor(epoch / (minutes * 60)) * (minutes * 60)
    return datetime.fromtimestamp(bucket, tz=timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def iter_windows(start: datetime, end: datetime, minutes: int = WINDOW_MINUTES):
    """生成 [start, end) 内对齐整点的所有窗口 (w_start, w_end)。"""
    step = timedelta(minutes=minutes)
    cur = window_start_for(start.timestamp(), minutes)
    while cur < end:
        if cur >= start:
            yield cur, cur + step
        cur += step


def row_hash(row: dict) -> str:
    """测量行内容指纹，用于同一任务内重复回放包去重。"""
    payload = json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_measurement(row: Any) -> tuple[dict | None, str | None]:
    """校验一条测量。返回 (规范化行, None) 或 (None, 隔离原因)。

    异常行进入隔离区但不影响同批合法行继续处理。
    """
    if not isinstance(row, dict):
        return None, "行不是 JSON 对象"

    unknown = sorted(set(row) - set(MEASUREMENT_FIELDS))
    if unknown:
        return None, f"未知字段: {', '.join(unknown)}"

    for f in REQUIRED_FIELDS:
        if f not in row or row[f] is None or row[f] == "":
            return None, f"必填字段缺失: {f}"

    out: dict[str, Any] = {}
    for f in ("farm_id", "collector_id", "local_measure_id", "turbine_id",
              "firmware_version"):
        v = row[f]
        if not isinstance(v, str):
            return None, f"字段 {f} 必须是字符串"
        out[f] = v

    rev = row["revision"]
    if isinstance(rev, bool) or not isinstance(rev, int) or rev < 0:
        return None, "revision 必须是非负整数"
    out["revision"] = rev

    try:
        dt = parse_instant(row["measured_at"])
    except (ValueError, TypeError) as exc:
        return None, f"measured_at 无法解析: {exc}"
    out["measured_at"] = iso(dt)
    out["measured_epoch"] = dt.timestamp()

    for f in NUMERIC_FIELDS:
        v = row.get(f)
        if v is None:
            out[f] = None
            continue
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return None, f"字段 {f} 必须是数值或 null"
        if not math.isfinite(float(v)):
            return None, f"字段 {f} 不是有限数值"
        out[f] = float(v)

    status = row.get("operating_status")
    if status is None:
        out["operating_status"] = None
    elif not isinstance(status, str) or not status.strip():
        return None, "operating_status 必须是非空字符串或 null"
    else:
        out["operating_status"] = status

    return out, None
