"""服务配置。所有项均可通过环境变量覆盖，便于测试与多实例部署。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


@dataclass(frozen=True)
class Config:
    db_path: str = os.environ.get("WINDWATCH_DB", "windwatch.db")
    host: str = os.environ.get("WINDWATCH_HOST", "127.0.0.1")
    port: int = _env_int("WINDWATCH_PORT", 8080)

    # 通知接收端（项目自带模拟器，默认指向本地）
    notify_url: str = os.environ.get(
        "WINDWATCH_NOTIFY_URL", "http://127.0.0.1:8090/notify"
    )
    notify_timeout: float = _env_float("WINDWATCH_NOTIFY_TIMEOUT", 3.0)
    notify_max_attempts: int = _env_int("WINDWATCH_NOTIFY_MAX_ATTEMPTS", 8)

    # 持续失败的提醒节流：每 30 分钟最多一次，按被检测窗口结束时间计
    notify_reminder_seconds: int = _env_int(
        "WINDWATCH_REMINDER_SECONDS", 30 * 60
    )

    # 后台 worker 轮询间隔
    poll_interval: float = _env_float("WINDWATCH_POLL_INTERVAL", 0.05)

    # PSI / TVD / 规则默认阈值（可在提交检测任务时覆盖）
    psi_threshold: float = _env_float("WINDWATCH_PSI_THRESHOLD", 0.25)
    tvd_threshold: float = _env_float("WINDWATCH_TVD_THRESHOLD", 0.2)
    rule_threshold: float = _env_float("WINDWATCH_RULE_THRESHOLD", 0.05)

    min_samples: int = _env_int("WINDWATCH_MIN_SAMPLES", 30)
    baseline_windows: int = _env_int("WINDWATCH_BASELINE_WINDOWS", 6)
    window_minutes: int = _env_int("WINDWATCH_WINDOW_MINUTES", 10)

    # 每条规则/检测项在报告中保留的违反样本上限
    max_violation_samples: int = _env_int("WINDWATCH_MAX_VIOLATION_SAMPLES", 10)
