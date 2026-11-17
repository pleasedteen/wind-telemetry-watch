"""告警生命周期与通知持久化的单元级测试（直接驱动 AlertManager）。"""
from __future__ import annotations

import json

from windwatch.alerts import AlertManager
from windwatch.db import Database


def _db(tmp_path):
    return Database(str(tmp_path / "alerts.db"))


def _windows(n):
    # 每个窗口 10 分钟，生成 (start, end) ISO
    from datetime import datetime, timedelta, timezone
    base = datetime(2026, 11, 10, tzinfo=timezone.utc)
    out = []
    for i in range(n):
        s = base + timedelta(minutes=10 * i)
        out.append((s.isoformat(), (s + timedelta(minutes=10)).isoformat()))
    return out


def test_open_then_recover_after_two_ok(tmp_path):
    db = _db(tmp_path)
    conn = db.connect()
    mgr = AlertManager(conn, reminder_seconds=1800)
    ws = _windows(4)
    # 失败 -> 打开
    e1 = mgr.process_result(1, "vibration_psi", "F1", *ws[0],
                            "fail", 0.9, 0.25)
    assert e1["event"] == "opened"
    alert = conn.execute("SELECT * FROM alerts").fetchone()
    assert alert["status"] == "open"
    # 第一个正常窗口：还不恢复
    e2 = mgr.process_result(1, "vibration_psi", "F1", *ws[1],
                            "pass", 0.1, 0.25)
    assert e2["event"] == "ok_pending_recovery"
    assert conn.execute("SELECT status FROM alerts").fetchone()["status"] == "open"
    # 第二个连续正常窗口：恢复
    e3 = mgr.process_result(1, "vibration_psi", "F1", *ws[2],
                            "pass", 0.1, 0.25)
    assert e3["event"] == "recovered"
    row = conn.execute("SELECT * FROM alerts").fetchone()
    assert row["status"] == "recovered"
    assert row["recovered_window_end"] == ws[2][1]
    # 打开与恢复都各有一条通知
    kinds = {r["event_type"] for r in conn.execute(
        "SELECT event_type FROM notifications")}
    assert kinds == {"opened", "recovered"}


def test_undetermined_is_neutral(tmp_path):
    db = _db(tmp_path)
    conn = db.connect()
    mgr = AlertManager(conn, reminder_seconds=1800)
    ws = _windows(3)
    mgr.process_result(1, "status_tvd", "F1", *ws[0], "fail", 0.5, 0.2)
    mgr.process_result(1, "status_tvd", "F1", *ws[1],
                       "undetermined", None, 0.2)
    row = conn.execute("SELECT * FROM alerts").fetchone()
    assert row["consecutive_ok"] == 0  # 中性不累加正常计数
    assert row["status"] == "open"


def test_reminder_throttled_by_window_end_time(tmp_path):
    db = _db(tmp_path)
    conn = db.connect()
    mgr = AlertManager(conn, reminder_seconds=1800)
    ws = _windows(6)
    mgr.process_result(1, "vibration_psi", "F1", *ws[0], "fail", 0.9, 0.25)
    # 20 分钟后再失败：小于 30 分钟，抑制
    e = mgr.process_result(1, "vibration_psi", "F1", *ws[2],
                           "fail", 0.8, 0.25)
    assert e["event"] == "suppressed"
    assert "30" in e["suppressed_reason"] or "1800" in e["suppressed_reason"]
    # 40 分钟（>=4 个窗口）后失败：允许提醒
    e2 = mgr.process_result(1, "vibration_psi", "F1", *ws[4],
                            "fail", 0.7, 0.25)
    assert e2["event"] == "reminder"
    # 抑制不等于跳过：事件流里每个窗口都有记录
    events = conn.execute("SELECT event FROM alert_events").fetchall()
    assert len(events) == 3


def test_alert_and_notification_persist_together(tmp_path):
    db = _db(tmp_path)
    conn = db.connect()
    mgr = AlertManager(conn, reminder_seconds=1800)
    ws = _windows(1)
    mgr.process_result(1, "vibration_psi", "F1", *ws[0], "fail", 0.9, 0.25)
    # 通知以 pending 与告警同事务存在，等待后台投递
    n = conn.execute("SELECT * FROM notifications").fetchone()
    assert n["status"] == "pending"
    payload = json.loads(n["payload_json"])
    assert payload["item"] == "vibration_psi"
    assert n["idempotency_key"]  # 幂等键存在
