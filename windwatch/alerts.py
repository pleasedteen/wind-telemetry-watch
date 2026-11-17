"""告警生命周期与通知持久化。

生命周期（规则版本 × 检测项 × 分组唯一）：
- 失败 -> 打开告警并通知；
- 打开期间失败：连续正常计数清零；距上次提醒窗口结束 ≥30 分钟才再提醒
  （以被检测窗口的结束时间计，而不是挂钟时间）；
- 打开期间正常：累计，连续两个正常窗口后恢复并通知；
- undetermined 视为中性，不改变连续正常计数；
- 告警变化与待发送通知在同一事务内落库，重启后可继续投递；
  通知被抑制不等于检测被跳过——每窗口明细始终保存。
"""
from __future__ import annotations

import json
import uuid
from typing import Any

from .common import parse_instant, utc_now_iso


class AlertManager:
    def __init__(self, conn, reminder_seconds: int):
        self.conn = conn
        self.reminder_seconds = reminder_seconds

    def process_result(self, rule_version: int, item: str, group_key: str,
                       window_start: str, window_end: str, status: str,
                       score: float | None, threshold: float) -> dict:
        """根据一条窗口检测结果推进告警机；返回事件说明（供审计）。"""
        conn = self.conn
        row = conn.execute(
            "SELECT * FROM alerts WHERE rule_version=? AND item=? AND group_key=?",
            (rule_version, item, group_key),
        ).fetchone()
        now = utc_now_iso()
        event: dict[str, Any] = {
            "rule_version": rule_version,
            "item": item, "group_key": group_key,
            "window_start": window_start, "window_end": window_end,
            "status": status,
        }

        if status == "fail":
            if row is None:
                cur = conn.execute(
                    "INSERT INTO alerts(rule_version, item, group_key, status, "
                    "opened_window_end, consecutive_ok, created_at, updated_at) "
                    "VALUES(?,?,?,?,?,0,?,?) RETURNING *",
                    (rule_version, item, group_key, "open", window_end, now, now),
                ).fetchone()
                self._notify(cur, "opened", window_start, window_end,
                             score, threshold)
                event["event"] = "opened"
                event["alert_id"] = cur["id"]
                self._record_event(conn, event)
                return event

            if row["status"] == "recovered":
                # 恢复后再次失败：重新打开一条新的生命周期
                cur = conn.execute(
                    "UPDATE alerts SET status='open', opened_window_end=?, "
                    "recovered_window_end=NULL, consecutive_ok=0, "
                    "last_remind_window_end=NULL, updated_at=? "
                    "WHERE id=? RETURNING *",
                    (window_end, now, row["id"]),
                ).fetchone()
                self._notify(cur, "opened", window_start, window_end,
                             score, threshold)
                event["event"] = "reopened"
                event["alert_id"] = cur["id"]
                self._record_event(conn, event)
                return event

            # 仍打开：节流提醒
            conn.execute(
                "UPDATE alerts SET consecutive_ok=0, updated_at=? WHERE id=?",
                (now, row["id"]),
            )
            last = row["last_remind_window_end"] or row["opened_window_end"]
            gap = (parse_instant(window_end) - parse_instant(last)).total_seconds()
            event["alert_id"] = row["id"]
            if gap >= self.reminder_seconds:
                cur = conn.execute(
                    "UPDATE alerts SET last_remind_window_end=?, updated_at=? "
                    "WHERE id=? RETURNING *", (window_end, now, row["id"])
                ).fetchone()
                self._notify(cur, "reminder", window_start, window_end,
                             score, threshold)
                event["event"] = "reminder"
            else:
                event["event"] = "suppressed"
                event["suppressed_reason"] = (
                    f"距上次提醒 {gap:.0f}s < {self.reminder_seconds}s"
                    "（按窗口结束时间计算）"
                )
            self._record_event(conn, event)
            return event

        if status == "pass":
            if row is None or row["status"] == "recovered":
                event["event"] = "no_alert"
                self._record_event(conn, event)
                return event
            ok = row["consecutive_ok"] + 1
            event["alert_id"] = row["id"]
            if ok >= 2:
                cur = conn.execute(
                    "UPDATE alerts SET status='recovered', "
                    "recovered_window_end=?, consecutive_ok=?, updated_at=? "
                    "WHERE id=? RETURNING *",
                    (window_end, ok, now, row["id"]),
                ).fetchone()
                self._notify(cur, "recovered", window_start, window_end,
                             score, threshold)
                event["event"] = "recovered"
            else:
                conn.execute(
                    "UPDATE alerts SET consecutive_ok=?, updated_at=? WHERE id=?",
                    (ok, now, row["id"]),
                )
                event["event"] = "ok_pending_recovery"
                event["consecutive_ok"] = ok
            self._record_event(conn, event)
            return event

        # undetermined：中性，不推进也不回退告警机
        event["event"] = "undetermined_neutral"
        self._record_event(conn, event)
        return event

    def _record_event(self, conn, event: dict) -> None:
        conn.execute(
            "INSERT INTO alert_events(alert_id, rule_version, item, group_key, "
            "window_start, window_end, result_status, event, detail, created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (event.get("alert_id"), event["rule_version"], event["item"],
             event["group_key"], event["window_start"], event["window_end"],
             event["status"], event["event"],
             event.get("suppressed_reason"), utc_now_iso()),
        )

    def _notify(self, alert_row, event_type: str, window_start: str,
                window_end: str, score: float | None, threshold: float) -> None:
        key = (
            f"{alert_row['rule_version']}:{alert_row['item']}:"
            f"{alert_row['group_key']}:{event_type}:{window_end}"
        )
        payload = {
            "event_type": event_type,
            "alert_id": alert_row["id"],
            "rule_version": alert_row["rule_version"],
            "item": alert_row["item"],
            "group_key": alert_row["group_key"],
            "window_start": window_start,
            "window_end": window_end,
            "score": score,
            "threshold": threshold,
            "message": _human_message(alert_row, event_type, window_end,
                                      score, threshold),
            "event_uuid": str(uuid.uuid4()),
        }
        self.conn.execute(
            "INSERT OR IGNORE INTO notifications(idempotency_key, alert_id, "
            "event_type, payload_json, window_end, status, created_at) "
            "VALUES(?,?,?,?,?,'pending',?)",
            (key, alert_row["id"], event_type,
             json.dumps(payload, ensure_ascii=False), window_end, utc_now_iso()),
        )


def _human_message(alert_row, event_type: str, window_end: str,
                   score: float | None, threshold: float) -> str:
    verb = {
        "opened": "打开", "reopened": "重新打开",
        "reminder": "持续失败提醒", "recovered": "恢复",
    }[event_type]
    return (
        f"质量告警{verb}：分组 {alert_row['group_key']} 的检测项 "
        f"{alert_row['item']}（规则版本 {alert_row['rule_version']}），"
        f"窗口结束 {window_end}，得分 {score}，阈值 {threshold}"
    )
