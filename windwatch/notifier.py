"""通知后台发送：HTTP POST + 幂等键 + 失败重试。

发送与检测解耦：检测任务只把通知以 pending 落库；
后台线程循环投递，进程重启后 pending/failed 通知会继续投递。
模拟接收端接受幂等键，支持“先收下再断连”，因此重试对它是安全的。
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Callable

from .common import utc_now_iso


class Notifier:
    def __init__(self, db, url: str, timeout: float, max_attempts: int,
                 poll_interval: float,
                 sleep: Callable[[float], None] = time.sleep):
        self.db = db
        self.url = url
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.poll_interval = poll_interval
        self._sleep = sleep
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    def run_forever(self) -> None:
        while not self._stop:
            try:
                worked = self.deliver_pending()
            except Exception:  # pragma: no cover - 守护线程不能因异常退出
                worked = False
            if not worked:
                self._sleep(self.poll_interval)

    def deliver_pending(self, limit: int = 100) -> bool:
        """投递一批待发通知；有处理动作返回 True。"""
        conn = self.db.connect()
        rows = conn.execute(
            "SELECT * FROM notifications WHERE status IN ('pending','failed') "
            "AND attempts < ? ORDER BY id LIMIT ?",
            (self.max_attempts, limit),
        ).fetchall()
        if not rows:
            return False
        for row in rows:
            self._deliver_one(row)
        return True

    def wait_until_drained(self, timeout: float = 10.0) -> bool:
        """同步等待所有可投递通知进入终态（测试用，避免后台轮询竞态）。"""
        deadline = time.perf_counter() + timeout
        while time.perf_counter() < deadline:
            conn = self.db.connect()
            pending = conn.execute(
                "SELECT COUNT(*) AS c FROM notifications "
                "WHERE status IN ('pending','failed') AND attempts < ?",
                (self.max_attempts,),
            ).fetchone()["c"]
            if pending == 0:
                return True
            self.deliver_pending()
            time.sleep(self.poll_interval)
        return False

    def _deliver_one(self, row) -> None:
        conn = self.db.connect()
        attempts = row["attempts"] + 1
        payload = json.loads(row["payload_json"])
        req = urllib.request.Request(
            self.url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "Idempotency-Key": row["idempotency_key"],
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                code = resp.getcode()
            if 200 <= code < 300:
                conn.execute(
                    "UPDATE notifications SET status='sent', attempts=?, "
                    "sent_at=?, last_error=NULL WHERE id=?",
                    (attempts, utc_now_iso(), row["id"]),
                )
            else:  # pragma: no cover - urllib 非 2xx 会走异常分支
                raise RuntimeError(f"接收端返回 {code}")
        except (urllib.error.URLError, urllib.error.HTTPError,
                TimeoutError, OSError, RuntimeError) as exc:
            status = "failed" if attempts >= self.max_attempts else "pending"
            conn.execute(
                "UPDATE notifications SET status=?, attempts=?, last_error=? "
                "WHERE id=?",
                (status, attempts, f"{type(exc).__name__}: {exc}", row["id"]),
            )
