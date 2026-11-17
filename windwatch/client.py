"""极简 HTTP 客户端（标准库 urllib），供 verify、测试与示例使用。"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any


class ClientError(RuntimeError):
    def __init__(self, status: int, body: str):
        super().__init__(f"HTTP {status}: {body}")
        self.status = status
        self.body = body


class Client:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def request(self, method: str, path: str,
                payload: Any = None, headers: dict | None = None):
        url = self.base + path
        data = None
        hdrs = {"Accept": "application/json"}
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            hdrs["Content-Type"] = "application/json; charset=utf-8"
        if headers:
            hdrs.update(headers)
        req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8")
                ctype = resp.headers.get("Content-Type", "")
                if "json" in ctype:
                    return json.loads(raw)
                return raw
        except urllib.error.HTTPError as exc:
            raise ClientError(exc.code, exc.read().decode("utf-8")) from exc

    def get(self, path: str):
        return self.request("GET", path)

    def post(self, path: str, payload: Any, headers: dict | None = None):
        return self.request("POST", path, payload, headers)

    # ---- 业务快捷方法 ----
    def submit_batch(self, batch: dict) -> dict:
        return self.post("/api/batches", batch)

    def ingest_task(self, task_id: int) -> dict:
        return self.get(f"/api/ingest-tasks/{task_id}")

    def create_rules(self, payload: dict) -> dict:
        return self.post("/api/rules", payload)

    def run_detection(self, payload: dict) -> dict:
        return self.post("/api/detections", payload)

    def run_review(self, payload: dict) -> dict:
        return self.post("/api/reviews", payload)

    def job(self, job_id: int) -> dict:
        return self.get(f"/api/detection-jobs/{job_id}")

    def results(self, job_id: int, with_samples: bool = False) -> dict:
        suffix = "?with_samples=1" if with_samples else ""
        return self.get(f"/api/detection-results/{job_id}{suffix}")

    def alerts(self, status: str | None = None) -> dict:
        return self.get("/api/alerts" + (f"?status={status}" if status else ""))

    def alert_detail(self, alert_id: int) -> dict:
        return self.get(f"/api/alerts/{alert_id}")

    def notifications(self, status: str | None = None) -> dict:
        return self.get(
            "/api/notifications" + (f"?status={status}" if status else ""))

    def review_report(self, job_id: int) -> dict:
        return self.get(f"/api/reviews/{job_id}")
