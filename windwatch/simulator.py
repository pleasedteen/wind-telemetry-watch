"""项目自带模拟器（全部本地，不碰真实外站）。

``NotificationReceiver`` — 通知接收端：
- POST /notify      接受幂等键；header ``X-Drop-Connection: 1`` 或
                    payload ``_drop_connection=true`` 时先收下消息再断开连接；
- GET  /messages    查询最终消息列表（按 Idempotency-Key 去重计数）；
- POST /reset       清空内存消息。

``MockCollector`` — 模拟采集服务：
- GET /health, GET /scenarios, GET /batches/<scenario>
  返回固定种子的回放批次，供 verify 流程通过真实 HTTP 拉取。
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .scenarios import build_replay_batches, list_scenarios, medium_dataset


# ---------------------------------------------------------------- 通知接收端

class NotificationReceiver:
    def __init__(self, host: str = "127.0.0.1", port: int = 8090):
        self.host, self.port = host, port
        self._lock = threading.Lock()
        self.messages: dict[str, dict] = {}  # idempotency_key -> 最终消息
        self.deliveries: list[dict] = []     # 每次投递（含断连）
        self.drop_once = False               # 下一次投递先收下再断连
        self.httpd = None

    def start(self) -> "NotificationReceiver":
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "mock-notify/1.0"

            def log_message(self, *a):
                return

            def _json(self, obj, status=200):
                body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type",
                                 "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                path = urlparse(self.path).path
                if path == "/reset":
                    with receiver._lock:
                        receiver.messages.clear()
                        receiver.deliveries.clear()
                        receiver.drop_once = False
                    self._json({"reset": True})
                    return
                if path == "/drop-once":
                    with receiver._lock:
                        receiver.drop_once = True
                    self._json({"drop_once": True})
                    return
                if path != "/notify":
                    self._json({"error": "not found"}, 404)
                    return
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length)
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    self._json({"error": "bad json"}, 400)
                    return
                key = self.headers.get("Idempotency-Key") or \
                    f"noid-{len(receiver.deliveries)}"
                with receiver._lock:
                    drop = receiver.drop_once
                    receiver.drop_once = False
                    drop = drop or self.headers.get(
                        "X-Drop-Connection") == "1" or \
                        bool(payload.pop("_drop_connection", False))

                with receiver._lock:
                    receiver.deliveries.append({
                        "idempotency_key": key,
                        "payload": payload,
                        "dropped": drop,
                    })
                    # “先收下”：断连也已进入消息存储；重试幂等覆盖
                    receiver.messages[key] = payload

                if drop:
                    # 模拟 TCP 中断：不回响应直接关闭连接
                    self.close_connection = True
                    try:
                        self.wfile.flush()
                    except OSError:
                        pass
                    return
                self._json({"accepted": True, "idempotency_key": key})

            def do_GET(self):
                if urlparse(self.path).path != "/messages":
                    self._json({"error": "not found"}, 404)
                    return
                with receiver._lock:
                    self._json({
                        "count": len(receiver.messages),
                        "delivery_attempts": len(receiver.deliveries),
                        "messages": [
                            {"idempotency_key": k, **v}
                            for k, v in sorted(receiver.messages.items())
                        ],
                        "deliveries": list(receiver.deliveries),
                    })

        self.httpd = ThreadingHTTPServer((self.host, self.port), Handler)
        thread = threading.Thread(
            target=self.httpd.serve_forever, name="mock-notify", daemon=True)
        thread.start()
        return self

    def stop(self) -> None:
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()


# ---------------------------------------------------------------- 模拟采集服务

class MockCollector:
    def __init__(self, host: str = "127.0.0.1", port: int = 8091,
                 seed: int = 20261117):
        self.host, self.port, self.seed = host, port, seed
        self.httpd = None

    def start(self) -> "MockCollector":
        collector = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "mock-collector/1.0"

            def log_message(self, *a):
                return

            def _json(self, obj, status=200):
                body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type",
                                 "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                path = urlparse(self.path).path
                if path == "/health":
                    self._json({"status": "ok"})
                elif path == "/scenarios":
                    self._json({"scenarios": list_scenarios()})
                elif path.startswith("/batches/"):
                    scenario = path.rsplit("/", 1)[-1]
                    try:
                        batches = build_replay_batches(
                            scenario, seed=collector.seed)
                    except KeyError as exc:
                        self._json({"error": str(exc)}, 404)
                        return
                    self._json({"scenario": scenario, "batches": batches})
                elif path == "/medium-dataset":
                    from urllib.parse import parse_qs
                    q = parse_qs(urlparse(self.path).query)
                    n = int(q.get("n", ["100000"])[0])
                    rules = int(q.get("rules", ["20"])[0])
                    batches, rule_payload = medium_dataset(
                        n, rules, seed=collector.seed)
                    self._json({
                        "measurements": n, "batches": batches,
                        "rules": rule_payload,
                    })
                else:
                    self._json({"error": "not found"}, 404)

        self.httpd = ThreadingHTTPServer((self.host, self.port), Handler)
        thread = threading.Thread(
            target=self.httpd.serve_forever, name="mock-collector", daemon=True)
        thread.start()
        return self

    def stop(self) -> None:
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
