"""服务装配：HTTP API + 入库/检测 worker + 通知发送线程。"""
from __future__ import annotations

import threading

from .api import serve
from .config import Config
from .db import Database
from .notifier import Notifier
from .worker import Worker


class Service:
    def __init__(self, config: Config):
        self.cfg = config
        self.db = Database(config.db_path)
        # 重启恢复：崩溃时 running 的任务重新排队
        self.db.reset_stuck_jobs()
        self.httpd = serve(self.db, config)
        self.worker = Worker(self.db, config)
        self.notifier = Notifier(
            self.db, config.notify_url, config.notify_timeout,
            config.notify_max_attempts, config.poll_interval,
        )
        self._threads: list[threading.Thread] = []

    def start(self) -> "Service":
        t1 = threading.Thread(
            target=self.worker.run_forever, name="windwatch-worker", daemon=True)
        t2 = threading.Thread(
            target=self.notifier.run_forever, name="windwatch-notifier",
            daemon=True)
        t3 = threading.Thread(
            target=self.httpd.serve_forever, name="windwatch-http", daemon=True)
        for t in (t1, t2, t3):
            t.start()
            self._threads.append(t)
        return self

    def stop(self) -> None:
        self.worker.stop()
        self.notifier.stop()
        self.httpd.shutdown()
        self.httpd.server_close()

    @property
    def address(self) -> str:
        host, port = self.httpd.server_address[:2]
        return f"http://{host}:{port}"
