"""pytest 公共夹具：临时数据库 + 真实 HTTP 服务 + 本地通知接收端。

所有测试都走真实网络栈（http.server 线程）、真实 SQLite 文件、
真实后台 worker/notifier 线程，数据库位于临时目录，测试间完全隔离。
"""
from __future__ import annotations

import socket

import pytest

from windwatch.app import Service
from windwatch.client import Client
from windwatch.config import Config
from windwatch.simulator import NotificationReceiver


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture()
def receiver():
    rec = NotificationReceiver("127.0.0.1", free_port()).start()
    yield rec
    rec.stop()


@pytest.fixture()
def service(receiver, tmp_path):
    cfg = Config(
        db_path=str(tmp_path / "test.db"),
        host="127.0.0.1", port=free_port(),
        notify_url=f"http://127.0.0.1:{receiver.port}/notify",
        poll_interval=0.01, notify_timeout=2.0,
    )
    svc = Service(cfg).start()
    svc.cfg = cfg
    yield svc
    svc.stop()


@pytest.fixture()
def client(service):
    return Client(service.address, timeout=15)


def wait_until(predicate, timeout: float = 15.0, interval: float = 0.02):
    import time
    deadline = time.perf_counter() + timeout
    last = None
    while time.perf_counter() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    return last


@pytest.fixture()
def wait():
    return wait_until
