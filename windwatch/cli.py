"""命令行入口：服务启动与模拟器启动。"""
from __future__ import annotations

import argparse
import signal
import time

from .app import Service
from .config import Config
from .simulator import MockCollector, NotificationReceiver


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="windwatch",
                                     description="风电场遥测质量监控服务")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--db", default=None, help="SQLite 数据库路径")
    parser.add_argument("--notify-url", default=None)
    parser.add_argument("--with-simulators", action="store_true",
                        help="同时启动本地模拟采集服务与通知接收端")
    parser.add_argument("--notify-port", type=int, default=8090)
    parser.add_argument("--collector-port", type=int, default=8091)
    args = parser.parse_args(argv)

    overrides = {}
    if args.host:
        overrides["host"] = args.host
    if args.port is not None:
        overrides["port"] = args.port
    if args.db:
        overrides["db_path"] = args.db
    if args.notify_url:
        overrides["notify_url"] = args.notify_url
    cfg = Config(**overrides) if overrides else Config()

    receiver = collector = None
    if args.with_simulators:
        receiver = NotificationReceiver(cfg.host, args.notify_port).start()
        collector = MockCollector(cfg.host, args.collector_port).start()
        if not args.notify_url:
            cfg = Config(**{**overrides,
                            "notify_url": f"http://{cfg.host}:{args.notify_port}/notify"})

    service = Service(cfg).start()
    print(f"windwatch 已启动: {service.address}")
    print(f"  API 文档/健康检查: {service.address}/health")
    if receiver:
        print(f"  模拟通知接收端: http://{cfg.host}:{args.notify_port}/messages")
    if collector:
        print(f"  模拟采集服务:   http://{cfg.host}:{args.collector_port}/scenarios")
    print("按 Ctrl+C 停止。")

    stop = {"flag": False}

    def _handle(sig, frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    try:
        while not stop["flag"]:
            time.sleep(0.2)
    finally:
        service.stop()
        if receiver:
            receiver.stop()
        if collector:
            collector.stop()
        print("已停止。")


if __name__ == "__main__":
    main()
