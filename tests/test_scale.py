"""中等规模性能测试：十万条测量、二十条规则。

检测（不含入库）必须在 30 秒内完成；用独立临时库，避免与功能测试耦合。
"""
from __future__ import annotations

import time

from windwatch.app import Service
from windwatch.client import Client
from windwatch.config import Config
from windwatch.scenarios import medium_dataset

from .conftest import free_port


def test_100k_ingestion_and_detection_performance(tmp_path, receiver):
    cfg = Config(
        db_path=str(tmp_path / "scale.db"),
        host="127.0.0.1", port=free_port(),
        notify_url=f"http://127.0.0.1:{receiver.port}/notify",
        poll_interval=0.01,
    )
    svc = Service(cfg).start()
    client = Client(svc.address, timeout=60)
    try:
        batches, rule_payload = medium_dataset(100_000, 20)
        t0 = time.perf_counter()
        ids = [client.submit_batch(b)["task_id"] for b in batches]
        deadline = time.perf_counter() + 60
        while time.perf_counter() < deadline and not all(
                client.ingest_task(t)["status"] == "done" for t in ids):
            time.sleep(0.05)
        ingest_elapsed = time.perf_counter() - t0
        total = sum(client.ingest_task(t)["received_count"] for t in ids)
        assert total == 100_000
        assert ingest_elapsed < 60

        rv = client.create_rules(rule_payload)["rule_version"]
        items = {"vibration_psi": {}, "temperature_psi": {},
                 "status_tvd": {}}
        for r in rule_payload["rules"]:
            items[f"rule:{r['id']}"] = {}
        t0 = time.perf_counter()
        job = client.run_detection({
            "farm_id": "F1", "group_mode": "farm_firmware",
            "rule_version": rv, "items": items,
        })
        deadline = time.perf_counter() + 35
        while time.perf_counter() < deadline and \
                client.job(job["job_id"])["status"] != "done":
            time.sleep(0.05)
        elapsed = time.perf_counter() - t0
        assert client.job(job["job_id"])["status"] == "done"
        assert client.results(job["job_id"])["results"]
        assert elapsed < 30, f"规模检测耗时 {elapsed:.1f}s 超过 30s 预算"
    finally:
        svc.stop()
