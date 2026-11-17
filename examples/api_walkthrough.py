"""API 走读示例：从零启动本地服务，走一遍 提交→检测→告警→复核。

运行方式（在仓库根目录）：
    python examples/api_walkthrough.py

脚本会使用临时目录启动自带的通知接收端与主服务，全部走真实 HTTP，
结束后自动清理，不依赖任何外部站点。
"""
from __future__ import annotations

import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 允许直接 `python examples/api_walkthrough.py` 运行（把仓库根目录加入路径）
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from windwatch.app import Service
from windwatch.client import Client
from windwatch.config import Config
from windwatch.scenarios import BASE_EPOCH, build_replay_batches
from windwatch.simulator import NotificationReceiver

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _ports import free_port  # 本地辅助，无第三方依赖


def main() -> None:
    work = Path(tempfile.mkdtemp(prefix="windwatch-example-"))
    receiver = NotificationReceiver("127.0.0.1", free_port()).start()
    cfg = Config(
        db_path=str(work / "demo.db"), host="127.0.0.1", port=free_port(),
        notify_url=f"http://127.0.0.1:{receiver.port}/notify",
        poll_interval=0.02,
    )
    service = Service(cfg).start()
    client = Client(service.address)
    try:
        print("== 1. 提交固件振动偏移场景的批次 ==")
        task_ids = []
        for batch in build_replay_batches("firmware_vibration_shift"):
            task_ids.append(client.submit_batch(batch)["task_id"])
        _wait(lambda: all(client.ingest_task(t)["status"] == "done"
                          for t in task_ids))
        print("   入库任务:", [client.ingest_task(t) for t in task_ids])

        print("\n== 2. 提交规则版本（结构化树） ==")
        rv = client.create_rules({
            "name": "demo",
            "rules": [{
                "id": "vib_range", "name": "振动范围",
                "expr": {"op": "range", "field": "vibration",
                         "min": 0.0, "max": 6.0},
            }],
        })["rule_version"]
        print("   规则版本号（不可变）:", rv)

        print("\n== 3. 发起实时检测（风场+固件分组） ==")
        job = client.run_detection({
            "farm_id": "F1", "group_mode": "farm_firmware",
            "rule_version": rv,
            "items": {"vibration_psi": {}, "temperature_psi": {},
                      "status_tvd": {}, "rule:vib_range": {}},
        })
        _wait(lambda: client.job(job["job_id"])["status"] == "done")
        results = client.results(job["job_id"], with_samples=True)["results"]
        fails = [r for r in results if r["status"] == "fail"]
        print(f"   窗口明细 {len(results)} 条，失败 {len(fails)} 条")
        for r in fails[:3]:
            print(f"   - {r['item']} @ {r['group_key']} "
                  f"{r['window_start'][11:16]} score={r['score']}")

        print("\n== 4. 查看告警与通知 ==")
        alerts = client.alerts()["alerts"]
        print("   告警:", [(a["item"], a["group_key"], a["status"])
                          for a in alerts])
        if alerts:
            detail = client.alert_detail(alerts[0]["id"])
            print("   首条告警事件流:",
                  [e["event"] for e in detail["event_stream"]])
        _wait(lambda: all(n["status"] == "sent"
                          for n in client.notifications()["notifications"]))
        print("   通知:", [(n["event_type"], n["status"])
                          for n in client.notifications()["notifications"]])

        print("\n== 5. 发起历史复核（严格规则版本，不发通知） ==")
        strict = client.create_rules({
            "name": "strict",
            "rules": [{"id": "vib_range", "name": "严格振动",
                       "expr": {"op": "range", "field": "vibration",
                                "min": 1.5, "max": 2.3}}],
        })["rule_version"]
        start = datetime.fromtimestamp(BASE_EPOCH, tz=timezone.utc)
        review = client.run_review({
            "farm_id": "F1", "group_mode": "farm_firmware",
            "rule_version": strict,
            "window_start": start.isoformat(),
            "window_end": (start + timedelta(minutes=120)).isoformat(),
            "items": {"rule:vib_range": {"threshold": 0.0}},
        })
        _wait(lambda: client.job(review["job_id"])["status"] == "done")
        report = client.review_report(review["job_id"])["report"]
        print("   问题三态计数:",
              {k: len(v) for k, v in report["issues"].items()})
        md = client.get(f"/api/reviews/{review['job_id']}?format=markdown")
        out = work / "review.md"
        out.write_text(md, encoding="utf-8")
        print("   Markdown 报告已写入:", out)
        print("\n全部流程完成。")
    finally:
        service.stop()
        receiver.stop()


def _wait(predicate, timeout: float = 15.0) -> None:
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise TimeoutError("等待后台任务超时")


if __name__ == "__main__":
    main()
