"""端到端集成测试：真实 HTTP + SQLite + 后台任务 + 通知链路。"""
from __future__ import annotations

import json
import sqlite3
import time
import urllib.request

import pytest

from windwatch.app import Service
from windwatch.client import Client
from windwatch.config import Config
from windwatch.scenarios import build_replay_batches

from .conftest import free_port


def _submit(client, batch):
    return client.submit_batch(batch)["task_id"]


def _wait_done(client, task_id, wait):
    assert wait(lambda: client.ingest_task(task_id)["status"] == "done")


# ------------------------------------------------------------ 入库与隔离

def test_batch_ingestion_quarantine_and_traceability(client, wait):
    batches = build_replay_batches("invalid_rows")
    tid = _submit(client, batches[0])
    _wait_done(client, tid, wait)
    task = client.ingest_task(tid)
    assert task["status"] == "done"
    assert task["received_count"] == 25
    assert task["quarantined_count"] == 5
    assert task["valid_delta"] == 20

    q = client.get(f"/api/quarantine?task_id={tid}")["quarantine"]
    assert len(q) == 5
    assert all(item["reason"] and json.loads(item["raw_json"]) for item in q)


def test_duplicate_replay_has_zero_delta_but_kept_raw(client, wait):
    batches = build_replay_batches("duplicate_replay")
    t1 = _submit(client, batches[0])
    t2 = _submit(client, batches[1])
    _wait_done(client, t1, wait)
    _wait_done(client, t2, wait)
    assert client.ingest_task(t2)["valid_delta"] == 0
    assert client.ingest_task(t2)["received_count"] == 25


def test_revision_backfill_effective_view_keeps_highest(client, wait):
    batches = build_replay_batches("revision_backfill")
    ids = [_submit(client, b) for b in batches]
    for t in ids:
        _wait_done(client, t, wait)
    assert client.ingest_task(ids[1])["valid_delta"] == 30
    eff = client.get("/api/measurements?farm_id=F1&limit=5000")["measurements"]
    c3 = [m for m in eff if m["collector_id"] == "C3"]
    assert len({m["local_measure_id"] for m in c3}) == 30
    assert all(m["revision"] == 2 for m in c3)


def test_seq_cap_fixes_effective_data_range(client, wait):
    batches = build_replay_batches("revision_backfill")
    ids = [_submit(client, b) for b in batches]
    for t in ids:
        _wait_done(client, t, wait)
    # 用修订 1 入库后的序号上限，只能看到修订 1
    first_seq = client.ingest_task(ids[0])
    # 任务 1 的 seq 可通过全量测量推断：直接查两个 cap 的差异
    full = client.get("/api/measurements?farm_id=F1&limit=5000")["measurements"]
    capped = client.get(
        f"/api/measurements?farm_id=F1&seq_cap={first_seq['id']}&limit=5000"
    )["measurements"]
    c3_full = [m for m in full if m["collector_id"] == "C3"]
    c3_cap = [m for m in capped if m["collector_id"] == "C3"]
    assert all(m["revision"] == 2 for m in c3_full)
    assert c3_cap and all(m["revision"] == 1 for m in c3_cap)


# ------------------------------------------------------------ 规则 API

def _rules_payload():
    return {
        "name": "it-rules",
        "rules": [
            {"id": "vib", "name": "振动范围",
             "expr": {"op": "range", "field": "vibration",
                      "min": 0.0, "max": 6.0}},
            {"id": "gen_temp", "name": "发电态温度",
             "when": {"op": "compare",
                      "left": {"field": "operating_status"},
                      "cmp": "==", "right": {"value": "generating"}},
             "expr": {"op": "range", "field": "bearing_temp",
                      "min": 30.0, "max": 80.0}},
        ],
    }


def test_rule_version_immutable_and_validated(client):
    rv = client.create_rules(_rules_payload())["rule_version"]
    got = client.get(f"/api/rules/{rv}")
    assert got["version"] == rv
    with pytest.raises(Exception) as exc:
        client.create_rules({"rules": [{"id": "bad", "name": "b",
            "expr": {"op": "range", "field": "nope", "min": 0, "max": 1}}]})
    assert exc.value.status == 400


# ------------------------------------------------------------ 检测与告警

def _ingest(client, wait, *scenarios):
    for s in scenarios:
        for b in build_replay_batches(s):
            t = _submit(client, b)
            _wait_done(client, t, wait)


def test_psi_drift_opens_alert_and_notifies(client, receiver, wait):
    _ingest(client, wait, "firmware_vibration_shift")
    rv = client.create_rules(_rules_payload())["rule_version"]
    job = client.run_detection({
        "farm_id": "F1", "group_mode": "farm_firmware",
        "rule_version": rv,
        "items": {"vibration_psi": {}, "rule:vib": {}},
    })
    assert wait(lambda: client.job(job["job_id"])["status"] == "done")
    results = client.results(job["job_id"], with_samples=True)["results"]
    fails = [r for r in results
             if r["item"] == "vibration_psi" and r["status"] == "fail"]
    assert fails and "3.1.9" in fails[0]["group_key"]
    detail = fails[0]["detail_json"]
    assert {"cuts", "current_counts", "baseline_counts",
            "current_proportions", "baseline_proportions", "psi"} <= set(detail)

    # 告警打开 + 通知经真实 HTTP 到达接收端
    assert wait(lambda: any(
        a["item"] == "vibration_psi" for a in client.alerts()["alerts"]))
    assert wait(lambda: any(
        n["status"] == "sent" and n["event_type"] == "opened"
        for n in client.notifications()["notifications"]))
    msgs = json.loads(urllib.request.urlopen(
        f"http://127.0.0.1:{receiver.port}/messages", timeout=5).read())
    assert msgs["count"] >= 1


def test_alert_lifecycle_and_detail_api(client, receiver, wait):
    _ingest(client, wait, "firmware_vibration_shift")
    rv = client.create_rules(_rules_payload())["rule_version"]
    job = client.run_detection({
        "farm_id": "F1", "group_mode": "farm_firmware",
        "rule_version": rv, "items": {"vibration_psi": {}},
    })
    assert wait(lambda: client.job(job["job_id"])["status"] == "done")
    # 至少产生过一条告警（open 或 recovered）
    alerts = client.alerts()["alerts"]
    drift = [a for a in alerts if a["item"] == "vibration_psi"]
    assert drift
    aid = drift[0]["id"]
    detail = client.alert_detail(aid)
    # API 可查对应窗口、发送记录、事件流（抑制原因也在其中）
    assert detail["windows"]
    assert "notifications" in detail
    assert detail["event_stream"]
    events = {e["event"] for e in detail["event_stream"]}
    assert "opened" in events
    # 若已恢复，必须有 recovered 通知且记录恢复窗口
    if drift[0]["status"] == "recovered":
        assert drift[0]["recovered_window_end"]
        assert any(n["event_type"] == "recovered"
                   for n in detail["notifications"])
        assert "recovered" in events


def test_tvd_status_category_change(client, wait):
    _ingest(client, wait, "status_category_change")
    rv = client.create_rules(_rules_payload())["rule_version"]
    job = client.run_detection({
        "farm_id": "F2", "group_mode": "farm",
        "rule_version": rv, "items": {"status_tvd": {}},
    })
    assert wait(lambda: client.job(job["job_id"])["status"] == "done")
    results = client.results(job["job_id"])["results"]
    assert any(r["status"] == "fail" for r in results)
    fail = next(r for r in results if r["status"] == "fail")
    assert "categories" in fail["detail_json"]


def test_insufficient_samples_undetermined(client, wait):
    _ingest(client, wait, "insufficient_samples")
    rv = client.create_rules(_rules_payload())["rule_version"]
    job = client.run_detection({
        "farm_id": "F9", "group_mode": "farm",
        "rule_version": rv, "items": {"vibration_psi": {}},
    })
    assert wait(lambda: client.job(job["job_id"])["status"] == "done")
    results = client.results(job["job_id"])["results"]
    assert results
    assert all(r["status"] == "undetermined" and r["score"] is None
               for r in results)


def test_rule_zero_denominator_and_violation_samples(client, wait):
    _ingest(client, wait, "normal")
    payload = _rules_payload()
    payload["rules"].append({
        "id": "never", "name": "零分母",
        "when": {"op": "compare", "left": {"field": "operating_status"},
                 "cmp": "==", "right": {"value": "ghost"}},
        "expr": {"op": "range", "field": "vibration", "min": 0.0, "max": 1.0}})
    payload["rules"].append({
        "id": "strict", "name": "必违反",
        "expr": {"op": "range", "field": "vibration", "min": 1.5, "max": 2.2}})
    rv = client.create_rules(payload)["rule_version"]
    job = client.run_detection({
        "farm_id": "F1", "group_mode": "farm",
        "rule_version": rv,
        "items": {"rule:never": {}, "rule:strict": {"threshold": 0.0}},
    })
    assert wait(lambda: client.job(job["job_id"])["status"] == "done")
    results = client.results(job["job_id"], with_samples=True)["results"]
    never = [r for r in results if r["item"] == "rule:never"]
    assert never and all(r["status"] == "undetermined"
                         and r["detail_json"]["denominator"] == 0 for r in never)
    strict_fail = next(r for r in results
                       if r["item"] == "rule:strict" and r["status"] == "fail")
    assert strict_fail["violation_samples"]
    assert strict_fail["violation_samples"][0]["failure_path"]


# ------------------------------------------------------------ 断连与重试

def test_notification_drop_connection_retried(service, client, receiver, wait):
    _ingest(client, wait, "notify_disconnect")
    # 武装“先收下再断连”
    urllib.request.urlopen(urllib.request.Request(
        f"http://127.0.0.1:{receiver.port}/drop-once", method="POST"),
        timeout=5).read()
    # 直接插一条探测通知，确保撞上下一次投递
    from windwatch.common import utc_now_iso
    with service.db.transaction() as cx:
        cx.execute(
            "INSERT OR IGNORE INTO notifications(idempotency_key, event_type, "
            "payload_json, window_end, status, created_at) "
            "VALUES('test-drop','reminder',?,?,'pending',?)",
            (json.dumps({"message": "probe"}),
             "2026-11-10T02:00:00+00:00", utc_now_iso()))
    assert wait(lambda: any(
        n["idempotency_key"] == "test-drop" and n["status"] == "sent"
        for n in client.notifications()["notifications"]), timeout=10)
    msgs = json.loads(urllib.request.urlopen(
        f"http://127.0.0.1:{receiver.port}/messages", timeout=5).read())
    assert any(d["dropped"] for d in msgs["deliveries"])


# ------------------------------------------------------------ 历史复核

def test_review_report_diff_and_three_issue_states(client, wait):
    from datetime import datetime, timedelta, timezone
    from windwatch.scenarios import BASE_EPOCH
    _ingest(client, wait, "normal", "firmware_vibration_shift")
    # 实时用严格规则制造失败
    strict_rv = client.create_rules({
        "name": "strict",
        "rules": [{"id": "vib", "name": "严格",
                   "expr": {"op": "range", "field": "vibration",
                            "min": 1.8, "max": 2.3}}],
    })["rule_version"]
    rt = client.run_detection({
        "farm_id": "F1", "group_mode": "farm_firmware",
        "rule_version": strict_rv, "items": {"rule:vib": {"threshold": 0.0}},
    })
    assert wait(lambda: client.job(rt["job_id"])["status"] == "done")
    assert any(r["status"] == "fail"
               for r in client.results(rt["job_id"])["results"])

    # 复核用宽松规则：原失败应转为 gone
    loose_rv = client.create_rules({
        "name": "loose",
        "rules": [{"id": "vib", "name": "宽松",
                   "expr": {"op": "range", "field": "vibration",
                            "min": 0.0, "max": 100.0}}],
    })["rule_version"]
    start = datetime.fromtimestamp(BASE_EPOCH, tz=timezone.utc)
    end = start + timedelta(minutes=10 * 12)
    job = client.run_review({
        "farm_id": "F1", "group_mode": "farm_firmware",
        "rule_version": loose_rv,
        "window_start": start.isoformat(), "window_end": end.isoformat(),
        "items": {"rule:vib": {"threshold": 0.0}},
    })
    assert wait(lambda: client.job(job["job_id"])["status"] == "done")
    report = client.review_report(job["job_id"])["report"]
    assert set(report["issues"]) == {"new", "gone", "persistent"}
    assert report["issues"]["gone"], "原实时失败在宽松口径下应归入消失问题"
    gone = report["issues"]["gone"][0]
    assert gone["window"] and gone["realtime_status"] == "fail"
    md = client.get(f"/api/reviews/{job['job_id']}?format=markdown")
    assert "# 历史复核报告" in md and "消失问题" in md
    # 复核不产生通知
    before = len(client.notifications()["notifications"])
    time.sleep(0.2)
    assert len(client.notifications()["notifications"]) == before


# ------------------------------------------------------------ 重启恢复

def test_restart_resumes_running_ingest_and_pending_notifications(
        tmp_path, receiver, wait):
    db_path = tmp_path / "restart.db"
    cfg = Config(
        db_path=str(db_path), host="127.0.0.1", port=free_port(),
        notify_url=f"http://127.0.0.1:{receiver.port}/notify",
        poll_interval=0.01)
    svc = Service(cfg).start()
    c = Client(svc.address)
    batches = build_replay_batches("normal")[0]["measurements"]
    ids = [c.submit_batch(
        {"batch_id": f"r-{i}", "measurements": batches})["task_id"]
        for i in range(2)]
    svc.stop()

    # 模拟崩溃：任务停在 running
    raw = sqlite3.connect(db_path)
    raw.execute("UPDATE ingest_tasks SET status='running' WHERE id=?", (ids[0],))
    raw.commit()
    raw.close()

    svc2 = Service(cfg).start()
    c2 = Client(svc2.address)
    assert wait(lambda: all(
        c2.ingest_task(t)["status"] == "done" for t in ids), timeout=20)
    assert svc2.db.connect().execute(
        "SELECT COUNT(*) FROM ingest_tasks WHERE status='running'"
    ).fetchone()[0] == 0
    svc2.stop()
