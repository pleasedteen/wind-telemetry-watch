"""实时检测“只向前发布”回归测试。

覆盖场景（全部走真实 HTTP + SQLite + 本地通知接收端）：
- 重复发起实时检测：旧窗口只保留首次发布结果，不重复产生已发布明细；
- 补传修订后再检测：旧窗口不重算发布、告警不被拉回过去，新窗口正常检测；
- 重叠区间：只发布可推进的新窗口；
- 实时任务指定 window_start/window_end：范围生效且不截断漂移基线；
- 进程中断重启：水位持久，重启后仍只向前推进；
- 历史复核：能展示补传后的差异，但不改现场告警、不产生通知。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

from windwatch.app import Service
from windwatch.client import Client
from windwatch.config import Config
from windwatch.scenarios import BASE_EPOCH, build_replay_batches

from .conftest import free_port


def _base_dt(minutes: int = 0) -> datetime:
    return datetime.fromtimestamp(BASE_EPOCH, tz=timezone.utc) + \
        timedelta(minutes=minutes)


def _submit(client, batch):
    return client.submit_batch(batch)["task_id"]


def _wait_ingest(client, ids, wait):
    assert wait(lambda: all(
        client.ingest_task(t)["status"] == "done" for t in ids), timeout=25)


def _run(client, wait, payload):
    job = client.run_detection(payload)
    assert wait(lambda: client.job(job["job_id"])["status"] == "done",
                timeout=25), client.job(job["job_id"])
    return job["job_id"], client.results(job["job_id"], with_samples=True)


def _shared_rules(client):
    return client.create_rules({
        "name": "forward-rules",
        "rules": [{
            "id": "vib", "name": "振动范围",
            "expr": {"op": "range", "field": "vibration",
                     "min": 0.0, "max": 6.0},
        }],
    })["rule_version"]


def _window_keys(results):
    return {(r["window_start"], r["group_key"], r["item"]) for r in results}


# ------------------------------------------------------------ 重复检测

def test_repeat_realtime_detection_publishes_no_old_windows(
        client, wait):
    _wait_ingest(client, [
        _submit(client, b)
        for b in build_replay_batches("firmware_vibration_shift")], wait)
    rv = _shared_rules(client)

    payload = {"farm_id": "F1", "group_mode": "farm_firmware",
               "rule_version": rv,
               "items": {"vibration_psi": {}, "rule:vib": {}}}
    first_id, first = _run(client, wait, payload)
    assert first["results"]
    first_keys = _window_keys(first["results"])

    second_id, second = _run(client, wait, payload)
    # 同样的数据重复检测：没有任何窗口被重新发布
    assert second["results"] == []
    assert _window_keys(client.results(first_id)["results"]) == first_keys

    # 告警事件流与首次发布一一对应，没有因为重复检测而增加
    conn = client  # 通过 HTTP 校验事件数量稳定
    alerts_before = client.alerts()["alerts"]
    assert alerts_before
    first_event_count = sum(
        len(client.alert_detail(a["id"])["event_stream"])
        for a in alerts_before)
    _run(client, wait, payload)
    alerts_after = client.alerts()["alerts"]
    assert len(alerts_after) == len(alerts_before)
    assert sum(len(client.alert_detail(a["id"])["event_stream"])
               for a in alerts_after) == first_event_count


def test_watermark_is_per_rule_item_and_group(client, receiver, wait):
    """不同规则版本、检测项、分组彼此独立推进，不互相阻塞。"""
    _wait_ingest(client, [
        _submit(client, b)
        for b in build_replay_batches("firmware_vibration_shift")], wait)
    rv1 = _shared_rules(client)
    rv2 = client.create_rules({
        "name": "another",
        "rules": [{"id": "vib", "name": "振动范围",
                   "expr": {"op": "range", "field": "vibration",
                            "min": 0.0, "max": 6.0}}],
    })["rule_version"]

    base = {"farm_id": "F1", "group_mode": "farm_firmware",
            "items": {"vibration_psi": {}}}
    _run(client, wait, {**base, "rule_version": rv1})
    # 新规则版本没有水位，应能正常发布全部窗口
    _, rv2_results = _run(client, wait, {**base, "rule_version": rv2})
    assert rv2_results["results"]

    # 同规则版本新增检测项：该项没有水位，可独立发布
    _, temp = _run(client, wait, {**base, "rule_version": rv1,
                                  "items": {"temperature_psi": {}}})
    assert temp["results"]

    # farm 分组与 farm_firmware 分组互不影响
    _, farm = _run(client, wait, {"farm_id": "F1", "group_mode": "farm",
                                  "rule_version": rv1,
                                  "items": {"vibration_psi": {}}})
    assert farm["results"]


# ------------------------------------------------------------ 补传修订

def test_backfill_revision_does_not_rewind_alerts(client, receiver, wait):
    """先发布原修订；补传修订后再检测，旧窗口不重发、告警时间不回退。"""
    batches = build_replay_batches("revision_backfill")
    _wait_ingest(client, [_submit(client, batches[0])], wait)
    rv = _shared_rules(client)
    payload = {"farm_id": "F1", "group_mode": "farm",
               "rule_version": rv,
               "items": {"vibration_psi": {}}}
    first_id, first = _run(client, wait, payload)
    first_keys = _window_keys(first["results"])
    assert first_keys

    alerts_before = client.alerts()["alerts"]
    notif_before = client.notifications()["notifications"]

    # 补传修订版 2 后重新检测
    _wait_ingest(client, [_submit(client, batches[1])], wait)
    second_id, second = _run(client, wait, payload)
    assert second["results"] == []
    assert _window_keys(client.results(first_id)["results"]) == first_keys

    assert client.alerts()["alerts"] == alerts_before
    assert client.notifications()["notifications"] == notif_before

    # 复核可以看到补传差异，但不写实时结果、不产生通知
    start = _base_dt()
    end = start + timedelta(minutes=10 * 4)
    job = client.run_review({
        "farm_id": "F1", "group_mode": "farm", "rule_version": rv,
        "window_start": start.isoformat(), "window_end": end.isoformat(),
        "items": {"vibration_psi": {}},
    })
    assert wait(lambda: client.job(job["job_id"])["status"] == "done")
    report = client.review_report(job["job_id"])["report"]
    assert report["windows"]
    assert client.alerts()["alerts"] == alerts_before
    assert client.notifications()["notifications"] == notif_before


def test_review_shows_backfill_diff_without_touching_live_alerts(
        client, wait):
    """补传后复核能展示差异（现场仍为首次发布的 pass），告警保持不变。"""
    from windwatch.scenarios import WINDOW_SECS
    base = _base_dt()
    original, revised = [], []
    for i in range(36 * 3):
        window = i // 36
        measured = (base + timedelta(
            seconds=window * WINDOW_SECS + (i % 36) * 10)).isoformat()
        row = {
            "farm_id": "F1", "collector_id": "C8",
            "local_measure_id": f"M{90000 + i:05d}", "revision": 1,
            "measured_at": measured, "turbine_id": f"T{(i % 6) + 1:02d}",
            "firmware_version": "3.1.7", "vibration": 2.0,
            "bearing_temp": 55.0, "operating_status": "generating",
        }
        original.append(row)
        revised.append({**row, "revision": 2, "vibration": 9.5})
    _wait_ingest(client, [_submit(client, {
        "batch_id": "rb-1", "measurements": original})], wait)

    rv = client.create_rules({
        "name": "range-rules",
        "rules": [{"id": "vib", "name": "振动范围",
                   "expr": {"op": "range", "field": "vibration",
                            "min": 0.0, "max": 6.0}}],
    })["rule_version"]
    payload = {"farm_id": "F1", "group_mode": "farm", "rule_version": rv,
               "items": {"rule:vib": {"threshold": 0.0}}}
    rt_id, rt = _run(client, wait, payload)
    assert rt["results"] and all(r["status"] == "pass" for r in rt["results"])
    alerts_before = client.alerts()["alerts"]
    notif_before = client.notifications()["notifications"]

    # 补传修订 2（振动超范围），实时再检测仍不重发旧窗口
    _wait_ingest(client, [_submit(client, {
        "batch_id": "rb-2", "measurements": revised})], wait)
    _, again = _run(client, wait, payload)
    assert again["results"] == []
    assert client.alerts()["alerts"] == alerts_before

    # 复核同区间：应基于补传后的数据给出与现场不同的结果
    end = base + timedelta(minutes=30)
    job = client.run_review({
        "farm_id": "F1", "group_mode": "farm", "rule_version": rv,
        "window_start": base.isoformat(), "window_end": end.isoformat(),
        "items": {"rule:vib": {"threshold": 0.0}},
    })
    assert wait(lambda: client.job(job["job_id"])["status"] == "done")
    report = client.review_report(job["job_id"])["report"]
    assert report["issues"]["new"], "复核应看到补传导致的差异"
    issue = report["issues"]["new"][0]
    assert issue["score"] > 0.9
    assert issue["window"]["start"] >= base.isoformat()

    # 现场告警与通知完全没有变化
    assert client.alerts()["alerts"] == alerts_before
    assert client.notifications()["notifications"] == notif_before
    assert _window_keys(client.results(rt_id)["results"]) == _window_keys(
        rt["results"])


def test_new_windows_after_backfill_are_detected(client, wait):
    """旧窗口冻结，但严格晚于水位的窗口仍正常检测并发布。"""
    _wait_ingest(client, [
        _submit(client, b) for b in build_replay_batches("normal")], wait)
    rv = _shared_rules(client)
    payload = {"farm_id": "F1", "group_mode": "farm", "rule_version": rv,
               "items": {"vibration_psi": {}}}

    # 只发布前两个窗口
    _first_id, first = _run(client, wait, {
        **payload, "window_end": _base_dt(20).isoformat()})
    assert first["results"]
    assert all(r["window_start"] < _base_dt(20).isoformat()
               for r in first["results"])

    # 下一次实时任务覆盖全部窗口：只补发后十个窗口
    _second_id, second = _run(client, wait, payload)
    assert second["results"]
    assert all(r["window_start"] >= _base_dt(20).isoformat()
               for r in second["results"])
    assert len({r["window_start"] for r in second["results"]}) == 10


# ------------------------------------------------------------ 指定范围 / 基线

def test_realtime_window_range_limits_publication_not_baseline(client, wait):
    """window_start/window_end 限制发布范围，但基线仍由更早窗口累积。"""
    _wait_ingest(client, [
        _submit(client, b) for b in build_replay_batches("normal")], wait)
    rv = _shared_rules(client)

    # 起点设为窗口 6：若基线被截断，窗口 6 不会有 6 个历史窗口可用
    start = _base_dt(60)
    end = start + timedelta(minutes=10 * 4)
    _job_id, results = _run(client, wait, {
        "farm_id": "F1", "group_mode": "farm", "rule_version": rv,
        "window_start": start.isoformat(), "window_end": end.isoformat(),
        "items": {"vibration_psi": {}},
    })
    assert results["results"]
    starts = {r["window_start"] for r in results["results"]}
    assert starts == {_base_dt(60 + 10 * i).isoformat() for i in range(4)}

    # 窗口 6 的基线是窗口 0..5（6 个窗口 × 36 样本），不被范围起点截断
    first = results["results"][0]
    assert first["detail_json"]["baseline_samples"] == 36 * 6


def test_realtime_window_range_is_validated(client):
    import pytest
    from windwatch.client import ClientError
    rv = _shared_rules(client)
    base = {"farm_id": "F1", "group_mode": "farm", "rule_version": rv,
            "items": {"vibration_psi": {}}}
    for bad in (
        {"window_start": "2026-11-10T01:00:00+00:00",
         "window_end": "2026-11-10T00:00:00+00:00"},
        {"window_start": "2026-11-10T00:00:00"},  # 无时区
        {"window_end": "not-a-time"},
    ):
        with pytest.raises(ClientError) as exc:
            client.run_detection({**base, **bad})
        assert exc.value.status == 400


def test_overlapping_realtime_ranges_only_publish_advancing_windows(client, wait):
    """两个重叠范围的连续实时任务：重叠部分只发布一次。"""
    _wait_ingest(client, [
        _submit(client, b) for b in build_replay_batches("normal")], wait)
    rv = _shared_rules(client)
    base = {"farm_id": "F1", "group_mode": "farm", "rule_version": rv,
            "items": {"vibration_psi": {}}}

    _, a = _run(client, wait, {
        **base, "window_start": _base_dt(0).isoformat(),
        "window_end": _base_dt(40).isoformat()})
    _, b = _run(client, wait, {
        **base, "window_start": _base_dt(20).isoformat(),
        "window_end": _base_dt(60).isoformat()})

    a_starts = {r["window_start"] for r in a["results"]}
    b_starts = {r["window_start"] for r in b["results"]}
    assert a_starts == {_base_dt(10 * i).isoformat() for i in range(4)}
    assert b_starts == {_base_dt(40).isoformat(), _base_dt(50).isoformat()}
    assert not (a_starts & b_starts)


# ------------------------------------------------------------ 重启恢复

def test_watermark_survives_process_restart(tmp_path, receiver, wait):
    db_path = tmp_path / "forward-restart.db"
    cfg = Config(
        db_path=str(db_path), host="127.0.0.1", port=free_port(),
        notify_url=f"http://127.0.0.1:{receiver.port}/notify",
        poll_interval=0.01, notify_timeout=2.0)
    batches = build_replay_batches("firmware_vibration_shift")

    svc = Service(cfg).start()
    c = Client(svc.address, timeout=15)
    try:
        ids = [_submit(c, b) for b in batches]
        assert wait(lambda: all(
            c.ingest_task(t)["status"] == "done" for t in ids), timeout=25)
        rv = c.create_rules({
            "name": "r",
            "rules": [{"id": "vib", "name": "振动范围",
                       "expr": {"op": "range", "field": "vibration",
                                "min": 0.0, "max": 6.0}}],
        })["rule_version"]
        payload = {"farm_id": "F1", "group_mode": "farm_firmware",
                   "rule_version": rv,
                   "items": {"vibration_psi": {}, "rule:vib": {}}}
        job = c.run_detection(payload)
        assert wait(lambda: c.job(job["job_id"])["status"] == "done")
        results = c.results(job["job_id"])["results"]
        alerts_before = c.alerts()["alerts"]
        assert results
    finally:
        svc.stop()

    # 模拟进程中断：把一次重复检测停在 running，重启后应续跑且不重发旧窗口
    raw = sqlite3.connect(db_path)
    raw.execute(
        "INSERT INTO detection_jobs(kind, status, farm_id, group_mode, "
        "rule_version, seq_cap, items_json, created_at, updated_at) "
        "SELECT 'realtime', 'running', farm_id, group_mode, rule_version, "
        "seq_cap, items_json, created_at, created_at "
        "FROM detection_jobs WHERE id=?", (job["job_id"],))
    interrupted = raw.execute(
        "SELECT MAX(id) FROM detection_jobs").fetchone()[0]
    raw.commit()
    raw.close()

    svc2 = Service(cfg).start()
    c2 = Client(svc2.address, timeout=15)
    try:
        assert wait(lambda: c2.job(interrupted)["status"] == "done",
                    timeout=25), c2.job(interrupted)
        assert c2.results(interrupted)["results"] == []
        assert c2.alerts()["alerts"] == alerts_before
        # 重启后的新任务仍然只向前推进
        again = c2.run_detection(payload)
        assert wait(lambda: c2.job(again["job_id"])["status"] == "done")
        assert c2.results(again["job_id"])["results"] == []
    finally:
        svc2.stop()


def test_restart_mid_realtime_detection_keeps_single_publication(
        tmp_path, receiver, wait):
    """检测任务提交后未开始即“中断”，重启续跑只发布一次。"""
    db_path = tmp_path / "forward-mid.db"
    cfg = Config(
        db_path=str(db_path), host="127.0.0.1", port=free_port(),
        notify_url=f"http://127.0.0.1:{receiver.port}/notify",
        poll_interval=0.01, notify_timeout=2.0)
    svc = Service(cfg).start()
    c = Client(svc.address, timeout=15)
    try:
        ids = [_submit(c, b)
               for b in build_replay_batches("status_category_change")]
        assert wait(lambda: all(
            c.ingest_task(t)["status"] == "done" for t in ids), timeout=25)
        rv = c.create_rules({
            "name": "r",
            "rules": [{"id": "vib", "name": "振动范围",
                       "expr": {"op": "range", "field": "vibration",
                                "min": 0.0, "max": 6.0}}],
        })["rule_version"]
        payload = {"farm_id": "F2", "group_mode": "farm",
                   "rule_version": rv, "items": {"status_tvd": {}}}
        first = c.run_detection(payload)
        assert wait(lambda: c.job(first["job_id"])["status"] == "done")
        published = c.results(first["job_id"])["results"]
        assert published
        alerts = c.alerts()["alerts"]
        notifs_before = c.notifications()["notifications"]
    finally:
        svc.stop()

    # 重复任务停留在 running 状态，模拟进程中断
    raw = sqlite3.connect(db_path)
    cur = raw.execute(
        "INSERT INTO detection_jobs(kind, status, farm_id, group_mode, "
        "rule_version, seq_cap, items_json, created_at, updated_at) "
        "SELECT 'realtime', 'running', farm_id, group_mode, rule_version, "
        "seq_cap, items_json, created_at, created_at "
        "FROM detection_jobs WHERE id=?", (first["job_id"],))
    interrupted = cur.lastrowid
    raw.commit()
    raw.close()

    svc2 = Service(cfg).start()
    c2 = Client(svc2.address, timeout=15)
    try:
        assert wait(lambda: c2.job(interrupted)["status"] == "done",
                    timeout=25)
        assert c2.results(interrupted)["results"] == []
        assert c2.alerts()["alerts"] == alerts
        # 通知没有因续跑而重复产生
        assert c2.notifications()["notifications"] == notifs_before
    finally:
        svc2.stop()
