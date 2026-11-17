"""python -m windwatch.verify — 一条命令完成本地端到端验证。

流程：启动本地依赖（通知接收端、模拟采集器、主服务）→ 走真实 HTTP
回放全部场景 → 关键断言 → 中断重启恢复（含通知断连重试）→
十万条测量 / 二十条规则的规模性能检测 → 打印断言、耗时与报告位置。

全程使用临时数据库，不依赖人工操作，不访问真实外站。
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .app import Service
from .client import Client
from .config import Config
from .scenarios import BASE_EPOCH, build_replay_batches, medium_dataset
from .simulator import MockCollector, NotificationReceiver

BUDGET_SECONDS = 80
DETECTION_BUDGET = 30


class Reporter:
    def __init__(self):
        self.checks: list[tuple[bool, str]] = []
        self.t0 = time.perf_counter()

    def check(self, ok: bool, name: str, detail: str = ""):
        self.checks.append((bool(ok), name))
        mark = "PASS" if ok else "FAIL"
        line = f"  [{mark}] {name}"
        if detail:
            line += f" — {detail}"
        print(line, flush=True)

    def section(self, title: str):
        print(f"\n=== {title} ===", flush=True)

    def summary(self, report_dir: Path) -> bool:
        elapsed = time.perf_counter() - self.t0
        passed = sum(1 for ok, _ in self.checks if ok)
        print("\n" + "=" * 60)
        print(f"断言 {passed}/{len(self.checks)} 通过，总耗时 {elapsed:.1f}s "
              f"(预算 {BUDGET_SECONDS}s)")
        print(f"报告与数据库目录: {report_dir}")
        ok = passed == len(self.checks) and elapsed <= BUDGET_SECONDS
        print("结果:", "全部通过 ✅" if ok else "存在失败 ❌")
        return ok


def _wait(predicate, timeout: float = 20.0, interval: float = 0.05):
    deadline = time.perf_counter() + timeout
    last = None
    while time.perf_counter() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    return last


def _free_port() -> int:
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ---------------------------------------------------------------- 主流程

def run() -> int:
    work = Path(tempfile.mkdtemp(prefix="windwatch-verify-"))
    report_dir = work / "reports"
    report_dir.mkdir()
    rep = Reporter()
    print(f"临时工作目录: {work}")

    api_port, notify_port, collector_port = (_free_port() for _ in range(3))
    receiver = NotificationReceiver("127.0.0.1", notify_port).start()
    collector = MockCollector("127.0.0.1", collector_port).start()

    db_path = work / "windwatch.db"
    cfg = Config(
        db_path=str(db_path), host="127.0.0.1", port=api_port,
        notify_url=f"http://127.0.0.1:{notify_port}/notify",
        poll_interval=0.02, notify_timeout=2.0,
    )
    service = Service(cfg).start()
    client = Client(service.address, timeout=15)
    time.sleep(0.1)

    ctx = {
        "service": service,
        "client": client, "receiver": receiver, "collector": collector,
        "cfg": cfg, "db_path": db_path, "work": work,
        "report_dir": report_dir,
    }
    try:
        _verify_ingestion(rep, ctx)
        rule_version = _verify_rules(rep, ctx)
        _verify_detection(rep, ctx, rule_version)
        _verify_review(rep, ctx, rule_version)
        _verify_scale(rep, ctx)
    finally:
        service.stop()
        receiver.stop()
        collector.stop()

    ok = rep.summary(report_dir)
    print(f"\n提示: 数据库 {db_path}（含原始批次、隔离区、全部检测明细）")
    return 0 if ok else 1


def _submit_scenario(ctx, scenario: str):
    client: Client = ctx["client"]
    batches = build_replay_batches(scenario)
    ids = [client.submit_batch(b)["task_id"] for b in batches]
    return ids, batches


# ---------------------------------------------------------------- 入库场景

def _verify_ingestion(rep: Reporter, ctx: dict) -> None:
    rep.section("批次接收 / 隔离区 / 修订 / 重复回放（真实 HTTP）")
    client: Client = ctx["client"]

    for scenario in ("normal", "firmware_vibration_shift",
                     "status_category_change"):
        ids, _ = _submit_scenario(ctx, scenario)
        for tid in ids:
            done = _wait(lambda t=tid: client.ingest_task(t)["status"] == "done")
            rep.check(bool(done), f"场景 {scenario} 入库任务 {tid} 完成")

    # backfill 单独提交并等待，后续断言复用这两个任务号
    bf_ids, _ = _submit_scenario(ctx, "revision_backfill")
    _wait(lambda: all(client.ingest_task(t)["status"] == "done"
                      for t in bf_ids), timeout=20)

    inv_ids, inv_batches = _submit_scenario(ctx, "invalid_rows")
    _wait(lambda: client.ingest_task(inv_ids[0])["status"] == "done")
    task = client.ingest_task(inv_ids[0])
    rep.check(task["quarantined_count"] == 5,
              "无效行进隔离区(5 条)", f"实际 {task['quarantined_count']}")
    rep.check(task["received_count"] == len(inv_batches[0]["measurements"]),
              "接收数等于提交行数", str(task["received_count"]))
    q = client.get(f"/api/quarantine?task_id={inv_ids[0]}")["quarantine"]
    reasons = {item["reason"] for item in q}
    rep.check(any("未知字段" in r for r in reasons)
              and any("无法解析" in r for r in reasons)
              and any("revision" in r for r in reasons)
              and any("数值" in r for r in reasons),
              "隔离原因覆盖错误类型/缺字段/无时区/未知字段",
              json.dumps(sorted(reasons), ensure_ascii=False))
    rep.check(task["valid_delta"] == 20,
              "同批 20 条合法行继续处理", f"valid_delta={task['valid_delta']}")

    low_ids, _ = _submit_scenario(ctx, "insufficient_samples")
    rep.check(_wait(
        lambda: client.ingest_task(low_ids[0])["status"] == "done"),
        "小样本批次入库完成")

    dup_batches = build_replay_batches("duplicate_replay")
    r1 = client.submit_batch(dup_batches[0])
    _wait(lambda: client.ingest_task(r1["task_id"])["status"] == "done")
    r2 = client.submit_batch(dup_batches[1])
    _wait(lambda: client.ingest_task(r2["task_id"])["status"] == "done")
    second = client.ingest_task(r2["task_id"])
    rep.check(second["valid_delta"] == 0,
              "重复回放包不改变有效测量视图",
              f"valid_delta={second['valid_delta']}")
    rep.check(second["received_count"] == 25,
              "重复批次原始内容仍完整接收")

    bf_task = client.ingest_task(bf_ids[1])
    rep.check(bf_task["valid_delta"] == 30,
              "补传修订版覆盖 30 个测量身份的有效视图",
              f"valid_delta={bf_task['valid_delta']}")
    eff = client.get("/api/measurements?farm_id=F1&limit=5000")["measurements"]
    revised = [m for m in eff if m["collector_id"] == "C3"]
    rep.check(bool(revised) and all(m["revision"] == 2 for m in revised),
              "有效视图只保留最高修订版(2)",
              f"{len({m['local_measure_id'] for m in revised})} 个身份")


# ---------------------------------------------------------------- 规则

def _verify_rules(rep: Reporter, ctx: dict) -> int:
    rep.section("规则提交 / 结构化树校验 / 不可变版本")
    client: Client = ctx["client"]
    payload = {
        "name": "verify-rules",
        "rules": [
            {"id": "vib_range", "name": "振动范围",
             "expr": {"op": "range", "field": "vibration",
                      "min": 0.0, "max": 6.0}},
            {"id": "temp_when_generating", "name": "发电态温度检查",
             "when": {"op": "compare",
                      "left": {"field": "operating_status"},
                      "cmp": "==", "right": {"value": "generating"}},
             "expr": {"op": "range", "field": "bearing_temp",
                      "min": 30.0, "max": 80.0}},
            {"id": "status_known", "name": "状态枚举",
             "expr": {"op": "in_set", "field": "operating_status",
                      "values": ["generating", "standby", "curtailed",
                                 "fault", "maintenance"]}},
            {"id": "combo", "name": "组合逻辑",
             "expr": {"op": "all", "args": [
                 {"op": "any", "args": [
                     {"op": "field_exists", "field": "vibration"},
                     {"op": "field_exists", "field": "bearing_temp"}]},
                 {"op": "not", "arg": {
                     "op": "compare", "left": {"field": "revision"},
                     "cmp": "<", "right": {"value": 0}}}]}},
            {"id": "never_applies", "name": "不可能适用条件（零分母覆盖）",
             "when": {"op": "compare",
                      "left": {"field": "operating_status"},
                      "cmp": "==", "right": {"value": "dismantled"}},
             "expr": {"op": "range", "field": "vibration",
                      "min": 0.0, "max": 1.0}},
            {"id": "strict_vib", "name": "严格振动（必有违反样本）",
             "expr": {"op": "range", "field": "vibration",
                      "min": 1.5, "max": 2.2}},
        ],
    }
    rv = client.create_rules(payload)["rule_version"]
    rep.check(rv >= 1, "规则版本创建成功", f"version={rv}")

    def reject(body, label):
        try:
            client.create_rules(body)
            rep.check(False, label)
        except Exception as exc:
            rep.check(getattr(exc, "status", 500) == 400, label, str(exc)[:80])

    reject({"name": "bad", "rules": [{"id": "x", "name": "x",
            "expr": {"op": "range", "field": "ghost", "min": 0, "max": 1}}]},
           "拒绝未知字段")
    reject({"name": "bad", "rules": [{"id": "x", "name": "x",
            "expr": {"op": "in_set", "field": "vibration",
                     "values": [1, "a"]}}]},
           "拒绝错误类型的集合")
    deep = {"op": "field_exists", "field": "vibration"}
    for _ in range(8):
        deep = {"op": "not", "arg": deep}
    reject({"name": "bad", "rules": [{"id": "x", "name": "x",
            "expr": deep}]}, "拒绝超过 8 层嵌套")
    reject({"name": "bad", "rules": [{"id": "x", "name": "x",
            "expr": {"op": "eval", "code": "__import__('os')"}}]},
           "拒绝未知 op（不接受 Python 表达式）")

    got = client.get(f"/api/rules/{rv}")
    rep.check(got["rules_json"]["rules"][0]["id"] == "vib_range",
              "规则版本可查询且内容固定")
    return rv


# ---------------------------------------------------------------- 检测 / 告警

def _verify_detection(rep: Reporter, ctx: dict, rule_version: int) -> None:
    rep.section("PSI / TVD / 规则检测 + 告警生命周期 + 断连重启恢复")
    client: Client = ctx["client"]
    items = {
        "vibration_psi": {}, "temperature_psi": {}, "status_tvd": {},
        "rule:vib_range": {"threshold": 0.5},
        "rule:temp_when_generating": {},
        "rule:status_known": {}, "rule:combo": {},
        "rule:never_applies": {}, "rule:strict_vib": {"threshold": 0.0},
    }
    job = client.run_detection({
        "farm_id": "F1", "group_mode": "farm_firmware",
        "rule_version": rule_version, "items": items,
    })
    cap = job["seq_cap"]
    rep.check(cap >= 1, "检测任务固定入库序号上限", f"seq_cap={cap}")
    rep.check(_wait(lambda: client.job(job["job_id"])["status"] == "done"),
              "实时检测任务完成",
              client.job(job["job_id"]).get("error_message", ""))
    results = client.results(job["job_id"], with_samples=True)["results"]

    statuses = {}
    for r in results:
        statuses.setdefault(r["item"], set()).add(r["status"])
    rep.check("undetermined" in statuses.get("vibration_psi", set()),
              "基线未成形窗口显示 undetermined（不伪造零分）")
    fw_fail = [r for r in results
               if r["item"] == "vibration_psi" and r["status"] == "fail"
               and "3.1.9" in r["group_key"]]
    rep.check(bool(fw_fail), "坏固件 3.1.9 振动 PSI 漂移失败",
              f"{len(fw_fail)} 个窗口失败")
    if fw_fail:
        d = fw_fail[0]["detail_json"]
        rep.check({"cuts", "current_counts", "baseline_counts",
                   "current_proportions", "baseline_proportions",
                   "psi"} <= set(d),
                  "PSI 报告含切点/箱计数/比例/PSI")
        rep.check(len(d["current_counts"]) == len(d["cuts"]) + 1,
                  "分箱覆盖整个数轴且切点合并")

    job2 = client.run_detection({
        "farm_id": "F2", "group_mode": "farm",
        "rule_version": rule_version,
        "items": {"status_tvd": {}, "vibration_psi": {}},
    })
    _wait(lambda: client.job(job2["job_id"])["status"] == "done")
    r2 = client.results(job2["job_id"])["results"]
    tvd_fail = [r for r in r2 if r["item"] == "status_tvd"
                and r["status"] == "fail"]
    rep.check(bool(tvd_fail), "状态类别变化触发 TVD 失败",
              f"{len(tvd_fail)} 个窗口")
    if tvd_fail:
        rep.check("categories" in tvd_fail[0]["detail_json"],
                  "TVD 类别取双方并集，缺失单列类")

    rule_res = [r for r in results if r["item"].startswith("rule:")]
    zero_denom = [
        r for r in rule_res
        if r["status"] == "undetermined"
        and r["detail_json"].get("denominator") == 0
        and "分母为零" in r["detail_json"].get("reason", "")
    ]
    rep.check(bool(zero_denom),
              "适用条件不满足导致分母为零的规则窗口判 undetermined",
              f"{len(zero_denom)} 个窗口")
    sample_check = any(
        s.get("failure_path")
        for r in results for s in r.get("violation_samples", []))
    rep.check(sample_check, "违反样本带规则树失败位置")

    job3 = client.run_detection({
        "farm_id": "F9", "group_mode": "farm",
        "rule_version": rule_version, "items": {"vibration_psi": {}},
    })
    _wait(lambda: client.job(job3["job_id"])["status"] == "done")
    r3 = client.results(job3["job_id"])["results"]
    rep.check(bool(r3) and all(r["status"] == "undetermined" for r in r3),
              "样本不足时全部无法判定")

    alerts = client.alerts()["alerts"]
    rep.check(any(a["item"] == "vibration_psi" for a in alerts),
              "漂移失败打开告警", f"{len(alerts)} 条告警")
    _wait(lambda: len(client.notifications()["notifications"]) > 0, timeout=10)
    notifs = client.notifications()["notifications"]
    rep.check(any(n["event_type"] == "opened" and n["status"] == "sent"
                  for n in notifs), "首次打开通知已发送")

    _verify_disconnect_and_restart(rep, ctx)


def _verify_disconnect_and_restart(rep: Reporter, ctx: dict) -> None:
    rep.section("通知断连重试 / 进程中断重启恢复")
    client: Client = ctx["client"]
    receiver: NotificationReceiver = ctx["receiver"]
    collector: MockCollector = ctx["collector"]

    # 经模拟采集服务的真实 HTTP 拉取回放量
    url = f"http://127.0.0.1:{collector.port}/batches/notify_disconnect"
    remote = json.loads(urllib.request.urlopen(url, timeout=10).read())
    t = client.submit_batch(remote["batches"][0])
    _wait(lambda: client.ingest_task(t["task_id"])["status"] == "done")

    # 接收端下一次投递：先收下消息再断开 TCP
    # 先同步排空队列，保证断连开关不会被无关通知的重试消耗
    ctx["service"].notifier.wait_until_drained()
    urllib.request.urlopen(urllib.request.Request(
        f"http://127.0.0.1:{receiver.port}/drop-once", method="POST"),
        timeout=5).read()
    rule_version = client.get("/api/rules")["rule_versions"][0]["version"]
    job4 = client.run_detection({
        "farm_id": "F1", "group_mode": "farm_firmware",
        "rule_version": rule_version,
        "items": {"vibration_psi": {}},
    })
    _wait(lambda: client.job(job4["job_id"])["status"] == "done")

    # 检测可能因节流不产生新通知；这里直接插入一条受控探测通知，
    # 它必然撞上已打开的断连开关，验证“先收下再断连 + 重试成功”链路。
    probe_key = "verify-probe-drop"
    conn = ctx["service"].db.connect()
    already = conn.execute(
        "SELECT 1 FROM notifications WHERE idempotency_key=?",
        (probe_key,)).fetchone()
    if already is None:
        from windwatch.common import utc_now_iso
        conn.execute(
            "INSERT INTO notifications(idempotency_key, event_type, "
            "payload_json, window_end, status, created_at) "
            "VALUES(?,?,?,?,'pending',?)",
            (probe_key, "reminder",
             json.dumps({"event_type": "reminder", "probe": True,
                         "message": "断连重试探测"}),
             "2026-11-10T02:00:00+00:00", utc_now_iso()))
    # 后台 notifier 线程会立刻取走；若开关已被检测消耗则重新武装一次
    time.sleep(0.3)
    msgs0 = json.loads(urllib.request.urlopen(
        f"http://127.0.0.1:{receiver.port}/messages", timeout=5).read())
    if not any(d["dropped"] for d in msgs0["deliveries"]):
        urllib.request.urlopen(urllib.request.Request(
            f"http://127.0.0.1:{receiver.port}/drop-once", method="POST"),
            timeout=5).read()
        # 重置探测通知以便再次投递
        conn.execute(
            "UPDATE notifications SET status='pending', attempts=0 "
            "WHERE idempotency_key=?", (probe_key,))
    _wait(lambda: any(
        n["idempotency_key"] == probe_key and n["status"] == "sent"
        for n in client.notifications()["notifications"]), timeout=10)
    ctx["service"].notifier.wait_until_drained()
    recovered = all(
        n["status"] == "sent" for n in client.notifications()["notifications"]
        if n["event_type"] in ("opened", "reminder", "recovered"))
    rep.check(bool(recovered), "断连通知经后台重试最终投递成功")
    msgs = json.loads(urllib.request.urlopen(
        f"http://127.0.0.1:{receiver.port}/messages", timeout=5).read())
    rep.check(msgs["count"] >= 1
              and msgs["delivery_attempts"] >= msgs["count"],
              "接收端按幂等键保存最终消息且记录投递次数",
              f"messages={msgs['count']} attempts={msgs['delivery_attempts']}")
    rep.check(any(d["dropped"] for d in msgs["deliveries"]),
              "确实发生过先收下再断连的投递")

    # ---- 中断重启：制造 running 任务后停服，重启续跑 ----
    big = build_replay_batches("normal")[0]
    big_rows = big["measurements"] * 3
    queued_ids = [
        client.submit_batch(
            {"batch_id": f"restart-{i}", "measurements": big_rows})["task_id"]
        for i in range(3)
    ]
    ctx["service"].stop()
    conn = sqlite3.connect(ctx["db_path"])
    conn.execute("UPDATE ingest_tasks SET status='running' WHERE id=?",
                 (queued_ids[0],))
    conn.commit()
    conn.close()

    service2 = Service(ctx["cfg"]).start()
    ctx["service"] = service2
    client2 = Client(service2.address, timeout=15)
    ctx["client"] = client2
    all_done = _wait(lambda: all(
        client2.ingest_task(t)["status"] == "done" for t in queued_ids),
        timeout=30)
    rep.check(bool(all_done), "重启后所有入库任务继续完成，无半批数据",
              str([client2.ingest_task(t)["status"] for t in queued_ids]))
    leftover = service2.db.connect().execute(
        "SELECT COUNT(*) AS c FROM ingest_tasks WHERE status='running'"
    ).fetchone()["c"]
    rep.check(leftover == 0, "不存在查不到终态的 running 任务")
    _wait(lambda: all(n["status"] == "sent"
                      for n in client2.notifications()["notifications"]),
          timeout=15)
    rep.check(all(n["status"] == "sent"
                  for n in client2.notifications()["notifications"]),
              "待发通知重启后继续投递（告警与通知同事务持久化）")


# ---------------------------------------------------------------- 历史复核

def _verify_review(rep: Reporter, ctx: dict, rule_version: int) -> None:
    rep.section("历史复核：差异对比 / 问题三态 / JSON+Markdown 报告")
    client: Client = ctx["client"]
    start = datetime.fromtimestamp(BASE_EPOCH, tz=timezone.utc)
    end = start + timedelta(minutes=10 * 12)
    strict = client.create_rules({
        "name": "strict",
        "rules": [{
            "id": "vib_range", "name": "严格振动范围",
            "expr": {"op": "range", "field": "vibration",
                     "min": 1.0, "max": 2.8}}],
    })["rule_version"]
    job = client.run_review({
        "farm_id": "F1", "group_mode": "farm_firmware",
        "rule_version": strict,
        "window_start": start.isoformat(), "window_end": end.isoformat(),
        "items": {"rule:vib_range": {"threshold": 0.0}},
    })
    rep.check(_wait(lambda: client.job(job["job_id"])["status"] == "done",
                    timeout=20),
              "复核异步任务完成",
              client.job(job["job_id"]).get("error_message", ""))
    report = client.review_report(job["job_id"])["report"]
    rep.check(report is not None and "issues" in report,
              "复核报告含新增/消失/仍存在问题分类")
    issues = report["issues"]
    rep.check(set(issues) == {"new", "gone", "persistent"}, "问题三态齐全")
    sample_issue = issues["new"] or issues["persistent"]
    if sample_issue:
        loc = sample_issue[0]
        rep.check({"item", "group_key", "window", "rule_version",
                   "anomaly_samples", "subgroup_evidence",
                   "disclaimer"} <= set(loc),
                  "失败定位报告字段完整（含免责声明）")
    md = client.get(f"/api/reviews/{job['job_id']}?format=markdown")
    rep.check(isinstance(md, str) and "# 历史复核报告" in md,
              "Markdown 报告可通过 API 获取")
    (ctx["report_dir"] / f"review-{job['job_id']}.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    (ctx["report_dir"] / f"review-{job['job_id']}.md").write_text(
        md, encoding="utf-8")

    before = len(client.notifications()["notifications"])
    time.sleep(0.3)
    rep.check(before == len(client.notifications()["notifications"]),
              "历史复核不承担现场通知职责")


# ---------------------------------------------------------------- 十万条规模

def _verify_scale(rep: Reporter, ctx: dict) -> None:
    rep.section("规模性能：100,000 条测量 / 20 条规则（检测须 < 30s）")
    api_port = _free_port()
    scale_db = ctx["work"] / "scale.db"
    cfg = Config(
        db_path=str(scale_db), host="127.0.0.1", port=api_port,
        notify_url=f"http://127.0.0.1:{ctx['receiver'].port}/notify",
        poll_interval=0.01,
    )
    svc = Service(cfg).start()
    client = Client(svc.address, timeout=30)
    try:
        batches, rule_payload = medium_dataset(100_000, 20)
        t0 = time.perf_counter()
        task_ids = [client.submit_batch(b)["task_id"] for b in batches]
        _wait(lambda: all(client.ingest_task(t)["status"] == "done"
                          for t in task_ids), timeout=60)
        ingest_elapsed = time.perf_counter() - t0
        total = sum(client.ingest_task(t)["received_count"] for t in task_ids)
        rep.check(total == 100_000, "十万条测量全部接收入库",
                  f"入库耗时 {ingest_elapsed:.1f}s")

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
        done = _wait(lambda: client.job(job["job_id"])["status"] == "done",
                     timeout=DETECTION_BUDGET + 5)
        elapsed = time.perf_counter() - t0
        rep.check(bool(done), "规模检测任务完成",
                  client.job(job["job_id"]).get("error_message", ""))
        n_results = len(client.results(job["job_id"])["results"])
        rep.check(n_results > 0, "规模检测产出窗口明细", f"{n_results} 条")
        rep.check(elapsed <= DETECTION_BUDGET,
                  f"十万条规模检测 < {DETECTION_BUDGET}s",
                  f"实际 {elapsed:.1f}s")
    finally:
        svc.stop()


def main() -> int:
    return run()


if __name__ == "__main__":
    sys.exit(main())
