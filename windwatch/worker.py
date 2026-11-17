"""后台 worker：批次入库与检测任务执行，支持崩溃重启恢复。

入库在单个事务内完成：原始行、隔离行、有效测量、任务计数一起提交，
因此任何时刻都不会留下“查不到状态的半批数据”。
重启时 db.reset_stuck_jobs() 已把 running 任务重新排队，由本模块续跑。
"""
from __future__ import annotations

import json
import time
from typing import Callable

from .alerts import AlertManager
from .common import iso, parse_instant, row_hash, utc_now_iso, validate_measurement
from .detection import (load_effective, organize, published_watermarks,
                        run_windows)
from .rules import CompiledRule, RuleValidationError, compile_rules


class Worker:
    def __init__(self, db, config, notifier=None,
                 sleep: Callable[[float], None] = time.sleep):
        self.db = db
        self.cfg = config
        self.notifier = notifier
        self._sleep = sleep
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    def run_forever(self) -> None:
        while not self._stop:
            try:
                worked = self.tick()
            except Exception:  # pragma: no cover - 守护线程保活
                worked = False
            if not worked:
                self._sleep(self.cfg.poll_interval)

    # ------------------------------------------------------------ 主循环

    def tick(self) -> bool:
        conn = self.db.connect()
        task = conn.execute(
            "SELECT * FROM ingest_tasks WHERE status='queued' ORDER BY id LIMIT 1"
        ).fetchone()
        if task is not None:
            self._run_ingest(task["id"])
            return True
        job = conn.execute(
            "SELECT * FROM detection_jobs WHERE status='queued' ORDER BY id LIMIT 1"
        ).fetchone()
        if job is not None:
            self._run_detection(job["id"])
            return True
        return False

    # ------------------------------------------------------------ 批次入库

    def _run_ingest(self, task_id: int) -> None:
        conn = self.db.connect()
        conn.execute(
            "UPDATE ingest_tasks SET status='running', updated_at=? WHERE id=?",
            (utc_now_iso(), task_id),
        )
        try:
            rows = [
                json.loads(r["raw_json"])
                for r in conn.execute(
                    "SELECT raw_json FROM raw_batches WHERE task_id=? ORDER BY row_index",
                    (task_id,),
                ).fetchall()
            ]
            received = len(rows)
            quarantined: list[tuple[int, dict, str]] = []
            valid: list[dict] = []
            seen_hashes: set[str] = set()
            for idx, raw in enumerate(rows):
                norm, reason = validate_measurement(raw)
                if reason is not None:
                    quarantined.append((idx, raw, reason))
                    continue
                h = row_hash(norm)
                if h in seen_hashes:
                    # 同一回放包内重复行：忽略，不计入有效也不隔离
                    continue
                seen_hashes.add(h)
                valid.append((h, norm))

            with self.db.transaction() as conn:
                seq = self._next_seq(conn)
                inserted = 0
                for h, norm in valid:
                    exists = conn.execute(
                        "SELECT 1 FROM measurements "
                        "WHERE ingest_task_id=? AND row_hash=?",
                        (task_id, h),
                    ).fetchone()
                    if exists:  # 重提的同一批次：幂等跳过
                        continue
                    conn.execute(
                        "INSERT INTO measurements(ingest_seq, ingest_task_id, "
                        "farm_id, collector_id, local_measure_id, revision, "
                        "measured_at, measured_epoch, turbine_id, firmware_version, "
                        "vibration, bearing_temp, operating_status, row_hash) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (seq, task_id, norm["farm_id"], norm["collector_id"],
                         norm["local_measure_id"], norm["revision"],
                         norm["measured_at"], norm["measured_epoch"],
                         norm["turbine_id"], norm["firmware_version"],
                         norm["vibration"], norm["bearing_temp"],
                         norm["operating_status"], h),
                    )
                    inserted += 1

                now = utc_now_iso()
                for idx, raw, reason in quarantined:
                    conn.execute(
                        "UPDATE raw_batches SET is_quarantined=1 "
                        "WHERE task_id=? AND row_index=?", (task_id, idx),
                    )
                    conn.execute(
                        "INSERT INTO quarantine(task_id, row_index, raw_json, "
                        "reason, created_at) VALUES(?,?,?,?,?)",
                        (task_id, idx,
                         json.dumps(raw, ensure_ascii=False), reason, now),
                    )
                valid_delta = self._compute_valid_delta(conn, task_id)
                conn.execute(
                    "UPDATE ingest_tasks SET status='done', received_count=?, "
                    "quarantined_count=?, valid_delta=?, updated_at=? WHERE id=?",
                    (received, len(quarantined), valid_delta, now, task_id),
                )
        except Exception as exc:  # 事务由 db.transaction() 自动回滚，不留半批
            self.db.connect().execute(
                "UPDATE ingest_tasks SET status='error', error_message=?, "
                "updated_at=? WHERE id=?",
                (f"{type(exc).__name__}: {exc}", utc_now_iso(), task_id),
            )

    @staticmethod
    def _next_seq(conn) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(ingest_seq),0)+1 AS s FROM measurements"
        ).fetchone()
        return row["s"]

    @staticmethod
    def _compute_valid_delta(conn, task_id: int) -> int:
        """本任务导致有效测量视图发生变化的身份数。

        新身份（此前无记录）或本任务最高修订高于此前最高修订 -> 计入；
        同修订/更低修订的重复回放（视图不变）-> 不计。
        用相关子查询避免多修订行 JOIN 膨胀。
        """
        return conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT farm_id, collector_id, local_measure_id,
                       MAX(revision) AS my_rev,
                       COALESCE((
                           SELECT MAX(m2.revision) FROM measurements m2
                           WHERE m2.farm_id = m.farm_id
                             AND m2.collector_id = m.collector_id
                             AND m2.local_measure_id = m.local_measure_id
                             AND m2.ingest_task_id <> ?
                       ), -1) AS prev_rev
                FROM measurements m
                WHERE ingest_task_id = ?
                GROUP BY farm_id, collector_id, local_measure_id
            )
            WHERE prev_rev < my_rev
            """,
            (task_id, task_id),
        ).fetchone()[0]

    # ------------------------------------------------------------ 检测任务

    def _run_detection(self, job_id: int) -> None:
        conn = self.db.connect()
        conn.execute(
            "UPDATE detection_jobs SET status='running', updated_at=? WHERE id=?",
            (utc_now_iso(), job_id),
        )
        try:
            job = dict(conn.execute(
                "SELECT * FROM detection_jobs WHERE id=?", (job_id,)
            ).fetchone())
            rv_row = conn.execute(
                "SELECT * FROM rule_versions WHERE version=?",
                (job["rule_version"],),
            ).fetchone()
            if rv_row is None:
                raise RuleValidationError(f"规则版本 {job['rule_version']} 不存在")
            rules = compile_rules(json.loads(rv_row["rules_json"]))
            items = json.loads(job["items_json"])
            self._validate_items(items, rules)
            # 规则项缺省阈值由配置统一提供
            for name, spec in items.items():
                if name.startswith("rule:") and "threshold" not in spec:
                    spec["threshold"] = self.cfg.rule_threshold

            records = load_effective(conn, job["seq_cap"], job["farm_id"])
            emit_start = emit_end = None
            watermarks = None
            if job["kind"] == "review":
                from .common import window_start_for
                start = _parse_epoch(job["window_start"])
                end = _parse_epoch(job["window_end"])
                # 只要窗口起点落在复核区间内（左闭右开，窗口对齐整点）
                records = [
                    r for r in records
                    if start <= window_start_for(
                        r["measured_epoch"], self.cfg.window_minutes
                    ).timestamp() < end
                ]
            else:
                # 实时发布：指定的时间范围只限制“发布哪些窗口”，
                # 数据仍全量读取，基线由完整历史累积，不因范围而截断。
                if job["window_start"]:
                    emit_start = iso(parse_instant(job["window_start"]))
                if job["window_end"]:
                    emit_end = iso(parse_instant(job["window_end"]))
            grouped = organize(records, job["group_mode"], self.cfg.window_minutes)

            alert_mgr = AlertManager(conn, self.cfg.notify_reminder_seconds)
            produced = 0
            rule_map = {r.rule_id: r for r in rules}
            thresholds = {
                "vibration_psi": self.cfg.psi_threshold,
                "temperature_psi": self.cfg.psi_threshold,
                "status_tvd": self.cfg.tvd_threshold,
                "__rule_default__": self.cfg.rule_threshold,
            }
            with self.db.transaction() as conn:
                if job["kind"] != "review":
                    # 与结果插入同一写事务读取发布水位：全局写锁 + BEGIN IMMEDIATE
                    # 保证并发检测（含多进程）看到已提交的最新水位，不会重复发布。
                    watermarks = published_watermarks(conn, job["rule_version"])
                for (ws, we, gk, item, status, score, threshold,
                     detail, samples) in run_windows(
                        grouped, rules, items, thresholds,
                        self.cfg.min_samples, self.cfg.baseline_windows,
                        self.cfg.max_violation_samples, self.cfg.window_minutes,
                        emit_start=emit_start, emit_end=emit_end,
                        watermarks=watermarks,
                ):
                    cur = conn.execute(
                        "INSERT INTO detection_results(job_id, window_start, "
                        "window_end, group_key, item, status, score, threshold, "
                        "detail_json, created_at) VALUES(?,?,?,?,?,?,?,?,?,?) "
                        "RETURNING id",
                        (job_id, ws, we, gk, item, status, score, threshold,
                         json.dumps(detail, ensure_ascii=False), utc_now_iso()),
                    ).fetchone()
                    for s in samples:
                        rule_id = item.split(":", 1)[1]
                        conn.execute(
                            "INSERT INTO violation_samples(result_id, rule_id, "
                            "sample_json, failure_path) VALUES(?,?,?,?)",
                            (cur["id"], rule_id,
                             json.dumps(s["measurement"], ensure_ascii=False),
                             s["failure_path"]),
                        )
                    if job["kind"] == "realtime":
                        alert_mgr.process_result(
                            job["rule_version"], item, gk, ws, we,
                            status, score, threshold,
                        )
                    produced += 1

                now = utc_now_iso()
                if job["kind"] == "review":
                    from .report import build_review_report
                    build_review_report(conn, job, records, grouped, rules,
                                        rule_map, produced)
                conn.execute(
                    "UPDATE detection_jobs SET status='done', updated_at=? WHERE id=?",
                    (now, job_id),
                )
        except Exception as exc:  # 事务由 db.transaction() 自动回滚
            self.db.connect().execute(
                "UPDATE detection_jobs SET status='error', error_message=?, "
                "updated_at=? WHERE id=?",
                (f"{type(exc).__name__}: {exc}", utc_now_iso(), job_id),
            )

    @staticmethod
    def _validate_items(items: dict, rules: list[CompiledRule]) -> None:
        if not isinstance(items, dict) or not items:
            raise RuleValidationError("检测项 items 必须是非空对象")
        known = {"vibration_psi", "temperature_psi", "status_tvd"}
        rule_ids = {r.rule_id for r in rules}
        for name, spec in items.items():
            if not isinstance(spec, dict):
                raise RuleValidationError(f"检测项 {name} 的配置必须是对象")
            if "threshold" in spec and (
                isinstance(spec["threshold"], bool)
                or not isinstance(spec["threshold"], (int, float))
            ):
                raise RuleValidationError(f"检测项 {name} 的 threshold 必须是数值")
            if name.startswith("rule:"):
                if name.split(":", 1)[1] not in rule_ids:
                    raise RuleValidationError(f"检测项引用了不存在的规则: {name}")
            elif name not in known:
                raise RuleValidationError(f"未知检测项: {name}")


def _parse_epoch(text: str) -> float:
    from .common import parse_instant
    return parse_instant(text).timestamp()
