"""HTTP API（标准库 http.server，线程化）。

覆盖批次接收、任务状态、隔离区、有效测量、规则版本、检测任务、
检测结果、告警、通知记录与历史复核报告的全部查询与操作入口。
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .common import utc_now_iso
from .rules import RuleValidationError, compile_rules


def _create_detection(db, payload: dict) -> tuple[int, int, str]:
    conn = db.connect()
    for f in ("farm_id", "group_mode", "rule_version", "items"):
        if f not in payload:
            raise ValueError(f"缺少字段: {f}")
    if payload["group_mode"] not in ("farm", "farm_firmware"):
        raise ValueError("group_mode 仅支持 farm / farm_firmware")
    kind = payload.get("kind", "realtime")
    if kind not in ("realtime", "review"):
        raise ValueError("kind 仅支持 realtime / review")
    cap = payload.get("seq_cap")
    if cap is None:
        cap = conn.execute(
            "SELECT COALESCE(MAX(ingest_seq),0) AS s FROM measurements"
        ).fetchone()["s"]
    now = utc_now_iso()
    with db.transaction() as conn:
        cur = conn.execute(
            "INSERT INTO detection_jobs(kind, status, farm_id, group_mode, "
            "rule_version, seq_cap, window_start, window_end, items_json, "
            "created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) "
            "RETURNING id",
            (kind, "queued", payload["farm_id"], payload["group_mode"],
             int(payload["rule_version"]), int(cap),
             payload.get("window_start"), payload.get("window_end"),
             json.dumps(payload["items"], ensure_ascii=False), now, now),
        )
        return cur.fetchone()["id"], int(cap), kind


def _default_items(db, rule_version: int) -> dict:
    row = db.connect().execute(
        "SELECT rules_json FROM rule_versions WHERE version=?",
        (rule_version,),
    ).fetchone()
    if row is None:
        raise RuleValidationError(f"规则版本 {rule_version} 不存在")
    items = {"vibration_psi": {}, "temperature_psi": {}, "status_tvd": {}}
    for rule in json.loads(row["rules_json"])["rules"]:
        items[f"rule:{rule['id']}"] = {}
    return items


def make_handler(db, cfg):
    class Handler(BaseHTTPRequestHandler):
        server_version = "windwatch/1.0"

        def log_message(self, fmt, *args):
            return

        # ------------------------------------------------ 基础收发

        def _send_json(self, obj, status: int = 200):
            body = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type",
                             "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self):
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                raise ValueError("请求体为空")
            raw = self.rfile.read(length)
            try:
                return json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"请求体不是合法 JSON: {exc}")

        def _query(self):
            return parse_qs(urlparse(self.path).query)

        def _error(self, status: int, message: str):
            self._send_json({"error": message}, status)

        # ------------------------------------------------ 路由

        def do_GET(self):
            try:
                path = urlparse(self.path).path.rstrip("/") or "/"
                route = GET_ROUTES.get(path)
                if route is not None:
                    route(self)
                    return
                for pattern, fn in GET_PARAM_ROUTES:
                    m = pattern.fullmatch(path)
                    if m:
                        fn(self, **m.groupdict())
                        return
                self._error(404, f"无此路径: {path}")
            except ValueError as exc:
                self._error(400, str(exc))
            except Exception as exc:  # pragma: no cover - 兜底
                self._error(500, f"{type(exc).__name__}: {exc}")

        def do_POST(self):
            try:
                path = urlparse(self.path).path.rstrip("/") or "/"
                route = POST_ROUTES.get(path)
                if route is None:
                    self._error(404, f"无此路径: {path}")
                    return
                route(self)
            except ValueError as exc:
                self._error(400, str(exc))
            except RuleValidationError as exc:
                self._error(400, f"规则校验失败: {exc}")
            except Exception as exc:  # pragma: no cover - 兜底
                self._error(500, f"{type(exc).__name__}: {exc}")

        # ================================================== 端点

        # ---------- 批次接收 ----------
        def post_batches(self):
            payload = self._read_json()
            if not isinstance(payload, dict):
                raise ValueError("批次提交必须是对象")
            batch_id = payload.get("batch_id")
            rows = payload.get("measurements")
            if not isinstance(batch_id, str) or not batch_id:
                raise ValueError("batch_id 必须是非空字符串")
            if not isinstance(rows, list) or not rows:
                raise ValueError("measurements 必须是非空数组")
            conn = db.connect()
            now = utc_now_iso()
            with db.transaction() as conn:
                task_id = conn.execute(
                    "INSERT INTO ingest_tasks(batch_id, status, created_at, "
                    "updated_at) VALUES(?,?,?,?) RETURNING id",
                    (batch_id, "queued", now, now),
                ).fetchone()["id"]
                conn.executemany(
                    "INSERT INTO raw_batches(task_id, row_index, raw_json) "
                    "VALUES(?,?,?)",
                    [(task_id, i,
                      json.dumps(r, ensure_ascii=False, default=str))
                     for i, r in enumerate(rows)],
                )
            self._send_json({
                "task_id": task_id, "batch_id": batch_id,
                "status": "queued", "received": len(rows),
            }, 202)

        def get_ingest_tasks(self):
            limit = min(int(self._query().get("limit", ["100"])[0]), 500)
            rows = db.connect().execute(
                "SELECT * FROM ingest_tasks ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            self._send_json({"tasks": [dict(r) for r in rows]})

        def get_ingest_task(self, task_id):
            r = db.connect().execute(
                "SELECT * FROM ingest_tasks WHERE id=?", (task_id,)
            ).fetchone()
            if r is None:
                self._error(404, f"入库任务 {task_id} 不存在")
                return
            self._send_json(dict(r))

        def get_quarantine(self):
            task_id = self._query().get("task_id", [None])[0]
            conn = db.connect()
            if task_id:
                rows = conn.execute(
                    "SELECT * FROM quarantine WHERE task_id=? ORDER BY id",
                    (task_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM quarantine ORDER BY id DESC LIMIT 200"
                ).fetchall()
            self._send_json({"quarantine": [dict(r) for r in rows]})

        def get_measurements(self):
            q = self._query()
            farm = q.get("farm_id", [None])[0]
            if not farm:
                raise ValueError("必须提供 farm_id")
            conn = db.connect()
            from .detection import load_effective
            cap = q.get("seq_cap", [None])[0]
            if cap is None:
                cap = conn.execute(
                    "SELECT COALESCE(MAX(ingest_seq),0) AS s FROM measurements"
                ).fetchone()["s"]
            cap = int(cap)
            recs = load_effective(conn, cap, farm)
            limit = int(q.get("limit", ["1000"])[0])
            self._send_json({
                "farm_id": farm, "seq_cap": cap, "count": len(recs),
                "measurements": recs[:limit],
            })

        # ---------- 规则 ----------
        def post_rules(self):
            payload = self._read_json()
            compile_rules(payload)  # 提交即校验，失败返回 400
            conn = db.connect()
            now = utc_now_iso()
            with db.transaction() as conn:
                ver = conn.execute(
                    "SELECT COALESCE(MAX(version),0)+1 AS v FROM rule_versions"
                ).fetchone()["v"]
                conn.execute(
                    "INSERT INTO rule_versions(version, name, rules_json, "
                    "created_at) VALUES(?,?,?,?)",
                    (ver, str(payload.get("name", f"rules-v{ver}")),
                     json.dumps(payload, ensure_ascii=False), now),
                )
            self._send_json({"rule_version": ver, "immutable": True}, 201)

        def get_rules(self):
            rows = db.connect().execute(
                "SELECT id, version, name, created_at FROM rule_versions "
                "ORDER BY version"
            ).fetchall()
            self._send_json({"rule_versions": [dict(r) for r in rows]})

        def get_rule_version(self, version):
            r = db.connect().execute(
                "SELECT * FROM rule_versions WHERE version=?", (version,)
            ).fetchone()
            if r is None:
                self._error(404, f"规则版本 {version} 不存在")
                return
            out = dict(r)
            out["rules_json"] = json.loads(out["rules_json"])
            self._send_json(out)

        # ---------- 检测任务 ----------
        def post_detections(self):
            payload = self._read_json()
            payload.setdefault("kind", "realtime")
            job_id, cap, kind = _create_detection(db, payload)
            self._send_json({"job_id": job_id, "kind": kind,
                             "status": "queued", "seq_cap": cap}, 202)

        def post_reviews(self):
            payload = self._read_json()
            for f in ("farm_id", "rule_version", "window_start", "window_end"):
                if f not in payload:
                    raise ValueError(f"缺少字段: {f}")
            payload["kind"] = "review"
            payload.setdefault("group_mode", "farm_firmware")
            payload.setdefault(
                "items", _default_items(db, int(payload["rule_version"])))
            job_id, cap, _ = _create_detection(db, payload)
            self._send_json({"job_id": job_id, "kind": "review",
                             "status": "queued", "seq_cap": cap}, 202)

        def get_detection_jobs(self):
            rows = db.connect().execute(
                "SELECT id, kind, status, farm_id, group_mode, rule_version, "
                "seq_cap, window_start, window_end, error_message, created_at "
                "FROM detection_jobs ORDER BY id DESC LIMIT 200"
            ).fetchall()
            self._send_json({"jobs": [dict(r) for r in rows]})

        def get_detection_job(self, job_id):
            r = db.connect().execute(
                "SELECT * FROM detection_jobs WHERE id=?", (job_id,)
            ).fetchone()
            if r is None:
                self._error(404, f"检测任务 {job_id} 不存在")
                return
            out = dict(r)
            out["items_json"] = json.loads(out["items_json"])
            self._send_json(out)

        def get_detection_results(self, job_id):
            with_samples = self._query().get("with_samples", ["0"])[0] == "1"
            conn = db.connect()
            rows = conn.execute(
                "SELECT * FROM detection_results WHERE job_id=? "
                "ORDER BY window_start, group_key, item", (job_id,)
            ).fetchall()
            results = []
            for r in rows:
                d = dict(r)
                d["detail_json"] = json.loads(d["detail_json"])
                if with_samples:
                    vs = conn.execute(
                        "SELECT rule_id, sample_json, failure_path "
                        "FROM violation_samples WHERE result_id=? ORDER BY id",
                        (r["id"],),
                    ).fetchall()
                    d["violation_samples"] = [
                        {**dict(v),
                         "sample_json": json.loads(v["sample_json"])}
                        for v in vs
                    ]
                results.append(d)
            self._send_json({"job_id": int(job_id), "results": results})

        # ---------- 告警与通知 ----------
        def get_alerts(self):
            q = self._query()
            sql = "SELECT * FROM alerts"
            args: list = []
            if "status" in q:
                sql += " WHERE status=?"
                args.append(q["status"][0])
            sql += " ORDER BY id DESC LIMIT 200"
            rows = db.connect().execute(sql, args).fetchall()
            self._send_json({"alerts": [dict(r) for r in rows]})

        def get_alert(self, alert_id):
            conn = db.connect()
            r = conn.execute(
                "SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
            if r is None:
                self._error(404, f"告警 {alert_id} 不存在")
                return
            alert = dict(r)
            wins = conn.execute(
                "SELECT dr.window_start, dr.window_end, dr.item, dr.status, "
                "dr.score, dr.threshold FROM detection_results dr "
                "JOIN detection_jobs dj ON dr.job_id=dj.id "
                "WHERE dj.kind='realtime' AND dr.group_key=? AND dr.item=? "
                "AND dr.window_end >= ? ORDER BY dr.window_start",
                (alert["group_key"], alert["item"],
                 alert["opened_window_end"]),
            ).fetchall()
            notifs = conn.execute(
                "SELECT id, event_type, status, attempts, window_end, "
                "last_error, sent_at, created_at FROM notifications "
                "WHERE alert_id=? ORDER BY id", (alert_id,),
            ).fetchall()
            events = conn.execute(
                "SELECT window_start, window_end, result_status, event, "
                "detail FROM alert_events WHERE alert_id=? ORDER BY id",
                (alert_id,),
            ).fetchall()
            self._send_json({
                "alert": alert,
                "windows": [dict(w) for w in wins],
                "notifications": [dict(n) for n in notifs],
                "event_stream": [dict(e) for e in events],
                "suppression": [
                    {"window_end": e["window_end"], "reason": e["detail"]}
                    for e in events if e["event"] == "suppressed"
                ],
                "recovery": next(
                    ({"recovered_window_end": alert["recovered_window_end"]}
                     for _ in [0] if alert["recovered_window_end"]), None),
            })

        def get_notifications(self):
            q = self._query()
            sql = ("SELECT id, idempotency_key, alert_id, event_type, status, "
                   "attempts, window_end, last_error, sent_at, created_at "
                   "FROM notifications")
            args: list = []
            if "status" in q:
                sql += " WHERE status=?"
                args.append(q["status"][0])
            sql += " ORDER BY id DESC LIMIT 200"
            rows = db.connect().execute(sql, args).fetchall()
            self._send_json({"notifications": [dict(r) for r in rows]})

        # ---------- 历史复核 ----------
        def get_review(self, job_id):
            conn = db.connect()
            job = conn.execute(
                "SELECT * FROM detection_jobs WHERE id=? AND kind='review'",
                (job_id,),
            ).fetchone()
            if job is None:
                self._error(404, f"复核任务 {job_id} 不存在")
                return
            r = conn.execute(
                "SELECT report_json, markdown FROM review_reports WHERE job_id=?",
                (job_id,),
            ).fetchone()
            if r is None:
                self._send_json({
                    "job_id": int(job_id), "status": job["status"],
                    "error_message": job["error_message"], "report": None,
                })
                return
            if self._query().get("format", ["json"])[0] == "markdown":
                body = r["markdown"].encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type",
                                 "text/markdown; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self._send_json({
                "job_id": int(job_id), "status": job["status"],
                "report": json.loads(r["report_json"]),
            })

        def get_health(self):
            seq = db.connect().execute(
                "SELECT COALESCE(MAX(ingest_seq),0) AS s FROM measurements"
            ).fetchone()["s"]
            self._send_json({"status": "ok", "max_ingest_seq": seq})

        def get_index(self):
            self._send_json({
                "service": "windwatch",
                "endpoints": {
                    "POST /api/batches": "提交采集批次，返回入库任务号",
                    "GET /api/ingest-tasks": "入库任务列表",
                    "GET /api/ingest-tasks/{id}": "入库任务状态与计数",
                    "GET /api/quarantine": "隔离区异常行与原因",
                    "GET /api/measurements?farm_id=": "有效测量视图(可带 seq_cap)",
                    "POST /api/rules": "提交规则版本(不可变)",
                    "GET /api/rules": "规则版本列表",
                    "GET /api/rules/{version}": "规则版本详情",
                    "POST /api/detections": "创建实时检测任务",
                    "GET /api/detection-jobs": "检测任务列表",
                    "GET /api/detection-jobs/{id}": "检测任务状态",
                    "GET /api/detection-results/{id}": "窗口检测明细(可带 with_samples=1)",
                    "GET /api/alerts": "告警列表(可按 status 过滤)",
                    "GET /api/alerts/{id}": "告警窗口/通知/抑制/恢复全过程",
                    "GET /api/notifications": "通知投递记录",
                    "POST /api/reviews": "创建历史复核异步任务",
                    "GET /api/reviews/{id}": "复核报告(JSON; ?format=markdown)",
                    "GET /health": "健康检查",
                },
            })

    GET_ROUTES = {
        "/": Handler.get_index,
        "/health": Handler.get_health,
        "/api/ingest-tasks": Handler.get_ingest_tasks,
        "/api/quarantine": Handler.get_quarantine,
        "/api/measurements": Handler.get_measurements,
        "/api/rules": Handler.get_rules,
        "/api/detection-jobs": Handler.get_detection_jobs,
        "/api/alerts": Handler.get_alerts,
        "/api/notifications": Handler.get_notifications,
    }
    GET_PARAM_ROUTES = [
        (re.compile(r"/api/ingest-tasks/(?P<task_id>\d+)$"),
         Handler.get_ingest_task),
        (re.compile(r"/api/rules/(?P<version>\d+)$"), Handler.get_rule_version),
        (re.compile(r"/api/detection-jobs/(?P<job_id>\d+)$"),
         Handler.get_detection_job),
        (re.compile(r"/api/detection-results/(?P<job_id>\d+)$"),
         Handler.get_detection_results),
        (re.compile(r"/api/alerts/(?P<alert_id>\d+)$"), Handler.get_alert),
        (re.compile(r"/api/reviews/(?P<job_id>\d+)$"), Handler.get_review),
    ]
    POST_ROUTES = {
        "/api/batches": Handler.post_batches,
        "/api/rules": Handler.post_rules,
        "/api/detections": Handler.post_detections,
        "/api/reviews": Handler.post_reviews,
    }
    Handler.GET_ROUTES = GET_ROUTES
    Handler.GET_PARAM_ROUTES = GET_PARAM_ROUTES
    Handler.POST_ROUTES = POST_ROUTES
    return Handler


def serve(db, cfg) -> ThreadingHTTPServer:
    handler = make_handler(db, cfg)
    return ThreadingHTTPServer((cfg.host, cfg.port), handler)
