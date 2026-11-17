"""SQLite 持久化层：连接管理与 schema 初始化。

设计要点：
- WAL 模式 + 忙等待，支持 API 线程与后台 worker 线程并发；
- 所有写操作走短事务，批次入库与告警/通知在同一事务内提交，
  保证服务崩溃后不会出现查不到状态的半批数据；
- ``ingest_seq`` 是每次入库提交的全局递增序号，检测任务固定该上限。
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

SCHEMA = r"""
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- 入库任务（批次接收后立即落库，后台执行）
CREATE TABLE IF NOT EXISTS ingest_tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id        TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('queued','running','done','error')),
    received_count  INTEGER NOT NULL DEFAULT 0,
    quarantined_count INTEGER NOT NULL DEFAULT 0,
    valid_delta     INTEGER NOT NULL DEFAULT 0,
    error_message   TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

-- 收到的原始批次内容（逐行保留，含隔离行，保证可追溯）
CREATE TABLE IF NOT EXISTS raw_batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL REFERENCES ingest_tasks(id),
    row_index       INTEGER NOT NULL,
    raw_json        TEXT NOT NULL,
    is_quarantined  INTEGER NOT NULL DEFAULT 0
);

-- 隔离区：异常行及原因
CREATE TABLE IF NOT EXISTS quarantine (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL REFERENCES ingest_tasks(id),
    row_index       INTEGER NOT NULL,
    raw_json        TEXT NOT NULL,
    reason          TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

-- 有效测量（合法行，保留全部修订版，有效视图按 identity 取最新修订）
CREATE TABLE IF NOT EXISTS measurements (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ingest_seq          INTEGER NOT NULL,
    ingest_task_id      INTEGER NOT NULL REFERENCES ingest_tasks(id),
    farm_id             TEXT NOT NULL,
    collector_id        TEXT NOT NULL,
    local_measure_id    TEXT NOT NULL,
    revision            INTEGER NOT NULL,
    measured_at         TEXT NOT NULL,      -- ISO-8601 UTC
    measured_epoch      REAL NOT NULL,      -- 窗口划分与排序用
    turbine_id          TEXT NOT NULL,
    firmware_version    TEXT NOT NULL,
    vibration           REAL,
    bearing_temp        REAL,
    operating_status    TEXT,               -- 允许 NULL（缺失状态单列一类）
    row_hash            TEXT NOT NULL,
    UNIQUE(ingest_task_id, row_hash)
);

CREATE INDEX IF NOT EXISTS idx_m_effective
    ON measurements(farm_id, collector_id, local_measure_id, ingest_seq, revision);
CREATE INDEX IF NOT EXISTS idx_m_window
    ON measurements(farm_id, measured_epoch);
CREATE INDEX IF NOT EXISTS idx_m_fw
    ON measurements(farm_id, firmware_version, measured_epoch);

-- 规则版本（提交即不可修改）
CREATE TABLE IF NOT EXISTS rule_versions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    version         INTEGER NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    rules_json      TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

-- 检测任务（实时 / 历史复核统一登记）
CREATE TABLE IF NOT EXISTS detection_jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    kind            TEXT NOT NULL CHECK (kind IN ('realtime','review')),
    status          TEXT NOT NULL CHECK (status IN ('queued','running','done','error')),
    farm_id         TEXT NOT NULL,
    group_mode      TEXT NOT NULL CHECK (group_mode IN ('farm','farm_firmware')),
    rule_version    INTEGER NOT NULL,
    seq_cap         INTEGER NOT NULL,
    window_start    TEXT,                  -- 实时：起点；复核：区间起点
    window_end      TEXT,                  -- 复核：区间终点（开区间语义对齐窗口）
    items_json      TEXT NOT NULL,        -- 检测项与阈值
    error_message   TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

-- 每个窗口 × 分组 × 检测项的明细结果
CREATE TABLE IF NOT EXISTS detection_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          INTEGER NOT NULL REFERENCES detection_jobs(id),
    window_start    TEXT NOT NULL,
    window_end      TEXT NOT NULL,
    group_key       TEXT NOT NULL,
    item            TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pass','fail','undetermined')),
    score           REAL,                  -- undetermined 时为 NULL，不伪造零分
    threshold       REAL NOT NULL,
    detail_json     TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dr_lookup
    ON detection_results(group_key, item, window_start);
-- 实时发布水位查询：按检测项×分组取已发布的最晚窗口起点
CREATE INDEX IF NOT EXISTS idx_dr_publish
    ON detection_results(item, group_key, window_start);

-- 规则违反样本（少量、带表达式失败位置）
CREATE TABLE IF NOT EXISTS violation_samples (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    result_id       INTEGER NOT NULL REFERENCES detection_results(id),
    rule_id         TEXT NOT NULL,
    sample_json     TEXT NOT NULL,
    failure_path    TEXT NOT NULL
);

-- 告警生命周期（规则版本 × 检测项 × 分组唯一）
CREATE TABLE IF NOT EXISTS alerts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_version        INTEGER NOT NULL,
    item                TEXT NOT NULL,
    group_key           TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('open','recovered')),
    opened_window_end   TEXT NOT NULL,
    recovered_window_end TEXT,
    consecutive_ok      INTEGER NOT NULL DEFAULT 0,
    last_remind_window_end TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    UNIQUE(rule_version, item, group_key)
);

-- 待发送 / 已发送通知（与告警变化同事务持久化）
CREATE TABLE IF NOT EXISTS notifications (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT NOT NULL UNIQUE,
    alert_id        INTEGER REFERENCES alerts(id),
    event_type      TEXT NOT NULL,         -- opened / recovered / reminder
    payload_json    TEXT NOT NULL,
    window_end      TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending','sent','failed')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TEXT NOT NULL,
    sent_at         TEXT
);

-- 每个窗口的告警机事件流（含被抑制的提醒及原因，抑制≠跳过检测）
CREATE TABLE IF NOT EXISTS alert_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id        INTEGER REFERENCES alerts(id),
    rule_version    INTEGER NOT NULL,
    item            TEXT NOT NULL,
    group_key       TEXT NOT NULL,
    window_start    TEXT NOT NULL,
    window_end      TEXT NOT NULL,
    result_status   TEXT NOT NULL,
    event           TEXT NOT NULL,
    detail          TEXT,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ae_lookup
    ON alert_events(rule_version, item, group_key, window_start);

-- 历史复核报告
CREATE TABLE IF NOT EXISTS review_reports (
    job_id              INTEGER PRIMARY KEY REFERENCES detection_jobs(id),
    report_json         TEXT NOT NULL,
    markdown            TEXT NOT NULL,
    created_at          TEXT NOT NULL
);
"""


class Database:
    """线程内自建连接的轻量连接管理器。

    写操作通过 ``write_lock`` 全局序列化，避免后台 worker / notifier /
    API 线程并发写时事务互相干扰（SQLite 单写者模型）。
    """

    def __init__(self, path: str):
        self.path = str(path)
        self._local = threading.local()
        self._init_lock = threading.Lock()
        self.write_lock = threading.RLock()
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                self.path, timeout=30, isolation_level=None, check_same_thread=False
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA busy_timeout=30000;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            self._local.conn = conn
        return conn

    def transaction(self):
        """返回写事务上下文，进入即取全局写锁并 BEGIN IMMEDIATE。"""
        conn = self.connect()
        return _WriteTransaction(conn, self.write_lock)

    def initialize(self) -> None:
        with self._init_lock:
            conn = self.connect()
            conn.executescript(SCHEMA)

    def reset_stuck_jobs(self) -> None:
        """重启恢复：把崩溃时停在 running 的任务重新排队，由 worker 继续完成。"""
        conn = self.connect()
        with self.transaction():
            conn.execute(
                "UPDATE ingest_tasks SET status='queued', updated_at=? "
                "WHERE status='running'",
                (_now(),),
            )
            conn.execute(
                "UPDATE detection_jobs SET status='queued', updated_at=? "
                "WHERE status='running'",
                (_now(),),
            )


class _WriteTransaction:
    def __init__(self, conn, lock):
        self.conn = conn
        self.lock = lock

    def __enter__(self):
        self.lock.acquire()
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.conn.execute("COMMIT")
            else:
                self.conn.execute("ROLLBACK")
        finally:
            self.lock.release()
        return False


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()
