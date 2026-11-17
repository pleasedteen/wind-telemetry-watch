# windwatch — 风电场遥测质量监控服务

帮助运维人员判断振动、温度异常是**设备问题**还是**某批采集器固件把数据传坏了**。
服务接收采集批次、留存可追溯数据、运行质量规则与分布漂移检测，
并把需要处理的问题推送到项目自带的本地通知接收端。

* 运行时**零第三方依赖**：仅 Python 3.11 标准库（`http.server` + `sqlite3` + `urllib`）
* 业务状态存 SQLite（WAL），HTTP API 覆盖批次、任务、结果、告警、复核报告
* 自带可启动的**模拟采集服务**与**通知接收端**，全部数据本地化，不访问真实外站

---

## 1. 安装与启动

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt        # 仅安装 pytest（运行本身不需要第三方包）

# 启动主服务 + 两个本地模拟器（采集器 :8091，通知接收端 :8090）
python -m windwatch --with-simulators
# 仅主服务（自定义端口/数据库/通知地址）
python -m windwatch --port 8080 --db ./windwatch.db \
    --notify-url http://127.0.0.1:8090/notify
```

启动后：

| 组件 | 地址 |
|---|---|
| 主服务 API | `http://127.0.0.1:8080` |
| 通知接收端消息查询 | `http://127.0.0.1:8090/messages` |
| 模拟采集服务场景列表 | `http://127.0.0.1:8091/scenarios` |

## 2. 一条命令验证 / 测试

```bash
# 端到端验证：启动本地依赖 → HTTP 回放全场景 → 断连/重启恢复 → 十万条性能
# 预算 80 秒，其中十万条规模检测要求 30 秒内完成；使用临时数据库，无需人工操作
python -m windwatch.verify

# 自动化测试（真实 HTTP + SQLite + 后台任务 + 通知链路 + 重启恢复）
python -m pytest
```

## 3. API 速览与示例

完整示例见 `examples/api_walkthrough.py`（可直接运行）。

### 3.1 提交批次（返回持久化任务号，后台异步入库）

```bash
curl -sX POST localhost:8080/api/batches -H 'Content-Type: application/json' -d '{
  "batch_id": "b-20261110-01",
  "measurements": [{
    "farm_id": "F1", "collector_id": "C2", "local_measure_id": "M00421",
    "revision": 1, "measured_at": "2026-11-10T00:05:00+00:00",
    "turbine_id": "T03", "firmware_version": "3.1.9",
    "vibration": 4.62, "bearing_temp": 58.1, "operating_status": "generating"
  }]
}'
# -> 202 {"task_id": 1, "status": "queued", ...}
```

测量字段：风场编号、站内采集器编号、采集器本地测量号、修订号、带时区测量时间、
机组编号、固件版本、振动值、轴承温度、运行状态（数值/状态允许 `null`）。

### 3.2 任务状态 / 隔离区 / 有效测量

```bash
curl -s localhost:8080/api/ingest-tasks/1          # 接收/隔离/有效变化数
curl -s localhost:8080/api/quarantine?task_id=1    # 异常行 + 原因
curl -s "localhost:8080/api/measurements?farm_id=F1&seq_cap=12"
```

### 3.3 规则版本（结构化表达式树，提交即不可变）

```bash
curl -sX POST localhost:8080/api/rules -H 'Content-Type: application/json' -d '{
  "name": "baseline",
  "rules": [{
    "id": "gen_temp", "name": "发电态轴承温度",
    "when": {"op":"compare","left":{"field":"operating_status"},
             "cmp":"==","right":{"value":"generating"}},
    "expr": {"op":"range","field":"bearing_temp","min":30,"max":80}
  }]
}'
```

支持 `field_exists` / `range` / `in_set` / `compare` 与 `all` / `any` / `not`；
拒绝未知字段、错误类型、超过 8 层嵌套和任何非结构化表达式。

### 3.4 检测任务（固定入库序号上限，按 UTC 十分钟窗口）

```bash
curl -sX POST localhost:8080/api/detections -H 'Content-Type: application/json' -d '{
  "farm_id": "F1", "group_mode": "farm_firmware",
  "rule_version": 1,
  "items": {"vibration_psi": {}, "temperature_psi": {},
            "status_tvd": {}, "rule:gen_temp": {}}
}'
curl -s localhost:8080/api/detection-results/1?with_samples=1
```

实时检测默认从最早已发布窗口之后继续发布；也可用 `window_start` / `window_end`
（左闭右开）指定时间范围，只限制**发布**哪些窗口，基线仍由完整历史累积：

```bash
curl -sX POST localhost:8080/api/detections -H 'Content-Type: application/json' -d '{
  "farm_id": "F1", "group_mode": "farm_firmware", "rule_version": 1,
  "window_start": "2026-11-10T00:20:00+00:00",
  "window_end": "2026-11-10T01:00:00+00:00",
  "items": {"vibration_psi": {}}
}'
```

重复发起或补传修订后重检时，旧窗口只保留首次发布结果，不会重新发布明细或通知；
严格晚于已发布水位的窗口照常检测。

### 3.5 告警 / 通知 / 历史复核

```bash
curl -s localhost:8080/api/alerts
curl -s localhost:8080/api/alerts/1            # 窗口、发送记录、抑制原因、恢复过程
curl -s localhost:8080/api/notifications
curl -sX POST localhost:8080/api/reviews -H 'Content-Type: application/json' -d '{
  "farm_id":"F1","rule_version":2,
  "window_start":"2026-11-10T00:00:00+00:00",
  "window_end":"2026-11-10T02:00:00+00:00"
}'
curl -s localhost:8080/api/reviews/2?format=markdown
```

## 4. 关键设计说明

### 批次与有效视图
* 原始批次逐行留存（`raw_batches`），异常行进隔离区（`quarantine`，带原因），
  合法行进入 `measurements`；每次入库提交分配全局递增 `ingest_seq`。
* 有效视图按 `(风场, 采集器, 本地测量号)` 取 `seq_cap` 内最高修订版；
  重复回放包幂等跳过，`valid_delta` 统计本任务导致有效视图变化的**测量身份数**。
* 入库在单事务内提交；崩溃后重启把 `running` 任务重新排队续跑，不产生半批数据。

### 检测
* 窗口为 UTC 十分钟整点、左闭右开；可按风场或风场+固件分组。
* 一次检测先固定 `seq_cap`，规则/基线/报告全部限定在该数据范围，数据只读一次、
  规则只编译一次并在任务内复用（不按记录查库）。
* **PSI**：当前窗口对比此前连续 6 个窗口合并基线；两侧有效数值样本各 ≥30 才可判定；
  基线分位点取排序后第 `ceil(p*n)` 个（p=0.25/0.5/0.75），重复切点合并、切点归左、
  分箱覆盖整个数轴；各箱计数 +0.5 后分别归一化，
  `PSI=Σ(cur-base)·ln(cur/base)`，超阈值失败；缺失值不参与分布。
* **状态总变差距离**：类别取双方并集、缺失状态单列一类，
  `TVD=½Σ|P_cur−P_base|`；两侧总样本各 ≥30，超阈值失败。
* 样本不足一律 `undetermined`（`score=null`），绝不伪造零分。
* 行级规则按窗口×分组汇总违反率；适用条件不满足的记录不进分母，分母为零无法判定；
  失败结果保留少量违反样本及规则树失败位置。
* **只向前发布**：同一 `(规则版本, 检测项, 分组)` 以已发布的实时明细为持久水位
  （跨任务、进程重启仍然有效）。重复发起实时检测、补传修订后重检时，旧窗口只保留
  首次发布结果，不再重算发布；严格晚于水位的窗口正常检测。历史复核始终按区间
  完整重算，可展示补传后的差异，但**不写实时结果、不推进水位、不触发通知**。
* 实时任务可用 `window_start` / `window_end`（左闭右开）限定发布范围；
  该范围只筛选发布窗口，基线仍由完整历史累积，不会因范围起点而截断漂移基线。

### 告警与通知
* 同一 `(规则版本, 检测项, 分组)` 维护一条生命周期：失败打开，连续两个正常窗口恢复；
  首次打开与恢复均通知；持续失败按**被检测窗口结束时间**间隔 30 分钟节流提醒，
  抑制原因和每窗口事件都持久化（抑制 ≠ 跳过检测）。
* 告警变化与待发通知在同一事务落库；后台线程经 HTTP 投递，带幂等键、失败重试；
  模拟接收端支持“先收下再断连”，重启后 pending 通知继续投递。

### 历史复核
* 指定窗口区间、规则版本、`seq_cap` 的异步任务；不倒写已发布的实时结果，
  报告对比原实时结果（同规则版本的现场明细优先），按新增 / 消失 / 仍存在归类问题，
  且**不触发任何现场通知**。补传修订后复核可展示与现场告警的差异。
* 每个失败项输出可读 JSON 与 Markdown：当前/基线数据范围、样本数、规则版本、
  主要异常样本、按机组与固件的子组证据（漂移项给箱概率差贡献前三，
  规则项给违反条数贡献排序），并附“仅定位线索、不宣称故障原因”的免责声明。

## 5. 目录结构

```
windwatch/
  config.py      环境变量配置
  db.py          SQLite schema、连接与写事务管理
  common.py      时间窗口、测量行校验
  rules.py       结构化规则树的校验/编译/失败路径
  detection.py   有效视图、PSI、TVD、窗口推进引擎
  alerts.py      告警生命周期与告警事件流
  notifier.py    后台通知投递与重试
  worker.py      入库/检测后台任务（含重启恢复）
  report.py      历史复核与定位报告（JSON + Markdown）
  api.py         HTTP 路由
  app.py         服务装配
  simulator.py   模拟采集服务 + 通知接收端
  scenarios.py   固定种子回放数据 + 十万条生成器
  verify.py      python -m windwatch.verify 端到端验证
tests/           pytest 套件（算法、集成、告警、规模性能）
examples/        API 走读脚本
```
