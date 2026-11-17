"""历史复核报告与失败项定位报告（JSON + Markdown）。

历史报告只解释过去，不承担现场通知职责（worker 中 review 任务不推进告警）。
对比对象是同窗口 × 同分组 × 同检测项的原实时结果，
问题按新增 / 消失 / 仍存在三态归类。
"""
from __future__ import annotations

import json
from collections import defaultdict

from .common import WINDOW_MINUTES, utc_now_iso
from .detection import MISSING_STATUS, assign_bins, group_key_of


def build_review_report(conn, job, records, grouped, rules, rule_map,
                        produced: int) -> None:
    """汇总复核任务结果，与原实时结果对比，落库 review_reports。"""
    review_rows = conn.execute(
        "SELECT * FROM detection_results WHERE job_id=? ORDER BY id",
        (job["id"],),
    ).fetchall()

    # 原实时结果：同风场的实时任务，按 (窗口, 分组, 检测项) 对齐。
    # 复核常使用不同规则版本，因此这里不限制 rule_version。
    realtime = {}
    rt_jobs = [
        r["id"] for r in conn.execute(
            "SELECT id FROM detection_jobs WHERE kind='realtime' AND farm_id=? "
            "ORDER BY id",
            (job["farm_id"],),
        ).fetchall()
    ]
    if rt_jobs:
        placeholders = ",".join("?" * len(rt_jobs))
        old = conn.execute(
            f"SELECT * FROM detection_results WHERE job_id IN ({placeholders})",
            rt_jobs,
        ).fetchall()
        for r in old:
            key = (r["window_start"], r["window_end"], r["group_key"], r["item"])
            realtime.setdefault(key, r)  # 最早任务为准

    per_window: dict[str, list] = defaultdict(list)
    new_issues, gone_issues, persistent_issues = [], [], []
    for r in review_rows:
        key = (r["window_start"], r["window_end"], r["group_key"], r["item"])
        old_row = realtime.get(key)
        entry = {
            "window_start": r["window_start"], "window_end": r["window_end"],
            "group_key": r["group_key"], "item": r["item"],
            "review_status": r["status"], "review_score": r["score"],
            "threshold": r["threshold"],
            "realtime_status": old_row["status"] if old_row else None,
            "realtime_score": old_row["score"] if old_row else None,
            "changed": (old_row is None) or old_row["status"] != r["status"],
        }
        per_window[r["window_start"]].append(entry)

        if r["status"] == "fail":
            locator = build_locator(conn, r, records, job, rule_map)
            entry["locator"] = locator
            if old_row is None or old_row["status"] != "fail":
                new_issues.append(locator)
            else:
                persistent_issues.append(locator)
        if old_row is not None and old_row["status"] == "fail" and r["status"] != "fail":
            gone_issues.append({
                "item": r["item"], "group_key": r["group_key"],
                "window": {"start": r["window_start"], "end": r["window_end"]},
                "rule_version": job["rule_version"], "seq_cap": job["seq_cap"],
                "score": r["score"], "threshold": r["threshold"],
                "realtime_status": "fail", "realtime_score": old_row["score"],
                "review_status": r["status"], "review_score": r["score"],
                "detail": json.loads(r["detail_json"]),
                "disclaimer": "该问题在本次复核口径下消失；历史报告仅解释过去，"
                              "不回写实时结果，也不触发现场通知。",
            })

    report = {
        "job_id": job["id"],
        "kind": "review",
        "farm_id": job["farm_id"],
        "group_mode": job["group_mode"],
        "rule_version": job["rule_version"],
        "seq_cap": job["seq_cap"],
        "window_range": {"start": job["window_start"], "end": job["window_end"]},
        "result_count": produced,
        "windows": [
            {"window_start": ws, "results": sorted(entries, key=lambda e: e["item"])}
            for ws, entries in sorted(per_window.items())
        ],
        "issues": {
            "new": new_issues,
            "gone": gone_issues,
            "persistent": persistent_issues,
        },
        "notice_policy": "历史复核仅解释过去，不触发任何现场通知",
    }
    markdown = render_markdown(report)
    conn.execute(
        "INSERT INTO review_reports(job_id, report_json, markdown, created_at) "
        "VALUES(?,?,?,?)",
        (job["id"], json.dumps(report, ensure_ascii=False, indent=2),
         markdown, utc_now_iso()),
    )


# ---------------------------------------------------------------- 失败定位

def build_locator(conn, result_row, records, job, rule_map) -> dict:
    """单个失败项的可读定位报告。"""
    detail = json.loads(result_row["detail_json"])
    ws, we = result_row["window_start"], result_row["window_end"]
    gk = result_row["group_key"]
    item = result_row["item"]

    in_window = [
        r for r in records
        if group_key_of(r, job["group_mode"]) == gk
        and ws <= r["measured_at"] < we
    ]
    # 基线：此前连续六个窗口（同分组），按与检测相同的窗口规则重建。
    base = _baseline_records(records, gk, job["group_mode"], ws,
                             WINDOW_MINUTES)

    locator: dict = {
        "item": item,
        "group_key": gk,
        "window": {"start": ws, "end": we},
        "rule_version": job["rule_version"],
        "seq_cap": job["seq_cap"],
        "score": result_row["score"],
        "threshold": result_row["threshold"],
        "detail": detail,
    }

    if item in ("vibration_psi", "temperature_psi"):
        field = "vibration" if item == "vibration_psi" else "bearing_temp"
        cur_vals = [r[field] for r in in_window if r[field] is not None]
        base_vals = [r[field] for r in base if r[field] is not None]
        locator["data_ranges"] = {
            "current": _range_desc(cur_vals),
            "baseline": _range_desc(base_vals),
        }
        locator["anomaly_samples"] = _drift_samples(in_window, field, detail)
        locator["subgroup_evidence"] = _drift_subgroups(
            in_window, base, field, detail.get("cuts", []),
            by_turbine=True, limit=3,
        )
    elif item == "status_tvd":
        locator["data_ranges"] = {
            "current": _status_desc(in_window),
            "baseline": _status_desc(base),
        }
        locator["anomaly_samples"] = [
            _short(r) for r in in_window[:5]
        ]
        locator["subgroup_evidence"] = _tvd_subgroups(in_window, base)
    else:
        samples = [
            {
                "measurement": json.loads(v["sample_json"]),
                "failure_path": v["failure_path"],
            }
            for v in conn.execute(
                "SELECT sample_json, failure_path FROM violation_samples "
                "WHERE result_id=? ORDER BY id", (result_row["id"],)
            ).fetchall()
        ]
        locator["anomaly_samples"] = samples
        locator["subgroup_evidence"] = _rule_subgroups(
            conn, result_row, job, rule_map
        )

    locator["disclaimer"] = (
        "子组证据仅为定位线索，不宣称找到设备故障原因；"
        "设备异常与采集器固件问题需结合现场信息进一步判断。"
    )
    return locator


def _baseline_records(records, gk, group_mode, ws_iso, minutes):
    from datetime import timedelta
    from .common import parse_instant, window_start_for
    ws = parse_instant(ws_iso)
    out = []
    for r in records:
        if group_key_of(r, group_mode) != gk:
            continue
        rws = window_start_for(r["measured_epoch"], minutes)
        if ws - timedelta(minutes=minutes * 6) <= rws < ws:
            out.append(r)
    return out


def _range_desc(values):
    if not values:
        return {"samples": 0, "min": None, "max": None, "mean": None}
    return {
        "samples": len(values), "min": min(values), "max": max(values),
        "mean": sum(values) / len(values),
    }


def _status_desc(recs):
    counts: dict[str, int] = defaultdict(int)
    for r in recs:
        counts[r["operating_status"] or MISSING_STATUS] += 1
    return {"samples": len(recs), "categories": dict(sorted(counts.items()))}


def _short(r):
    return {k: r[k] for k in (
        "collector_id", "local_measure_id", "turbine_id", "firmware_version",
        "measured_at", "vibration", "bearing_temp", "operating_status",
    )}


def _drift_samples(in_window, field, detail):
    """落在与基线概率差最大的箱中的当前样本。"""
    cuts = detail.get("cuts", [])
    if not cuts:
        return [_short(r) for r in in_window[:5]]
    cur_p = detail["current_proportions"]
    base_p = detail["baseline_proportions"]
    target_bin = max(
        range(len(cur_p)), key=lambda i: cur_p[i] - base_p[i]
    )
    picked = []
    for r in in_window:
        v = r[field]
        if v is None:
            continue
        if assign_bins([v], cuts)[0] == target_bin:
            picked.append(_short(r))
        if len(picked) >= 5:
            break
    return picked


def _subgroup_split(recs, by_turbine):
    groups: dict[str, list] = defaultdict(list)
    for r in recs:
        key = r["turbine_id"] if by_turbine else r["firmware_version"]
        groups[key].append(r)
    return groups


def _drift_subgroups(in_window, base, field, cuts, by_turbine, limit):
    """漂移子组贡献：子组各箱计数 / 各侧总体有效样本数 所得向量之差，
    绝对值之和。按机组与固件两个维度都给出，取贡献前三。"""
    cur_vals = [r[field] for r in in_window if r[field] is not None]
    base_vals = [r[field] for r in base if r[field] is not None]
    n_cur, n_base = len(cur_vals), len(base_vals)
    if not cuts or n_cur == 0 or n_base == 0:
        return []

    def vector(recs):
        vals = [r[field] for r in recs if r[field] is not None]
        counts = [0] * (len(cuts) + 1)
        for b in assign_bins(vals, cuts):
            counts[b] += 1
        return counts, len(vals)

    evidence = []
    for dimension, turbine in (("turbine", True), ("firmware", False)):
        cur_groups = _subgroup_split(
            [r for r in in_window if r[field] is not None], turbine)
        base_groups = _subgroup_split(
            [r for r in base if r[field] is not None], turbine)
        for key in sorted(set(cur_groups) | set(base_groups)):
            c_counts, c_n = vector(cur_groups.get(key, []))
            b_counts, b_n = vector(base_groups.get(key, []))
            contrib = sum(
                abs((c_counts[i] / n_cur) - (b_counts[i] / n_base))
                for i in range(len(c_counts))
            )
            evidence.append({
                "dimension": dimension, "subgroup": key,
                "contribution": contrib,
                "current_samples": c_n, "baseline_samples": b_n,
            })
    evidence.sort(key=lambda e: e["contribution"], reverse=True)
    return evidence[:limit]


def _tvd_subgroups(in_window, base):
    def cat_vec(recs):
        counts: dict[str, float] = defaultdict(float)
        for r in recs:
            counts[r["operating_status"] or MISSING_STATUS] += 1
        return counts, len(recs)

    n_cur, n_base = len(in_window), len(base)
    evidence = []
    for dimension, turbine in (("turbine", True), ("firmware", False)):
        cg = _subgroup_split(in_window, turbine)
        bg = _subgroup_split(base, turbine)
        for key in sorted(set(cg) | set(bg)):
            c_counts, c_n = cat_vec(cg.get(key, []))
            b_counts, b_n = cat_vec(bg.get(key, []))
            cats = set(c_counts) | set(b_counts)
            contrib = sum(
                abs(c_counts.get(c, 0) / max(n_cur, 1)
                    - b_counts.get(c, 0) / max(n_base, 1))
                for c in cats
            )
            evidence.append({
                "dimension": dimension, "subgroup": key,
                "contribution": contrib,
                "current_samples": c_n, "baseline_samples": b_n,
            })
    evidence.sort(key=lambda e: e["contribution"], reverse=True)
    return evidence[:3]


def _rule_subgroups(conn, result_row, job, rule_map):
    """规则违反率：按各子组违反条数对总体违反条数的贡献降序。"""
    detail = json.loads(result_row["detail_json"])
    total_violations = detail.get("violations", 0)
    samples = [
        json.loads(v["sample_json"])
        for v in conn.execute(
            "SELECT sample_json FROM violation_samples WHERE result_id=?",
            (result_row["id"],)
        ).fetchall()
    ]
    if not samples or total_violations == 0:
        return []
    evidence = []
    for dimension, keyfield in (("turbine", "turbine_id"),
                                ("firmware", "firmware_version")):
        counts: dict[str, int] = defaultdict(int)
        for s in samples:
            counts[s[keyfield]] += 1
        for key, n in sorted(counts.items(), key=lambda kv: -kv[1]):
            evidence.append({
                "dimension": dimension, "subgroup": key,
                "violations_in_sample": n,
                "share_of_total_violations": n / total_violations,
            })
    evidence.sort(key=lambda e: e["share_of_total_violations"], reverse=True)
    return evidence[:3]


# ---------------------------------------------------------------- Markdown

def render_markdown(report: dict) -> str:
    lines = [
        f"# 历史复核报告 (job {report['job_id']})",
        "",
        f"- 风场: `{report['farm_id']}`；分组模式: `{report['group_mode']}`",
        f"- 规则版本: {report['rule_version']}；入库序号上限: {report['seq_cap']}",
        f"- 窗口区间: {report['window_range']['start']} → "
        f"{report['window_range']['end']}",
        f"- 明细条数: {report['result_count']}",
        f"- 通知策略: {report['notice_policy']}",
        "",
        "## 问题总览",
        f"- 新增问题: {len(report['issues']['new'])}",
        f"- 消失问题: {len(report['issues']['gone'])}",
        f"- 仍存在问题: {len(report['issues']['persistent'])}",
        "",
    ]
    for block, title in (("new", "新增问题"), ("persistent", "仍存在问题"),
                         ("gone", "消失问题")):
        issues = report["issues"][block]
        if not issues:
            continue
        lines.append(f"## {title}")
        for it in issues:
            lines.append(f"### {it['item']} @ {it['group_key']} "
                         f"({it['window']['start']} ~ {it['window']['end']})")
            lines.append(f"- 得分 {it['score']} / 阈值 {it['threshold']}")
            if "data_ranges" in it:
                lines.append(
                    f"- 当前侧: {json.dumps(it['data_ranges']['current'], ensure_ascii=False)}")
                lines.append(
                    f"- 基线侧: {json.dumps(it['data_ranges']['baseline'], ensure_ascii=False)}")
            if it.get("anomaly_samples"):
                lines.append("- 主要异常样本:")
                for s in it["anomaly_samples"][:3]:
                    lines.append(f"  - `{json.dumps(s, ensure_ascii=False)}`")
            if it.get("subgroup_evidence"):
                lines.append("- 子组证据（仅定位线索）:")
                for e in it["subgroup_evidence"]:
                    if "contribution" in e:
                        lines.append(
                            f"  - {e['dimension']}={e['subgroup']} "
                            f"贡献={e['contribution']:.4f} "
                            f"(当前样本 {e['current_samples']}/基线 "
                            f"{e['baseline_samples']})")
                    else:
                        lines.append(
                            f"  - {e['dimension']}={e['subgroup']} "
                            f"违反占比={e['share_of_total_violations']:.4f} "
                            f"(样本内违反 {e['violations_in_sample']} 条)")
            lines.append(f"- {it['disclaimer']}")
            lines.append("")

    lines.append("## 逐窗口结果")
    for w in report["windows"]:
        lines.append(f"### {w['window_start']}")
        lines.append("| 分组 | 检测项 | 复核 | 实时 | 得分 | 阈值 |")
        lines.append("|---|---|---|---|---|---|")
        for e in w["results"]:
            lines.append(
                f"| {e['group_key']} | {e['item']} | {e['review_status']} | "
                f"{e['realtime_status'] or '—'} | {e['review_score']} | "
                f"{e['threshold']} |")
        lines.append("")
    return "\n".join(lines)
