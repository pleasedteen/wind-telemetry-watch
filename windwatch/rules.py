"""质量规则引擎。

规则以受校验的结构化 JSON 树提交（不接受 Python 表达式），支持：
- ``field_exists``  字段存在（非 null）
- ``range``         数值范围（min/max 可只给一个）
- ``in_set``        集合成员
- ``compare``       字段间 / 字段与常量比较
- ``all`` / ``any`` / ``not``  逻辑组合

安全约束：未知字段、错误类型、嵌套超过 8 层一律拒绝。
编译结果（字段索引、闭包）在同一次检测任务中复用，不按记录重复编译。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .common import MEASUREMENT_FIELDS

MAX_DEPTH = 8
COMPARE_OPS = (">", ">=", "<", "<=", "==", "!=")
LEAF_OPS = ("field_exists", "range", "in_set", "compare")
LOGIC_OPS = ("all", "any", "not")

# 记录在检测阶段以元组承载，这里固定字段下标
FIELD_INDEX = {name: i for i, name in enumerate(MEASUREMENT_FIELDS)}


class RuleValidationError(ValueError):
    pass


# ---------------------------------------------------------------- 校验与编译

def _require_dict(node: Any, where: str) -> dict:
    if not isinstance(node, dict):
        raise RuleValidationError(f"{where} 必须是表达式对象(dict)")
    return node


def _check_field(field: Any, where: str) -> int:
    if not isinstance(field, str) or field not in FIELD_INDEX:
        raise RuleValidationError(
            f"{where} 引用了未知字段: {field!r}，允许: {', '.join(MEASUREMENT_FIELDS)}"
        )
    return FIELD_INDEX[field]


def _depth(node: dict) -> int:
    op = node["op"]
    if op in LEAF_OPS:
        return 1
    if op == "not":
        return 1 + _depth(node["arg"])
    return 1 + max(_depth(child) for child in node["args"])


def validate_tree(node: Any, where: str = "expr") -> dict:
    """递归校验表达式树，返回带归一化信息的可编译副本。"""
    node = _require_dict(node, where)
    op = node.get("op")
    if not isinstance(op, str):
        raise RuleValidationError(f"{where} 缺少字符串类型的 op")

    if op == "field_exists":
        _check_field(node.get("field"), where)
        if set(node) - {"op", "field"}:
            raise RuleValidationError(f"{where}(field_exists) 含多余键")
        return {"op": op, "field": node["field"]}

    if op == "range":
        _check_field(node.get("field"), where)
        extra = set(node) - {"op", "field", "min", "max"}
        if extra:
            raise RuleValidationError(f"{where}(range) 含多余键: {sorted(extra)}")
        if "min" not in node and "max" not in node:
            raise RuleValidationError(f"{where}(range) 至少需要 min 或 max")
        for bound in ("min", "max"):
            if bound in node:
                v = node[bound]
                if isinstance(v, bool) or not isinstance(v, (int, float)):
                    raise RuleValidationError(f"{where}(range) {bound} 必须是数值")
        if ("min" in node and "max" in node) and node["min"] > node["max"]:
            raise RuleValidationError(f"{where}(range) min 不能大于 max")
        return {
            "op": op, "field": node["field"],
            "min": node.get("min"), "max": node.get("max"),
        }

    if op == "in_set":
        _check_field(node.get("field"), where)
        values = node.get("values")
        if not isinstance(values, list) or not values:
            raise RuleValidationError(f"{where}(in_set) values 必须是非空数组")
        types = {type(v) for v in values}
        if len(types) != 1 or next(iter(types)) not in (str, int, float, bool):
            raise RuleValidationError(
                f"{where}(in_set) values 必须是同类型的标量数组"
            )
        if set(node) - {"op", "field", "values"}:
            raise RuleValidationError(f"{where}(in_set) 含多余键")
        return {"op": op, "field": node["field"], "values": list(values)}

    if op == "compare":
        left = node.get("left")
        right = node.get("right")
        cmp = node.get("cmp")
        if cmp not in COMPARE_OPS:
            raise RuleValidationError(
                f"{where}(compare) cmp 必须是 {COMPARE_OPS} 之一"
            )
        if set(node) - {"op", "left", "cmp", "right"}:
            raise RuleValidationError(f"{where}(compare) 含多余键")
        return {
            "op": op,
            "left": _validate_operand(left, f"{where}.left"),
            "cmp": cmp,
            "right": _validate_operand(right, f"{where}.right"),
        }

    if op == "not":
        if "arg" not in node:
            raise RuleValidationError(f"{where}(not) 需要 arg")
        child = validate_tree(node["arg"], f"{where}.not")
        out = {"op": op, "arg": child}
        if _depth(out) > MAX_DEPTH:
            raise RuleValidationError(f"{where} 嵌套超过 {MAX_DEPTH} 层")
        return out

    if op in ("all", "any"):
        args = node.get("args")
        if not isinstance(args, list) or not args:
            raise RuleValidationError(f"{where}({op}) 需要非空 args 数组")
        children = [validate_tree(c, f"{where}.{op}[{i}]") for i, c in enumerate(args)]
        out = {"op": op, "args": children}
        if _depth(out) > MAX_DEPTH:
            raise RuleValidationError(f"{where} 嵌套超过 {MAX_DEPTH} 层")
        return out

    raise RuleValidationError(f"{where} 未知 op: {op!r}")


def _validate_operand(operand: Any, where: str) -> dict:
    operand = _require_dict(operand, where)
    keys = set(operand)
    if keys == {"field"}:
        return {"field": operand["field"], "idx": _check_field(operand["field"], where)}
    if keys == {"value"}:
        v = operand["value"]
        if isinstance(v, (dict, list)) or v is None:
            raise RuleValidationError(f"{where} 常量必须是非 null 标量")
        return {"value": v}
    raise RuleValidationError(f"{where} 只能是 {{'field': ...}} 或 {{'value': ...}}")


@dataclass(frozen=True)
class CompiledRule:
    rule_id: str
    name: str
    when: Callable[[tuple], bool] | None
    expr: Callable[[tuple], bool]
    raw_when: dict | None
    raw_expr: dict


def compile_rules(payload: Any) -> list[CompiledRule]:
    """校验并编译整份规则提交。"""
    if not isinstance(payload, dict):
        raise RuleValidationError("规则提交必须是对象，含 name 与 rules")
    rules = payload.get("rules")
    if not isinstance(rules, list) or not rules:
        raise RuleValidationError("rules 必须是非空数组")
    ids: set[str] = set()
    compiled: list[CompiledRule] = []
    for i, rule in enumerate(rules):
        if not isinstance(rule, dict):
            raise RuleValidationError(f"rules[{i}] 必须是对象")
        rid = rule.get("id")
        if not isinstance(rid, str) or not rid:
            raise RuleValidationError(f"rules[{i}].id 必须是非空字符串")
        if rid in ids:
            raise RuleValidationError(f"规则 id 重复: {rid}")
        ids.add(rid)
        name = rule.get("name", rid)
        if not isinstance(name, str):
            raise RuleValidationError(f"规则 {rid} 的 name 必须是字符串")
        raw_expr = validate_tree(rule.get("expr"), f"规则 {rid}.expr")
        raw_when = None
        when_fn = None
        if "when" in rule and rule["when"] is not None:
            raw_when = validate_tree(rule["when"], f"规则 {rid}.when")
            when_fn = _compile(raw_when)
        compiled.append(
            CompiledRule(
                rule_id=rid, name=name, when=when_fn, expr=_compile(raw_expr),
                raw_when=raw_when, raw_expr=raw_expr,
            )
        )
    return compiled


# ---------------------------------------------------------------- 快速编译路径

def _compile(node: dict) -> Callable[[tuple], bool]:
    op = node["op"]

    if op == "field_exists":
        idx = FIELD_INDEX[node["field"]]
        return lambda rec: rec[idx] is not None

    if op == "range":
        idx = FIELD_INDEX[node["field"]]
        lo, hi = node["min"], node["max"]
        if lo is not None and hi is not None:
            return lambda rec: (v := rec[idx]) is not None and lo <= v <= hi
        if lo is not None:
            return lambda rec: (v := rec[idx]) is not None and v >= lo
        return lambda rec: (v := rec[idx]) is not None and v <= hi

    if op == "in_set":
        idx = FIELD_INDEX[node["field"]]
        values = frozenset(node["values"])
        return lambda rec: (v := rec[idx]) is not None and v in values

    if op == "compare":
        l_idx = node["left"].get("idx")
        l_val = node["left"].get("value")
        r_idx = node["right"].get("idx")
        r_val = node["right"].get("value")
        cmp = node["cmp"]

        def left_of(rec):
            return l_val if l_idx is None else rec[l_idx]

        def right_of(rec):
            return r_val if r_idx is None else rec[r_idx]

        import operator
        fn = {
            ">": operator.gt, ">=": operator.ge, "<": operator.lt,
            "<=": operator.le, "==": operator.eq, "!=": operator.ne,
        }[cmp]
        return lambda rec: (a := left_of(rec)) is not None and (
            b := right_of(rec)) is not None and fn(a, b)

    if op == "not":
        inner = _compile(node["arg"])
        return lambda rec: not inner(rec)

    children = [_compile(c) for c in node["args"]]
    if op == "all":
        return lambda rec: all(fn(rec) for fn in children)
    return lambda rec: any(fn(rec) for fn in children)


# ---------------------------------------------------------------- 失败路径（仅样本用）

def evaluate_with_path(node: dict, rec: tuple, prefix: str = "expr") -> tuple[bool, str]:
    """评估表达式并返回人类可读的失败位置（用于违反样本定位）。"""
    op = node["op"]

    if op in LEAF_OPS:
        ok = _compile(node)(rec)
        label = _leaf_label(node)
        return ok, f"{prefix}.{label}"

    if op == "not":
        inner_ok, inner_path = evaluate_with_path(node["arg"], rec, f"{prefix}.not")
        return (not inner_ok), (inner_path if inner_ok else f"{prefix}.not(内层成立)")

    paths = []
    results = []
    for i, child in enumerate(node["args"]):
        ok, path = evaluate_with_path(child, rec, f"{prefix}.{op}[{i}]")
        results.append(ok)
        if not ok:
            paths.append(path)
    if op == "all":
        return all(results), " | ".join(paths) if paths else f"{prefix}.all"
    # any：全部失败才算失败
    return any(results), " | ".join(paths) if paths else f"{prefix}.any"


def _leaf_label(node: dict) -> str:
    op = node["op"]
    if op == "field_exists":
        return f"field_exists({node['field']})"
    if op == "range":
        return f"range({node['field']})"
    if op == "in_set":
        return f"in_set({node['field']})"
    return f"compare({node['left']} {node['cmp']} {node['right']})"
