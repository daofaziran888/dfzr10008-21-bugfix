"""数据管线编排业务规则：状态流转、字段校验与筛选口径都收在这里。

管线节点生命周期固定为 草稿 → 运行中 → 已暂停 → 已完成，只能单向推进：
- 启动管线：草稿 → 运行中；已暂停 → 运行中（暂停后恢复回到原运行状态）；
  处于失败（异常）状态的节点走“按依赖恢复”，恢复后回到运行中。
- 暂停管线：运行中 → 已暂停。
- 归档管线：已暂停 → 已完成；归档后不允许再切换或跳级，完成节点不得重跑。

进入运行中之前必须满足节点依赖：已登记的上游节点全部完成且未失败，
否则节点保持等待。启动成功时生成唯一的管线运行结果，输出格式快照与
节点依赖状态写在同一个版本里，不允许分叉；并发启动由模块级锁保证只留
一个运行结果。
"""
from __future__ import annotations

import re
import threading
from datetime import datetime, timezone
from typing import Any

from app.store import store

MODULE = "pipeline"
REQUIRED_FIELDS = ["节点编号", "节点名称", "上游节点"]
ENTRY_FIELDS = [
    "节点编号",
    "节点名称",
    "上游节点",
    "处理逻辑",
    "输出格式",
    "错误处理",
    "重试策略",
    "节点状态",
]
STATUS_ORDER = ["草稿", "运行中", "已暂停", "已完成"]
TERMINAL_STATUS = STATUS_ORDER[-1]
DRAFT_STATUS = STATUS_ORDER[0]
RUNNING_STATUS = STATUS_ORDER[1]
PAUSED_STATUS = STATUS_ORDER[2]

# 动作 → 允许执行该动作的当前状态；目标状态由生命周期顺序推导，不允许跳级。
ACTION_RULES = {
    "启动管线": {DRAFT_STATUS, RUNNING_STATUS, PAUSED_STATUS},
    "暂停管线": {RUNNING_STATUS},
    "归档管线": {PAUSED_STATUS},
    "标记失败": {RUNNING_STATUS},
    "恢复节点": {DRAFT_STATUS, RUNNING_STATUS, PAUSED_STATUS},
}
NEGATIVE_ACTIONS = ["标记失败"]

# 同一管线的并发启动只留一个运行结果：状态检查、流转与快照写入放在同一把锁里。
ACTION_LOCK = threading.Lock()

_UPSTREAM_SPLIT = re.compile(r"[,，、;；\s]+")
_RETRY_LIMIT = re.compile(r"\d+")


def _retry_limit(raw: Any) -> int | None:
    """从重试策略文本（如“3 次”）里解析允许的重试次数；解析不了就不设上限。"""
    match = _RETRY_LIMIT.search(str(raw or ""))
    return int(match.group()) if match else None


class PipelineService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("节点编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        with ACTION_LOCK:
            rows = store.rows(MODULE)
            entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
            # 登记时保留全部节点字段，上游节点、处理逻辑、输出格式、重试策略不再丢失。
            for field in ENTRY_FIELDS:
                value = values.get(field)
                if value is not None and str(value).strip() != "":
                    entry[field] = value
            entry["status"] = DRAFT_STATUS
            entry["节点状态"] = DRAFT_STATUS  # 列表页节点状态与生命周期同版
            entry["pending"] = True
            entry["abnormal"] = False
            entry["已重试次数"] = 0
            rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        with ACTION_LOCK:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"管线节点 {entry_id} 不存在或已归档"
            if action not in ACTION_RULES:
                return None, f"动作「{action}」不属于数据管线编排可执行范围"
            status = str(entry.get("status") or DRAFT_STATUS)
            if status == TERMINAL_STATUS:
                return None, "管线节点已归档完成，生命周期已结束，不允许再切换状态或重跑"
            if status not in ACTION_RULES[action]:
                return None, self._order_message(action, status)
            if action == "启动管线":
                return self._start(entry, status)
            if action == "暂停管线":
                return self._pause(entry)
            if action == "归档管线":
                return self._archive(entry)
            if action == "标记失败":
                return self._fail(entry)
            return self._recover(entry)

    def _order_message(self, action: str, status: str) -> str:
        hints = {
            "启动管线": "启动仅用于草稿启动、暂停后恢复或失败恢复",
            "暂停管线": "仅运行中的节点可以暂停",
            "归档管线": "节点需先暂停再归档，启动、暂停、归档只能按顺序单向推进",
            "标记失败": "仅运行中的节点可以标记失败",
            "恢复节点": "仅失败（异常）的节点需要恢复",
        }
        hint = hints.get(action, "生命周期只能按状态顺序流转")
        return f"当前状态为「{status}」，{hint}"

    def _start(self, entry: dict[str, Any], status: str) -> tuple[dict[str, Any] | None, str]:
        if entry.get("abnormal"):
            # 失败节点的“启动”就是按依赖恢复，走同一条恢复链路。
            return self._recover(entry)
        if status == RUNNING_STATUS:
            # 并发或重复启动：只保留当前这一个运行结果，不再生成新的。
            return None, "管线节点已在运行中，重复启动已忽略，只保留当前运行结果"
        blockers = self._dependency_blockers(entry)
        if blockers:
            code = entry.get("节点编号") or "该节点"
            return None, f"{'；'.join(blockers)}，{code} 保持等待"
        self._transition(entry, RUNNING_STATUS)
        if status == DRAFT_STATUS:
            # 全新启动才生成新的运行结果；暂停后恢复沿用原运行结果。
            self._open_run_result(entry, "启动管线")
            return entry, "管线节点已启动管线，进入运行中"
        return entry, "管线节点已从暂停恢复到运行中"

    def _pause(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        self._transition(entry, PAUSED_STATUS)
        return entry, "管线节点已暂停管线"

    def _archive(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        self._transition(entry, TERMINAL_STATUS)
        return entry, "管线节点已归档管线"

    def _fail(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        if entry.get("abnormal"):
            return None, "管线节点已处于失败状态，请勿重复标记"
        entry["abnormal"] = True
        entry["pending"] = True
        return entry, "管线节点已标记失败，下游节点将保持等待"

    def _recover(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        if not entry.get("abnormal"):
            return None, "管线节点未处于失败状态，无需恢复"
        blockers = self._dependency_blockers(entry)
        if blockers:
            return None, f"{'；'.join(blockers)}，失败节点暂不能按依赖恢复"
        limit = _retry_limit(entry.get("重试策略"))
        used = int(entry.get("已重试次数") or 0)
        if limit is not None and used >= limit:
            return None, f"重试策略允许的 {limit} 次重试已用尽，节点保持失败"
        entry["abnormal"] = False
        entry["已重试次数"] = used + 1
        self._transition(entry, RUNNING_STATUS)
        # 恢复产生新的运行结果版本，输出格式快照与节点依赖状态仍保持同版。
        self._open_run_result(entry, "恢复节点")
        return entry, "管线节点已按依赖恢复，重新进入运行中"

    def _transition(self, entry: dict[str, Any], target: str) -> None:
        entry["status"] = target
        entry["节点状态"] = target  # 列表页节点状态同步流转，不再残留旧状态
        entry["pending"] = target != TERMINAL_STATUS

    def _open_run_result(self, entry: dict[str, Any], action: str) -> None:
        previous = entry.get("运行结果") or {}
        version = int(previous.get("版本") or 0) + 1
        entry["运行结果"] = {
            "版本": version,
            "输出格式快照": entry.get("输出格式"),
            "节点依赖状态": self._dependency_snapshot(entry),
            "触发动作": action,
            "生成时间": datetime.now(timezone.utc).isoformat(),
        }

    def _upstream_codes(self, entry: dict[str, Any]) -> list[str]:
        raw = str(entry.get("上游节点") or "")
        return [code for code in _UPSTREAM_SPLIT.split(raw) if code]

    def _find_by_code(self, code: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if str(row.get("节点编号") or "") == code:
                return row
        return None

    def _dependency_blockers(self, entry: dict[str, Any]) -> list[str]:
        """上游未就绪（未完成或已失败）时给出阻塞原因；未登记的上游不参与拦截。"""
        blockers: list[str] = []
        for code in self._upstream_codes(entry):
            upstream = self._find_by_code(code)
            if upstream is None or upstream is entry:
                continue
            if upstream.get("abnormal"):
                blockers.append(f"上游节点 {code} 处于失败状态")
            elif str(upstream.get("status")) != TERMINAL_STATUS:
                blockers.append(f"上游节点 {code} 当前为「{upstream.get('status')}」，尚未完成")
        return blockers

    def _dependency_snapshot(self, entry: dict[str, Any]) -> dict[str, str]:
        snapshot: dict[str, str] = {}
        for code in self._upstream_codes(entry):
            upstream = self._find_by_code(code)
            if upstream is None or upstream is entry:
                snapshot[code] = "未登记"
            else:
                status = str(upstream.get("status"))
                snapshot[code] = f"{status}（失败）" if upstream.get("abnormal") else status
        return snapshot
