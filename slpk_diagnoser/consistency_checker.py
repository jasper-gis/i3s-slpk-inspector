"""节点树一致性：可达性、环、孤儿、层级连续性、nodePage 与节点文档交叉验证。"""

from __future__ import annotations

from dataclasses import dataclass

from slpk_diagnoser.logger import get_logger
from slpk_diagnoser.node_parser import NodeIndexDocSummary
from slpk_diagnoser.nodepage_parser import NodePageRecord

logger = get_logger(__name__)


@dataclass
class ConsistencyIssue:
    severity: str
    code: str
    message: str
    node_index: int | None = None


def _infer_root(records: dict[int, NodePageRecord]) -> int:
    if 0 in records:
        return 0
    parents: set[int] = set()
    children: set[int] = set()
    for r in records.values():
        for c in r.children:
            children.add(c)
            parents.add(r.index)
    roots = [idx for idx in records if idx not in children]
    if len(roots) == 1:
        return roots[0]
    return min(records.keys()) if records else 0


def check_tree_reachability(records: dict[int, NodePageRecord]) -> list[ConsistencyIssue]:
    issues: list[ConsistencyIssue] = []
    if not records:
        issues.append(ConsistencyIssue("ERROR", "NO_NODES", "nodePage 中无有效节点", None))
        logger.warning("未发现有效节点")
        return issues
    root = _infer_root(records)
    if root not in records:
        issues.append(ConsistencyIssue("ERROR", "NO_ROOT", "无法确定根节点索引", None))
        logger.error("无法确定根节点")
        return issues

    logger.debug(f"使用根节点: {root}")
    visited: set[int] = set()
    stack = [root]

    def dfs_cycle(idx: int, path: set[int]) -> bool:
        if idx in path:
            issues.append(ConsistencyIssue("ERROR", "CYCLE", f"检测到包含节点 {idx} 的环", idx))
            logger.error(f"检测到环: 节点 {idx}")
            return True
        if idx in visited:
            return False
        rec = records.get(idx)
        if rec is None:
            issues.append(
                ConsistencyIssue(
                    "ERROR",
                    "MISSING_CHILD",
                    f"父节点引用了不存在的子索引 {idx}",
                    idx,
                )
            )
            return False
        visited.add(idx)
        path.add(idx)
        for c in rec.children:
            dfs_cycle(c, path)
        path.discard(idx)
        return False

    dfs_cycle(root, set())

    orphans = set(records.keys()) - visited
    if orphans:
        logger.warning(f"发现 {len(orphans)} 个孤儿节点")
    for o in sorted(orphans):
        issues.append(
            ConsistencyIssue(
                "WARNING",
                "ORPHAN_NODE",
                f"节点 {o} 自根 {root} 不可达（孤儿或独立子树）",
                o,
            )
        )
    return issues


def check_level_continuity(records: dict[int, NodePageRecord]) -> list[ConsistencyIssue]:
    issues: list[ConsistencyIssue] = []
    skip_count = 0
    for r in records.values():
        for c in r.children:
            child = records.get(c)
            if child is None:
                continue
            pl, cl = r.level, child.level
            if pl is None or cl is None:
                continue
            if cl - pl != 1:
                issues.append(
                    ConsistencyIssue(
                        "INFO",
                        "LEVEL_SKIP",
                        f"父子 level 差非 1：{r.index}(L{pl}) -> {c}(L{cl})",
                        c,
                    )
                )
                skip_count += 1
    if skip_count:
        logger.debug(f"发现 {skip_count} 个层级不连续的父子关系")
    return issues


def check_nodepage_vs_doc(
    records: dict[int, NodePageRecord],
    node_docs: dict[int, NodeIndexDocSummary],
) -> list[ConsistencyIssue]:
    """同一 index 上 nodePage 与 3dNodeIndexDocument 的 children/parent 是否冲突。"""
    issues: list[ConsistencyIssue] = []
    mismatch_count = 0
    for idx, doc in node_docs.items():
        rec = records.get(idx)
        if rec is None:
            continue
        if doc.children:
            sch = set(rec.children)
            dch = set(doc.children)
            if sch != dch and sch and dch:
                issues.append(
                    ConsistencyIssue(
                        "WARNING",
                        "CHILDREN_MISMATCH",
                        f"节点 {idx} nodePage children 与节点文档 children 不一致（page={sorted(sch)[:8]}... doc={sorted(dch)[:8]}...）",
                        idx,
                    )
                )
                mismatch_count += 1
        if doc.parent is not None and rec.parent_index is not None and doc.parent != rec.parent_index:
            issues.append(
                ConsistencyIssue(
                    "WARNING",
                    "PARENT_MISMATCH",
                    f"节点 {idx} parentIndex nodePage={rec.parent_index} doc={doc.parent}",
                    idx,
                )
            )
            mismatch_count += 1
    if mismatch_count:
        logger.debug(f"发现 {mismatch_count} 个 nodePage 与节点文档不一致的情况")
    return issues


def level_statistics(records: dict[int, NodePageRecord]) -> dict[int, dict[str, float | int]]:
    """各 level 节点数、几何/纹理计数。"""
    by_lv: dict[int, list[NodePageRecord]] = {}
    for r in records.values():
        lv = r.level if r.level is not None else -1
        by_lv.setdefault(lv, []).append(r)
    out: dict[int, dict[str, float | int]] = {}
    for lv, group in sorted(by_lv.items()):
        out[lv] = {
            "node_count": len(group),
            "with_geometry": sum(1 for x in group if x.has_geometry_ref),
            "with_texture": sum(1 for x in group if x.has_texture_ref),
            "total_feature_count": sum(x.feature_count or 0 for x in group),
        }
    return out
