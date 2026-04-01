"""LOD：版本适配、lodThreshold / lodSelection 解析、单调性与前层几何占位。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from slpk_diagnoser.node_parser import NodeIndexDocSummary
from slpk_diagnoser.nodepage_parser import NodePageRecord


@dataclass
class LodIssue:
    severity: str
    code: str
    message: str
    node_index: int | None = None


def summarize_lod_mechanism(
    records: dict[int, NodePageRecord],
    node_docs: dict[int, NodeIndexDocSummary],
) -> dict[str, Any]:
    """识别包主要使用的 LOD 描述字段。"""
    has_thresh = sum(1 for r in records.values() if r.lod_threshold is not None)
    metric_types: set[str] = set()
    doc_with_sel = 0
    for d in node_docs.values():
        if d.lod_selection:
            doc_with_sel += 1
            for s in d.lod_selection:
                mt = s.get("metricType")
                if isinstance(mt, str):
                    metric_types.add(mt)
    primary = "lodThreshold(nodePage)" if has_thresh >= doc_with_sel else "lodSelection(nodeDoc)"
    if has_thresh and doc_with_sel:
        primary = "mixed"
    return {
        "nodepage_lod_threshold_nodes": has_thresh,
        "node_doc_lod_selection_entries": doc_with_sel,
        "lod_selection_metric_types": sorted(metric_types),
        "primary_mechanism": primary,
    }


def check_lod_threshold_monotonicity(
    records: dict[int, NodePageRecord],
    root: int = 0,
) -> list[LodIssue]:
    """沿父子链检查 lodThreshold：通常子层阈值应整体不大于父（更精细层允许更小阈值）。"""
    issues: list[LodIssue] = []
    by_idx = records

    def walk(idx: int, parent_val: float | None, depth: int) -> None:
        rec = by_idx.get(idx)
        if rec is None:
            return
        v = rec.lod_threshold
        if parent_val is not None and v is not None:
            # 若子显著大于父，可能出现倒挂（启发式）
            if v > parent_val * 1.5 + 1e-6:
                issues.append(
                    LodIssue(
                        "WARNING",
                        "LOD_THRESH_INVERT",
                        f"节点 {idx} lodThreshold ({v}) 相对父级 ({parent_val}) 显著偏大，可能出现切换不稳定",
                        idx,
                    )
                )
        for c in rec.children:
            walk(c, v if v is not None else parent_val, depth + 1)

    if root in by_idx:
        walk(root, None, 0)
    elif by_idx:
        # 无显式 0 根时，取最小 index 作根
        r = min(by_idx.keys())
        walk(r, None, 0)
    return issues


def _max_error_from_selection(sel: list[dict[str, Any]]) -> float | None:
    best: float | None = None
    for s in sel:
        me = s.get("maxError")
        try:
            f = float(me) if me is not None else None
        except (TypeError, ValueError):
            f = None
        if f is not None:
            best = f if best is None else max(best, f)
    return best


def check_lod_selection_monotonicity(node_docs: dict[int, NodeIndexDocSummary]) -> list[LodIssue]:
    """父子 maxError：粗层通常更大；子不应普遍大于父。"""
    issues: list[LodIssue] = []
    by_idx: dict[int, NodeIndexDocSummary] = {}
    for d in node_docs.values():
        if d.index is not None:
            by_idx[d.index] = d

    for doc in by_idx.values():
        if doc.parent is None:
            continue
        par = by_idx.get(doc.parent)
        if par is None:
            continue
        ce = _max_error_from_selection(doc.lod_selection)
        pe = _max_error_from_selection(par.lod_selection)
        if ce is not None and pe is not None and ce > pe * 1.5 + 1e-9:
            issues.append(
                LodIssue(
                    "WARNING",
                    "LOD_MAXERROR_ORDER",
                    f"节点 {doc.index} lodSelection maxError ({ce}) 显著大于父 {doc.parent} ({pe})",
                    doc.index,
                )
            )
    return issues


def check_early_levels_geometry(
    records: dict[int, NodePageRecord],
    max_level: int = 2,
) -> list[LodIssue]:
    """前几层是否缺少几何占位。"""
    issues: list[LodIssue] = []
    by_level: dict[int, list[NodePageRecord]] = {}
    for r in records.values():
        lv = r.level
        if lv is None:
            continue
        by_level.setdefault(lv, []).append(r)

    for lv in range(0, max_level + 1):
        group = by_level.get(lv, [])
        if not group:
            continue
        with_geom = sum(1 for x in group if x.has_geometry_ref)
        if with_geom == 0:
            issues.append(
                LodIssue(
                    "WARNING",
                    "EARLY_LEVEL_NO_GEOM",
                    f"level {lv} 共 {len(group)} 个节点在 nodePage 中均未见 geometryData，远景占位能力可能不足",
                    None,
                )
            )
    return issues


def check_missing_lod_metrics(
    records: dict[int, NodePageRecord],
    node_docs: dict[int, NodeIndexDocSummary],
    max_reports: int = 80,
) -> list[LodIssue]:
    """内部节点若既无 lodThreshold 又无 lodSelection，则提示缺少 LOD 度量。"""
    issues: list[LodIssue] = []
    for idx, r in sorted(records.items()):
        if len(issues) >= max_reports:
            break
        if not r.children:
            continue
        doc = node_docs.get(idx)
        has_sel = bool(doc and doc.lod_selection)
        if r.lod_threshold is None and not has_sel:
            issues.append(
                LodIssue(
                    "INFO",
                    "LOD_METRIC_MISSING",
                    f"内部节点 {idx} 未见 lodThreshold 亦无节点文档 lodSelection（客户端可能依赖默认策略）",
                    idx,
                )
            )
    return issues


def lod_smoothness_scores(
    records: dict[int, NodePageRecord],
    node_docs: dict[int, NodeIndexDocSummary],
    lod_thresh_issues: list[LodIssue],
    lod_sel_issues: list[LodIssue],
    early_issues: list[LodIssue],
) -> dict[str, float]:
    """0–100 分段的启发式子评分（非规范定义）。"""
    n = max(len(records), 1)
    warn_penalty = (len(lod_thresh_issues) + len(lod_sel_issues)) / n * 30
    early_penalty = min(40.0, len([i for i in early_issues if i.severity == "WARNING"]) * 12)
    depth = max((r.level or 0 for r in records.values()), default=0)
    depth_bonus = min(20.0, depth * 4)
    mech = summarize_lod_mechanism(records, node_docs)
    mech_bonus = 10.0 if mech["primary_mechanism"] != "lodSelection(nodeDoc)" or mech["nodepage_lod_threshold_nodes"] else 5.0
    base = 85.0 + depth_bonus * 0.1 + mech_bonus
    switching = max(0.0, min(100.0, base - warn_penalty - early_penalty))
    early_place = max(0.0, 100.0 - early_penalty * 2)
    return {
        "lod_switching_smoothness": round(switching, 1),
        "early_level_placeholder": round(early_place, 1),
        "lod_depth_adequacy": round(min(100.0, depth * 15), 1),
    }
