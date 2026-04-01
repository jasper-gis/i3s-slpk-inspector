"""包围体合法性及父子空间关系启发式检查。"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from slpk_diagnoser.nodepage_parser import NodePageRecord


@dataclass
class SpatialIssue:
    severity: str  # ERROR | WARNING | INFO
    code: str
    message: str
    node_index: int | None = None


def _quat_norm(q: dict[str, Any]) -> float | None:
    try:
        x = float(q.get("x", 0))
        y = float(q.get("y", 0))
        z = float(q.get("z", 0))
        w = float(q.get("w", 0))
        return math.sqrt(x * x + y * y + z * z + w * w)
    except (TypeError, ValueError):
        return None


def check_mbs_obb_record(rec: NodePageRecord) -> list[SpatialIssue]:
    issues: list[SpatialIssue] = []
    if rec.mbs is not None:
        if len(rec.mbs) != 4:
            issues.append(
                SpatialIssue(
                    "ERROR",
                    "MBS_LEN",
                    f"节点 {rec.index} 的 mbs 应为 4 元组（cx,cy,cz,r）",
                    rec.index,
                )
            )
        else:
            r = rec.mbs[3]
            if r <= 0:
                issues.append(
                    SpatialIssue(
                        "ERROR",
                        "MBS_RADIUS",
                        f"节点 {rec.index} MBS 半径应大于 0（当前 {r}）",
                        rec.index,
                    )
                )
    if rec.obb is not None:
        hs = rec.obb.get("halfSize")
        if isinstance(hs, dict):
            try:
                hx = float(hs.get("x", 0))
                hy = float(hs.get("y", 0))
                hz = float(hs.get("z", 0))
                if hx <= 0 or hy <= 0 or hz <= 0:
                    issues.append(
                        SpatialIssue(
                            "WARNING",
                            "OBB_HALF",
                            f"节点 {rec.index} OBB halfSize 应全为正",
                            rec.index,
                        )
                    )
            except (TypeError, ValueError):
                issues.append(
                    SpatialIssue(
                        "ERROR",
                        "OBB_HALF_PARSE",
                        f"节点 {rec.index} OBB halfSize 无法解析",
                        rec.index,
                    )
                )
        q = rec.obb.get("quaternion")
        if isinstance(q, dict):
            n = _quat_norm(q)
            if n is None:
                issues.append(
                    SpatialIssue(
                        "WARNING",
                        "OBB_QUAT",
                        f"节点 {rec.index} 四元数格式异常",
                        rec.index,
                    )
                )
            elif abs(n - 1.0) > 0.05:
                issues.append(
                    SpatialIssue(
                        "INFO",
                        "OBB_QUAT_UNIT",
                        f"节点 {rec.index} 四元数模长偏离单位（|q|≈{n:.4f}）",
                        rec.index,
                    )
                )
    return issues


def _mbs_center_dist(a: list[float], b: list[float]) -> float:
    dx = a[0] - b[0]
    dy = a[1] - b[1]
    dz = a[2] - b[2]
    return math.sqrt(dx * dx + dy * dy + dz * dz)


def check_parent_child_mbs(
    parent: NodePageRecord,
    child: NodePageRecord,
    slack: float = 1.05,
) -> list[SpatialIssue]:
    """启发式：子 MBS 是否大致落在父 MBS 内（球近似，容忍 slack）。"""
    issues: list[SpatialIssue] = []
    pm, cm = parent.mbs, child.mbs
    if pm is None or cm is None or len(pm) < 4 or len(cm) < 4:
        return issues
    pr, cr = pm[3], cm[3]
    if pr <= 0 or cr <= 0:
        return issues
    d = _mbs_center_dist(pm, cm)
    # 子球超出父球：球心距 + 子半径 > 父半径 * slack
    if d + cr > pr * slack:
        issues.append(
            SpatialIssue(
                "WARNING",
                "MBS_CHILD_OUTSIDE_PARENT",
                f"子节点 {child.index} 相对父 {parent.index} 的 MBS 可能未包含（球近似，d+r_child={d+cr:.3f} > r_parent*{slack}={pr*slack:.3f}）",
                child.index,
            )
        )
    if cr > pr * 1.5 and d < pr * 0.5:
        issues.append(
            SpatialIssue(
                "INFO",
                "MBS_CHILD_MUCH_LARGER",
                f"子节点 {child.index} MBS 半径显著大于父 {parent.index}，可能存在包围体过松或层级异常",
                child.index,
            )
        )
    return issues


def check_sibling_mbs_overlap(nodes_at_level: list[NodePageRecord]) -> list[SpatialIssue]:
    """同级节点 MBS 严重重叠（抽样两两）。"""
    issues: list[SpatialIssue] = []
    if len(nodes_at_level) < 2:
        return issues
    # O(n^2) 仅对小集或截断
    max_pairs = 200
    count = 0
    for i, a in enumerate(nodes_at_level):
        if a.mbs is None or a.mbs[3] <= 0:
            continue
        for b in nodes_at_level[i + 1 :]:
            if count >= max_pairs:
                return issues
            if b.mbs is None or b.mbs[3] <= 0:
                continue
            count += 1
            d = _mbs_center_dist(a.mbs, b.mbs)
            if d < 1e-9:
                continue
            # 两球 heavily overlap: d < r_a + r_b and d < min(r_a,r_b)
            ra, rb = a.mbs[3], b.mbs[3]
            if d < min(ra, rb) * 0.3:
                issues.append(
                    SpatialIssue(
                        "INFO",
                        "MBS_SIBLING_OVERLAP",
                        f"同级节点 {a.index} 与 {b.index} MBS 中心过近，可能存在过度重叠",
                        a.index,
                    )
                )
    return issues
