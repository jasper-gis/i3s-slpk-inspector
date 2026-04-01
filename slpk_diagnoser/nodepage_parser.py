"""解析 nodePages 资源：合并多页节点、抽取空间与 LOD 相关字段。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class NodePageRecord:
    """单节点在 nodePage 中的摘要（跨页全局 index）。"""

    index: int
    level: int | None
    parent_index: int | None
    children: list[int]
    mbs: list[float] | None
    obb: dict[str, Any] | None
    lod_threshold: float | None
    has_geometry_ref: bool
    has_texture_ref: bool
    feature_count: int | None
    source_page: str
    raw_fragment: dict[str, Any] = field(repr=False, default_factory=dict)


def _as_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _as_int(x: Any) -> int | None:
    try:
        if x is None:
            return None
        return int(x)
    except (TypeError, ValueError):
        return None


def _children_from_node(n: dict[str, Any]) -> list[int]:
    ch = n.get("children")
    if isinstance(ch, list):
        out: list[int] = []
        for c in ch:
            ci = _as_int(c)
            if ci is not None:
                out.append(ci)
        return out
    fc = _as_int(n.get("firstChild"))
    cc = _as_int(n.get("childCount"))
    if fc is not None and cc is not None and cc > 0:
        return list(range(fc, fc + cc))
    return []


def _has_resource_field(n: dict[str, Any], key: str) -> bool:
    v = n.get(key)
    if v is None:
        return False
    if isinstance(v, list):
        return len(v) > 0
    if isinstance(v, dict):
        return bool(v)
    return True


def parse_node_page_json(page_key: str, data: dict[str, Any]) -> list[NodePageRecord]:
    nodes = data.get("nodes")
    if not isinstance(nodes, list):
        return []
    out: list[NodePageRecord] = []
    for item in nodes:
        if not isinstance(item, dict):
            continue
        idx = _as_int(item.get("index"))
        if idx is None:
            continue
        obb = item.get("obb") if isinstance(item.get("obb"), dict) else None
        mbs = item.get("mbs")
        mbs_list: list[float] | None = None
        if isinstance(mbs, list) and len(mbs) >= 4:
            try:
                mbs_list = [float(x) for x in mbs[:4]]
            except (TypeError, ValueError):
                mbs_list = None
        out.append(
            NodePageRecord(
                index=idx,
                level=_as_int(item.get("level")),
                parent_index=_as_int(item.get("parentIndex")),
                children=_children_from_node(item),
                mbs=mbs_list,
                obb=obb,
                lod_threshold=_as_float(item.get("lodThreshold")),
                has_geometry_ref=_has_resource_field(item, "geometryData"),
                has_texture_ref=_has_resource_field(item, "textureData"),
                feature_count=_as_int(item.get("featureCount")),
                source_page=page_key,
                raw_fragment=item,
            )
        )
    return out
