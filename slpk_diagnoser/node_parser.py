"""解析节点目录文档 3dNodeIndexDocument.json.gz（若存在）。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class NodeIndexDocSummary:
    node_path: str
    index: int | None
    level: int | None
    parent: int | None
    children: list[int]
    neighbors: list[int]
    mbs: list[float] | None
    obb: dict[str, Any] | None
    lod_selection: list[dict[str, Any]]
    geometry_resources: list[str]
    texture_resources: list[str]
    shared_resource: str | None
    raw: dict[str, Any] = field(repr=False)


def _ints_from_list(v: Any) -> list[int]:
    if not isinstance(v, list):
        return []
    out: list[int] = []
    for x in v:
        try:
            out.append(int(x))
        except (TypeError, ValueError):
            continue
    return out


def _resource_paths_from_data(v: Any, key: str = "href") -> list[str]:
    if isinstance(v, list):
        paths: list[str] = []
        for it in v:
            if isinstance(it, dict):
                p = it.get(key) or it.get("resource")
                if isinstance(p, str):
                    paths.append(p.replace("\\", "/"))
            elif isinstance(it, str):
                paths.append(it.replace("\\", "/"))
        return paths
    return []


def parse_3d_node_index_document(node_folder: str, doc: dict[str, Any]) -> NodeIndexDocSummary:
    ch = doc.get("children")
    children = _ints_from_list(ch) if ch is not None else []
    if not children and isinstance(ch, dict):
        # 少数版本可能嵌套
        children = _ints_from_list(ch.get("nodes"))

    nb = doc.get("neighbors") or doc.get("neighborNodes")
    neighbors = _ints_from_list(nb) if nb is not None else []

    mbs = doc.get("mbs")
    mbs_list: list[float] | None = None
    if isinstance(mbs, list) and len(mbs) >= 4:
        try:
            mbs_list = [float(x) for x in mbs[:4]]
        except (TypeError, ValueError):
            mbs_list = None

    obb = doc.get("obb") if isinstance(doc.get("obb"), dict) else None
    ls = doc.get("lodSelection")
    lod_sel: list[dict[str, Any]] = []
    if isinstance(ls, list):
        for x in ls:
            if isinstance(x, dict):
                lod_sel.append(x)

    geom_paths = _resource_paths_from_data(doc.get("geometryData"))
    tex_paths = _resource_paths_from_data(doc.get("textureData"))
    sr = doc.get("sharedResource")
    shared: str | None = None
    if isinstance(sr, dict) and isinstance(sr.get("href"), str):
        shared = sr["href"].replace("\\", "/")
    elif isinstance(sr, str):
        shared = sr.replace("\\", "/")

    idx = doc.get("index")
    try:
        index_i = int(idx) if idx is not None else None
    except (TypeError, ValueError):
        index_i = None
    lev = doc.get("level")
    try:
        level_i = int(lev) if lev is not None else None
    except (TypeError, ValueError):
        level_i = None
    par = doc.get("parentIndex")
    try:
        parent_i = int(par) if par is not None else None
    except (TypeError, ValueError):
        parent_i = None

    return NodeIndexDocSummary(
        node_path=node_folder,
        index=index_i,
        level=level_i,
        parent=parent_i,
        children=children,
        neighbors=neighbors,
        mbs=mbs_list,
        obb=obb,
        lod_selection=lod_sel,
        geometry_resources=geom_paths,
        texture_resources=tex_paths,
        shared_resource=shared,
        raw=doc,
    )
