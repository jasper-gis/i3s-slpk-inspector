"""解析 3dSceneLayer.json.gz：版本、图层类型、store、nodePages 声明等。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SceneLayerInfo:
    raw: dict[str, Any]
    version: str | None
    layer_type: str | None
    store_version: str | None
    store_profile: str | None
    has_node_pages_decl: bool
    node_pages_hint: dict[str, Any] | None
    statistics_refs: list[str]
    layer_name: str | None
    material_def_count: int
    geometry_def_count: int


def _first_layer(doc: dict[str, Any]) -> dict[str, Any] | None:
    layers = doc.get("layers")
    if isinstance(layers, list) and layers:
        first = layers[0]
        if isinstance(first, dict):
            return first
    return None


def parse_scene_layer(doc: dict[str, Any] | None) -> SceneLayerInfo | None:
    if not isinstance(doc, dict):
        return None
    layer = _first_layer(doc) or doc
    store = layer.get("store") if isinstance(layer.get("store"), dict) else {}
    version = doc.get("version")
    if version is None:
        version = layer.get("version")
    node_pages = layer.get("nodePages")
    has_np = bool(node_pages)
    np_hint: dict[str, Any] | None = None
    if isinstance(node_pages, dict):
        np_hint = {
            k: node_pages.get(k)
            for k in ("nodesPerPage", "lodType", "messaging", "extent")
            if k in node_pages
        }

    stats_refs: list[str] = []
    stats = layer.get("statistics")
    if isinstance(stats, list):
        for s in stats:
            if isinstance(s, dict) and isinstance(s.get("href"), str):
                stats_refs.append(s["href"])
            elif isinstance(s, str):
                stats_refs.append(s)

    mats = layer.get("materialDefinitions")
    geoms = layer.get("geometryDefinitions")
    mat_c = len(mats) if isinstance(mats, list) else 0
    geom_c = len(geoms) if isinstance(geoms, list) else 0

    return SceneLayerInfo(
        raw=doc,
        version=str(version) if version is not None else None,
        layer_type=str(layer["layerType"]) if layer.get("layerType") else None,
        store_version=str(store.get("version")) if store.get("version") is not None else None,
        store_profile=str(store.get("profile")) if store.get("profile") else None,
        has_node_pages_decl=has_np,
        node_pages_hint=np_hint,
        statistics_refs=stats_refs,
        layer_name=str(layer["name"]) if layer.get("name") else None,
        material_def_count=mat_c,
        geometry_def_count=geom_c,
    )
