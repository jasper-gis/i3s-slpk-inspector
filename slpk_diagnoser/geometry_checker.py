"""几何资源：引用路径在包内是否存在（一期：存在性与 gzip 可读性）。"""

from __future__ import annotations

from dataclasses import dataclass

from slpk_diagnoser.package_reader import BasePackageReader


@dataclass
class GeometryIssue:
    severity: str
    code: str
    message: str
    path: str | None = None


def resolve_node_path(node_id: int, href: str) -> str:
    h = href.strip().replace("\\", "/").lstrip("/")
    if h.startswith("nodes/"):
        return h
    return f"nodes/{node_id}/{h}"


def check_geometry_refs_exist(
    reader: BasePackageReader,
    node_id: int,
    hrefs: list[str],
) -> list[GeometryIssue]:
    issues: list[GeometryIssue] = []
    for h in hrefs:
        logical = resolve_node_path(node_id, h)
        if reader.raw_exists(logical):
            continue
        # 常见备选：追加 .gz
        if not logical.endswith(".gz") and reader.raw_exists(logical + ".gz"):
            continue
        issues.append(
            GeometryIssue(
                "ERROR",
                "GEOM_MISSING",
                f"节点 {node_id} 几何资源缺失：{logical}",
                logical,
            )
        )
    return issues
