"""纹理资源：路径存在性（一期不含像素解码）。"""

from __future__ import annotations

from dataclasses import dataclass

from slpk_diagnoser.geometry_checker import resolve_node_path
from slpk_diagnoser.package_reader import BasePackageReader


@dataclass
class TextureIssue:
    severity: str
    code: str
    message: str
    path: str | None = None


def check_texture_refs_exist(
    reader: BasePackageReader,
    node_id: int,
    hrefs: list[str],
) -> list[TextureIssue]:
    issues: list[TextureIssue] = []
    for h in hrefs:
        logical = resolve_node_path(node_id, h)
        if reader.raw_exists(logical):
            continue
        if not logical.endswith(".gz") and reader.raw_exists(logical + ".gz"):
            continue
        issues.append(
            TextureIssue(
                "ERROR",
                "TEX_MISSING",
                f"节点 {node_id} 纹理资源缺失：{logical}",
                logical,
            )
        )
    return issues
