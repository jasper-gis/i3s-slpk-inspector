"""综合质量评分：结构、几何/空间、纹理、LOD、发布兼容性（启发式）。"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any


@dataclass
class ScoreCard:
    structure_integrity: float
    spatial_quality: float
    texture_quality: float
    lod_quality: float
    publish_readiness: float
    overall: float


def _clamp(x: float) -> float:
    return max(0.0, min(100.0, x))


def compute_scores(summary: dict[str, Any]) -> ScoreCard:
    """根据摘要中的计数与子评分计算五维分数。"""
    sev = Counter(summary.get("severity_counts", {}))
    err = sev.get("ERROR", 0)
    warn = sev.get("WARNING", 0)
    info_n = sev.get("INFO", 0)
    mapping_missing = int(summary.get("mapping_missing_targets", 0) or 0)
    mapping_dup_logical = int(summary.get("mapping_duplicate_logicals", 0) or 0)
    mapping_dup_target = int(summary.get("mapping_duplicate_targets", 0) or 0)

    mapping_penalty = mapping_missing * 10 + mapping_dup_logical * 6 + mapping_dup_target * 4
    structure = 100.0 - min(85.0, err * 8 + warn * 2 + mapping_penalty)
    spatial = 100.0 - min(60.0, summary.get("spatial_warning_count", 0) * 3)
    texture = 100.0 - min(80.0, summary.get("texture_error_count", 0) * 10)
    geom = max(0, summary.get("geometry_error_count", 0))
    texture = min(texture, 100.0 - min(60.0, geom * 5))

    lod_sub = summary.get("lod_subscores") or {}
    lod = float(lod_sub.get("lod_switching_smoothness", 75))
    lod = _clamp(lod - warn * 0.5)

    broken_gzip = int(summary.get("broken_gzip_count", 0))
    pub = 100.0 - min(60.0, broken_gzip * 5 + err * 3 + mapping_penalty * 0.8)
    if summary.get("has_3d_scene_layer"):
        pub += 2
    if summary.get("central_dir_ok"):
        pub += 3
    pub = _clamp(pub - (1.0 if summary.get("i3s_old_version") else 0.0) * 10)

    overall = (
        structure * 0.28 + spatial * 0.18 + texture * 0.18 + lod * 0.28 + pub * 0.08
    ) - min(15.0, info_n * 0.05)
    return ScoreCard(
        structure_integrity=_clamp(structure),
        spatial_quality=_clamp(spatial),
        texture_quality=_clamp(texture),
        lod_quality=_clamp(lod),
        publish_readiness=_clamp(pub),
        overall=_clamp(overall),
    )


def grade_label(score: float) -> str:
    if score >= 90:
        return "优秀：层级与健康度整体良好"
    if score >= 70:
        return "可用：存在局部风险，建议针对性复查"
    if score >= 50:
        return "关注：客户端切换或加载体验可能不稳定"
    return "较差：建议重建缓存或升级 I3S 后复检"
