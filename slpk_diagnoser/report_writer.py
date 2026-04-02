"""诊断报告：控制台文本与 JSON 导出。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from slpk_diagnoser.logger import get_logger

logger = get_logger(__name__)


def format_report_text(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    summary = payload.get("summary", {})
    lines.append("=" * 72)
    lines.append("SLPK / I3S 质量诊断报告")
    lines.append("=" * 72)
    lines.append(f"输入: {payload.get('package_path', '')}")
    lines.append(
        f"来源: {summary.get('reader_type', '?')} | 存储: {summary.get('storage_provider', '?')} | 检查范围: {summary.get('inspection_scope', '?')}"
    )
    if summary.get("storage_bucket") or summary.get("storage_prefix"):
        lines.append(
            f"对象存储: bucket={summary.get('storage_bucket', '-')}, prefix={summary.get('storage_prefix', '-')}, endpoint={summary.get('storage_endpoint', '-')}"
        )
    lines.append(
        f"图层: {summary.get('layer_type', '?')} | I3S 版本: {summary.get('i3s_version', '?')} | Store: {summary.get('store_profile', '?')} / {summary.get('store_version', '?')}"
    )
    lines.append(
        f"节点数: {summary.get('total_nodes', 0)} | 最大层级: {summary.get('max_level', '?')} | nodePages: {summary.get('node_pages_files', 0)} | 节点文档: {summary.get('node_documents', 0)}"
    )
    lines.append(
        f"ZIP 中央目录: {'正常' if summary.get('central_dir_ok') else '异常'} | 坏 gzip 条目: {summary.get('broken_gzip_count', 0)} | Special Hash Index: {'是' if summary.get('has_hash_index') else '否'}"
    )
    lines.append("")

    lines.append("--- i3s-mapping / 对象映射 ---")
    lines.append(
        f"  来源: {summary.get('mapping_source', '?')} | 映射文件: {summary.get('mapping_document', '-') or '-'} | 逻辑条目: {summary.get('mapping_entries', 0)}"
    )
    lines.append(
        f"  缺失目标: {summary.get('mapping_missing_targets', 0)} | 重复逻辑路径: {summary.get('mapping_duplicate_logicals', 0)} | 重复对象键: {summary.get('mapping_duplicate_targets', 0)} | 未使用对象: {summary.get('mapping_unused_objects', 0)}"
    )
    mapping_notes = summary.get("mapping_notes") or []
    if mapping_notes:
        for note in mapping_notes:
            lines.append(f"  说明: {note}")
    lines.append("")

    scores = payload.get("scores", {})
    if scores:
        lines.append("--- 质量评分（0-100，启发式） ---")
        for key, value in scores.items():
            lines.append(f"  {key}: {value}")
        lines.append(f"  总体结论: {payload.get('grade', '')}")
        lines.append("")

    mechanism = summary.get("lod_mechanism", {})
    if mechanism:
        lines.append("--- LOD 机制识别 ---")
        lines.append(f"  主机制: {mechanism.get('primary_mechanism', '?')}")
        lines.append(f"  nodePage 含 lodThreshold 节点数: {mechanism.get('nodepage_lod_threshold_nodes', 0)}")
        lines.append(
            f"  节点文档 lodSelection 数: {mechanism.get('node_doc_lod_selection_entries', 0)} | metricTypes: {mechanism.get('lod_selection_metric_types', [])}"
        )
        lines.append("")

    level_stats = summary.get("level_stats", {})
    if level_stats:
        lines.append("--- 各 Level 统计（节选） ---")
        for level in sorted(level_stats.keys(), key=lambda item: int(item) if str(item).lstrip("-").isdigit() else 0)[:12]:
            lines.append(f"  L{level}: {level_stats[level]}")
        lines.append("")

    lines.append("--- 问题清单 ---")
    issues = payload.get("issues", [])
    for item in issues:
        node_info = f" [node={item['node_index']}]" if "node_index" in item else ""
        lines.append(f"[{item['severity']}] {item['code']}{node_info}: {item['message']}")
    if not issues:
        lines.append("（无）")
    lines.append("")

    suggestions = payload.get("suggestions", [])
    if suggestions:
        lines.append("--- 修复与优化建议 ---")
        for text in suggestions:
            lines.append(f"  - {text}")
    lines.append("=" * 72)
    return "\n".join(lines)


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    logger.debug("准备写入 JSON: %s", target)
    if target.exists() and not target.is_file():
        raise IsADirectoryError(f"目标路径已存在且不是文件: {target}")

    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    logger.info("JSON 报告已写入: %s", target)
