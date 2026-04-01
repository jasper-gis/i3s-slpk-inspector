"""诊断报告：控制台文本与 JSON 导出。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from slpk_diagnoser.logger import get_logger

logger = get_logger(__name__)


def format_report_text(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    s = payload.get("summary", {})
    lines.append("=" * 60)
    lines.append("SLPK / I3S 质量诊断报告")
    lines.append("=" * 60)
    lines.append(f"包路径: {payload.get('package_path', '')}")
    lines.append(
        f"图层: {s.get('layer_type', '?')} | I3S 版本: {s.get('i3s_version', '?')} | 节点数: {s.get('total_nodes', 0)}"
    )
    lines.append(
        f"最大层级 depth: {s.get('max_level', '?')} | nodePages: {s.get('node_pages_files', 0)} | 节点文档: {s.get('node_documents', 0)}"
    )
    lines.append(
        f"ZIP 中央目录: {'正常' if s.get('central_dir_ok') else '异常'} | 坏 gzip 条目: {s.get('broken_gzip_count', 0)} | 哈希索引: {'是' if s.get('has_hash_index') else '否'}"
    )
    lines.append("")

    sc = payload.get("scores", {})
    if sc:
        lines.append("--- 质量评分（0–100，启发式） ---")
        for k, v in sc.items():
            lines.append(f"  {k}: {v}")
        lines.append(f"  总体结论: {payload.get('grade', '')}")
        lines.append("")

    mech = s.get("lod_mechanism", {})
    if mech:
        lines.append("--- LOD 机制识别 ---")
        lines.append(f"  主机制: {mech.get('primary_mechanism', '?')}")
        lines.append(f"  nodePage 含 lodThreshold 节点数: {mech.get('nodepage_lod_threshold_nodes', 0)}")
        lines.append(
            f"  节点文档 lodSelection 数: {mech.get('node_doc_lod_selection_entries', 0)} | metricTypes: {mech.get('lod_selection_metric_types', [])}"
        )
        lines.append("")

    lv = s.get("level_stats", {})
    if lv:
        lines.append("--- 各 level 统计（节选） ---")
        for k in sorted(lv.keys(), key=lambda x: int(x) if str(x).lstrip("-").isdigit() else 0)[:12]:
            lines.append(f"  L{k}: {lv[k]}")
        lines.append("")

    lines.append("--- 问题清单 ---")
    for it in payload.get("issues", []):
        lines.append(f"[{it['severity']}] {it['code']}: {it['message']}")
    if not payload.get("issues"):
        lines.append("（无）")
    lines.append("")

    sug = payload.get("suggestions", [])
    if sug:
        lines.append("--- 修复与优化建议 ---")
        for t in sug:
            lines.append(f"  · {t}")
    lines.append("=" * 60)
    return "\n".join(lines)


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    try:
        p = Path(path)
        logger.debug(f"准备写入 JSON 到: {p}")

        if p.exists() and not p.is_file():
            raise IsADirectoryError(f"目标路径已存在且不是文件: {p}")

        p.parent.mkdir(parents=True, exist_ok=True)

        with p.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        logger.info(f"JSON 报告成功写入: {p}")

    except PermissionError as e:
        logger.error(f"没有权限写入文件: {path}")
        raise
    except OSError as e:
        logger.error(f"写入 JSON 时发生操作系统错误: {e}")
        raise
    except TypeError as e:
        logger.error(f"JSON 序列化失败: {e}")
        raise
    except Exception as e:
        logger.error(f"写入 JSON 时发生未预期的错误: {e}")
        raise
