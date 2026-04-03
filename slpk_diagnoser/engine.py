"""编排包级、节点级检查并汇总报告。"""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

from slpk_diagnoser.consistency_checker import (
    check_level_continuity,
    check_nodepage_vs_doc,
    check_tree_reachability,
    level_statistics,
)
from slpk_diagnoser.geometry_checker import check_geometry_refs_exist
from slpk_diagnoser.lod_checker import (
    check_early_levels_geometry,
    check_lod_selection_monotonicity,
    check_lod_threshold_monotonicity,
    check_missing_lod_metrics,
    lod_smoothness_scores,
    summarize_lod_mechanism,
)
from slpk_diagnoser.logger import (
    ensure_default_logging,
    get_logger,
    log_timed_operation,
    sanitize_text,
)
from slpk_diagnoser.node_parser import NodeIndexDocSummary, parse_3d_node_index_document
from slpk_diagnoser.nodepage_parser import NodePageRecord, parse_node_page_json
from slpk_diagnoser.package_reader import is_cloud_storage_uri, open_package_reader
from slpk_diagnoser.scene_layer_parser import parse_scene_layer
from slpk_diagnoser.scoring import compute_scores, grade_label
from slpk_diagnoser.spatial_checker import (
    check_mbs_obb_record,
    check_parent_child_mbs,
    check_sibling_mbs_overlap,
)
from slpk_diagnoser.texture_checker import check_texture_refs_exist

logger = get_logger(__name__)


def _i3s_tuple_version(value: str | None) -> tuple[int, ...]:
    if not value:
        return ()
    return tuple(int(part) for part in re.findall(r"\d+", value))


def _is_old_i3s(value: str | None) -> bool:
    version = _i3s_tuple_version(value)
    if not version:
        return False
    return version[0] < 2 and (len(version) < 2 or (version[0] == 1 and version[1] <= 6))


def _extract_node_id_from_path(norm_path: str) -> int | None:
    match = re.match(r"nodes/(\d+)/", norm_path)
    if match:
        return int(match.group(1))
    return None


def _normalize_package_display(package_path: str) -> str:
    if is_cloud_storage_uri(package_path):
        return sanitize_text(package_path)
    return str(Path(package_path).expanduser().resolve())


def _is_json_resource(logical_path: str) -> bool:
    lowered = logical_path.lower()
    return lowered.endswith(".json") or lowered.endswith(".json.gz")


def _canonical_json_resource(logical_path: str) -> str:
    if logical_path.lower().endswith(".json.gz"):
        return logical_path[:-3]
    return logical_path


def _preferred_json_resources(logical_paths: list[str]) -> list[str]:
    selected: dict[str, str] = {}
    for logical in sorted(logical_paths):
        if not _is_json_resource(logical):
            continue
        canonical = _canonical_json_resource(logical)
        key = canonical.lower()
        current = selected.get(key)
        if current is None or (
            logical.lower().endswith(".json.gz") and not current.lower().endswith(".json.gz")
        ):
            selected[key] = logical
    return [selected[key] for key in sorted(selected)]


def _json_resource_variants(logical_path: str) -> tuple[str, ...]:
    normalized = logical_path.replace("\\", "/").lstrip("/")
    if normalized.lower().endswith(".json.gz"):
        return normalized, normalized[:-3]
    if normalized.lower().endswith(".json"):
        return normalized + ".gz", normalized
    return normalized + ".gz", normalized


def _read_json_resource(reader: Any, logical_path: str) -> tuple[Any | None, str | None]:
    for candidate in _json_resource_variants(logical_path):
        if not reader.raw_exists(candidate):
            continue
        if candidate.lower().endswith(".gz"):
            return reader.read_json_gz(candidate)

        raw = reader.read_bytes(candidate)
        if raw is None:
            return None, "missing"
        try:
            return json.loads(raw.decode("utf-8")), None
        except UnicodeDecodeError as exc:
            return None, f"UTF-8 解码失败: {exc}"
        except json.JSONDecodeError as exc:
            return None, f"JSON 解析失败: 位置 {exc.pos}, 错误: {exc.msg}"
        except Exception as exc:
            return None, f"JSON 处理错误: {exc}"

    return None, "missing"


def diagnose_slpk(package_path: str) -> dict[str, Any]:
    ensure_default_logging()
    display_path = _normalize_package_display(package_path)

    with log_timed_operation(logger, "SLPK/I3S 诊断", package=display_path):
        issues: list[dict[str, Any]] = []
        summary: dict[str, Any] = {
            "package_path": display_path,
            "input_kind": None,
            "reader_type": None,
            "storage_provider": None,
            "storage_endpoint": None,
            "storage_bucket": None,
            "storage_prefix": None,
            "inspection_scope": None,
            "has_3d_scene_layer": False,
            "i3s_version": None,
            "layer_type": None,
            "store_version": None,
            "store_profile": None,
            "total_nodes": 0,
            "max_level": None,
            "node_pages_files": 0,
            "node_documents": 0,
            "broken_gzip_count": 0,
            "central_dir_ok": True,
            "has_hash_index": False,
            "has_metadata_json": False,
            "statistics_checked": 0,
            "statistics_missing": 0,
            "geometry_error_count": 0,
            "texture_error_count": 0,
            "spatial_warning_count": 0,
            "lod_mechanism": {},
            "level_stats": {},
            "severity_counts": {},
            "lod_subscores": {},
            "i3s_old_version": False,
            "mapping_source": None,
            "mapping_document": None,
            "mapping_entries": 0,
            "mapping_duplicate_logicals": 0,
            "mapping_duplicate_targets": 0,
            "mapping_missing_targets": 0,
            "mapping_unused_objects": 0,
            "mapping_notes": [],
        }

        with open_package_reader(package_path) as reader:
            source_info = reader.describe_source()
            summary.update(source_info)
            summary["input_kind"] = source_info.get("reader_type")

            with log_timed_operation(
                logger,
                "包结构检查",
                reader_type=summary["reader_type"],
                provider=summary["storage_provider"],
            ):
                inspection = reader.inspect()
                summary["inspection_scope"] = inspection.inspection_scope
                summary["broken_gzip_count"] = len(inspection.broken_gzip)
                summary["central_dir_ok"] = inspection.central_dir_ok
                summary["has_hash_index"] = reader.has_special_hash_index()
                summary["has_metadata_json"] = reader.raw_exists("metadata.json")

                for message in inspection.zip_errors:
                    issues.append({"severity": "ERROR", "code": "ZIP", "message": message})
                for message in inspection.broken_gzip:
                    issues.append({"severity": "ERROR", "code": "BAD_GZIP", "message": message})
                issues.extend(inspection.issues)
                for note in inspection.notes:
                    issues.append({"severity": "INFO", "code": "INSPECT_NOTE", "message": note})

            scene_name = "3dSceneLayer.json"
            doc, error = _read_json_resource(reader, scene_name)
            if error == "missing":
                issues.append(
                    {
                        "severity": "ERROR",
                        "code": "NO_SCENE_LAYER",
                        "message": f"缺少根文档 {scene_name}[.gz]",
                    }
                )
                return _finalize(display_path, summary, issues)

            summary["has_3d_scene_layer"] = True
            with log_timed_operation(logger, "解析场景层根文档", path=f"{scene_name}[.gz]"):
                if error or not isinstance(doc, dict):
                    issues.append(
                        {
                            "severity": "ERROR",
                            "code": "SCENE_LAYER_PARSE",
                            "message": f"无法解析 {scene_name}[.gz]: {error}",
                        }
                    )
                    return _finalize(display_path, summary, issues)

                scene_info = parse_scene_layer(doc)
                if scene_info:
                    summary["i3s_version"] = scene_info.version
                    summary["layer_type"] = scene_info.layer_type
                    summary["store_version"] = scene_info.store_version
                    summary["store_profile"] = scene_info.store_profile
                    summary["i3s_old_version"] = _is_old_i3s(scene_info.version)

                    for href in scene_info.statistics_refs:
                        summary["statistics_checked"] += 1
                        logical = href.strip().lstrip("/").replace("\\", "/")
                        if not reader.raw_exists(logical) and not reader.raw_exists(logical + ".gz"):
                            summary["statistics_missing"] += 1
                            issues.append(
                                {
                                    "severity": "WARNING",
                                    "code": "STATS_MISSING",
                                    "message": f"统计资源未在包内找到: {href}",
                                }
                            )
                else:
                    scene_info = None

            node_records: dict[int, NodePageRecord] = {}
            node_pages = _preferred_json_resources(
                [
                    key
                    for key in reader.find_prefix("nodepages")
                    if _canonical_json_resource(key).lower().startswith("nodepages/")
                ]
            )
            summary["node_pages_files"] = len(node_pages)
            if scene_info and scene_info.has_node_pages_decl and not node_pages:
                issues.append(
                    {
                        "severity": "WARNING",
                        "code": "NODEPAGES_DECL_ONLY",
                        "message": "场景层声明了 nodePages，但包内未发现 nodepages/*.json[.gz]。",
                    }
                )

            with log_timed_operation(logger, "解析 nodePages", file_count=len(node_pages)):
                for key in node_pages:
                    data, error = _read_json_resource(reader, key)
                    if error or not isinstance(data, dict):
                        issues.append(
                            {
                                "severity": "ERROR",
                                "code": "NODEPAGE_PARSE",
                                "message": f"{key}: {error}",
                            }
                        )
                        continue
                    for record in parse_node_page_json(key, data):
                        if record.index in node_records:
                            issues.append(
                                {
                                    "severity": "INFO",
                                    "code": "NODEPAGE_DUP_INDEX",
                                    "message": f"节点 index={record.index} 在多个 nodePage 中重复出现，后者覆盖前者。",
                                    "node_index": record.index,
                                }
                            )
                        node_records[record.index] = record

            summary["total_nodes"] = len(node_records)
            summary["max_level"] = max(
                (record.level for record in node_records.values() if record.level is not None),
                default=None,
            )
            summary["level_stats"] = level_statistics(node_records)

            node_docs = _load_node_documents(reader, node_records, issues)
            summary["node_documents"] = len(node_docs)

            with log_timed_operation(logger, "一致性检查", total_nodes=summary["total_nodes"]):
                issues.extend(_consistency_issues(node_records, node_docs))

            with log_timed_operation(logger, "空间检查", total_nodes=summary["total_nodes"]):
                spatial_issues, warning_count = _spatial_issues(node_records)
                issues.extend(spatial_issues)
                summary["spatial_warning_count"] = warning_count

            with log_timed_operation(logger, "LOD 检查", total_nodes=summary["total_nodes"]):
                mechanism = summarize_lod_mechanism(node_records, node_docs)
                summary["lod_mechanism"] = mechanism
                threshold_issues = check_lod_threshold_monotonicity(node_records)
                selection_issues = check_lod_selection_monotonicity(node_docs)
                early_geometry_issues = check_early_levels_geometry(node_records)
                missing_metric_issues = check_missing_lod_metrics(node_records, node_docs)
                for batch in (
                    threshold_issues,
                    selection_issues,
                    early_geometry_issues,
                    missing_metric_issues,
                ):
                    for item in batch:
                        issues.append(
                            {
                                "severity": item.severity,
                                "code": item.code,
                                "message": item.message,
                                "node_index": item.node_index,
                            }
                        )
                summary["lod_subscores"] = lod_smoothness_scores(
                    node_records,
                    node_docs,
                    threshold_issues,
                    selection_issues,
                    early_geometry_issues,
                )

            with log_timed_operation(logger, "资源引用检查", node_doc_count=len(node_docs)):
                for node_id, doc_summary in node_docs.items():
                    for item in check_geometry_refs_exist(reader, node_id, doc_summary.geometry_resources):
                        issues.append(
                            {"severity": item.severity, "code": item.code, "message": item.message}
                        )
                        summary["geometry_error_count"] += 1
                    for item in check_texture_refs_exist(reader, node_id, doc_summary.texture_resources):
                        issues.append(
                            {"severity": item.severity, "code": item.code, "message": item.message}
                        )
                        summary["texture_error_count"] += 1

                for record in node_records.values():
                    if not record.has_geometry_ref or record.index in node_docs:
                        continue
                    for item in check_geometry_refs_exist(reader, record.index, ["geometries/0"]):
                        if item.severity == "ERROR":
                            issues.append(
                                {
                                    "severity": "INFO",
                                    "code": "GEOM_UNVERIFIED",
                                    "message": f"节点 {record.index} 仅在 nodePage 中出现几何标记，但无 3dNodeIndexDocument，无法完整验证资源路径。",
                                    "node_index": record.index,
                                }
                            )

        return _finalize(display_path, summary, issues)


def _load_node_documents(
    reader: Any,
    node_records: dict[int, NodePageRecord],
    issues: list[dict[str, Any]],
) -> dict[int, NodeIndexDocSummary]:
    node_docs: dict[int, NodeIndexDocSummary] = {}
    if node_records:
        candidates = [f"nodes/{idx}/3dNodeIndexDocument.json" for idx in sorted(node_records)]
    else:
        candidates = _preferred_json_resources(
            [
                key
                for key in reader.find_prefix("nodes")
                if _canonical_json_resource(key).lower().endswith("3dnodeindexdocument.json")
            ]
        )

    with log_timed_operation(logger, "解析节点文档", candidate_count=len(candidates)):
        for logical in candidates:
            node_id = _extract_node_id_from_path(logical)
            data, error = _read_json_resource(reader, logical)
            if error or not isinstance(data, dict):
                issues.append(
                    {
                        "severity": "WARNING",
                        "code": "NODE_DOC_PARSE",
                        "message": f"{logical}: {error}",
                    }
                )
                continue
            summary = parse_3d_node_index_document(logical, data)
            idx = summary.index if summary.index is not None else node_id
            if idx is None:
                continue
            node_docs[idx] = summary
    return node_docs


def _consistency_issues(
    node_records: dict[int, NodePageRecord],
    node_docs: dict[int, NodeIndexDocSummary],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for item in check_tree_reachability(node_records):
        output.append(
            {
                "severity": item.severity,
                "code": item.code,
                "message": item.message,
                "node_index": item.node_index,
            }
        )
    for item in check_level_continuity(node_records):
        output.append(
            {
                "severity": item.severity,
                "code": item.code,
                "message": item.message,
                "node_index": item.node_index,
            }
        )
    for item in check_nodepage_vs_doc(node_records, node_docs):
        output.append(
            {
                "severity": item.severity,
                "code": item.code,
                "message": item.message,
                "node_index": item.node_index,
            }
        )
    return output


def _spatial_issues(node_records: dict[int, NodePageRecord]) -> tuple[list[dict[str, Any]], int]:
    output: list[dict[str, Any]] = []
    warning_count = 0

    for record in node_records.values():
        for item in check_mbs_obb_record(record):
            output.append(
                {
                    "severity": item.severity,
                    "code": item.code,
                    "message": item.message,
                    "node_index": item.node_index,
                }
            )
            if item.severity == "WARNING":
                warning_count += 1

    for record in node_records.values():
        for child_index in record.children:
            child = node_records.get(child_index)
            if child is None:
                continue
            for item in check_parent_child_mbs(record, child):
                output.append(
                    {
                        "severity": item.severity,
                        "code": item.code,
                        "message": item.message,
                        "node_index": item.node_index,
                    }
                )
                if item.severity == "WARNING":
                    warning_count += 1

    by_level: dict[int | None, list[NodePageRecord]] = {}
    for record in node_records.values():
        by_level.setdefault(record.level, []).append(record)

    for level, items in by_level.items():
        if level is None or level < 0:
            continue
        for item in check_sibling_mbs_overlap(items):
            output.append(
                {
                    "severity": item.severity,
                    "code": item.code,
                    "message": item.message,
                    "node_index": item.node_index,
                }
            )

    return output, warning_count


def _finalize(path: str, summary: dict[str, Any], issues: list[dict[str, Any]]) -> dict[str, Any]:
    summary["severity_counts"] = dict(Counter(item["severity"] for item in issues))
    scores = compute_scores(summary)
    suggestions = _build_suggestions(summary, issues)
    return {
        "package_path": path,
        "summary": summary,
        "issues": issues,
        "scores": {
            "结构完整性": round(scores.structure_integrity, 1),
            "几何与空间质量": round(scores.spatial_quality, 1),
            "纹理与资源闭合": round(scores.texture_quality, 1),
            "LOD 切换质量": round(scores.lod_quality, 1),
            "发布兼容性": round(scores.publish_readiness, 1),
            "综合": round(scores.overall, 1),
        },
        "grade": grade_label(scores.overall),
        "suggestions": suggestions,
    }


def _build_suggestions(summary: dict[str, Any], issues: list[dict[str, Any]]) -> list[str]:
    suggestions: list[str] = []
    codes = {item["code"] for item in issues}
    if summary.get("mapping_missing_targets", 0) > 0:
        suggestions.append("i3s-mapping 中存在失效对象键，需先修复逻辑路径到对象键的映射关系。")
    if summary.get("mapping_duplicate_logicals", 0) > 0:
        suggestions.append("i3s-mapping 出现重复逻辑路径，建议统一为单一来源，避免客户端命中不确定对象。")
    if summary.get("geometry_error_count", 0) > 0:
        suggestions.append("存在缺失的几何资源引用，建议在发布端重建场景层缓存并核对节点资源路径。")
    if summary.get("texture_error_count", 0) > 0:
        suggestions.append("存在缺失的纹理资源，请检查 textureData 与共享材质定义是否一致。")
    if "EARLY_LEVEL_NO_GEOM" in codes:
        suggestions.append("前几层缺少粗几何占位，可考虑补充 coarse mesh 以改善远景与首屏加载体验。")
    if "LOD_THRESH_INVERT" in codes or "LOD_MAXERROR_ORDER" in codes:
        suggestions.append("LOD 阈值或 maxError 在父子链上出现倒挂，建议复查切片参数与层级组织。")
    if "MBS_CHILD_OUTSIDE_PARENT" in codes:
        suggestions.append("父子包围体关系异常，可能导致可视裁剪或 LOD 切换误判，建议重算包围体。")
    if summary.get("i3s_old_version"):
        suggestions.append("当前 I3S 版本偏旧，建议升级到较新的 I3S 版本后再复检。")
    if summary.get("broken_gzip_count", 0) > 0:
        suggestions.append("包内存在无法解压的 .gz 条目，需要修复压缩数据或重新导出场景层。")
    if not suggestions:
        suggestions.append("未发现需要立即处理的严重问题；如客户端仍有异常，可继续做几何抽样与纹理分辨率专项分析。")
    return suggestions


def run_diagnose(package_path: str, json_out: str | None = None) -> str:
    payload = diagnose_slpk(package_path)
    from slpk_diagnoser.report_writer import format_report_text, write_json

    text = format_report_text(payload)
    if json_out:
        with log_timed_operation(logger, "写入 JSON 报告", output=json_out):
            write_json(json_out, payload)
    return text
