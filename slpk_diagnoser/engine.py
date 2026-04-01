"""编排包级、节点级检查并汇总报告。"""

from __future__ import annotations

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
from slpk_diagnoser.node_parser import NodeIndexDocSummary, parse_3d_node_index_document
from slpk_diagnoser.nodepage_parser import NodePageRecord, parse_node_page_json
from slpk_diagnoser.package_reader import SlpkPackageReader
from slpk_diagnoser.scene_layer_parser import parse_scene_layer
from slpk_diagnoser.scoring import compute_scores, grade_label
from slpk_diagnoser.spatial_checker import (
    check_mbs_obb_record,
    check_parent_child_mbs,
    check_sibling_mbs_overlap,
)
from slpk_diagnoser.texture_checker import check_texture_refs_exist


def _i3s_tuple_version(v: str | None) -> tuple[int, ...]:
    if not v:
        return ()
    parts = re.findall(r"\d+", v)
    return tuple(int(x) for x in parts)


def _is_old_i3s(v: str | None) -> bool:
    t = _i3s_tuple_version(v)
    if not t:
        return False
    return t[0] < 2 and (len(t) < 2 or (t[0] == 1 and t[1] <= 6))


def _extract_node_id_from_path(norm_path: str) -> int | None:
    m = re.match(r"nodes/(\d+)/", norm_path)
    if m:
        return int(m.group(1))
    return None


def diagnose_slpk(package_path: str) -> dict[str, Any]:
    path = str(Path(package_path).expanduser().resolve())
    issues: list[dict[str, Any]] = []
    summary: dict[str, Any] = {
        "package_path": path,
        "has_3d_scene_layer": False,
        "i3s_version": None,
        "layer_type": None,
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
    }

    with SlpkPackageReader(path) as reader:
        insp = reader.inspect()
        summary["broken_gzip_count"] = len(insp.broken_gzip)
        summary["central_dir_ok"] = insp.central_dir_ok
        summary["has_hash_index"] = reader.has_special_hash_index()
        summary["has_metadata_json"] = reader.raw_exists("metadata.json")

        for msg in insp.zip_errors:
            issues.append({"severity": "ERROR", "code": "ZIP", "message": msg})
        for bg in insp.broken_gzip:
            issues.append({"severity": "ERROR", "code": "BAD_GZIP", "message": bg})

        scene_name = "3dSceneLayer.json.gz"
        if not reader.raw_exists(scene_name):
            issues.append(
                {
                    "severity": "ERROR",
                    "code": "NO_SCENE_LAYER",
                    "message": f"缺少根文档 {scene_name}",
                }
            )
            payload = _finalize(path, summary, issues)
            return payload

        summary["has_3d_scene_layer"] = True
        doc, jerr = reader.read_json_gz(scene_name)
        if jerr or not isinstance(doc, dict):
            issues.append(
                {
                    "severity": "ERROR",
                    "code": "SCENE_LAYER_PARSE",
                    "message": f"无法解析 {scene_name}: {jerr}",
                }
            )
            payload = _finalize(path, summary, issues)
            return payload

        sinfo = parse_scene_layer(doc)
        if sinfo:
            summary["i3s_version"] = sinfo.version
            summary["layer_type"] = sinfo.layer_type
            summary["i3s_old_version"] = _is_old_i3s(sinfo.version)

            for href in sinfo.statistics_refs:
                summary["statistics_checked"] += 1
                h = href.strip().lstrip("/").replace("\\", "/")
                if not reader.raw_exists(h) and not reader.raw_exists(h + ".gz"):
                    summary["statistics_missing"] += 1
                    issues.append(
                        {
                            "severity": "WARNING",
                            "code": "STATS_MISSING",
                            "message": f"统计资源未在包内找到: {href}",
                        }
                    )

        np_keys = sorted(
            k
            for k in reader.normalized_keys()
            if k.lower().startswith("nodepages/") and k.lower().endswith(".json.gz")
        )
        summary["node_pages_files"] = len(np_keys)
        if sinfo and sinfo.has_node_pages_decl and not np_keys:
            issues.append(
                {
                    "severity": "WARNING",
                    "code": "NODEPAGES_DECL_ONLY",
                    "message": "场景层声明了 nodePages，但包内未找到 nodePages/*.json.gz 条目",
                }
            )

        records: dict[int, NodePageRecord] = {}
        for nk in np_keys:
            data, err = reader.read_json_gz(nk)
            if err or not isinstance(data, dict):
                issues.append(
                    {
                        "severity": "ERROR",
                        "code": "NODEPAGE_PARSE",
                        "message": f"{nk}: {err}",
                    }
                )
                continue
            for rec in parse_node_page_json(nk, data):
                if rec.index in records:
                    issues.append(
                        {
                            "severity": "INFO",
                            "code": "NODEPAGE_DUP_INDEX",
                            "message": f"节点 index={rec.index} 在多个 nodePage 重复出现，后解析覆盖",
                            "node_index": rec.index,
                        }
                    )
                records[rec.index] = rec

        summary["total_nodes"] = len(records)
        summary["max_level"] = max((r.level for r in records.values() if r.level is not None), default=None)
        summary["level_stats"] = level_statistics(records)

        # 节点文档
        node_docs: dict[int, NodeIndexDocSummary] = {}
        for logical in reader.normalized_keys():
            if not logical.lower().endswith("3dnodeindexdocument.json.gz"):
                continue
            nid = _extract_node_id_from_path(logical)
            data, err = reader.read_json_gz(logical)
            if err or not isinstance(data, dict):
                issues.append(
                    {
                        "severity": "WARNING",
                        "code": "NODE_DOC_PARSE",
                        "message": f"{logical}: {err}",
                    }
                )
                continue
            summ = parse_3d_node_index_document(logical, data)
            idx_key = summ.index if summ.index is not None else nid
            if idx_key is None:
                continue
            node_docs[idx_key] = summ
        summary["node_documents"] = len(node_docs)

        # 一致性
        for it in check_tree_reachability(records):
            issues.append(
                {
                    "severity": it.severity,
                    "code": it.code,
                    "message": it.message,
                    "node_index": it.node_index,
                }
            )
        for it in check_level_continuity(records):
            issues.append(
                {
                    "severity": it.severity,
                    "code": it.code,
                    "message": it.message,
                    "node_index": it.node_index,
                }
            )
        for it in check_nodepage_vs_doc(records, node_docs):
            issues.append(
                {
                    "severity": it.severity,
                    "code": it.code,
                    "message": it.message,
                    "node_index": it.node_index,
                }
            )

        # 空间
        for r in records.values():
            for it in check_mbs_obb_record(r):
                issues.append(
                    {
                        "severity": it.severity,
                        "code": it.code,
                        "message": it.message,
                        "node_index": it.node_index,
                    }
                )
                if it.severity == "WARNING":
                    summary["spatial_warning_count"] += 1

        for r in records.values():
            for cidx in r.children:
                ch = records.get(cidx)
                if ch is None:
                    continue
                for it in check_parent_child_mbs(r, ch):
                    issues.append(
                        {
                            "severity": it.severity,
                            "code": it.code,
                            "message": it.message,
                            "node_index": it.node_index,
                        }
                    )
                    if it.severity == "WARNING":
                        summary["spatial_warning_count"] += 1

        by_level_nodes: dict[int | None, list[NodePageRecord]] = {}
        for r in records.values():
            by_level_nodes.setdefault(r.level, []).append(r)
        for lv, rec_group in by_level_nodes.items():
            if lv is None or lv < 0:
                continue
            for it in check_sibling_mbs_overlap(rec_group):
                issues.append(
                    {
                        "severity": it.severity,
                        "code": it.code,
                        "message": it.message,
                        "node_index": it.node_index,
                    }
                )

        # LOD
        mech = summarize_lod_mechanism(records, node_docs)
        summary["lod_mechanism"] = mech
        lod_t = check_lod_threshold_monotonicity(records)
        lod_s = check_lod_selection_monotonicity(node_docs)
        early = check_early_levels_geometry(records)
        missing_m = check_missing_lod_metrics(records, node_docs)
        for lst in (lod_t, lod_s, early, missing_m):
            for it in lst:
                issues.append(
                    {
                        "severity": it.severity,
                        "code": it.code,
                        "message": it.message,
                        "node_index": it.node_index,
                    }
                )
        summary["lod_subscores"] = lod_smoothness_scores(
            records, node_docs, lod_t, lod_s, early
        )

        # 几何 / 纹理引用
        for idx, d in node_docs.items():
            for it in check_geometry_refs_exist(reader, idx, d.geometry_resources):
                issues.append({"severity": it.severity, "code": it.code, "message": it.message})
                summary["geometry_error_count"] += 1
            for it in check_texture_refs_exist(reader, idx, d.texture_resources):
                issues.append({"severity": it.severity, "code": it.code, "message": it.message})
                summary["texture_error_count"] += 1

        # nodePage 有几何标记但无节点文档时，尝试默认 geometries/0
        for r in records.values():
            if not r.has_geometry_ref:
                continue
            if r.index in node_docs:
                continue
            for it in check_geometry_refs_exist(reader, r.index, ["geometries/0"]):
                if it.severity == "ERROR":
                    issues.append(
                        {
                            "severity": "INFO",
                            "code": "GEOM_UNVERIFIED",
                            "message": f"节点 {r.index} 仅有 nodePage 几何标记，无 3dNodeIndexDocument，未完整验证资源路径",
                            "node_index": r.index,
                        }
                    )

    return _finalize(path, summary, issues)


def _finalize(path: str, summary: dict[str, Any], issues: list[dict[str, Any]]) -> dict[str, Any]:
    summary["severity_counts"] = dict(Counter(i["severity"] for i in issues))
    scores = compute_scores(summary)
    suggestions = _build_suggestions(summary, issues)
    payload = {
        "package_path": path,
        "summary": summary,
        "issues": issues,
        "scores": {
            "结构完整性": round(scores.structure_integrity, 1),
            "几何与包围体质量": round(scores.spatial_quality, 1),
            "纹理与资源闭合": round(scores.texture_quality, 1),
            "LOD 切换质量": round(scores.lod_quality, 1),
            "发布兼容性": round(scores.publish_readiness, 1),
            "综合": round(scores.overall, 1),
        },
        "grade": grade_label(scores.overall),
        "suggestions": suggestions,
    }
    return payload


def _build_suggestions(summary: dict[str, Any], issues: list[dict[str, Any]]) -> list[str]:
    sug: list[str] = []
    codes = {i["code"] for i in issues}
    if summary.get("geometry_error_count", 0) > 0:
        sug.append("存在缺失的几何资源引用，建议在 Pro 或生成端重建场景层缓存并核对节点路径。")
    if summary.get("texture_error_count", 0) > 0:
        sug.append("存在缺失的纹理资源，请检查 textureData 与共享材质定义是否一致。")
    if "EARLY_LEVEL_NO_GEOM" in codes:
        sug.append("前几层缺少粗几何占位，可考虑增加根—低层 coarse mesh 以改善远景与初次加载体验。")
    if "LOD_THRESH_INVERT" in codes or "LOD_MAXERROR_ORDER" in codes:
        sug.append("LOD 阈值或 maxError 在父子链上存在异常顺序，建议复查切片与升级工具参数。")
    if "MBS_CHILD_OUTSIDE_PARENT" in codes:
        sug.append("父子包围体关系异常，可能导致视锥剔除与 LOD 切换误判，建议核对包围体计算。")
    if summary.get("i3s_old_version"):
        sug.append("当前 I3S 版本偏旧，ArcGIS 文档建议升级至较新版本以获得性能与结构优化。")
    if summary.get("broken_gzip_count", 0) > 0:
        sug.append("包内存在无法解压的 .gz 条目，需修复压缩或重新导出 SLPK。")
    if not sug:
        sug.append("未生成专项建议：若仍有显示问题，可开启二期几何抽样与纹理分辨率分析。")
    return sug


def run_diagnose(package_path: str, json_out: str | None = None) -> str:
    payload = diagnose_slpk(package_path)
    from slpk_diagnoser.report_writer import format_report_text, write_json

    text = format_report_text(payload)
    if json_out:
        write_json(json_out, payload)
    return text
