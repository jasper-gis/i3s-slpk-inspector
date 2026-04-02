"""I3S 逻辑路径与对象键映射。"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from slpk_diagnoser.cloud_storage import join_prefix, strip_prefix

DEFAULT_MAPPING_FILES = (
    "i3s-mapping.json",
    "i3s_mapping.json",
    ".i3s-mapping.json",
    "metadata/i3s-mapping.json",
)


@dataclass
class MappingIssue:
    severity: str
    code: str
    message: str
    logical_path: str | None = None
    object_key: str | None = None


@dataclass
class I3SMappingIndex:
    """逻辑资源路径到对象键的映射索引。"""

    source: str
    logical_to_object: dict[str, str]
    mapping_document: str | None = None
    direct_index: dict[str, str] = field(default_factory=dict)
    duplicate_logicals: list[str] = field(default_factory=list)
    duplicate_targets: list[str] = field(default_factory=list)
    missing_targets: list[str] = field(default_factory=list)
    unused_object_keys: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def logical_keys(self) -> list[str]:
        keys = set(self.logical_to_object)
        keys.update(self.direct_index)
        return sorted(k for k in keys if k)

    def find_prefix(self, prefix: str) -> list[str]:
        p = normalize_logical_path(prefix).rstrip("/")
        if not p:
            return self.logical_keys()
        with_sep = p + "/"
        return sorted(x for x in self.logical_keys() if x == p or x.startswith(with_sep))

    def resolve_object_key(self, logical_path: str) -> str | None:
        logical = normalize_logical_path(logical_path)
        target = self.logical_to_object.get(logical)
        if target:
            return target
        return self.direct_index.get(logical)

    def describe(self) -> dict[str, Any]:
        return {
            "mapping_source": self.source,
            "mapping_document": self.mapping_document,
            "mapping_entries": len(self.logical_to_object),
            "mapping_duplicate_logicals": len(self.duplicate_logicals),
            "mapping_duplicate_targets": len(self.duplicate_targets),
            "mapping_missing_targets": len(self.missing_targets),
            "mapping_unused_objects": len(self.unused_object_keys),
            "mapping_notes": list(self.notes),
        }

    def issues(self, max_unused_reports: int = 20) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for logical in self.duplicate_logicals:
            items.append(
                {
                    "severity": "WARNING",
                    "code": "I3S_MAPPING_DUP_LOGICAL",
                    "message": f"i3s-mapping 中逻辑路径重复定义：{logical}",
                    "logical_path": logical,
                }
            )
        for target in self.duplicate_targets:
            items.append(
                {
                    "severity": "WARNING",
                    "code": "I3S_MAPPING_DUP_TARGET",
                    "message": f"多个逻辑路径映射到了同一个对象键：{target}",
                    "object_key": target,
                }
            )
        for target in self.missing_targets:
            items.append(
                {
                    "severity": "ERROR",
                    "code": "I3S_MAPPING_TARGET_MISSING",
                    "message": f"i3s-mapping 指向的对象不存在：{target}",
                    "object_key": target,
                }
            )
        for note in self.notes:
            items.append(
                {
                    "severity": "INFO",
                    "code": "I3S_MAPPING_NOTE",
                    "message": note,
                }
            )
        if self.unused_object_keys:
            preview = ", ".join(self.unused_object_keys[:max_unused_reports])
            suffix = "" if len(self.unused_object_keys) <= max_unused_reports else " ..."
            items.append(
                {
                    "severity": "INFO",
                    "code": "I3S_MAPPING_UNUSED_OBJECTS",
                    "message": f"存在 {len(self.unused_object_keys)} 个未被 i3s-mapping 使用的对象键：{preview}{suffix}",
                }
            )
        return items


def normalize_logical_path(value: str | None) -> str:
    if not value:
        return ""
    return value.replace("\\", "/").strip("/")


def discover_mapping_document(
    object_keys: list[str],
    prefix: str,
    explicit_mapping_key: str | None = None,
) -> str | None:
    available = set(object_keys)
    if explicit_mapping_key:
        explicit = normalize_logical_path(explicit_mapping_key)
        if explicit in available:
            return explicit
        prefixed = join_prefix(prefix, explicit)
        if prefixed in available:
            return prefixed
    for candidate in DEFAULT_MAPPING_FILES:
        full_key = join_prefix(prefix, candidate)
        if full_key in available:
            return full_key
    return None


def build_mapping_index(
    *,
    object_keys: list[str],
    prefix: str,
    mapping_document_key: str | None = None,
    mapping_document: Any | None = None,
) -> I3SMappingIndex:
    direct_index = {
        relative: full_key
        for full_key in object_keys
        if (relative := strip_prefix(full_key, prefix))
    }

    if mapping_document is None:
        return I3SMappingIndex(
            source="direct-prefix",
            logical_to_object=dict(direct_index),
            mapping_document=None,
            direct_index=direct_index,
            notes=["未发现 i3s-mapping，按对象键与逻辑路径同名处理。"],
        )

    parsed_entries = _parse_mapping_document(mapping_document)
    logical_to_object: dict[str, str] = {}
    duplicate_logicals: list[str] = []
    target_values: list[str] = []

    for logical_raw, target_raw in parsed_entries:
        logical = normalize_logical_path(logical_raw)
        target = _resolve_target_key(target_raw, prefix, object_keys)
        if not logical or not target:
            continue
        if logical in logical_to_object:
            duplicate_logicals.append(logical)
            continue
        logical_to_object[logical] = target
        target_values.append(target)

    target_counts = Counter(target_values)
    duplicate_targets = sorted(target for target, count in target_counts.items() if count > 1)
    missing_targets = sorted(target for target in target_values if target not in object_keys)
    used_targets = set(target_values)
    unused_object_keys = sorted(
        key for key in object_keys if key != mapping_document_key and key not in used_targets
    )
    notes: list[str] = []
    if not logical_to_object:
        notes.append("i3s-mapping 已发现，但未能解析出有效条目，已回退到直接对象键模式。")
        return I3SMappingIndex(
            source="mapping-fallback-direct",
            logical_to_object=dict(direct_index),
            mapping_document=mapping_document_key,
            direct_index=direct_index,
            duplicate_logicals=duplicate_logicals,
            duplicate_targets=duplicate_targets,
            missing_targets=missing_targets,
            unused_object_keys=unused_object_keys,
            notes=notes,
        )

    if direct_index and not set(logical_to_object).issuperset(direct_index):
        notes.append("i3s-mapping 模式已启用；缺失映射时会回退到同名对象键。")

    return I3SMappingIndex(
        source="mapping-document",
        logical_to_object=logical_to_object,
        mapping_document=mapping_document_key,
        direct_index=direct_index,
        duplicate_logicals=sorted(set(duplicate_logicals)),
        duplicate_targets=duplicate_targets,
        missing_targets=missing_targets,
        unused_object_keys=unused_object_keys,
        notes=notes,
    )


def _resolve_target_key(target_raw: Any, prefix: str, object_keys: list[str]) -> str:
    target = normalize_logical_path(str(target_raw) if target_raw is not None else "")
    if not target:
        return ""
    available = set(object_keys)
    if target in available:
        return target
    prefixed = join_prefix(prefix, target)
    if prefixed in available:
        return prefixed
    return prefixed if prefix else target


def _parse_mapping_document(doc: Any) -> list[tuple[str, str]]:
    if isinstance(doc, dict):
        structured = _parse_structured_mapping_dict(doc)
        if structured:
            return structured
        if _looks_like_flat_mapping(doc):
            return [(str(k), str(v)) for k, v in doc.items()]
    if isinstance(doc, list):
        return _parse_mapping_list(doc)
    return []


def _parse_structured_mapping_dict(doc: dict[str, Any]) -> list[tuple[str, str]]:
    for key in ("mappings", "mapping", "entries", "items", "resources"):
        value = doc.get(key)
        if isinstance(value, dict) and _looks_like_flat_mapping(value):
            return [(str(k), str(v)) for k, v in value.items()]
        if isinstance(value, list):
            parsed = _parse_mapping_list(value)
            if parsed:
                return parsed
    return []


def _looks_like_flat_mapping(doc: dict[Any, Any]) -> bool:
    if not doc:
        return False
    return all(isinstance(k, str) and isinstance(v, str) for k, v in doc.items())


def _parse_mapping_list(items: list[Any]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            logical = None
            target = None
            for key in ("logical", "logicalPath", "path", "resourcePath", "i3sPath", "key"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    logical = value
                    break
            for key in ("target", "targetKey", "objectKey", "storageKey", "physicalPath", "value"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    target = value
                    break
            if logical and target:
                out.append((logical, target))
    return out
