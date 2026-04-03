"""SLPK/ESLPK/云对象存储读取抽象。"""

from __future__ import annotations

import gzip
import json
import zipfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from slpk_diagnoser.cloud_storage import (
    CloudStorageLocation,
    create_object_storage_backend,
    is_cloud_storage_uri,
    join_prefix,
    parse_cloud_storage_uri,
)
from slpk_diagnoser.i3s_mapping import build_mapping_index, discover_mapping_document
from slpk_diagnoser.logger import get_logger

logger = get_logger(__name__)


def _norm_name(name: str) -> str:
    return name.replace("\\", "/").strip("/")


@dataclass
class PackageReadResult:
    """包读取元信息与预检查结果。"""

    entry_names: list[str] = field(default_factory=list)
    central_dir_ok: bool = True
    broken_gzip: list[str] = field(default_factory=list)
    zip_errors: list[str] = field(default_factory=list)
    issues: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    inspection_scope: str = "full"


class BasePackageReader(ABC):
    """通用只读包接口：本地 SLPK、ESLPK 目录、云对象存储。"""

    def __init__(self, path: str) -> None:
        self.path = path

    def __enter__(self) -> BasePackageReader:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        return None

    @abstractmethod
    def raw_exists(self, logical_path: str) -> bool:
        """逻辑路径是否存在。"""

    @abstractmethod
    def read_bytes(self, logical_path: str) -> bytes | None:
        """按逻辑路径读取原始字节。"""

    @abstractmethod
    def inspect(self) -> PackageReadResult:
        """枚举条目并执行基础健康检查。"""

    @abstractmethod
    def find_prefix(self, prefix: str) -> list[str]:
        """按前缀查找逻辑路径。"""

    @abstractmethod
    def has_special_hash_index(self) -> bool:
        """是否包含 SLPK special hash index。"""

    @abstractmethod
    def normalized_keys(self) -> list[str]:
        """返回排序后的规范化路径键。"""

    @abstractmethod
    def describe_source(self) -> dict[str, Any]:
        """返回输入源概况。"""

    def read_gunzip_bytes(self, logical_path: str) -> tuple[bytes | None, str | None]:
        raw = self.read_bytes(logical_path)
        if raw is None:
            return None, "missing"
        try:
            return gzip.decompress(raw), None
        except gzip.BadGzipFile as exc:
            err_msg = f"无效的 gzip 格式: {exc}"
            logger.debug("%s: %s", logical_path, err_msg)
            return None, err_msg
        except EOFError as exc:
            err_msg = f"gzip 文件意外结束: {exc}"
            logger.debug("%s: %s", logical_path, err_msg)
            return None, err_msg
        except OSError as exc:
            err_msg = str(exc)
            logger.debug("%s: gzip 解压错误 - %s", logical_path, err_msg)
            return None, err_msg

    def read_json_gz(self, logical_path: str) -> tuple[Any | None, str | None]:
        data, err = self.read_gunzip_bytes(logical_path)
        if err or data is None:
            return None, err or "empty"
        try:
            return json.loads(data.decode("utf-8")), None
        except UnicodeDecodeError as exc:
            err_msg = f"UTF-8 解码失败: {exc}"
            logger.debug("%s: %s", logical_path, err_msg)
            return None, err_msg
        except json.JSONDecodeError as exc:
            err_msg = f"JSON 解析失败: 位置 {exc.pos}, 错误: {exc.msg}"
            logger.debug("%s: %s", logical_path, err_msg)
            return None, err_msg
        except Exception as exc:
            err_msg = f"JSON 处理错误: {exc}"
            logger.debug("%s: %s", logical_path, err_msg)
            return None, err_msg


class SlpkPackageReader(BasePackageReader):
    """只读 ZIP SLPK/ESLPK。"""

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self._zip_reader: zipfile.ZipFile | None = None
        self._index: dict[str, str] = {}

    def __enter__(self) -> SlpkPackageReader:
        file_path = Path(self.path)
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {self.path}")
        if not file_path.is_file():
            raise IsADirectoryError(f"路径不是文件: {self.path}")

        self._zip_reader = zipfile.ZipFile(file_path, "r")
        self._index = {_norm_name(name): name for name in self._zip_reader.namelist()}
        logger.debug("成功打开 SLPK/ESLPK ZIP: %s (条目数=%s)", file_path, len(self._index))
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if self._zip_reader:
            try:
                self._zip_reader.close()
            finally:
                self._zip_reader = None
                self._index = {}

    def _check_initialized(self) -> None:
        if self._zip_reader is None:
            raise RuntimeError("SlpkPackageReader 未初始化，请在 with 语句中使用。")

    def raw_exists(self, logical_path: str) -> bool:
        self._check_initialized()
        return _norm_name(logical_path) in self._index

    def read_bytes(self, logical_path: str) -> bytes | None:
        self._check_initialized()
        key = _norm_name(logical_path)
        if key not in self._index:
            return None
        try:
            return self._zip_reader.read(self._index[key])
        except Exception as exc:
            logger.error("读取 ZIP 条目失败 %s: %s", key, exc)
            return None

    def inspect(self) -> PackageReadResult:
        self._check_initialized()
        result = PackageReadResult()
        try:
            bad_entry = self._zip_reader.testzip()
            if bad_entry:
                result.central_dir_ok = False
                result.zip_errors.append(f"CRC 或条目损坏: {bad_entry}")
        except Exception as exc:
            result.central_dir_ok = False
            result.zip_errors.append(f"ZIP 检查异常: {exc}")

        result.entry_names = sorted(self._index.keys())
        for logical in result.entry_names:
            if logical.lower().endswith(".gz"):
                _, err = self.read_gunzip_bytes(logical)
                if err and err != "missing":
                    result.broken_gzip.append(f"{logical}: {err}")
        return result

    def find_prefix(self, prefix: str) -> list[str]:
        self._check_initialized()
        p = _norm_name(prefix).rstrip("/")
        if not p:
            return sorted(self._index)
        with_sep = p + "/"
        return sorted(x for x in self._index if x == p or x.startswith(with_sep))

    def has_special_hash_index(self) -> bool:
        self._check_initialized()
        return any(
            "@specialindexfilehash128@" in key.lower() or "specialindexfilehash128" in key.lower()
            for key in self._index
        )

    def normalized_keys(self) -> list[str]:
        self._check_initialized()
        return sorted(self._index.keys())

    def describe_source(self) -> dict[str, Any]:
        return {
            "reader_type": "slpk_zip",
            "storage_provider": "zip",
            "storage_bucket": None,
            "storage_prefix": None,
            "mapping_source": "zip-central-directory",
            "mapping_document": None,
            "mapping_entries": len(self._index),
            "mapping_duplicate_logicals": 0,
            "mapping_duplicate_targets": 0,
            "mapping_missing_targets": 0,
            "mapping_unused_objects": 0,
            "mapping_notes": [],
        }


class EslpkDirectoryReader(BasePackageReader):
    """只读 ESLPK 目录或本地对象存储镜像目录。"""

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self._root: Path | None = None
        self._index: dict[str, Path] = {}

    def __enter__(self) -> EslpkDirectoryReader:
        root = Path(self.path).expanduser().resolve()
        if not root.exists():
            raise FileNotFoundError(f"目录不存在: {self.path}")
        if not root.is_dir():
            raise NotADirectoryError(f"路径不是目录: {self.path}")

        self._root = root
        self._index = {}
        for file_path in root.rglob("*"):
            if file_path.is_file():
                logical = _norm_name(str(file_path.relative_to(root)))
                self._index[logical] = file_path
        logger.debug("成功打开 ESLPK/目录镜像: %s (条目数=%s)", root, len(self._index))
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self._root = None
        self._index = {}

    def _check_initialized(self) -> None:
        if self._root is None:
            raise RuntimeError("EslpkDirectoryReader 未初始化，请在 with 语句中使用。")

    def raw_exists(self, logical_path: str) -> bool:
        self._check_initialized()
        return _norm_name(logical_path) in self._index

    def read_bytes(self, logical_path: str) -> bytes | None:
        self._check_initialized()
        path = self._index.get(_norm_name(logical_path))
        if path is None:
            return None
        try:
            return path.read_bytes()
        except Exception as exc:
            logger.error("读取目录对象失败 %s: %s", logical_path, exc)
            return None

    def inspect(self) -> PackageReadResult:
        self._check_initialized()
        result = PackageReadResult(entry_names=sorted(self._index.keys()))
        for logical in result.entry_names:
            if logical.lower().endswith(".gz"):
                _, err = self.read_gunzip_bytes(logical)
                if err and err != "missing":
                    result.broken_gzip.append(f"{logical}: {err}")
        return result

    def find_prefix(self, prefix: str) -> list[str]:
        self._check_initialized()
        p = _norm_name(prefix).rstrip("/")
        if not p:
            return sorted(self._index)
        with_sep = p + "/"
        return sorted(x for x in self._index if x == p or x.startswith(with_sep))

    def has_special_hash_index(self) -> bool:
        self._check_initialized()
        return any(
            "@specialindexfilehash128@" in key.lower() or "specialindexfilehash128" in key.lower()
            for key in self._index
        )

    def normalized_keys(self) -> list[str]:
        self._check_initialized()
        return sorted(self._index.keys())

    def describe_source(self) -> dict[str, Any]:
        return {
            "reader_type": "eslpk_directory",
            "storage_provider": "filesystem",
            "storage_bucket": None,
            "storage_prefix": None,
            "mapping_source": "direct-path",
            "mapping_document": None,
            "mapping_entries": len(self._index),
            "mapping_duplicate_logicals": 0,
            "mapping_duplicate_targets": 0,
            "mapping_missing_targets": 0,
            "mapping_unused_objects": 0,
            "mapping_notes": [],
        }


class CloudObjectStorageReader(BasePackageReader):
    """云对象存储读取器。"""

    def __init__(self, path: str, location: CloudStorageLocation) -> None:
        super().__init__(path)
        self.location = location
        self._backend = create_object_storage_backend(location)
        self._object_keys: list[str] = []
        self._mapping = None

    def __enter__(self) -> CloudObjectStorageReader:
        self._object_keys = self._backend.list_keys(prefix=self.location.prefix)
        logger.debug(
            "成功连接云对象存储: provider=%s, bucket=%s, prefix=%s, object_count=%s",
            self.location.provider,
            self.location.bucket,
            self.location.prefix,
            len(self._object_keys),
        )
        mapping_document_key = discover_mapping_document(
            self._object_keys,
            self.location.prefix,
            explicit_mapping_key=self.location.mapping_key,
        )
        mapping_document = None
        if mapping_document_key:
            raw = self._backend.read_bytes(mapping_document_key)
            if raw is not None:
                try:
                    mapping_document = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    logger.warning("i3s-mapping 解析失败，将回退到直接对象键模式: %s", exc)
        self._mapping = build_mapping_index(
            object_keys=self._object_keys,
            prefix=self.location.prefix,
            mapping_document_key=mapping_document_key,
            mapping_document=mapping_document,
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self._object_keys = []
        self._mapping = None

    def _check_initialized(self) -> None:
        if self._mapping is None:
            raise RuntimeError("CloudObjectStorageReader 未初始化，请在 with 语句中使用。")

    def raw_exists(self, logical_path: str) -> bool:
        self._check_initialized()
        target = self._mapping.resolve_object_key(logical_path)
        if not target:
            return False
        return self._backend.exists(target)

    def read_bytes(self, logical_path: str) -> bytes | None:
        self._check_initialized()
        target = self._mapping.resolve_object_key(logical_path)
        if not target:
            return None
        try:
            return self._backend.read_bytes(target)
        except Exception as exc:
            logger.error("读取云对象失败 %s -> %s: %s", logical_path, target, exc)
            return None

    def inspect(self) -> PackageReadResult:
        self._check_initialized()
        result = PackageReadResult(
            entry_names=self._mapping.logical_keys(),
            central_dir_ok=True,
            inspection_scope="metadata-and-on-demand",
        )
        result.notes.append("云对象存储默认只做映射与关键元数据校验，gzip 采用按需检查。")
        result.issues.extend(self._mapping.issues())
        return result

    def find_prefix(self, prefix: str) -> list[str]:
        self._check_initialized()
        return self._mapping.find_prefix(prefix)

    def has_special_hash_index(self) -> bool:
        self._check_initialized()
        return any(
            "@specialindexfilehash128@" in key.lower() or "specialindexfilehash128" in key.lower()
            for key in self._object_keys
        )

    def normalized_keys(self) -> list[str]:
        self._check_initialized()
        return self._mapping.logical_keys()

    def describe_source(self) -> dict[str, Any]:
        self._check_initialized()
        summary = self._mapping.describe()
        summary.update(
            {
                "reader_type": "cloud_object_storage",
                "storage_provider": self.location.provider,
                "storage_bucket": self.location.bucket,
                "storage_prefix": self.location.prefix or "",
                "storage_endpoint": self.location.endpoint,
                "inspection_scope": "metadata-and-on-demand",
            }
        )
        return summary


def open_package_reader(package_uri: str) -> BasePackageReader:
    """按路径或 URI 自动选择读取器。"""

    cloud_location = parse_cloud_storage_uri(package_uri)
    if cloud_location is not None:
        return CloudObjectStorageReader(package_uri, cloud_location)

    path = Path(package_uri).expanduser()
    suffix = path.suffix.lower()

    if path.exists() and path.is_dir():
        return EslpkDirectoryReader(str(path))
    if suffix in {".slpk", ".eslpk"}:
        return SlpkPackageReader(str(path))
    if path.exists() and path.is_file() and zipfile.is_zipfile(path):
        return SlpkPackageReader(str(path))

    raise ValueError(
        "无法识别输入类型。支持本地 .slpk/.eslpk ZIP、本地 ESLPK 目录，"
        "以及 minio://、oss://、ozone:// 云对象存储 URI。"
    )


__all__ = [
    "BasePackageReader",
    "CloudObjectStorageReader",
    "EslpkDirectoryReader",
    "PackageReadResult",
    "SlpkPackageReader",
    "is_cloud_storage_uri",
    "open_package_reader",
]
