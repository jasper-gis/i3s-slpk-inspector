"""SLPK/ESLPK 读取抽象：支持 ZIP 包、目录包（含对象存储镜像目录）与只读接口。"""

from __future__ import annotations

import gzip
import json
import zipfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from slpk_diagnoser.logger import get_logger

logger = get_logger(__name__)


def _norm_name(name: str) -> str:
    return name.replace("\\", "/").strip("/")


@dataclass
class PackageReadResult:
    """包读取元信息与诊断。"""

    entry_names: list[str] = field(default_factory=list)
    central_dir_ok: bool = True
    broken_gzip: list[str] = field(default_factory=list)
    zip_errors: list[str] = field(default_factory=list)


class BasePackageReader(ABC):
    """通用只读包接口：供 SLPK/ESLPK/对象存储布局实现。"""

    def __init__(self, path: str) -> None:
        self.path = path

    def __enter__(self) -> BasePackageReader:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
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
        """是否包含 special hash index。"""

    @abstractmethod
    def normalized_keys(self) -> list[str]:
        """返回排序后的规范化路径键。"""

    def read_gunzip_bytes(self, logical_path: str) -> tuple[bytes | None, str | None]:
        """返回 (解压数据, 错误信息)。"""
        raw = self.read_bytes(logical_path)
        if raw is None:
            return None, "missing"
        try:
            decompressed = gzip.decompress(raw)
            return decompressed, None
        except gzip.BadGzipFile as e:
            err_msg = f"无效的 gzip 格式: {e}"
            logger.debug(f"{logical_path}: {err_msg}")
            return None, err_msg
        except EOFError as e:
            err_msg = f"gzip 文件意外结束: {e}"
            logger.debug(f"{logical_path}: {err_msg}")
            return None, err_msg
        except OSError as e:
            err_msg = str(e)
            logger.debug(f"{logical_path}: gzip 解压错误 - {err_msg}")
            return None, err_msg

    def read_json_gz(self, logical_path: str) -> tuple[Any | None, str | None]:
        data, err = self.read_gunzip_bytes(logical_path)
        if err or data is None:
            return None, err or "empty"
        try:
            json_str = data.decode("utf-8")
            return json.loads(json_str), None
        except UnicodeDecodeError as e:
            err_msg = f"UTF-8 解码失败: {e}"
            logger.debug(f"{logical_path}: {err_msg}")
            return None, err_msg
        except json.JSONDecodeError as e:
            err_msg = f"JSON 解析失败: 位置 {e.pos}, 错误: {e.msg}"
            logger.debug(f"{logical_path}: {err_msg}")
            return None, err_msg
        except Exception as e:
            err_msg = f"JSON 处理错误: {e}"
            logger.debug(f"{logical_path}: {err_msg}")
            return None, err_msg


class SlpkPackageReader(BasePackageReader):
    """只读 SLPK：验证 ZIP 结构并提供按路径读取。"""

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self._zr: zipfile.ZipFile | None = None
        self._index: dict[str, str] = {}

    def __enter__(self) -> SlpkPackageReader:
        path_obj = Path(self.path)
        if not path_obj.exists():
            raise FileNotFoundError(f"文件不存在: {self.path}")
        if not path_obj.is_file():
            raise IsADirectoryError(f"路径不是文件: {self.path}")

        self._zr = zipfile.ZipFile(self.path, "r")
        self._index = {_norm_name(n): n for n in self._zr.namelist()}
        logger.debug(f"成功打开 SLPK 包: {self.path} (条目数: {len(self._index)})")
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        if self._zr:
            try:
                self._zr.close()
            except Exception as e:
                logger.warning(f"关闭 ZIP 文件时发生错误: {e}")
            finally:
                self._zr = None
                self._index = {}

    def _check_initialized(self) -> None:
        if self._zr is None:
            raise RuntimeError("SlpkPackageReader 未初始化，请在 with 语句中使用")

    def raw_exists(self, logical_path: str) -> bool:
        self._check_initialized()
        return _norm_name(logical_path) in self._index

    def read_bytes(self, logical_path: str) -> bytes | None:
        self._check_initialized()
        key = _norm_name(logical_path)
        if key not in self._index:
            logger.debug(f"条目不存在: {key}")
            return None
        try:
            return self._zr.read(self._index[key])
        except Exception as e:
            logger.error(f"读取条目失败 {key}: {e}")
            return None

    def inspect(self) -> PackageReadResult:
        self._check_initialized()
        result = PackageReadResult()
        try:
            bad = self._zr.testzip()
            if bad:
                result.central_dir_ok = False
                result.zip_errors.append(f"CRC 或条目损坏: {bad}")
        except Exception as e:
            result.central_dir_ok = False
            result.zip_errors.append(f"ZIP 检查异常: {e}")

        result.entry_names = sorted(self._index.keys())
        for logical in result.entry_names:
            if logical.lower().endswith(".gz"):
                _, err = self.read_gunzip_bytes(logical)
                if err and err != "missing":
                    result.broken_gzip.append(f"{logical}: {err}")
        return result

    def find_prefix(self, prefix: str) -> list[str]:
        self._check_initialized()
        p = _norm_name(prefix).rstrip("/") + "/"
        return sorted(x for x in self._index if x == p[:-1] or x.startswith(p))

    def has_special_hash_index(self) -> bool:
        self._check_initialized()
        return any(
            "@specialindexfilehash128@" in k.lower() or "specialindexfilehash128" in k.lower()
            for k in self._index
        )

    def normalized_keys(self) -> list[str]:
        self._check_initialized()
        return sorted(self._index.keys())


class EslpkDirectoryReader(BasePackageReader):
    """只读 ESLPK/对象存储镜像目录：按目录树键名读取对象。"""

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
        for p in root.rglob("*"):
            if p.is_file():
                logical = _norm_name(str(p.relative_to(root)))
                self._index[logical] = p
        logger.debug(f"成功打开 ESLPK 目录: {root} (条目数: {len(self._index)})")
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        self._root = None
        self._index = {}

    def _check_initialized(self) -> None:
        if self._root is None:
            raise RuntimeError("EslpkDirectoryReader 未初始化，请在 with 语句中使用")

    def raw_exists(self, logical_path: str) -> bool:
        self._check_initialized()
        return _norm_name(logical_path) in self._index

    def read_bytes(self, logical_path: str) -> bytes | None:
        self._check_initialized()
        key = _norm_name(logical_path)
        p = self._index.get(key)
        if p is None:
            return None
        try:
            return p.read_bytes()
        except Exception as e:
            logger.error(f"读取目录对象失败 {key}: {e}")
            return None

    def inspect(self) -> PackageReadResult:
        self._check_initialized()
        result = PackageReadResult(entry_names=sorted(self._index.keys()), central_dir_ok=True)
        for logical in result.entry_names:
            if logical.lower().endswith(".gz"):
                _, err = self.read_gunzip_bytes(logical)
                if err and err != "missing":
                    result.broken_gzip.append(f"{logical}: {err}")
        return result

    def find_prefix(self, prefix: str) -> list[str]:
        self._check_initialized()
        p = _norm_name(prefix).rstrip("/") + "/"
        return sorted(x for x in self._index if x == p[:-1] or x.startswith(p))

    def has_special_hash_index(self) -> bool:
        self._check_initialized()
        return any(
            "@specialindexfilehash128@" in k.lower() or "specialindexfilehash128" in k.lower()
            for k in self._index
        )

    def normalized_keys(self) -> list[str]:
        self._check_initialized()
        return sorted(self._index.keys())


class ObjectStorageReader(EslpkDirectoryReader):
    """对象存储本地挂载读取器。

    当前版本支持将对象存储桶通过挂载/同步映射为本地目录后进行读取。
    """


def open_package_reader(package_uri: str) -> BasePackageReader:
    """按路径/URI 自动选择读取器。"""
    p = Path(package_uri).expanduser()
    suffix = p.suffix.lower()

    if p.exists() and p.is_dir():
        return ObjectStorageReader(str(p))
    if suffix in {".slpk", ".eslpk"}:
        return SlpkPackageReader(str(p))
    if p.exists() and p.is_file() and zipfile.is_zipfile(p):
        return SlpkPackageReader(str(p))

    raise ValueError(
        "无法识别输入类型。支持: .slpk/.eslpk ZIP 包，或 ESLPK/对象存储的本地目录镜像。"
    )
