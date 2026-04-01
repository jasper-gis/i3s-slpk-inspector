"""SLPK（ZIP）封装读取：中央目录、条目枚举、gzip/JSON 解码。"""

from __future__ import annotations

import gzip
import io
import json
import zipfile
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any


def _norm_name(name: str) -> str:
    return name.replace("\\", "/").strip("/")


@dataclass
class PackageReadResult:
    """包读取元信息与诊断。"""

    entry_names: list[str] = field(default_factory=list)
    central_dir_ok: bool = True
    broken_gzip: list[str] = field(default_factory=list)
    zip_errors: list[str] = field(default_factory=list)


class SlpkPackageReader:
    """只读 SLPK：验证 ZIP 结构并提供按路径读取。"""

    def __init__(self, path: str) -> None:
        self.path = path
        self._zr: zipfile.ZipFile | None = None
        self._index: dict[str, str] = {}

    def __enter__(self) -> SlpkPackageReader:
        self._zr = zipfile.ZipFile(self.path, "r")
        # 标准化键：POSIX、无首尾斜杠，保留大小写
        self._index = {_norm_name(n): n for n in self._zr.namelist()}
        return self

    def __exit__(self, *args: object) -> None:
        if self._zr:
            self._zr.close()
            self._zr = None
            self._index = {}

    def raw_exists(self, logical_path: str) -> bool:
        assert self._zr is not None
        return _norm_name(logical_path) in self._index

    def read_bytes(self, logical_path: str) -> bytes | None:
        assert self._zr is not None
        key = _norm_name(logical_path)
        if key not in self._index:
            return None
        return self._zr.read(self._index[key])

    def read_gunzip_bytes(self, logical_path: str) -> tuple[bytes | None, str | None]:
        """返回 (解压数据, 错误信息)。"""
        raw = self.read_bytes(logical_path)
        if raw is None:
            return None, "missing"
        try:
            return gzip.decompress(raw), None
        except OSError as e:
            return None, str(e)

    def read_json_gz(self, logical_path: str) -> tuple[Any | None, str | None]:
        data, err = self.read_gunzip_bytes(logical_path)
        if err or data is None:
            return None, err or "empty"
        try:
            return json.loads(data.decode("utf-8")), None
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            return None, str(e)

    def inspect(self) -> PackageReadResult:
        """枚举条目并抽样验证 .gz 可解压性。"""
        assert self._zr is not None
        result = PackageReadResult()
        try:
            bad = self._zr.testzip()
            if bad:
                result.central_dir_ok = False
                result.zip_errors.append(f"CRC 或条目损坏: {bad}")
        except RuntimeError as e:
            result.central_dir_ok = False
            result.zip_errors.append(str(e))
        names = list(self._zr.namelist())
        result.entry_names = [_norm_name(n) for n in names]
        for logical in result.entry_names:
            if logical.lower().endswith(".gz"):
                _, err = self.read_gunzip_bytes(logical)
                if err and err != "missing":
                    result.broken_gzip.append(f"{logical}: {err}")
        return result

    def find_prefix(self, prefix: str) -> list[str]:
        p = _norm_name(prefix).rstrip("/") + "/"
        return sorted(x for x in self._index if x == p[:-1] or x.startswith(p))

    def has_special_hash_index(self) -> bool:
        return any(
            "@specialindexfilehash128@" in k.lower() or "specialindexfilehash128" in k.lower()
            for k in self._index
        )

    def normalized_keys(self) -> list[str]:
        """返回已规范化的 ZIP 条目路径（排序）。"""
        assert self._zr is not None
        return sorted(self._index.keys())
