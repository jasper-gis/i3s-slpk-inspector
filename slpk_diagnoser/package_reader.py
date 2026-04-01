"""SLPK（ZIP）封装读取：中央目录、条目枚举、gzip/JSON 解码。"""

from __future__ import annotations

import gzip
import json
import zipfile
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


class SlpkPackageReader:
    """只读 SLPK：验证 ZIP 结构并提供按路径读取。"""

    def __init__(self, path: str) -> None:
        self.path = path
        self._zr: zipfile.ZipFile | None = None
        self._index: dict[str, str] = {}

    def __enter__(self) -> SlpkPackageReader:
        try:
            path_obj = Path(self.path)
            if not path_obj.exists():
                raise FileNotFoundError(f"文件不存在: {self.path}")
            if not path_obj.is_file():
                raise IsADirectoryError(f"路径不是文件: {self.path}")

            self._zr = zipfile.ZipFile(self.path, "r")
            self._index = {_norm_name(n): n for n in self._zr.namelist()}
            logger.debug(f"成功打开 SLPK 包: {self.path} (条目数: {len(self._index)})")
            return self
        except zipfile.BadZipFile as e:
            logger.error(f"无效的 ZIP 文件: {self.path}")
            raise
        except PermissionError as e:
            logger.error(f"没有权限读取文件: {self.path}")
            raise
        except Exception as e:
            logger.error(f"打开 SLPK 包时发生未知错误: {self.path}")
            raise

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
        """检查是否已正确初始化。"""
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

    def inspect(self) -> PackageReadResult:
        """枚举条目并抽样验证 .gz 可解压性。"""
        self._check_initialized()
        result = PackageReadResult()
        try:
            logger.debug("开始检查 ZIP 中央目录...")
            bad = self._zr.testzip()
            if bad:
                result.central_dir_ok = False
                result.zip_errors.append(f"CRC 或条目损坏: {bad}")
                logger.warning(f"检测到损坏的 ZIP 条目: {bad}")
        except RuntimeError as e:
            result.central_dir_ok = False
            result.zip_errors.append(str(e))
            logger.error(f"ZIP 中央目录检查失败: {e}")
        except Exception as e:
            result.central_dir_ok = False
            result.zip_errors.append(f"ZIP 检查异常: {e}")
            logger.error(f"ZIP 检查异常: {e}")

        try:
            names = list(self._zr.namelist())
            result.entry_names = [_norm_name(n) for n in names]
            logger.debug(f"共发现 {len(result.entry_names)} 个条目")

            gzip_count = 0
            for logical in result.entry_names:
                if logical.lower().endswith(".gz"):
                    gzip_count += 1
                    _, err = self.read_gunzip_bytes(logical)
                    if err and err != "missing":
                        result.broken_gzip.append(f"{logical}: {err}")

            if result.broken_gzip:
                logger.warning(f"检测到 {len(result.broken_gzip)} 个损坏的 gzip 条目 (共检查 {gzip_count} 个)")
            else:
                logger.debug(f"所有 {gzip_count} 个 gzip 条目检查通过")

        except Exception as e:
            logger.error(f"枚举条目时发生错误: {e}")

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
        """返回已规范化的 ZIP 条目路径（排序）。"""
        self._check_initialized()
        return sorted(self._index.keys())
