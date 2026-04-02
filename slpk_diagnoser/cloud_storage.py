"""云对象存储位置解析与后端封装。"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

from slpk_diagnoser.logger import get_logger

logger = get_logger(__name__)

S3_COMPATIBLE_PROVIDERS = {"minio", "ozone"}
CLOUD_STORAGE_PROVIDERS = {"minio", "oss", "ozone"}


@dataclass(frozen=True)
class CloudStorageLocation:
    provider: str
    bucket: str
    prefix: str
    endpoint: str
    secure: bool = True
    access_key: str | None = None
    secret_key: str | None = None
    session_token: str | None = None
    region: str | None = None
    mapping_key: str | None = None
    timeout_seconds: float = 30.0
    raw_uri: str = ""

    @property
    def provider_label(self) -> str:
        if self.provider == "oss":
            return "Alibaba Cloud OSS"
        if self.provider == "ozone":
            return "ArcGIS Enterprise Ozone"
        return "MinIO"


class ObjectStorageBackend(ABC):
    """对象存储最小只读接口。"""

    def __init__(self, location: CloudStorageLocation) -> None:
        self.location = location

    @abstractmethod
    def list_keys(self, prefix: str = "") -> list[str]:
        """列举 bucket 下的键。"""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """对象是否存在。"""

    @abstractmethod
    def read_bytes(self, key: str) -> bytes | None:
        """读取对象字节。"""


def is_cloud_storage_uri(value: str) -> bool:
    scheme = urlparse(value).scheme.lower()
    return scheme in CLOUD_STORAGE_PROVIDERS


def parse_cloud_storage_uri(value: str) -> CloudStorageLocation | None:
    parsed = urlparse(value)
    provider = parsed.scheme.lower()
    if provider not in CLOUD_STORAGE_PROVIDERS:
        return None

    bucket = parsed.netloc.strip()
    if not bucket:
        raise ValueError(
            f"云存储 URI 缺少 bucket，期望格式示例：{provider}://bucket/prefix?endpoint=host"
        )

    query = {k.lower(): v[-1] for k, v in parse_qs(parsed.query, keep_blank_values=True).items()}
    prefix = _normalize_prefix(parsed.path)
    endpoint = _pick_config(
        query,
        provider,
        "endpoint",
        env_suffixes=("ENDPOINT",),
        required=True,
    )
    access_key = _pick_config(
        query,
        provider,
        "access_key",
        aliases=("accesskey", "access_key_id", "accesskeyid"),
        env_suffixes=("ACCESS_KEY", "ACCESS_KEY_ID"),
    )
    secret_key = _pick_config(
        query,
        provider,
        "secret_key",
        aliases=("secret", "secretkey", "access_key_secret", "accesskeysecret"),
        env_suffixes=("SECRET_KEY", "ACCESS_KEY_SECRET"),
    )
    session_token = _pick_config(
        query,
        provider,
        "session_token",
        aliases=("token", "security_token", "securitytoken"),
        env_suffixes=("SESSION_TOKEN", "SECURITY_TOKEN"),
    )
    region = _pick_config(query, provider, "region", env_suffixes=("REGION",))
    mapping_key = _pick_config(
        query,
        provider,
        "mapping",
        aliases=("mapping_key", "mapping_file"),
        env_suffixes=("MAPPING", "MAPPING_KEY"),
    )
    timeout_text = _pick_config(query, provider, "timeout", env_suffixes=("TIMEOUT",))
    secure = _parse_bool(
        query.get("secure")
        or _pick_config(query, provider, "secure", env_suffixes=("SECURE",))
        or (
            "false"
            if query.get("insecure", "").strip().lower() in {"1", "true", "yes", "on"}
            else None
        ),
        default=True,
    )
    timeout_seconds = _parse_float(timeout_text, default=30.0)

    if provider in S3_COMPATIBLE_PROVIDERS:
        endpoint, inferred_secure = _normalize_s3_endpoint(endpoint)
        if "secure" not in query and _pick_config(query, provider, "secure", env_suffixes=("SECURE",)) is None:
            secure = inferred_secure
    else:
        endpoint = endpoint.rstrip("/")

    return CloudStorageLocation(
        provider=provider,
        bucket=bucket,
        prefix=prefix,
        endpoint=endpoint,
        secure=secure,
        access_key=access_key,
        secret_key=secret_key,
        session_token=session_token,
        region=region,
        mapping_key=_normalize_object_key(mapping_key) if mapping_key else None,
        timeout_seconds=timeout_seconds,
        raw_uri=value,
    )


def create_object_storage_backend(location: CloudStorageLocation) -> ObjectStorageBackend:
    if location.provider == "oss":
        return AliyunOssBackend(location)
    if location.provider in S3_COMPATIBLE_PROVIDERS:
        return S3CompatibleBackend(location)
    raise ValueError(f"不支持的云存储提供方: {location.provider}")


def join_prefix(prefix: str, logical_path: str) -> str:
    logical = _normalize_object_key(logical_path)
    if not prefix:
        return logical
    if not logical:
        return prefix
    return f"{prefix}/{logical}"


def strip_prefix(full_key: str, prefix: str) -> str:
    key = _normalize_object_key(full_key)
    p = _normalize_prefix(prefix)
    if not p:
        return key
    if key == p:
        return ""
    if key.startswith(p + "/"):
        return key[len(p) + 1 :]
    return key


def _normalize_prefix(value: str) -> str:
    return value.replace("\\", "/").strip("/")


def _normalize_object_key(value: str | None) -> str:
    if not value:
        return ""
    return value.replace("\\", "/").strip("/")


def _env_candidates(provider: str, suffix: str) -> list[str]:
    provider_upper = provider.upper()
    return [f"SLPK_DIAGNOSE_{provider_upper}_{suffix}", f"SLPK_DIAGNOSE_{suffix}"]


def _pick_config(
    query: dict[str, str],
    provider: str,
    name: str,
    *,
    aliases: tuple[str, ...] = (),
    env_suffixes: tuple[str, ...] = (),
    required: bool = False,
) -> str | None:
    candidates = (name, *aliases)
    for key in candidates:
        value = query.get(key.lower())
        if value:
            return value

    for suffix in env_suffixes:
        for env_name in _env_candidates(provider, suffix):
            env_value = os.getenv(env_name)
            if env_value:
                return env_value

    if required:
        joined = ", ".join(candidates)
        raise ValueError(f"云存储配置缺少必要参数 {joined}")
    return None


def _parse_bool(value: str | None, *, default: bool) -> bool:
    if value is None or value == "":
        return default
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_float(value: str | None, *, default: float) -> float:
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _normalize_s3_endpoint(endpoint: str) -> tuple[str, bool]:
    if "://" not in endpoint:
        return endpoint.strip("/"), True
    parsed = urlparse(endpoint)
    host = parsed.netloc or parsed.path
    secure = parsed.scheme.lower() != "http"
    return host.strip("/"), secure


class S3CompatibleBackend(ObjectStorageBackend):
    """MinIO / Ozone 等 S3 兼容对象存储。"""

    def __init__(self, location: CloudStorageLocation) -> None:
        super().__init__(location)
        try:
            from minio import Minio
            from minio.error import S3Error
        except ImportError as exc:
            raise ImportError(
                "MinIO/Ozone 云存储支持依赖可选包 `minio`，请执行 `pip install .[cloud]`。"
            ) from exc

        self._error_type = S3Error
        self._client = Minio(
            endpoint=location.endpoint,
            access_key=location.access_key,
            secret_key=location.secret_key,
            session_token=location.session_token,
            secure=location.secure,
            region=location.region,
        )

    def list_keys(self, prefix: str = "") -> list[str]:
        items = self._client.list_objects(
            self.location.bucket,
            prefix=prefix,
            recursive=True,
        )
        return sorted(obj.object_name for obj in items if getattr(obj, "object_name", None))

    def exists(self, key: str) -> bool:
        try:
            self._client.stat_object(self.location.bucket, key)
            return True
        except self._error_type as exc:
            code = getattr(exc, "code", "")
            if code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket", "ResourceNotFound"}:
                return False
            raise

    def read_bytes(self, key: str) -> bytes | None:
        response = self._client.get_object(self.location.bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()


class AliyunOssBackend(ObjectStorageBackend):
    """阿里云 OSS 对象存储。"""

    def __init__(self, location: CloudStorageLocation) -> None:
        super().__init__(location)
        try:
            import oss2
        except ImportError as exc:
            raise ImportError(
                "阿里云 OSS 支持依赖可选包 `oss2`，请执行 `pip install .[cloud]`。"
            ) from exc

        self._oss2 = oss2
        if location.session_token:
            auth = oss2.StsAuth(
                location.access_key or "",
                location.secret_key or "",
                location.session_token,
            )
        else:
            auth = oss2.Auth(location.access_key or "", location.secret_key or "")
        self._bucket = oss2.Bucket(auth, location.endpoint, location.bucket)

    def list_keys(self, prefix: str = "") -> list[str]:
        return sorted(obj.key for obj in self._oss2.ObjectIteratorV2(self._bucket, prefix=prefix))

    def exists(self, key: str) -> bool:
        return bool(self._bucket.object_exists(key))

    def read_bytes(self, key: str) -> bytes | None:
        response = self._bucket.get_object(key)
        try:
            return response.read()
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()
