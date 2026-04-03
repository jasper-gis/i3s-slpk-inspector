# i3s-slpk-inspector

面向 `SLPK / ESLPK / I3S` 场景层包的质量诊断工具，重点覆盖以下能力：

- 本地 `SLPK` / `ESLPK ZIP`
- 本地 `ESLPK` 目录
- 云对象存储
  - `MinIO`
  - `阿里云 OSS`
  - `ArcGIS Enterprise Ozone`
- `i3s-mapping` 逻辑资源路径与对象键映射校验
- 结构、节点树、LOD、包围体、几何与纹理资源闭合检查
- 文本报告与 JSON 报告输出

## 1. 功能概览

工具会对以下内容做诊断：

- 包结构
  - 根文档 `3dSceneLayer.json.gz`
  - ZIP 中央目录与 gzip 可读性
  - `metadata.json`
  - `@specialIndexFileHash128@`
- 节点结构
  - `nodePages` 解析
  - 节点树可达性、孤儿节点、层级连续性
  - `nodePage` 与 `3dNodeIndexDocument` 一致性
- 空间质量
  - `mbs` / `obb` 合法性
  - 父子包围体关系
  - 同级节点包围体重叠
- LOD 质量
  - `lodThreshold`
  - `lodSelection`
  - 前层粗几何占位
  - 缺失 LOD 度量
- 资源闭合
  - 几何资源存在性
  - 纹理资源存在性
- 云对象存储与 `i3s-mapping`
  - 逻辑路径到对象键映射解析
  - 重复逻辑路径
  - 重复对象键
  - 映射目标缺失
  - 未被映射使用的对象键

## 2. 安装

### 本地包诊断

```bash
pip install .
```

### 启用云对象存储支持

```bash
pip install ".[cloud]"
```

说明：

- `MinIO` 与 `Ozone` 使用 `minio` SDK。
- `阿里云 OSS` 使用 `oss2` SDK。

## 3. 快速开始

### 诊断本地 SLPK

```bash
slpk-diagnose D:\data\sample.slpk
```

### 诊断本地 ESLPK 目录

```bash
slpk-diagnose D:\data\sample_eslpk
```

### 输出 JSON 报告

```bash
slpk-diagnose D:\data\sample.slpk -o D:\output\report.json
```

### 启用详细日志并写入日志文件

```bash
slpk-diagnose D:\data\sample.slpk -v --log-file D:\output\diagnose.log
```

## 4. 云对象存储 URI 约定

工具支持以下 URI：

- `minio://bucket/prefix?...`
- `oss://bucket/prefix?...`
- `ozone://bucket/prefix?...`

其中：

- `bucket` 是对象存储桶名称。
- `prefix` 是场景层根前缀，可为空。
- `endpoint` 为必须参数。
- 凭证优先从 URI 查询参数读取，也支持环境变量。

### 4.1 MinIO

```text
minio://scene-bucket/i3s/building_01?endpoint=127.0.0.1:9000&access_key=minioadmin&secret_key=minioadmin&secure=false
```

对应环境变量：

- `SLPK_DIAGNOSE_MINIO_ENDPOINT`
- `SLPK_DIAGNOSE_MINIO_ACCESS_KEY`
- `SLPK_DIAGNOSE_MINIO_SECRET_KEY`
- `SLPK_DIAGNOSE_MINIO_SESSION_TOKEN`
- `SLPK_DIAGNOSE_MINIO_REGION`
- `SLPK_DIAGNOSE_MINIO_MAPPING`

### 4.2 阿里云 OSS

```text
oss://scene-bucket/i3s/building_01?endpoint=https://oss-cn-hangzhou.aliyuncs.com&access_key_id=***&access_key_secret=***
```

对应环境变量：

- `SLPK_DIAGNOSE_OSS_ENDPOINT`
- `SLPK_DIAGNOSE_OSS_ACCESS_KEY_ID`
- `SLPK_DIAGNOSE_OSS_ACCESS_KEY_SECRET`
- `SLPK_DIAGNOSE_OSS_SECURITY_TOKEN`
- `SLPK_DIAGNOSE_OSS_MAPPING`

### 4.3 ArcGIS Enterprise Ozone

```text
ozone://scene-bucket/i3s/building_01?endpoint=ozone-gateway.example.com:9000&access_key=***&secret_key=***&secure=false
```

说明：

- 当前实现按 **S3 兼容网关** 方式接入 Ozone。
- 如果你的 ArcGIS Enterprise Ozone 部署没有暴露 S3 兼容入口，建议先通过网关或对象存储镜像目录接入。

对应环境变量：

- `SLPK_DIAGNOSE_OZONE_ENDPOINT`
- `SLPK_DIAGNOSE_OZONE_ACCESS_KEY`
- `SLPK_DIAGNOSE_OZONE_SECRET_KEY`
- `SLPK_DIAGNOSE_OZONE_SESSION_TOKEN`
- `SLPK_DIAGNOSE_OZONE_REGION`
- `SLPK_DIAGNOSE_OZONE_MAPPING`

## 5. i3s-mapping 说明

云对象存储场景下，诊断器会优先尝试发现以下映射文件：

- `i3s-mapping.json`
- `i3s_mapping.json`
- `.i3s-mapping.json`
- `metadata/i3s-mapping.json`

如果找不到映射文件，则退化为：

- 对象键与 I3S 逻辑路径同名

当前支持的映射结构包括：

- 平铺字典

```json
{
  "3dSceneLayer.json.gz": "objects/root/0001.gz",
  "nodepages/0.json.gz": "objects/nodepages/a001.gz"
}
```

- 带 `mappings` / `mapping` / `entries` / `items` / `resources` 的结构化对象
- 条目数组，条目中可使用如下字段组合：
  - 逻辑路径：`logical` / `logicalPath` / `path` / `resourcePath` / `i3sPath`
  - 对象键：`target` / `targetKey` / `objectKey` / `storageKey` / `physicalPath`

诊断结果会给出：

- 映射来源
- 映射文件名
- 逻辑条目数量
- 缺失目标数
- 重复逻辑路径数
- 重复对象键数
- 未使用对象数

## 6. 日志

日志分为两个方向：

- `stdout`
  - 诊断报告正文
- `stderr`
  - 阶段化日志

日志能力包括：

- 开始/结束阶段
- 耗时统计
- 源信息（读取器、存储类型、bucket、prefix）
- 失败上下文
- 对敏感 URI 查询参数做脱敏处理

常用参数：

- `-v`, `--verbose`
  - 输出 DEBUG 级别日志
- `-q`, `--quiet`
  - 只输出错误日志
- `--log-file`
  - 将完整日志追加写入文件

## 7. 输出说明

文本报告包含：

- 输入源摘要
- `i3s-mapping` 摘要
- 五维评分
- LOD 机制识别
- Level 统计
- 问题清单
- 修复建议

JSON 报告适合继续接入自动化流程或平台侧审计系统。

## 8. 已知边界

- 云对象存储默认采用“映射 + 关键元数据 + 按需 gzip”模式，不会像本地 ZIP 一样对所有对象做全量解压扫描。
- `ArcGIS Enterprise Ozone` 当前依赖可访问的 S3 兼容网关。
- 几何与纹理检查目前聚焦“引用闭合”，不做二进制几何内容正确性解码。

## 9. 建议用法

如果你正在做云端 I3S 发布链路，建议把这个工具放在以下环节：

1. 切片完成后，先对本地 `SLPK / ESLPK` 进行诊断。
2. 上传到对象存储后，再对云端 URI 做一次复检。
3. 如果云端使用 `i3s-mapping`，把映射校验作为发布门禁的一部分。
