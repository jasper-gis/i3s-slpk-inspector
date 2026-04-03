from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from pathlib import Path

from slpk_diagnoser.engine import diagnose_slpk


def _write_json_resource(path: Path, payload: dict, *, compressed: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    if compressed:
        path.write_bytes(gzip.compress(data))
        return
    path.write_bytes(data)


def _build_minimal_package(root: Path, *, scene_compressed: bool, nodepage_compressed: bool, node_doc_compressed: bool) -> None:
    scene_path = root / ("3dSceneLayer.json.gz" if scene_compressed else "3dSceneLayer.json")
    _write_json_resource(
        scene_path,
        {
            "version": "1.7",
            "layerType": "3DObject",
            "nodePages": {"nodesPerPage": 64},
        },
        compressed=scene_compressed,
    )

    nodepage_path = root / "nodepages" / ("0.json.gz" if nodepage_compressed else "0.json")
    _write_json_resource(
        nodepage_path,
        {
            "nodes": [
                {
                    "index": 0,
                    "level": 0,
                    "children": [],
                    "mbs": [0, 0, 0, 1],
                    "geometryData": [{"href": "geometries/0.bin"}],
                    "textureData": [{"href": "textures/0.bin"}],
                }
            ]
        },
        compressed=nodepage_compressed,
    )

    node_doc_path = root / "nodes" / "0" / (
        "3dNodeIndexDocument.json.gz" if node_doc_compressed else "3dNodeIndexDocument.json"
    )
    _write_json_resource(
        node_doc_path,
        {
            "index": 0,
            "level": 0,
            "children": [],
            "mbs": [0, 0, 0, 1],
            "geometryData": [{"href": "geometries/0.bin"}],
            "textureData": [{"href": "textures/0.bin"}],
        },
        compressed=node_doc_compressed,
    )

    (root / "nodes" / "0" / "geometries").mkdir(parents=True, exist_ok=True)
    (root / "nodes" / "0" / "textures").mkdir(parents=True, exist_ok=True)
    (root / "nodes" / "0" / "geometries" / "0.bin").write_bytes(b"geom")
    (root / "nodes" / "0" / "textures" / "0.bin").write_bytes(b"tex")


class JsonResourceSupportTests(unittest.TestCase):
    def test_supports_uncompressed_directory_json_resources(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _build_minimal_package(
                root,
                scene_compressed=False,
                nodepage_compressed=False,
                node_doc_compressed=False,
            )

            payload = diagnose_slpk(str(root))

        summary = payload["summary"]
        issue_codes = {item["code"] for item in payload["issues"]}
        self.assertTrue(summary["has_3d_scene_layer"])
        self.assertEqual(summary["node_pages_files"], 1)
        self.assertEqual(summary["total_nodes"], 1)
        self.assertEqual(summary["node_documents"], 1)
        self.assertNotIn("NO_SCENE_LAYER", issue_codes)
        self.assertNotIn("NODEPAGES_DECL_ONLY", issue_codes)
        self.assertNotIn("NODE_DOC_PARSE", issue_codes)

    def test_supports_mixed_json_and_gzip_resources(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _build_minimal_package(
                root,
                scene_compressed=True,
                nodepage_compressed=False,
                node_doc_compressed=True,
            )

            payload = diagnose_slpk(str(root))

        summary = payload["summary"]
        issue_codes = {item["code"] for item in payload["issues"]}
        self.assertTrue(summary["has_3d_scene_layer"])
        self.assertEqual(summary["node_pages_files"], 1)
        self.assertEqual(summary["total_nodes"], 1)
        self.assertEqual(summary["node_documents"], 1)
        self.assertNotIn("NO_SCENE_LAYER", issue_codes)
        self.assertNotIn("NODEPAGES_DECL_ONLY", issue_codes)
        self.assertNotIn("NODEPAGE_PARSE", issue_codes)


if __name__ == "__main__":
    unittest.main()
