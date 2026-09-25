from pathlib import Path
import json
import os
import time
import unittest

from click.testing import CliRunner

from codeql_bundle.cache import BUNDLE_PLATFORMS, CatalogLoader, source_asset_name
from codeql_bundle.cache_cli import _release_source_assets, main


class CacheCliTests(unittest.TestCase):
    def test_prune_removes_only_expired_cache_entries(self) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem():
            cache_dir = Path("cache")
            old_entry = cache_dir / "sources" / "old"
            recent_entry = cache_dir / "sources" / "recent"
            old_entry.mkdir(parents=True)
            recent_entry.mkdir(parents=True)
            (old_entry / "bundle").write_bytes(b"old")
            (recent_entry / "bundle").write_bytes(b"recent")
            old_time = time.time() - 40 * 24 * 60 * 60
            os.utime(old_entry / "bundle", (old_time, old_time))
            os.utime(old_entry, (old_time, old_time))

            result = runner.invoke(
                main,
                [
                    "prune",
                    "--cache-dir",
                    str(cache_dir),
                    "--max-age-days",
                    "30",
                ],
            )

            self.assertEqual(0, result.exit_code, result.output)
            self.assertFalse(old_entry.exists())
            self.assertTrue(recent_entry.exists())

    def test_build_rejects_invalid_release_plan(self) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem():
            plan_path = Path("plan.json")
            plan_path.write_text("{}")
            result = runner.invoke(
                main,
                [
                    "build",
                    "--plan",
                    str(plan_path),
                    "--target",
                    "codeql/cpp-all",
                    "--output-dir",
                    "dist",
                ],
            )

        self.assertNotEqual(0, result.exit_code)
        self.assertIn("Invalid release plan", result.output)

    def test_release_source_assets_include_arm64_when_available(self) -> None:
        for platforms in (
            ("linux64", "osx64", "win64"),
            BUNDLE_PLATFORMS,
        ):
            with self.subTest(platforms=platforms):
                release = {
                    "tag_name": "codeql-bundle-v1.2.3",
                    "assets": [
                        {
                            "browser_download_url": f"https://example.test/{source_asset_name(platform)}",
                            "digest": f"sha256:{index:064x}",
                            "name": source_asset_name(platform),
                            "size": 100,
                        }
                        for index, platform in enumerate(platforms, start=1)
                    ],
                }

                assets = _release_source_assets(release)

                self.assertEqual(
                    platforms, tuple(asset.platform for asset in assets)
                )

    def test_arm64_catalog_entry_can_be_added_to_catalog(self) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem():
            root = Path.cwd()
            assets = root / "assets"
            assets.mkdir()
            cache_asset = assets / "codeql-compilation-cache-cpp.tar.gz"
            cache_asset.write_bytes(b"cache")
            plan_path = root / "plan.json"
            plan = {
                "cache_format": 1,
                "cache_release": "codeql-compilation-cache-v1.2.3",
                "cli_version": "1.2.3",
                "pack_fingerprint": "0" * 64,
                "release": "codeql-bundle-v1.2.3",
                "source_assets": [
                    {
                        "name": source_asset_name(platform),
                        "platform": platform,
                        "sha256": str(index) * 64,
                        "size": 100,
                        "url": f"https://example.test/{source_asset_name(platform)}",
                    }
                    for index, platform in enumerate(
                        BUNDLE_PLATFORMS,
                        start=1,
                    )
                ],
                "source_repository": "github/codeql-action",
                "targets": [
                    {
                        "language": "cpp",
                        "query_packs": ["codeql/cpp-queries@1.2.3"],
                        "target": "codeql/cpp-all",
                    }
                ],
            }
            plan_path.write_text(json.dumps(plan))
            catalog_path = root / "catalog.json"
            catalog_path.write_text(
                json.dumps({"schema_version": 1, "bundles": []})
            )
            entry_path = root / "entry.json"
            catalog_entry_args = [
                "catalog-entry",
                "--plan",
                str(plan_path),
                "--assets-dir",
                str(assets),
                "--validated-platform",
                "linux-arm64",
                "--output",
                str(entry_path),
            ]

            result = runner.invoke(main, catalog_entry_args)
            self.assertEqual(0, result.exit_code, result.output)

            result = runner.invoke(
                main,
                [
                    "update-catalog",
                    "--catalog",
                    str(catalog_path),
                    "--entry",
                    str(entry_path),
                ],
            )
            self.assertEqual(0, result.exit_code, result.output)

            catalog = CatalogLoader().load(str(catalog_path))
            bundle = catalog.find_release("codeql-bundle-v1.2.3")
            self.assertIsNotNone(bundle)
            self.assertEqual(("linux-arm64",), bundle.validated_platforms)

            plan["source_assets"] = [
                asset
                for asset in plan["source_assets"]
                if asset["platform"] != "linux-arm64"
            ]
            plan_path.write_text(json.dumps(plan))
            result = runner.invoke(main, catalog_entry_args)

            self.assertNotEqual(0, result.exit_code)
            self.assertIn(
                "Release plan has no source bundle for validated platform(s): "
                "linux-arm64.",
                result.output,
            )


if __name__ == "__main__":
    unittest.main()
