from contextlib import contextmanager
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
import json
import tarfile
import unittest
from unittest.mock import patch

from semantic_version import Version

from codeql_bundle.cache import (
    CACHE_FORMAT_VERSION,
    BundleCatalog,
    BundleSourceResolver,
    CacheException,
    CatalogException,
    CatalogLoader,
    CompilationCache,
    CompilationCacheManager,
    DownloadException,
    GitHubReleaseClient,
    ReleaseAsset,
    SourceAsset,
    SupportedBundle,
    compute_pack_fingerprint,
    current_bundle_platform,
    safe_extract_tar,
    sha256_file,
    source_platform_for_request,
)
from codeql_bundle.helpers.codeql import CodeQLPack, CodeQLPackConfig


class CountingHandler(SimpleHTTPRequestHandler):
    requests = 0

    def do_GET(self) -> None:
        type(self).requests += 1
        super().do_GET()

    def log_message(self, format: str, *args: object) -> None:
        pass


@contextmanager
def serve(directory: Path):
    CountingHandler.requests = 0
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), partial(CountingHandler, directory=str(directory))
    )
    thread = Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def bundle_with_assets(
    source: SourceAsset, cache: ReleaseAsset
) -> SupportedBundle:
    return SupportedBundle(
        release="codeql-bundle-v1.2.3",
        cli_version="1.2.3",
        source_repository="github/codeql-action",
        pack_fingerprint="0" * 64,
        source_assets=(source,),
        cache_release="codeql-compilation-cache-v1.2.3",
        cache_format=CACHE_FORMAT_VERSION,
        compilation_caches={
            "codeql/cpp-all": CompilationCache(
                language="cpp",
                query_packs=("codeql/cpp-queries@1.2.3",),
                asset=cache,
            )
        },
        validated_platforms=(current_bundle_platform(),),
    )


class CatalogTests(unittest.TestCase):
    def test_missing_packaged_resources_fall_back_to_empty_catalog(self) -> None:
        with TemporaryDirectory() as directory:
            loader = CatalogLoader(Path(directory))
            with patch(
                "codeql_bundle.cache.CatalogLoader._read_url",
                side_effect=CatalogException("offline"),
            ), patch(
                "codeql_bundle.cache.files",
                side_effect=FileNotFoundError("not frozen"),
            ):
                catalog = loader.load()

        self.assertEqual((), catalog.bundles)
        self.assertFalse(
            (Path(directory) / "catalog" / "supported-codeql-bundles.json").exists()
        )

    def test_catalog_round_trip_and_update(self) -> None:
        source = SourceAsset(
            name="codeql-bundle.tar.gz",
            url="https://example.test/codeql-bundle.tar.gz",
            sha256="1" * 64,
            size=100,
            platform="all",
        )
        cache = ReleaseAsset(
            name="codeql-compilation-cache-cpp.tar.gz",
            url="https://example.test/codeql-compilation-cache-cpp.tar.gz",
            sha256="2" * 64,
            size=50,
        )
        bundle = bundle_with_assets(source, cache)

        with TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            BundleCatalog([]).updated(bundle).write(path)
            loaded = CatalogLoader().load(str(path))

        self.assertEqual(bundle, loaded.find_release(bundle.release))
        self.assertEqual(
            bundle,
            loaded.find_source_digest(source.sha256)[0],
        )

    def test_catalog_rejects_invalid_digest(self) -> None:
        value = {
            "schema_version": 1,
            "bundles": [
                {
                    "release": "codeql-bundle-v1.2.3",
                    "cli_version": "1.2.3",
                    "source_repository": "github/codeql-action",
                    "pack_fingerprint": "invalid",
                    "source_assets": [],
                    "cache_release": "cache",
                    "cache_format": 1,
                    "compilation_caches": {},
                    "validated_platforms": [],
                }
            ],
        }
        with self.assertRaises(CatalogException):
            BundleCatalog.from_dict(value)

    def test_catalog_rejects_unsafe_asset_name_and_url(self) -> None:
        source = SourceAsset(
            name="codeql-bundle.tar.gz",
            url="https://example.test/codeql-bundle.tar.gz",
            sha256="1" * 64,
            size=100,
            platform="all",
        )
        cache = ReleaseAsset(
            name="cache.tar.gz",
            url="https://example.test/cache.tar.gz",
            sha256="2" * 64,
            size=10,
        )
        value = {
            "schema_version": 1,
            "bundles": [bundle_with_assets(source, cache).to_dict()],
        }
        value["bundles"][0]["source_assets"][0]["name"] = "../../outside"
        with self.assertRaises(CatalogException):
            BundleCatalog.from_dict(value)

        value["bundles"][0]["source_assets"][0]["name"] = "bundle.tar.gz"
        value["bundles"][0]["source_assets"][0]["url"] = "not a URL"
        with self.assertRaises(CatalogException):
            BundleCatalog.from_dict(value)

    def test_pack_fingerprint_is_path_independent(self) -> None:
        config = CodeQLPackConfig(
            name="codeql/example-all",
            version=Version("1.0.0"),
            library=True,
        )
        first = CodeQLPack(path=Path("/one/qlpack.yml"), config=config)
        second = CodeQLPack(path=Path("/two/qlpack.yml"), config=config)
        self.assertEqual(
            compute_pack_fingerprint("2.0.0", [first]),
            compute_pack_fingerprint("2.0.0", [second]),
        )

    def test_pack_fingerprint_changes_with_source(self) -> None:
        with TemporaryDirectory() as directory:
            pack_path = Path(directory) / "qlpack.yml"
            pack_path.write_text("name: codeql/example-all\n")
            source_path = pack_path.parent / "Example.qll"
            source_path.write_text("class Example extends string {}\n")
            pack = CodeQLPack(
                path=pack_path,
                config=CodeQLPackConfig(
                    name="codeql/example-all",
                    version=Version("1.0.0"),
                    library=True,
                ),
            )
            first = compute_pack_fingerprint("2.0.0", [pack])
            source_path.write_text("class Changed extends string {}\n")
            second = compute_pack_fingerprint("2.0.0", [pack])

        self.assertNotEqual(first, second)


class ArtifactTests(unittest.TestCase):
    def test_empty_catalog_does_not_hash_local_archive(self) -> None:
        with TemporaryDirectory() as directory:
            archive = Path(directory) / "codeql-bundle.tar.gz"
            archive.write_bytes(b"bundle")
            with patch(
                "codeql_bundle.cache.sha256_file",
                side_effect=AssertionError("unexpected hash"),
            ):
                resolved = BundleSourceResolver(
                    BundleCatalog.empty(), Path(directory) / "cache"
                ).resolve(str(archive))

        self.assertIsNone(resolved.digest)
        self.assertIsNone(resolved.supported_bundle)

    def test_release_source_is_runnable_on_current_platform(self) -> None:
        current = current_bundle_platform()
        other = next(
            platform
            for platform in ("linux64", "osx64", "win64")
            if platform != current
        )
        self.assertEqual("all", source_platform_for_request(()))
        self.assertEqual(current, source_platform_for_request((current,)))
        self.assertEqual("all", source_platform_for_request((other,)))
        self.assertEqual(
            "all", source_platform_for_request(("linux64", "win64"))
        )

    def test_local_archive_matches_catalog_digest(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "codeql-bundle.tar.gz"
            archive.write_bytes(b"bundle")
            source = SourceAsset(
                name=archive.name,
                url="https://example.test/codeql-bundle.tar.gz",
                sha256=sha256_file(archive),
                size=archive.stat().st_size,
                platform="all",
            )
            cache = ReleaseAsset(
                name="cache.tar.gz",
                url="https://example.test/cache.tar.gz",
                sha256="2" * 64,
                size=10,
            )
            bundle = bundle_with_assets(source, cache)
            resolved = BundleSourceResolver(
                BundleCatalog([bundle]), root / "downloads"
            ).resolve(str(archive))

        self.assertEqual(bundle, resolved.supported_bundle)

    def test_url_download_is_reused(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            served = root / "served"
            served.mkdir()
            archive = served / "codeql-bundle.tar.gz"
            archive.write_bytes(b"bundle")
            with serve(served) as base_url:
                source = SourceAsset(
                    name=archive.name,
                    url=f"{base_url}/{archive.name}",
                    sha256=sha256_file(archive),
                    size=archive.stat().st_size,
                    platform="all",
                )
                cache = ReleaseAsset(
                    name="cache.tar.gz",
                    url=f"{base_url}/cache.tar.gz",
                    sha256="2" * 64,
                    size=10,
                )
                resolver = BundleSourceResolver(
                    BundleCatalog([bundle_with_assets(source, cache)]),
                    root / "downloads",
                )
                first = resolver.resolve(source.url)
                second = resolver.resolve(source.url)

            self.assertEqual(first.path, second.path)
            self.assertEqual(1, CountingHandler.requests)

    def test_arbitrary_url_uses_a_safe_local_archive_name(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            served = root / "served"
            served.mkdir()
            archive = served / "bundle with spaces.tar.gz"
            archive.write_bytes(b"bundle")
            with serve(served) as base_url:
                resolved = BundleSourceResolver(
                    BundleCatalog([]), root / "downloads"
                ).resolve(f"{base_url}/bundle%20with%20spaces.tar.gz")

            self.assertEqual("codeql-bundle.tar.gz", resolved.path.name)
            self.assertEqual(archive.read_bytes(), resolved.path.read_bytes())

    def test_release_uses_checksum_asset_when_digest_is_unavailable(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            served = root / "served"
            served.mkdir()
            archive = served / f"codeql-bundle-{current_bundle_platform()}.tar.gz"
            archive.write_bytes(b"bundle")
            checksum = served / f"{archive.name}.checksum.txt"
            checksum.write_text(f"{sha256_file(archive)}  {archive.name}\n")
            with serve(served) as base_url:
                release = {
                    "tag_name": "codeql-bundle-v1.2.3",
                    "assets": [
                        {
                            "browser_download_url": f"{base_url}/{archive.name}",
                            "digest": None,
                            "name": archive.name,
                            "size": archive.stat().st_size,
                        },
                        {
                            "browser_download_url": f"{base_url}/{checksum.name}",
                            "name": checksum.name,
                        },
                    ],
                }

                class ReleaseClient(GitHubReleaseClient):
                    def release(self, tag: str) -> dict[str, object]:
                        return release

                resolved = BundleSourceResolver(
                    BundleCatalog([]),
                    root / "downloads",
                    release_client=ReleaseClient(),
                ).resolve(
                    "codeql-bundle-v1.2.3",
                    [current_bundle_platform()],
                )

            self.assertEqual(sha256_file(archive), resolved.digest)

    def test_safe_extract_rejects_parent_path(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            archive_path = root / "unsafe.tar.gz"
            with tarfile.open(archive_path, "w:gz") as archive:
                member = tarfile.TarInfo("../escape")
                contents = b"unsafe"
                member.size = len(contents)
                archive.addfile(member, BytesIO(contents))

            with self.assertRaises((CacheException, tarfile.TarError)):
                safe_extract_tar(archive_path, root / "output")
            self.assertFalse((root / "escape").exists())

    def test_safe_extract_fallback_rejects_escaping_hard_link(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            archive_path = root / "unsafe-link.tar.gz"
            with tarfile.open(archive_path, "w:gz") as archive:
                member = tarfile.TarInfo("nested/link")
                member.type = tarfile.LNKTYPE
                member.linkname = "../outside"
                archive.addfile(member)

            data_filter = getattr(tarfile, "data_filter", None)
            if data_filter is not None:
                delattr(tarfile, "data_filter")
            try:
                with self.assertRaises(CacheException):
                    safe_extract_tar(archive_path, root / "output")
            finally:
                if data_filter is not None:
                    setattr(tarfile, "data_filter", data_filter)


class CompilationCacheManagerTests(unittest.TestCase):
    def test_listed_bundle_requires_validated_platform(self) -> None:
        source = SourceAsset(
            name="codeql-bundle.tar.gz",
            url="https://example.test/codeql-bundle.tar.gz",
            sha256="1" * 64,
            size=100,
            platform="all",
        )
        cache = ReleaseAsset(
            name="cache.tar.gz",
            url="https://example.test/cache.tar.gz",
            sha256="2" * 64,
            size=10,
        )
        bundle = bundle_with_assets(source, cache)
        manager = CompilationCacheManager(
            bundle, platform_name="unsupported-platform"
        )
        with self.assertRaises(CacheException):
            manager.cache_for("codeql/cpp-all")

        disabled_manager = CompilationCacheManager(
            bundle,
            enabled=False,
            platform_name="unsupported-platform",
        )
        self.assertIsNone(disabled_manager.cache_for("codeql/cpp-all"))

    def test_listed_bundle_requires_target_cache(self) -> None:
        source = SourceAsset(
            name="codeql-bundle.tar.gz",
            url="https://example.test/codeql-bundle.tar.gz",
            sha256="1" * 64,
            size=100,
            platform="all",
        )
        cache = ReleaseAsset(
            name="cache.tar.gz",
            url="https://example.test/cache.tar.gz",
            sha256="2" * 64,
            size=10,
        )
        manager = CompilationCacheManager(bundle_with_assets(source, cache))
        with self.assertRaises(CacheException):
            manager.cache_for("codeql/java-all")

    def test_cache_is_downloaded_verified_and_reused(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            served = root / "served"
            served.mkdir()
            archive_path = served / "cache.tar.gz"
            self._write_cache_archive(
                archive_path, "codeql-bundle-v1.2.3", "codeql/cpp-all"
            )
            with serve(served) as base_url:
                source = SourceAsset(
                    name="codeql-bundle.tar.gz",
                    url=f"{base_url}/codeql-bundle.tar.gz",
                    sha256="1" * 64,
                    size=100,
                    platform="all",
                )
                cache = ReleaseAsset(
                    name=archive_path.name,
                    url=f"{base_url}/{archive_path.name}",
                    sha256=sha256_file(archive_path),
                    size=archive_path.stat().st_size,
                )
                manager = CompilationCacheManager(
                    bundle_with_assets(source, cache), root / "downloads"
                )
                first = manager.cache_for("codeql/cpp-all")
                second = manager.cache_for("codeql/cpp-all")

            self.assertEqual(first, second)
            self.assertEqual(b"cached", (first / "entry").read_bytes())
            self.assertEqual(1, CountingHandler.requests)

    def test_cache_checksum_mismatch_fails(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            served = root / "served"
            served.mkdir()
            archive_path = served / "cache.tar.gz"
            self._write_cache_archive(
                archive_path, "codeql-bundle-v1.2.3", "codeql/cpp-all"
            )
            with serve(served) as base_url:
                source = SourceAsset(
                    name="codeql-bundle.tar.gz",
                    url=f"{base_url}/codeql-bundle.tar.gz",
                    sha256="1" * 64,
                    size=100,
                    platform="all",
                )
                cache = ReleaseAsset(
                    name=archive_path.name,
                    url=f"{base_url}/{archive_path.name}",
                    sha256="f" * 64,
                    size=archive_path.stat().st_size,
                )
                manager = CompilationCacheManager(
                    bundle_with_assets(source, cache), root / "downloads"
                )
                with self.assertRaises(DownloadException):
                    manager.cache_for("codeql/cpp-all")

    @staticmethod
    def _write_cache_archive(
        path: Path, release: str, target: str
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            cache = root / "cache"
            cache.mkdir()
            (cache / "entry").write_bytes(b"cached")
            (root / "metadata.json").write_text(
                json.dumps(
                    {
                        "cache_format": CACHE_FORMAT_VERSION,
                        "cli_version": "1.2.3",
                        "language": "cpp",
                        "query_packs": ["codeql/cpp-queries@1.2.3"],
                        "release": release,
                        "target": target,
                    }
                )
            )
            with tarfile.open(path, "w:gz") as archive:
                archive.add(cache, arcname="cache")
                archive.add(root / "metadata.json", arcname="metadata.json")


if __name__ == "__main__":
    unittest.main()
