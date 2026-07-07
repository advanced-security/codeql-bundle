from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from importlib.resources import files
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen
from contextlib import contextmanager
import json
import logging
import os
import platform
import re
import shutil
import tarfile
import time
import uuid

from jsonschema import Draft202012Validator, FormatChecker

from .helpers.codeql import CodeQLPack


logger = logging.getLogger(__name__)

CATALOG_RESOURCE = "supported-codeql-bundles.json"
CATALOG_SCHEMA_RESOURCE = "supported-codeql-bundles.schema.json"
DEFAULT_CATALOG_URL = (
    "https://raw.githubusercontent.com/advanced-security/codeql-bundle/"
    f"main/codeql_bundle/{CATALOG_RESOURCE}"
)
CODEQL_ACTION_REPOSITORY = "github/codeql-action"
CACHE_FORMAT_VERSION = 1
DOWNLOAD_CHUNK_SIZE = 1024 * 1024
CATALOG_REFRESH_SECONDS = 60 * 60
RELEASE_PATTERN = re.compile(r"^codeql-bundle-v\d+\.\d+\.\d+$")
CACHE_RELEASE_PATTERN = re.compile(
    r"^codeql-compilation-cache-v\d+\.\d+\.\d+(?:-[A-Za-z0-9._-]+)?$"
)


class CacheException(Exception):
    pass


class CatalogException(CacheException):
    pass


class DownloadException(CacheException):
    pass


@dataclass(frozen=True)
class ReleaseAsset:
    name: str
    url: str
    sha256: str
    size: int

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ReleaseAsset":
        return cls(
            name=value["name"],
            url=value["url"],
            sha256=value["sha256"],
            size=value["size"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "url": self.url,
            "sha256": self.sha256,
            "size": self.size,
        }


@dataclass(frozen=True)
class SourceAsset(ReleaseAsset):
    platform: str

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SourceAsset":
        return cls(
            name=value["name"],
            url=value["url"],
            sha256=value["sha256"],
            size=value["size"],
            platform=value["platform"],
        )

    def to_dict(self) -> dict[str, Any]:
        value = ReleaseAsset.to_dict(self)
        value["platform"] = self.platform
        return value


@dataclass(frozen=True)
class CompilationCache:
    language: str
    query_packs: tuple[str, ...]
    asset: ReleaseAsset

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "CompilationCache":
        return cls(
            language=value["language"],
            query_packs=tuple(value["query_packs"]),
            asset=ReleaseAsset.from_dict(value["asset"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "language": self.language,
            "query_packs": list(self.query_packs),
            "asset": self.asset.to_dict(),
        }


@dataclass(frozen=True)
class SupportedBundle:
    release: str
    cli_version: str
    source_repository: str
    pack_fingerprint: str
    source_assets: tuple[SourceAsset, ...]
    cache_release: str
    cache_format: int
    compilation_caches: dict[str, CompilationCache]
    validated_platforms: tuple[str, ...]

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SupportedBundle":
        return cls(
            release=value["release"],
            cli_version=value["cli_version"],
            source_repository=value["source_repository"],
            pack_fingerprint=value["pack_fingerprint"],
            source_assets=tuple(
                SourceAsset.from_dict(asset) for asset in value["source_assets"]
            ),
            cache_release=value["cache_release"],
            cache_format=value["cache_format"],
            compilation_caches={
                target: CompilationCache.from_dict(cache)
                for target, cache in value["compilation_caches"].items()
            },
            validated_platforms=tuple(value["validated_platforms"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "release": self.release,
            "cli_version": self.cli_version,
            "source_repository": self.source_repository,
            "pack_fingerprint": self.pack_fingerprint,
            "source_assets": [
                asset.to_dict()
                for asset in sorted(self.source_assets, key=lambda item: item.platform)
            ],
            "cache_release": self.cache_release,
            "cache_format": self.cache_format,
            "compilation_caches": {
                target: cache.to_dict()
                for target, cache in sorted(self.compilation_caches.items())
            },
            "validated_platforms": sorted(self.validated_platforms),
        }

    def source_asset_for_platform(self, platform_name: str) -> Optional[SourceAsset]:
        return next(
            (
                asset
                for asset in self.source_assets
                if asset.platform == platform_name
            ),
            None,
        )


class BundleCatalog:
    def __init__(self, bundles: Iterable[SupportedBundle]):
        self.bundles = tuple(bundles)
        _validate_catalog(
            {
                "schema_version": 1,
                "bundles": [bundle.to_dict() for bundle in self.bundles],
            }
        )
        self._by_release = {bundle.release: bundle for bundle in self.bundles}

        if len(self._by_release) != len(self.bundles):
            raise CatalogException("The supported bundle catalog has duplicate releases.")
        for bundle in self.bundles:
            platforms = [asset.platform for asset in bundle.source_assets]
            if len(set(platforms)) != len(platforms):
                raise CatalogException(
                    f"Bundle {bundle.release} has duplicate source platforms."
                )

        source_digests = [
            asset.sha256
            for bundle in self.bundles
            for asset in bundle.source_assets
        ]
        if len(set(source_digests)) != len(source_digests):
            raise CatalogException(
                "The supported bundle catalog has duplicate source digests."
            )

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "BundleCatalog":
        _validate_catalog(value)
        return cls(SupportedBundle.from_dict(bundle) for bundle in value["bundles"])

    def to_dict(self) -> dict[str, Any]:
        return {
            "$schema": f"./{CATALOG_SCHEMA_RESOURCE}",
            "schema_version": 1,
            "bundles": [
                bundle.to_dict()
                for bundle in sorted(self.bundles, key=lambda item: item.release)
            ],
        }

    def find_release(self, release: str) -> Optional[SupportedBundle]:
        return self._by_release.get(release)

    def find_source_url(
        self, url: str
    ) -> Optional[tuple[SupportedBundle, SourceAsset]]:
        for bundle in self.bundles:
            for asset in bundle.source_assets:
                if asset.url == url:
                    return bundle, asset
        return None

    def find_source_digest(
        self, digest: str
    ) -> Optional[tuple[SupportedBundle, SourceAsset]]:
        matches = [
            (bundle, asset)
            for bundle in self.bundles
            for asset in bundle.source_assets
            if asset.sha256 == digest
        ]
        if len(matches) > 1:
            raise CatalogException(
                f"Source digest {digest} identifies multiple catalog assets."
            )
        return matches[0] if matches else None

    def find_fingerprint(
        self, cli_version: str, pack_fingerprint: str
    ) -> Optional[SupportedBundle]:
        matches = [
            bundle
            for bundle in self.bundles
            if bundle.cli_version == cli_version
            and bundle.pack_fingerprint == pack_fingerprint
        ]
        if len(matches) > 1:
            raise CatalogException(
                "The bundle CLI version and pack fingerprint identify multiple releases."
            )
        return matches[0] if matches else None

    def updated(self, bundle: SupportedBundle) -> "BundleCatalog":
        return BundleCatalog(
            candidate
            for candidate in self.bundles
            if candidate.release != bundle.release
        ).with_added_bundle(bundle)

    def with_added_bundle(self, bundle: SupportedBundle) -> "BundleCatalog":
        return BundleCatalog((*self.bundles, bundle))

    def write(self, path: Path) -> None:
        value = self.to_dict()
        _validate_catalog(value)
        write_json(path, value)

    @classmethod
    def empty(cls) -> "BundleCatalog":
        return cls(())


@dataclass(frozen=True)
class ResolvedBundleSource:
    path: Path
    supported_bundle: Optional[SupportedBundle]
    digest: Optional[str]


def default_cache_dir() -> Path:
    configured = os.environ.get("CODEQL_BUNDLE_CACHE_DIR")
    if configured:
        return Path(configured).expanduser()

    system = platform.system()
    if system == "Windows":
        root = os.environ.get("LOCALAPPDATA")
        return (
            Path(root) / "codeql-bundle"
            if root
            else Path.home() / "AppData" / "Local" / "codeql-bundle"
        )
    if system == "Darwin":
        return Path.home() / "Library" / "Caches" / "codeql-bundle"

    root = os.environ.get("XDG_CACHE_HOME")
    return (
        Path(root) / "codeql-bundle"
        if root
        else Path.home() / ".cache" / "codeql-bundle"
    )


def current_bundle_platform() -> str:
    system = platform.system()
    if system == "Linux":
        return "linux64"
    if system == "Darwin":
        return "osx64"
    if system == "Windows":
        return "win64"
    raise CacheException(f"Unsupported system: {system}")


def sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(DOWNLOAD_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compute_pack_fingerprint(cli_version: str, packs: Iterable[CodeQLPack]) -> str:
    inventory = []
    for pack in packs:
        sources = []
        if pack.path.parent.is_dir():
            for source_path in sorted(pack.path.parent.rglob("*")):
                if (
                    source_path.is_file()
                    and source_path.suffix.lower() != ".qlx"
                    and ".codeql" not in source_path.parts
                    and ".cache" not in source_path.parts
                ):
                    sources.append(
                        {
                            "path": source_path.relative_to(
                                pack.path.parent
                            ).as_posix(),
                            "sha256": sha256_file(source_path),
                        }
                    )
        sources.sort(key=lambda source: source["path"])
        inventory.append(
            {
                "dependencies": {
                    name: str(version)
                    for name, version in sorted(pack.config.dependencies.items())
                },
                "extractor": pack.config.extractor,
                "library": pack.config.library,
                "name": pack.config.name,
                "sources": sources,
                "version": str(pack.config.version),
            }
        )

    encoded = json.dumps(
        {
            "cli_version": cli_version,
            "packs": sorted(inventory, key=lambda item: (item["name"], item["version"])),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return sha256(encoded).hexdigest()


def safe_extract_tar(archive_path: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    try:
        with tarfile.open(archive_path) as archive:
            if hasattr(tarfile, "data_filter"):
                archive.extractall(destination, filter="data")
                return

            root = destination.resolve()
            for member in archive.getmembers():
                member_path = (destination / member.name).resolve()
                if not member_path.is_relative_to(root):
                    raise CacheException(
                        f"Archive {archive_path} contains an unsafe path: {member.name}"
                    )
                if member.ischr() or member.isblk() or member.isfifo():
                    raise CacheException(
                        f"Archive {archive_path} contains a special file: {member.name}"
                    )
                if member.issym():
                    link_path = (member_path.parent / member.linkname).resolve()
                    if not link_path.is_relative_to(root):
                        raise CacheException(
                            f"Archive {archive_path} contains an unsafe link: {member.name}"
                        )
                if member.islnk():
                    link_path = (destination / member.linkname).resolve()
                    if not link_path.is_relative_to(root):
                        raise CacheException(
                            f"Archive {archive_path} contains an unsafe link: {member.name}"
                        )
            archive.extractall(destination)
    except (OSError, tarfile.TarError) as error:
        raise CacheException(f"Failed to extract {archive_path}: {error}") from error


def download_file(
    url: str,
    destination: Path,
    *,
    expected_sha256: Optional[str] = None,
    expected_size: Optional[int] = None,
) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists():
        if expected_sha256 is None and expected_size is None:
            destination.unlink()
        elif expected_size is not None and destination.stat().st_size != expected_size:
            destination.unlink()
        else:
            digest = sha256_file(destination)
            if expected_sha256 is None or digest == expected_sha256:
                _touch(destination)
                logger.info(f"Using cached download {destination}.")
                return digest
            destination.unlink()

    temporary_path = destination.with_name(
        f".{destination.name}.{uuid.uuid4().hex}.part"
    )
    digest = sha256()
    size = 0
    try:
        request = Request(
            url, headers={"User-Agent": "advanced-security/codeql-bundle"}
        )
        with urlopen(request, timeout=60) as response, temporary_path.open("wb") as file:
            while chunk := response.read(DOWNLOAD_CHUNK_SIZE):
                file.write(chunk)
                digest.update(chunk)
                size += len(chunk)
    except (HTTPError, URLError, OSError, ValueError) as error:
        temporary_path.unlink(missing_ok=True)
        raise DownloadException(f"Failed to download {url}: {error}") from error

    actual_sha256 = digest.hexdigest()
    if expected_size is not None and size != expected_size:
        temporary_path.unlink(missing_ok=True)
        raise DownloadException(
            f"Downloaded {url} with size {size}, expected {expected_size}."
        )
    if expected_sha256 is not None and actual_sha256 != expected_sha256:
        temporary_path.unlink(missing_ok=True)
        raise DownloadException(
            f"Downloaded {url} with SHA-256 {actual_sha256}, "
            f"expected {expected_sha256}."
        )

    try:
        os.replace(temporary_path, destination)
    except OSError as error:
        temporary_path.unlink(missing_ok=True)
        raise DownloadException(
            f"Failed to install download at {destination}: {error}"
        ) from error
    return actual_sha256


class CatalogLoader:
    def __init__(self, cache_dir: Optional[Path] = None):
        self.cache_dir = cache_dir if cache_dir is not None else default_cache_dir()

    def load(self, source: Optional[str] = None) -> BundleCatalog:
        if source:
            return BundleCatalog.from_dict(self._read_source(source))

        cached_catalog_path = self.cache_dir / "catalog" / CATALOG_RESOURCE
        if (
            cached_catalog_path.is_file()
            and time.time() - cached_catalog_path.stat().st_mtime
            < CATALOG_REFRESH_SECONDS
        ):
            try:
                return BundleCatalog.from_dict(_read_json(cached_catalog_path))
            except CacheException as error:
                logger.warning(
                    f"Unable to use the cached bundle catalog: {error}"
                )

        try:
            value = self._read_url(DEFAULT_CATALOG_URL)
            catalog = BundleCatalog.from_dict(value)
            try:
                write_json(cached_catalog_path, catalog.to_dict())
            except CacheException as error:
                logger.warning(
                    f"Unable to cache the supported bundle catalog: {error}"
                )
            return catalog
        except CacheException as error:
            if cached_catalog_path.exists():
                try:
                    catalog = BundleCatalog.from_dict(
                        _read_json(cached_catalog_path)
                    )
                    logger.warning(
                        f"Unable to refresh the supported bundle catalog: {error}. "
                        "Using the last downloaded catalog."
                    )
                    return catalog
                except CacheException as cached_error:
                    logger.warning(
                        f"Unable to use the downloaded bundle catalog: {cached_error}"
                    )

            logger.warning(
                f"Unable to download the supported bundle catalog: {error}. "
                "Using the catalog bundled with this installation."
            )
            return self._load_bundled_catalog()

    @staticmethod
    def _load_bundled_catalog() -> BundleCatalog:
        try:
            value = json.loads(
                files("codeql_bundle").joinpath(CATALOG_RESOURCE).read_text()
            )
            return BundleCatalog.from_dict(value)
        except (CacheException, FileNotFoundError, OSError, TypeError) as error:
            logger.warning(
                "The packaged supported bundle catalog is unavailable. "
                f"Continuing without compilation caches: {error}"
            )
            return BundleCatalog.empty()
        except json.JSONDecodeError as error:
            logger.warning(
                "The packaged supported bundle catalog is invalid. "
                f"Continuing without compilation caches: {error}"
            )
            return BundleCatalog.empty()

    def _read_source(self, source: str) -> dict[str, Any]:
        path = Path(source).expanduser()
        if path.exists():
            return _read_json(path)
        if _is_url(source):
            return self._read_url(source)
        raise CatalogException(f"Catalog {source} does not exist.")

    def _read_url(self, url: str) -> dict[str, Any]:
        try:
            request = Request(
                url, headers={"User-Agent": "advanced-security/codeql-bundle"}
            )
            with urlopen(request, timeout=30) as response:
                return json.load(response)
        except (
            HTTPError,
            URLError,
            OSError,
            ValueError,
            json.JSONDecodeError,
        ) as error:
            raise CatalogException(f"Failed to read catalog {url}: {error}") from error


class GitHubReleaseClient:
    def __init__(self, repository: str = CODEQL_ACTION_REPOSITORY):
        self.repository = repository

    def release(self, tag: str) -> dict[str, Any]:
        url = (
            f"https://api.github.com/repos/{self.repository}/releases/tags/"
            f"{quote(tag, safe='')}"
        )
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "advanced-security/codeql-bundle",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            with urlopen(Request(url, headers=headers), timeout=30) as response:
                return json.load(response)
        except (
            HTTPError,
            URLError,
            OSError,
            ValueError,
            json.JSONDecodeError,
        ) as error:
            raise DownloadException(
                f"Failed to read release {self.repository}@{tag}: {error}"
            ) from error

    def releases(self, limit: int = 100) -> list[dict[str, Any]]:
        url = (
            f"https://api.github.com/repos/{self.repository}/releases"
            f"?per_page={min(limit, 100)}"
        )
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "advanced-security/codeql-bundle",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            with urlopen(Request(url, headers=headers), timeout=30) as response:
                return json.load(response)
        except (
            HTTPError,
            URLError,
            OSError,
            ValueError,
            json.JSONDecodeError,
        ) as error:
            raise DownloadException(
                f"Failed to list releases for {self.repository}: {error}"
            ) from error

    def asset(self, tag: str, name: str) -> dict[str, Any]:
        release = self.release(tag)
        return self.find_asset(release, name)

    @staticmethod
    def find_asset(release: dict[str, Any], name: str) -> dict[str, Any]:
        for asset in release.get("assets", []):
            if asset["name"] == name:
                return asset
        raise DownloadException(
            f"Release {release.get('tag_name')} does not contain {name}."
        )


class BundleSourceResolver:
    def __init__(
        self,
        catalog: BundleCatalog,
        cache_dir: Optional[Path] = None,
        release_client: Optional[GitHubReleaseClient] = None,
    ):
        self.catalog = catalog
        self.cache_dir = cache_dir if cache_dir is not None else default_cache_dir()
        self.release_client = release_client or GitHubReleaseClient()

    def resolve(
        self, source: str, requested_platforms: Iterable[str] = ()
    ) -> ResolvedBundleSource:
        path = Path(source).expanduser()
        if path.exists():
            return self._resolve_local(path)
        if _is_url(source):
            return self._resolve_url(source)
        if source.startswith("codeql-bundle-"):
            validate_release(source)
            return self._resolve_release(source, tuple(requested_platforms))
        raise CacheException(
            f"Bundle {source} is not a local path, URL, or CodeQL bundle release tag."
        )

    def _resolve_local(self, path: Path) -> ResolvedBundleSource:
        path = path.resolve()
        if path.is_dir() or not self.catalog.bundles:
            return ResolvedBundleSource(path, None, None)
        digest = sha256_file(path)
        match = self.catalog.find_source_digest(digest)
        return ResolvedBundleSource(path, match[0] if match else None, digest)

    def _resolve_url(self, url: str) -> ResolvedBundleSource:
        match = self.catalog.find_source_url(url)
        if match:
            bundle, asset = match
            destination = cache_path(
                self.cache_dir, "sources", bundle.release, asset.name
            )
            digest = download_file(
                url,
                destination,
                expected_sha256=asset.sha256,
                expected_size=asset.size,
            )
            return ResolvedBundleSource(destination, bundle, digest)

        destination = cache_path(
            self.cache_dir,
            "sources",
            f"url-{sha256(url.encode()).hexdigest()[:16]}",
            "codeql-bundle.tar.gz",
        )
        digest = download_file(url, destination)
        digest_match = self.catalog.find_source_digest(digest)
        return ResolvedBundleSource(
            destination, digest_match[0] if digest_match else None, digest
        )

    def _resolve_release(
        self, release: str, requested_platforms: tuple[str, ...]
    ) -> ResolvedBundleSource:
        platform_name = source_platform_for_request(requested_platforms)
        validate_release(release)
        bundle = self.catalog.find_release(release)
        if bundle:
            asset = bundle.source_asset_for_platform(platform_name)
            if asset is None and platform_name != "all":
                asset = bundle.source_asset_for_platform("all")
            if asset is None:
                raise CatalogException(
                    f"Bundle {release} has no source asset for {platform_name}."
                )
            destination = cache_path(
                self.cache_dir, "sources", release, asset.name
            )
            digest = download_file(
                asset.url,
                destination,
                expected_sha256=asset.sha256,
                expected_size=asset.size,
            )
            return ResolvedBundleSource(destination, bundle, digest)

        asset_name = source_asset_name(platform_name)
        release_value = self.release_client.release(release)
        asset = self.release_client.find_asset(release_value, asset_name)
        destination = cache_path(
            self.cache_dir, "sources", release, asset_name
        )
        expected_digest = github_asset_digest(asset)
        if expected_digest is None:
            checksum_asset = self.release_client.find_asset(
                release_value, f"{asset_name}.checksum.txt"
            )
            expected_digest = download_checksum(
                checksum_asset["browser_download_url"], asset_name
            )
        digest = download_file(
            asset["browser_download_url"],
            destination,
            expected_sha256=expected_digest,
            expected_size=asset.get("size"),
        )
        digest_match = self.catalog.find_source_digest(digest)
        return ResolvedBundleSource(
            destination, digest_match[0] if digest_match else None, digest
        )


class CompilationCacheManager:
    def __init__(
        self,
        supported_bundle: Optional[SupportedBundle],
        cache_dir: Optional[Path] = None,
        *,
        enabled: bool = True,
        platform_name: Optional[str] = None,
    ):
        self.supported_bundle = supported_bundle
        self.cache_dir = cache_dir if cache_dir is not None else default_cache_dir()
        self.enabled = enabled
        self.platform_name = platform_name or current_bundle_platform()
        self._warned_unknown_bundle = False

    def cache_for(self, target: str) -> Optional[Path]:
        if not self.enabled:
            return None
        if self.supported_bundle is None:
            if not self._warned_unknown_bundle:
                logger.warning(
                    "The source bundle is not in the supported bundle catalog. "
                    "Continuing without a compilation cache."
                )
                self._warned_unknown_bundle = True
            return None
        if self.platform_name not in self.supported_bundle.validated_platforms:
            raise CacheException(
                f"Compilation caches for {self.supported_bundle.release} have not "
                f"been validated on {self.platform_name}. Use "
                "--no-compilation-cache to continue without one."
            )

        cache = self.supported_bundle.compilation_caches.get(target)
        if cache is None:
            raise CacheException(
                f"No compilation cache is published for {target} in "
                f"{self.supported_bundle.release}. Use --no-compilation-cache "
                "to continue without one."
            )
        return self._install(target, cache)

    def _install(self, target: str, cache: CompilationCache) -> Path:
        safe_target = _safe_name(target)
        install_path = cache_path(
            self.cache_dir,
            "compilation",
            self.supported_bundle.release,
            safe_target,
            cache.asset.sha256,
        )
        marker_path = install_path / ".installed.json"
        installed_cache_path = install_path / "cache"
        if self._is_installed(
            marker_path, installed_cache_path, target, cache.asset.sha256
        ):
            _touch(install_path)
            return installed_cache_path

        archive_path = cache_path(
            self.cache_dir,
            "cache-archives",
            self.supported_bundle.release,
            cache.asset.name,
        )
        download_file(
            cache.asset.url,
            archive_path,
            expected_sha256=cache.asset.sha256,
            expected_size=cache.asset.size,
        )

        temporary_path = install_path.with_name(
            f".{install_path.name}.{uuid.uuid4().hex}.tmp"
        )
        shutil.rmtree(temporary_path, ignore_errors=True)
        try:
            safe_extract_tar(archive_path, temporary_path)
            extracted_cache_path = temporary_path / "cache"
            if not extracted_cache_path.is_dir():
                raise CacheException(
                    f"Compilation cache archive {cache.asset.name} has no cache directory."
                )
            metadata_path = temporary_path / "metadata.json"
            if not metadata_path.is_file():
                raise CacheException(
                    f"Compilation cache {cache.asset.name} has no metadata."
                )
            metadata = _read_json(metadata_path)
            if metadata.get("release") != self.supported_bundle.release:
                raise CacheException(
                    f"Compilation cache {cache.asset.name} is for "
                    f"{metadata.get('release')}, not {self.supported_bundle.release}."
                )
            if metadata.get("target") != target:
                raise CacheException(
                    f"Compilation cache {cache.asset.name} is for "
                    f"{metadata.get('target')}, not {target}."
                )
            if metadata.get("cache_format") != CACHE_FORMAT_VERSION:
                raise CacheException(
                    f"Compilation cache {cache.asset.name} uses unsupported "
                    f"format {metadata.get('cache_format')}."
                )
            if metadata.get("cli_version") != self.supported_bundle.cli_version:
                raise CacheException(
                    f"Compilation cache {cache.asset.name} is for CodeQL "
                    f"{metadata.get('cli_version')}, not "
                    f"{self.supported_bundle.cli_version}."
                )
            if metadata.get("language") != cache.language:
                raise CacheException(
                    f"Compilation cache {cache.asset.name} is for language "
                    f"{metadata.get('language')}, not {cache.language}."
                )
            if tuple(metadata.get("query_packs", ())) != cache.query_packs:
                raise CacheException(
                    f"Compilation cache {cache.asset.name} has an unexpected "
                    "standard query pack inventory."
                )

            write_json(
                temporary_path / ".installed.json",
                {
                    "cache_format": CACHE_FORMAT_VERSION,
                    "sha256": cache.asset.sha256,
                    "target": target,
                },
            )
            install_path.parent.mkdir(parents=True, exist_ok=True)
            lock_path = install_path.parent / f".{install_path.name}.lock"
            with _exclusive_lock(lock_path):
                if self._is_installed(
                    marker_path,
                    installed_cache_path,
                    target,
                    cache.asset.sha256,
                ):
                    shutil.rmtree(temporary_path)
                    _touch(install_path)
                    return installed_cache_path
                quarantine_path = None
                if install_path.exists():
                    quarantine_path = install_path.with_name(
                        f".{install_path.name}.{uuid.uuid4().hex}.invalid"
                    )
                    os.rename(install_path, quarantine_path)
                try:
                    os.rename(temporary_path, install_path)
                except OSError:
                    if (
                        quarantine_path is not None
                        and not install_path.exists()
                    ):
                        os.rename(quarantine_path, install_path)
                    raise
                if quarantine_path is not None:
                    shutil.rmtree(quarantine_path, ignore_errors=True)
        except (CacheException, OSError):
            shutil.rmtree(temporary_path, ignore_errors=True)
            raise
        return installed_cache_path

    @staticmethod
    def _is_installed(
        marker_path: Path,
        cache_path: Path,
        target: str,
        digest: str,
    ) -> bool:
        if not marker_path.is_file() or not cache_path.is_dir():
            return False
        try:
            marker = _read_json(marker_path)
        except CatalogException:
            return False
        return (
            marker.get("sha256") == digest
            and marker.get("target") == target
            and marker.get("cache_format") == CACHE_FORMAT_VERSION
        )


def _validate_catalog(value: dict[str, Any]) -> None:
    try:
        schema = json.loads(
            files("codeql_bundle").joinpath(CATALOG_SCHEMA_RESOURCE).read_text()
        )
    except (FileNotFoundError, OSError, TypeError) as error:
        if (
            isinstance(value, dict)
            and value.get("schema_version") == 1
            and value.get("bundles") == []
        ):
            return
        raise CatalogException(
            f"The packaged catalog schema is unavailable: {error}"
        ) from error
    except json.JSONDecodeError as error:
        raise CatalogException(
            f"The packaged catalog schema is invalid: {error}"
        ) from error
    errors = sorted(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(value),
        key=lambda error: list(error.absolute_path),
    )
    if errors:
        error = errors[0]
        location = ".".join(map(str, error.absolute_path)) or "<root>"
        raise CatalogException(
            f"Invalid supported bundle catalog at {location}: {error.message}"
        )
    for bundle in value["bundles"]:
        for asset in bundle["source_assets"]:
            validate_remote_url(asset["url"])
        for cache in bundle["compilation_caches"].values():
            validate_remote_url(cache["asset"]["url"])


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open() as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError) as error:
        raise CatalogException(f"Failed to read {path}: {error}") from error


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary_path.open("w") as file:
            json.dump(value, file, indent=2)
            file.write("\n")
        os.replace(temporary_path, path)
    except OSError as error:
        temporary_path.unlink(missing_ok=True)
        raise CacheException(f"Failed to write {path}: {error}") from error


def source_asset_name(platform_name: str) -> str:
    if platform_name == "all":
        return "codeql-bundle.tar.gz"
    if platform_name not in {"linux64", "osx64", "win64"}:
        raise CacheException(f"Unsupported bundle platform: {platform_name}")
    return f"codeql-bundle-{platform_name}.tar.gz"


def source_platform_for_request(requested_platforms: Iterable[str]) -> str:
    requested = tuple(requested_platforms)
    current = current_bundle_platform()
    if not requested:
        return "all"
    if requested == (current,):
        return current
    return "all"


def github_asset_digest(asset: dict[str, Any]) -> Optional[str]:
    digest = asset.get("digest")
    if not digest:
        return None
    algorithm, separator, value = digest.partition(":")
    if separator and algorithm == "sha256" and re.fullmatch(r"[0-9a-f]{64}", value):
        return value
    raise DownloadException(
        f"Release asset {asset.get('name')} has unsupported digest {digest}."
    )


def download_checksum(url: str, expected_name: str) -> str:
    try:
        request = Request(
            url, headers={"User-Agent": "advanced-security/codeql-bundle"}
        )
        with urlopen(request, timeout=30) as response:
            value = response.read().decode()
    except (
        HTTPError,
        URLError,
        OSError,
        UnicodeDecodeError,
        ValueError,
    ) as error:
        raise DownloadException(f"Failed to read checksum {url}: {error}") from error
    for line in value.splitlines():
        match = re.fullmatch(
            r"([0-9a-fA-F]{64})\s+\*?(.+)",
            line.strip(),
        )
        if match is not None and match.group(2) == expected_name:
            return match.group(1).lower()
    raise DownloadException(
        f"Checksum asset for {expected_name} has no matching SHA-256 digest."
    )


def _is_url(value: str) -> bool:
    try:
        return urlparse(value).scheme in {"http", "https", "file"}
    except ValueError:
        return False


def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")


def validate_release(value: str) -> None:
    if not RELEASE_PATTERN.fullmatch(value):
        raise CacheException(f"Invalid CodeQL bundle release tag: {value}")


def validate_cache_release(value: str) -> None:
    if not CACHE_RELEASE_PATTERN.fullmatch(value):
        raise CacheException(f"Invalid compilation cache release tag: {value}")


def validate_remote_url(value: str) -> None:
    try:
        value.encode("ascii")
        parsed = urlparse(value)
        hostname = parsed.hostname
    except (UnicodeEncodeError, ValueError) as error:
        raise CatalogException(f"Invalid catalog URL: {value}") from error
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise CatalogException(f"Invalid catalog URL: {value}")


def cache_path(root: Path, *parts: str) -> Path:
    resolved_root = root.expanduser().resolve()
    candidate = resolved_root.joinpath(*parts).resolve()
    if not candidate.is_relative_to(resolved_root):
        raise CacheException(f"Cache path escapes {resolved_root}: {candidate}")
    return candidate


def _touch(path: Path) -> None:
    try:
        path.touch()
    except OSError as error:
        logger.debug(f"Unable to update cache access time for {path}: {error}")


@contextmanager
def _exclusive_lock(path: Path, timeout: float = 60.0):
    deadline = time.monotonic() + timeout
    path.parent.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(descriptor)
            break
        except FileExistsError:
            try:
                if time.time() - path.stat().st_mtime > timeout:
                    path.unlink()
                    continue
            except FileNotFoundError:
                continue
            if time.monotonic() >= deadline:
                raise CacheException(f"Timed out waiting for cache lock {path}.")
            time.sleep(0.1)
    try:
        yield
    finally:
        path.unlink(missing_ok=True)
