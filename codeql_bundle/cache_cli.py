from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Optional
import json
import logging
import re
import shutil
import tarfile
import time

import click
from semantic_version import Version

from codeql_bundle.cache import (
    CACHE_FORMAT_VERSION,
    CODEQL_ACTION_REPOSITORY,
    BundleCatalog,
    BundleSourceResolver,
    CacheException,
    CatalogLoader,
    CompilationCache,
    CompilationCacheManager,
    GitHubReleaseClient,
    ReleaseAsset,
    SourceAsset,
    SupportedBundle,
    cache_path,
    compute_pack_fingerprint,
    current_bundle_platform,
    default_cache_dir,
    download_checksum,
    download_file,
    github_asset_digest,
    safe_extract_tar,
    sha256_file,
    source_asset_name,
    validate_cache_release,
    validate_release,
    validate_remote_url,
    write_json,
)
from codeql_bundle.helpers.bundle import (
    Bundle,
    ResolvedCodeQLPack,
    get_compilation_cache_targets,
)


logger = logging.getLogger(__name__)

SOURCE_PLATFORMS = ("all", "linux64", "osx64", "win64")
MAX_RELEASE_ASSET_SIZE = 2 * 1024 * 1024 * 1024
DEFAULT_COMPILATION_CACHE_SIZE_MB = 1536


@click.group()
def main() -> None:
    """Build and manage published CodeQL compilation caches."""
    logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


@main.command("latest-release")
def latest_release() -> None:
    """Print the latest stable upstream CodeQL bundle release tag."""
    releases = GitHubReleaseClient().releases()
    candidates = []
    for release in releases:
        match = re.fullmatch(r"codeql-bundle-v(\d+\.\d+\.\d+)", release["tag_name"])
        if (
            match
            and not release.get("draft")
            and not release.get("prerelease")
        ):
            candidates.append((Version(match.group(1)), release["tag_name"]))
    if not candidates:
        raise click.ClickException("No stable CodeQL bundle release was found.")
    click.echo(max(candidates)[1])


@main.command("catalog-has")
@click.option(
    "--catalog",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option("--release", required=True)
def catalog_has(catalog: Path, release: str) -> None:
    """Print whether a catalog already contains a release."""
    click.echo(
        "true"
        if CatalogLoader().load(str(catalog)).find_release(release)
        else "false"
    )


@main.command("prune")
@click.option(
    "--cache-dir",
    type=click.Path(path_type=Path),
    default=default_cache_dir,
    show_default=True,
)
@click.option(
    "--max-age-days",
    type=click.FloatRange(min=0),
    default=30.0,
    show_default=True,
)
@click.option("--dry-run", is_flag=True)
def prune_cache(cache_dir: Path, max_age_days: float, dry_run: bool) -> None:
    """Remove source bundles and compilation caches older than the given age."""
    cutoff = time.time() - max_age_days * 24 * 60 * 60
    removed_bytes = 0
    removed_entries = 0
    for area in ("sources", "cache-archives", "compilation"):
        root = cache_path(cache_dir, area)
        if not root.is_dir():
            continue
        for entry in sorted(root.iterdir()):
            modified = _path_modified_time(entry)
            if modified > cutoff:
                continue
            size = _path_size(entry)
            action = "Would remove" if dry_run else "Removing"
            click.echo(f"{action} {entry} ({size} bytes).")
            if not dry_run:
                if entry.is_dir() and not entry.is_symlink():
                    shutil.rmtree(entry)
                else:
                    entry.unlink(missing_ok=True)
            removed_bytes += size
            removed_entries += 1
    verb = "Would remove" if dry_run else "Removed"
    click.echo(f"{verb} {removed_entries} entries ({removed_bytes} bytes).")


@main.command("inspect")
@click.option("--bundle", "bundle_source", required=True)
@click.option(
    "--platform",
    "platforms",
    multiple=True,
    type=click.Choice(["linux64", "osx64", "win64"]),
)
@click.option(
    "--cache-dir",
    type=click.Path(path_type=Path),
    default=default_cache_dir,
    show_default=True,
)
@click.option("--catalog")
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
)
def inspect_bundle(
    bundle_source: str,
    platforms: tuple[str, ...],
    cache_dir: Path,
    catalog: Optional[str],
    output: Optional[Path],
) -> None:
    """Inspect a local bundle, release tag, or bundle URL."""
    loaded_catalog = CatalogLoader(cache_dir).load(catalog)
    source = BundleSourceResolver(loaded_catalog, cache_dir).resolve(
        bundle_source, platforms
    )
    bundle = Bundle(source.path)
    supported_bundle = source.supported_bundle or loaded_catalog.find_fingerprint(
        str(bundle.codeql.version()), bundle.pack_fingerprint()
    )
    value = {
        "cli_version": str(bundle.codeql.version()),
        "digest": source.digest,
        "pack_fingerprint": bundle.pack_fingerprint(),
        "platforms": sorted(map(str, bundle.platforms)),
        "source_path": str(source.path),
        "supported_release": (
            supported_bundle.release if supported_bundle is not None else None
        ),
        "targets": _target_values(bundle),
    }
    _emit_json(value, output)


@main.command("plan-release")
@click.option("--release", required=True)
@click.option("--cache-release")
@click.option(
    "--cache-dir",
    type=click.Path(path_type=Path),
    default=default_cache_dir,
    show_default=True,
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
)
def plan_release(
    release: str,
    cache_release: Optional[str],
    cache_dir: Path,
    output: Path,
) -> None:
    """Inspect an upstream release and write a deterministic cache build plan."""
    validate_release(release)
    cache_release = cache_release or _cache_release_tag(release)
    validate_cache_release(cache_release)
    client = GitHubReleaseClient()
    release_value = client.release(release)
    source_assets = _release_source_assets(release_value)
    source_asset = next(
        asset
        for asset in source_assets
        if asset.platform == current_bundle_platform()
    )
    source_path = cache_path(cache_dir, "sources", release, source_asset.name)
    download_file(
        source_asset.url,
        source_path,
        expected_sha256=source_asset.sha256,
        expected_size=source_asset.size,
    )
    bundle = Bundle(source_path)
    plan = {
        "cache_format": CACHE_FORMAT_VERSION,
        "cache_release": cache_release,
        "cli_version": str(bundle.codeql.version()),
        "pack_fingerprint": bundle.pack_fingerprint(),
        "release": release,
        "source_assets": [asset.to_dict() for asset in source_assets],
        "source_repository": CODEQL_ACTION_REPOSITORY,
        "targets": _target_values(bundle),
    }
    write_json(output, plan)
    click.echo(str(output))


@main.command("build")
@click.option(
    "--plan",
    "plan_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option("--target", required=True)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "--cache-dir",
    type=click.Path(path_type=Path),
    default=default_cache_dir,
    show_default=True,
)
@click.option(
    "--cache-size",
    type=click.IntRange(min=1),
    default=DEFAULT_COMPILATION_CACHE_SIZE_MB,
    show_default=True,
)
@click.option("--threads", type=int, default=0)
@click.option("--ram", type=click.IntRange(min=1))
def build_cache(
    plan_path: Path,
    target: str,
    output_dir: Path,
    cache_dir: Path,
    cache_size: int,
    threads: int,
    ram: Optional[int],
) -> None:
    """Build and verify one cache target from a release plan."""
    plan = _read_plan(plan_path)
    target_value = _find_plan_target(plan, target)
    asset_name = _cache_asset_name(target_value["language"])
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_path = output_dir / asset_name
    metadata_path = output_dir / f"{asset_name}.metadata.json"
    checksum_path = output_dir / f"{asset_name}.sha256"

    if archive_path.exists() and metadata_path.exists() and checksum_path.exists():
        try:
            metadata = _read_json(metadata_path)
            digest = sha256_file(archive_path)
            checksum_parts = checksum_path.read_text().split()
            if (
                metadata.get("release") == plan["release"]
                and metadata.get("target") == target
                and checksum_parts
                and checksum_parts[0] == digest
            ):
                _emit_json(_built_asset_value(archive_path, metadata), None)
                return
        except (click.ClickException, OSError) as error:
            logger.warning(f"Rebuilding invalid cache output: {error}")

    bundle = _bundle_from_plan(plan, cache_dir)
    actual_target, query_packs = _find_bundle_target(bundle, target)
    work_cache = (
        output_dir
        / ".work"
        / plan["release"]
        / target_value["language"]
        / "cache"
    )
    work_cache.mkdir(parents=True, exist_ok=True)
    compilation = bundle.codeql.query_compile(
        (pack.path.parent for pack in query_packs),
        work_cache,
        bundle.bundle_path,
        threads=threads,
        ram=ram,
        compilation_cache_size=cache_size,
    )
    logger.info(compilation.stderr)

    representative_query = _representative_query(query_packs)
    verification = bundle.codeql.query_compile(
        [representative_query],
        work_cache,
        bundle.bundle_path,
        threads=threads,
        ram=ram,
        compilation_cache_size=cache_size,
    )
    if "Compilation cache hit" not in verification.stderr:
        raise click.ClickException(
            f"Cache verification for {target} did not report a compilation cache hit."
        )

    metadata = {
        "cache_format": CACHE_FORMAT_VERSION,
        "cli_version": plan["cli_version"],
        "language": target_value["language"],
        "query_packs": target_value["query_packs"],
        "release": plan["release"],
        "target": actual_target.config.name,
    }
    with TemporaryDirectory() as temporary_directory:
        temporary_metadata = Path(temporary_directory) / "metadata.json"
        write_json(temporary_metadata, metadata)
        with tarfile.open(archive_path, mode="w:gz") as archive:
            archive.add(work_cache, arcname="cache")
            archive.add(temporary_metadata, arcname="metadata.json")

    if archive_path.stat().st_size > MAX_RELEASE_ASSET_SIZE:
        archive_path.unlink()
        raise click.ClickException(
            f"Cache asset {asset_name} exceeds GitHub's release asset size limit."
        )
    write_json(metadata_path, metadata)
    digest = sha256_file(archive_path)
    checksum_path.write_text(f"{digest}  {asset_name}\n")
    _emit_json(_built_asset_value(archive_path, metadata), None)


@main.command("verify")
@click.option(
    "--plan",
    "plan_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option("--target", required=True)
@click.option(
    "--cache",
    "cache_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "--cache-dir",
    type=click.Path(path_type=Path),
    default=default_cache_dir,
    show_default=True,
)
def verify_cache(
    plan_path: Path, target: str, cache_path: Path, cache_dir: Path
) -> None:
    """Verify a built cache against the current platform's upstream bundle."""
    plan = _read_plan(plan_path)
    bundle = _bundle_from_plan(plan, cache_dir)
    _verify_cache_with_bundle(plan, bundle, target, cache_path)
    click.echo(f"Verified {target} on {current_bundle_platform()}.")


@main.command("verify-all")
@click.option(
    "--plan",
    "plan_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "--assets-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "--cache-dir",
    type=click.Path(path_type=Path),
    default=default_cache_dir,
    show_default=True,
)
def verify_all(plan_path: Path, assets_dir: Path, cache_dir: Path) -> None:
    """Verify every cache in a release plan on the current platform."""
    plan = _read_plan(plan_path)
    bundle = _bundle_from_plan(plan, cache_dir)
    for target in plan["targets"]:
        cache_path = assets_dir / _cache_asset_name(target["language"])
        if not cache_path.is_file():
            raise click.ClickException(f"Missing cache asset {cache_path}.")
        _verify_cache_with_bundle(plan, bundle, target["target"], cache_path)
        click.echo(
            f"Verified {target['target']} on {current_bundle_platform()}."
        )


@main.command("verify-entry")
@click.option(
    "--entry",
    "entry_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "--cache-dir",
    type=click.Path(path_type=Path),
    default=default_cache_dir,
    show_default=True,
)
def verify_entry(entry_path: Path, cache_dir: Path) -> None:
    """Download and verify every public cache asset in a catalog entry."""
    bundle = _read_bundle_entry(entry_path)
    manager = CompilationCacheManager(bundle, cache_dir)
    for target in sorted(bundle.compilation_caches):
        manager.cache_for(target)
        click.echo(f"Downloaded and verified {target}.")


def _verify_cache_with_bundle(
    plan: dict[str, Any],
    bundle: Bundle,
    target: str,
    cache_path: Path,
) -> None:
    _, query_packs = _find_bundle_target(bundle, target)
    with TemporaryDirectory() as temporary_directory:
        extracted_path = Path(temporary_directory)
        safe_extract_tar(cache_path, extracted_path)
        metadata = _read_json(extracted_path / "metadata.json")
        if (
            metadata.get("release") != plan["release"]
            or metadata.get("target") != target
            or metadata.get("cache_format") != CACHE_FORMAT_VERSION
        ):
            raise click.ClickException(
                f"Cache {cache_path} metadata does not match {plan['release']}:{target}."
            )
        verification = bundle.codeql.query_compile(
            [_representative_query(query_packs)],
            extracted_path / "cache",
            bundle.bundle_path,
            threads=0,
        )
        if "Compilation cache hit" not in verification.stderr:
            raise click.ClickException(
                f"Cache {cache_path} did not report a compilation cache hit."
            )


@main.command("catalog-entry")
@click.option(
    "--plan",
    "plan_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "--assets-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
)
@click.option("--repository", default="advanced-security/codeql-bundle")
@click.option(
    "--validated-platform",
    "validated_platforms",
    multiple=True,
    required=True,
    type=click.Choice(["linux64", "osx64", "win64"]),
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
)
def catalog_entry(
    plan_path: Path,
    assets_dir: Path,
    repository: str,
    validated_platforms: tuple[str, ...],
    output: Path,
) -> None:
    """Create a catalog entry for verified release assets."""
    plan = _read_plan(plan_path)
    compilation_caches = {}
    for target in plan["targets"]:
        asset_name = _cache_asset_name(target["language"])
        asset_path = assets_dir / asset_name
        if not asset_path.is_file():
            raise click.ClickException(f"Missing cache asset {asset_path}.")
        asset = ReleaseAsset(
            name=asset_name,
            url=(
                f"https://github.com/{repository}/releases/download/"
                f"{plan['cache_release']}/{asset_name}"
            ),
            sha256=sha256_file(asset_path),
            size=asset_path.stat().st_size,
        )
        compilation_caches[target["target"]] = CompilationCache(
            language=target["language"],
            query_packs=tuple(target["query_packs"]),
            asset=asset,
        )

    bundle = SupportedBundle(
        release=plan["release"],
        cli_version=plan["cli_version"],
        source_repository=plan["source_repository"],
        pack_fingerprint=plan["pack_fingerprint"],
        source_assets=tuple(
            SourceAsset.from_dict(asset) for asset in plan["source_assets"]
        ),
        cache_release=plan["cache_release"],
        cache_format=CACHE_FORMAT_VERSION,
        compilation_caches=compilation_caches,
        validated_platforms=tuple(validated_platforms),
    )
    BundleCatalog.from_dict(
        {"schema_version": 1, "bundles": [bundle.to_dict()]}
    )
    write_json(output, bundle.to_dict())
    click.echo(str(output))


@main.command("update-catalog")
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "--entry",
    "entry_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
)
def update_catalog(
    catalog_path: Path, entry_path: Path, output: Optional[Path]
) -> None:
    """Insert or replace a verified entry in the authoritative catalog."""
    catalog = CatalogLoader().load(str(catalog_path))
    bundle = _read_bundle_entry(entry_path)
    destination = output or catalog_path
    catalog.updated(bundle).write(destination)
    click.echo(str(destination))


def _release_source_assets(release: dict[str, Any]) -> tuple[SourceAsset, ...]:
    assets = {asset["name"]: asset for asset in release.get("assets", [])}
    result = []
    for platform_name in SOURCE_PLATFORMS:
        name = source_asset_name(platform_name)
        asset = assets.get(name)
        if asset is None:
            raise click.ClickException(
                f"Upstream release {release['tag_name']} has no {name}."
            )
        digest = github_asset_digest(asset)
        if digest is None:
            checksum_asset = assets.get(f"{name}.checksum.txt")
            if checksum_asset is None:
                raise click.ClickException(
                    f"Upstream release asset {name} has no SHA-256 digest."
                )
            digest = download_checksum(
                checksum_asset["browser_download_url"], expected_name=name
            )
        result.append(
            SourceAsset(
                name=name,
                url=asset["browser_download_url"],
                sha256=digest,
                size=asset["size"],
                platform=platform_name,
            )
        )
    return tuple(result)


def _target_values(bundle: Bundle) -> list[dict[str, Any]]:
    targets = get_compilation_cache_targets(bundle.get_bundle_packs())
    values = []
    languages = set()
    for target, query_packs in sorted(
        targets.items(), key=lambda item: item[0].config.name
    ):
        language = target.config.get_pack_name().removesuffix("-all")
        if language in languages:
            raise CacheException(
                f"Multiple compilation cache targets map to language {language}."
            )
        languages.add(language)
        values.append(
            {
                "language": language,
                "query_packs": [
                    f"{pack.config.name}@{pack.config.version}"
                    for pack in sorted(query_packs, key=lambda item: item.config.name)
                ],
                "target": target.config.name,
            }
        )
    return values


def _bundle_from_plan(plan: dict[str, Any], cache_dir: Path) -> Bundle:
    validate_release(plan["release"])
    platform_name = current_bundle_platform()
    asset = next(
        (
            SourceAsset.from_dict(value)
            for value in plan["source_assets"]
            if value["platform"] == platform_name
        ),
        None,
    )
    if asset is None:
        raise click.ClickException(
            f"Release plan has no source bundle for {platform_name}."
        )
    validate_remote_url(asset.url)
    source_path = cache_path(
        cache_dir, "sources", plan["release"], asset.name
    )
    download_file(
        asset.url,
        source_path,
        expected_sha256=asset.sha256,
        expected_size=asset.size,
    )
    bundle = Bundle(source_path)
    fingerprint = compute_pack_fingerprint(
        str(bundle.codeql.version()), bundle.get_bundle_packs()
    )
    if (
        str(bundle.codeql.version()) != plan["cli_version"]
        or fingerprint != plan["pack_fingerprint"]
    ):
        raise click.ClickException(
            f"Source bundle for {plan['release']} does not match its release plan."
        )
    return bundle


def _find_bundle_target(
    bundle: Bundle, target_name: str
) -> tuple[ResolvedCodeQLPack, list[ResolvedCodeQLPack]]:
    targets = get_compilation_cache_targets(bundle.get_bundle_packs())
    for target, query_packs in targets.items():
        if target.config.name == target_name:
            return target, query_packs
    raise click.ClickException(f"Bundle has no cache target {target_name}.")


def _find_plan_target(plan: dict[str, Any], target_name: str) -> dict[str, Any]:
    for target in plan["targets"]:
        if target["target"] == target_name:
            return target
    raise click.ClickException(f"Release plan has no cache target {target_name}.")


def _representative_query(query_packs: list[ResolvedCodeQLPack]) -> Path:
    for query_pack in sorted(query_packs, key=lambda pack: pack.config.name):
        for query in sorted(query_pack.path.parent.rglob("*.ql")):
            relative_path = query.relative_to(query_pack.path.parent)
            if (
                ".codeql" not in relative_path.parts
                and ".cache" not in relative_path.parts
            ):
                return query
    raise click.ClickException("No standard query was found for cache verification.")


def _cache_release_tag(release: str) -> str:
    return f"codeql-compilation-cache-{release.removeprefix('codeql-bundle-')}"


def _cache_asset_name(language: str) -> str:
    return f"codeql-compilation-cache-{language}.tar.gz"


def _built_asset_value(
    archive_path: Path, metadata: dict[str, Any]
) -> dict[str, Any]:
    return {
        "asset": archive_path.name,
        "language": metadata["language"],
        "sha256": sha256_file(archive_path),
        "size": archive_path.stat().st_size,
        "target": metadata["target"],
    }


def _path_size(path: Path) -> int:
    if path.is_symlink() or path.is_file():
        try:
            return path.stat(follow_symlinks=False).st_size
        except FileNotFoundError:
            return 0
    total = 0
    for candidate in path.rglob("*"):
        if candidate.is_file() and not candidate.is_symlink():
            try:
                total += candidate.stat().st_size
            except FileNotFoundError:
                continue
    return total


def _path_modified_time(path: Path) -> float:
    try:
        modified = path.stat(follow_symlinks=False).st_mtime
    except FileNotFoundError:
        return 0
    if path.is_dir() and not path.is_symlink():
        for candidate in path.rglob("*"):
            try:
                modified = max(
                    modified,
                    candidate.stat(follow_symlinks=False).st_mtime,
                )
            except FileNotFoundError:
                continue
    return modified


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open() as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError) as error:
        raise click.ClickException(f"Failed to read {path}: {error}") from error


def _read_plan(path: Path) -> dict[str, Any]:
    value = _read_json(path)
    try:
        if (
            type(value["cache_format"]) is not int
            or value["cache_format"] != CACHE_FORMAT_VERSION
        ):
            raise ValueError(
                f"unsupported cache format {value['cache_format']}"
            )
        validate_release(value["release"])
        validate_cache_release(value["cache_release"])
        if not isinstance(value["cli_version"], str):
            raise ValueError("invalid CLI version")
        Version(value["cli_version"])
        if not re.fullmatch(r"[0-9a-f]{64}", value["pack_fingerprint"]):
            raise ValueError("invalid pack fingerprint")
        if value["source_repository"] != CODEQL_ACTION_REPOSITORY:
            raise ValueError("invalid source repository")

        source_platforms = set()
        for source_value in value["source_assets"]:
            source = SourceAsset.from_dict(source_value)
            if source.name != source_asset_name(source.platform):
                raise ValueError(
                    f"unexpected source asset {source.name} for {source.platform}"
                )
            if (
                not re.fullmatch(r"[0-9a-f]{64}", source.sha256)
                or source.size < 1
            ):
                raise ValueError(f"invalid source asset {source.name}")
            validate_remote_url(source.url)
            source_platforms.add(source.platform)
        if (
            source_platforms != set(SOURCE_PLATFORMS)
            or len(value["source_assets"]) != len(SOURCE_PLATFORMS)
        ):
            raise ValueError("incomplete source platform inventory")

        target_names = set()
        languages = set()
        if not isinstance(value["targets"], list) or not value["targets"]:
            raise ValueError("empty cache target inventory")
        for target in value["targets"]:
            if (
                not re.fullmatch(
                    r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+",
                    target["target"],
                )
                or not re.fullmatch(r"[a-z][a-z0-9-]*", target["language"])
                or not isinstance(target["query_packs"], list)
                or not target["query_packs"]
                or not all(
                    isinstance(query_pack, str) and "@" in query_pack
                    for query_pack in target["query_packs"]
                )
            ):
                raise ValueError("invalid cache target")
            target_names.add(target["target"])
            languages.add(target["language"])
        if (
            len(target_names) != len(value["targets"])
            or len(languages) != len(value["targets"])
        ):
            raise ValueError("duplicate cache target")
    except (KeyError, TypeError, ValueError, CacheException) as error:
        raise click.ClickException(
            f"Invalid release plan {path}: {error}"
        ) from error
    return value


def _read_bundle_entry(path: Path) -> SupportedBundle:
    catalog = BundleCatalog.from_dict(
        {"schema_version": 1, "bundles": [_read_json(path)]}
    )
    return catalog.bundles[0]


def _emit_json(value: dict[str, Any], output: Optional[Path]) -> None:
    if output is not None:
        write_json(output, value)
    else:
        click.echo(json.dumps(value, sort_keys=True))


def run() -> None:
    try:
        main()
    except CacheException as error:
        click.echo(f"Error: {error}", err=True)
        raise SystemExit(1) from error


if __name__ == "__main__":
    run()
