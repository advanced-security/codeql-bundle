# CodeQL bundle

A CLI application for building a custom CodeQL bundle.

## Introduction

A CodeQL bundle is an archive containing a CodeQL CLI and compatible [standard library packs](https://github.com/github/codeql) that provides a single deployable artifact for using CodeQL locally or in a CI/CD environment.

A custom CodeQL bundle contains additional CodeQL query packs, library packs, or customization packs.
CodeQL customization packs are special CodeQL library packs that provide unofficial support to extend the CodeQL standard library packs with *sources*, *sinks*, *data-flow steps*, *barriers*, *sanitizer*, and models that describe frameworks
aimed to increase the coverage and/or precision of the CodeQL queries.

Customizations are used to add, or extend, support for open-source frameworks or proprietary frameworks.
For more details on CodeQL customization packs see the section [CodeQL customization packs](#codeql-customization-packs).

## Installation

The CodeQL bundle application can be installed using `pip` with the command:

```bash
python3.11 -m pip install https://github.com/advanced-security/codeql-bundle/releases/download/v0.5.0/codeql_bundle-0.5.0-py3-none-any.whl
```

## Usage

The source bundle can be an existing local archive or directory, a
`github/codeql-action` release tag, or an HTTP(S) URL. Release tags and URLs are
downloaded into a persistent local cache.

The CodeQL bundle application requires a [CodeQL workspace](https://codeql.github.com/docs/codeql-cli/about-codeql-workspaces/) to locate the packs you want to include in a custom bundle.
You can see the packs available in your workspace by running `codeql pack ls -- <dir>` where `<dir>` is the root directory of your CodeQL workspace.

With a CodeQL bundle release and a CodeQL workspace you can create a bundle
with the command:

```bash
codeql-bundle --bundle codeql-bundle-v2.26.1 --output codeql-custom-bundle.tar.gz --workspace <path-to-workspace> --log INFO <packs>
```

If the source bundle is the platform agnostic bundle then you can create platform specific bundles to reduce the size of the used bundle(s).
The following example creates platform specific bundles for all the currently supported platforms.

```bash
codeql-bundle --bundle <path-to-platform-agnostic-bundle> --output <path-to-bundles-dir> --workspace <path-to-workspace> --log INFO -p linux64 -p osx64 -p win64 <packs>
```

### Compilation caches

The repository maintains
[`supported-codeql-bundles.json`](codeql_bundle/supported-codeql-bundles.json)
as the authoritative catalog of validated source bundles and compilation-cache
assets. Each entry pins:

- the upstream release, CLI version, source asset URLs, sizes, and SHA-256
  digests;
- a content fingerprint of the CodeQL packs;
- the consumer platforms on which the cache was validated; and
- one cache asset for each customizable standard library.

When a customization requires standard query packs to be recreated,
`codeql-bundle` downloads only the relevant cache assets and passes them to
`codeql pack create`. Local archives downloaded by tools such as QLT are
recognized by their exact SHA-256 digest. Extracted directories are recognized
by their CLI version and pack-content fingerprint.

Compilation caches are content-addressed by the query, its transitive
dependencies, and the compiler. Customizing a standard library therefore
invalidates the entries that depend on the changed library while preserving
entries for unaffected compiler stages. Cache-aware builds reduce compilation
time, but they do not eliminate compilation; the speedup depends on the
customization and query packs involved.

An input that is not in the catalog remains supported, but is built without
this optimization and produces a warning. For a catalog-listed bundle, a
missing, unvalidated, or corrupt cache is an error. Use
`--no-compilation-cache` to explicitly continue without the optimization.
`--no-precompile` also skips cache resolution because no query compilation is
performed.

### Resource tuning

The source tree for the next release after v0.5.0 adds controls for constrained
or high-capacity build agents:

- `--threads` sets CodeQL compiler parallelism; the default uses all available
  cores.
- `--ram` caps the memory in MB available to CodeQL pack compilation.
- `--compression-level` selects the gzip level for generated bundle archives.
  The default is 6, which materially reduces archive time with little size
  increase compared with level 9.

Set RAM below the machine's physical limit so the operating system and archive
creation retain headroom. Avoid running multiple `pack create` processes with
`--threads=0` on the same agent because each process will try to use every core.

By default, downloads are stored in the platform cache directory. Override it
with `--cache-dir` or `CODEQL_BUNDLE_CACHE_DIR`. Use `--cache-manifest` with a
local path or URL to test a candidate catalog or to operate against a pinned
catalog. Remove source bundles and caches that have not been used in 30 days with
`codeql-bundle-cache prune`; use `--max-age-days` to choose a different
retention period or `--dry-run` to inspect what would be removed.

### Building cache releases locally

The `codeql-bundle-cache` command contains the implementation used by GitHub
Actions. A release can be prepared locally with:

```bash
codeql-bundle-cache plan-release \
  --release codeql-bundle-v2.26.1 \
  --output release-plan.json

jq -r '.targets[].target' release-plan.json | while read -r target; do
  codeql-bundle-cache build \
    --plan release-plan.json \
    --target "$target" \
    --output-dir dist
done

codeql-bundle-cache verify-all \
  --plan release-plan.json \
  --assets-dir dist
```

`plan-release` validates upstream release metadata and records every source
asset. `build` compiles the real standard query packs into a per-language cache
bounded to 1536 MiB and requires a second real compilation to report a cache
hit. `verify-all` repeats that check using the current platform's upstream
bundle.

After release assets have been published, `catalog-entry`, `verify-entry`, and
`update-catalog` create, download-test, and insert the candidate catalog entry.
Run `codeql-bundle-cache --help` for the complete command surface.

### Repository automation

The
[`Build CodeQL compilation caches`](.github/workflows/codeql-compilation-caches.yml)
workflow polls for the latest stable upstream release and runs immediately when
the cache implementation lands on `main`. It uses the local commands above,
parallelizes cache construction, validates every cache on Linux, macOS, and
Windows, publishes a dedicated release, performs a consumer-path customization
test, and opens a pull request for manual review of the catalog update.

Use `workflow_dispatch` with `bundle_version` to backfill a specific release.
The latest stable release is independent of backfill work; releases from
v2.18.0 onward can be dispatched separately in any order. `force` rebuilds a
release that already has a catalog entry or open update pull request.

Repository variables can opt slow cache stages into larger runners without
changing the workflow:

- `CODEQL_CACHE_JAVA_RUNNER` selects the Java cache-build runner.
- `CODEQL_CACHE_CONSUMER_RUNNER` selects the customized C++ acceptance runner.
- `CODEQL_CACHE_RAM` optionally sets a common RAM cap.
- `CODEQL_CACHE_JAVA_RAM`, `CODEQL_CACHE_CPP_RAM`, and
  `CODEQL_CACHE_CONSUMER_RAM` override that cap for their respective jobs.

## CodeQL customization packs

The CodeQL bundle CLI application provides a development experience for customization packs that mimics the development experience for official CodeQL packs.

A customization pack is a library pack with the following properties:

1. Has a module `Customizations.qll` located in the subdirectory `<scope>/<package_name>` where any character `-` in the `scope` or `package_name` is replaced with `_`.
   For example, a library pack with the name `foo/cpp-customizations` must have a `Customizations.qll` at `foo/cpp_customizations/Customizations.qll`.
1. Has a dependency on the standard library pack it wants to customize. For example, `codeql/cpp-all`.

If both properties hold, then the CodeQL bundle application will import the customizations pack `Customizations.qll` module in the standard library pack and recompile all
the CodeQL query packs in CodeQL bundle that have a dependency on that standard library pack to ensure the customizations are available to the queries.

### Steps to create a customization pack

This example targets the C/C++ language, but you can use this for any supported language.

1. Create a new pack `codeql pack init foo/cpp-customizations`.
2. Turn the pack into a library pack `sed -i '' -e 's/library: false/library: true/' cpp-customizations/qlpack.yml`.
3. Add a dependency on `codeql/cpp-all` with `codeql pack add --dir=cpp-customizations codeql/cpp-all`
4. Implement the customizations module with `mkdir -p cpp-customizations/foo/cpp_customizations && echo "import cpp" > cpp-customizations/foo/cpp_customizations/Customizations.qll`

## Limitations

- The customization pack must directly rely on a CodeQL language pack.
- The customization pack cannot have a dependency besides a CodeQL language pack, otherwise this will result in a cyclic dependency that we cannot resolve.
- All packs must directly be specified. That is if a pack relies on another pack in the workspace, but that pack isn't specified as part of the `packs` argument, the pack will not be implicitly added and `codeql-bundle` will fail.
