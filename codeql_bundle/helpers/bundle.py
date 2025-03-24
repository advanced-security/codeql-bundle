from .codeql import CodeQL, CodeQLException, CodeQLPack
from pathlib import Path
from tempfile import TemporaryDirectory
import tarfile
from typing import List, cast, Callable, Optional
from collections import defaultdict
import shutil
import yaml
import dataclasses
import logging
import json
import os
import subprocess
from jsonschema import validate, ValidationError
from enum import Enum, verify, UNIQUE
from dataclasses import dataclass
from graphlib import TopologicalSorter
import platform
import concurrent.futures

logger = logging.getLogger(__name__)


@verify(UNIQUE)
class CodeQLPackKind(Enum):
    QUERY_PACK = 1
    LIBRARY_PACK = 2
    CUSTOMIZATION_PACK = 3


@dataclass(kw_only=True, frozen=True, eq=True)
class ResolvedCodeQLPack(CodeQLPack):
    kind: CodeQLPackKind
    dependencies: List["ResolvedCodeQLPack"] = dataclasses.field(default_factory=list)

    def __hash__(self):
        return CodeQLPack.__hash__(self)

    def is_customizable(self) -> bool:
        return self.get_customizations_module_path().exists()

    def get_module_name(self) -> str:
        return self.config.name.replace("-", "_").replace("/", ".")

    def get_customizations_module_path(self) -> Path:
        return self.path.parent / "Customizations.qll"

    def get_lock_file_path(self) -> Path:
        return self.path.parent / "codeql-pack.lock.yml"

    def get_dependencies_path(self) -> Path:
        return self.path.parent / ".codeql"

    def get_cache_path(self) -> Path:
        return self.path.parent / ".cache"

    def is_stdlib_module(self) -> bool:
        return self.config.get_scope() == "codeql"


class BundleException(Exception):
    pass


class PackResolverException(Exception):
    pass


def build_pack_resolver(
    packs: List[CodeQLPack], already_resolved_packs: List[ResolvedCodeQLPack] = []
) -> Callable[[CodeQLPack], ResolvedCodeQLPack]:
    def builder() -> Callable[[CodeQLPack], ResolvedCodeQLPack]:
        resolved_packs: dict[CodeQLPack, ResolvedCodeQLPack] = {
            pack: pack for pack in already_resolved_packs
        }

        candidates: dict[str, List[CodeQLPack]] = defaultdict(list)
        for pack in packs + already_resolved_packs:
            candidates[pack.config.name].append(pack)

        def get_pack_kind(pack: CodeQLPack) -> CodeQLPackKind:
            kind = CodeQLPackKind.QUERY_PACK
            if pack.config.library:
                if (
                    pack.path.parent
                    / pack.config.name.replace("-", "_")
                    / "Customizations.qll"
                ).exists():
                    kind = CodeQLPackKind.CUSTOMIZATION_PACK
                else:
                    kind = CodeQLPackKind.LIBRARY_PACK
            return kind

        def resolve(pack: CodeQLPack) -> ResolvedCodeQLPack:
            def inner(pack_to_be_resolved: CodeQLPack) -> ResolvedCodeQLPack:
                logger.debug(
                    f"Resolving pack {pack_to_be_resolved.config.name}@{pack_to_be_resolved.config.version}"
                )
                if pack_to_be_resolved in resolved_packs:
                    logger.debug(
                        f"Resolved pack {pack_to_be_resolved.config.name}@{pack_to_be_resolved.config.version}, already resolved."
                    )
                    return resolved_packs[pack_to_be_resolved]
                else:
                    resolved_deps: List[ResolvedCodeQLPack] = []
                    for (
                        dep_name,
                        dep_version,
                    ) in pack_to_be_resolved.config.dependencies.items():
                        logger.debug(f"Resolving dependency {dep_name}:{dep_version}.")
                        resolved_dep = None
                        for candidate_pack in candidates[dep_name]:
                            logger.debug(
                                f"Considering candidate pack {candidate_pack.config.name}@{candidate_pack.config.version}."
                            )
                            if candidate_pack == pack:
                                raise PackResolverException(
                                    f"Pack {pack.config.name}@{str(pack.config.version)} (transitively) depends on itself via {pack_to_be_resolved.config.name}@{str(pack_to_be_resolved.config.version)}!"
                                )
                            if dep_version.match(candidate_pack.config.version):
                                logger.debug(
                                    f"Found candidate pack {candidate_pack.config.name}@{candidate_pack.config.version}."
                                )
                                resolved_dep = inner(candidate_pack)

                        if not resolved_dep:
                            raise PackResolverException(
                                f"Could not resolve dependency {dep_name}@{dep_version} for pack {pack_to_be_resolved.config.name}@{str(pack_to_be_resolved.config.version)}!"
                            )
                        resolved_deps.append(resolved_dep)

                    resolved_pack = ResolvedCodeQLPack(
                        path=pack_to_be_resolved.path,
                        config=pack_to_be_resolved.config,
                        kind=get_pack_kind(pack_to_be_resolved),
                        dependencies=resolved_deps,
                    )
                    resolved_packs[pack_to_be_resolved] = resolved_pack
                    return resolved_pack

            return inner(pack)

        return resolve

    return builder()


@verify(UNIQUE)
class BundlePlatform(Enum):
    LINUX = 1
    WINDOWS = 2
    OSX = 3

    @staticmethod
    def from_string(platform: str) -> "BundlePlatform":
        if platform.lower() == "linux" or platform.lower() == "linux64":
            return BundlePlatform.LINUX
        elif platform.lower() == "windows" or platform.lower() == "win64":
            return BundlePlatform.WINDOWS
        elif platform.lower() == "osx" or platform.lower() == "osx64":
            return BundlePlatform.OSX
        else:
            raise BundleException(f"Invalid platform {platform}")

    def __str__(self):
        if self == BundlePlatform.LINUX:
            return "linux64"
        elif self == BundlePlatform.WINDOWS:
            return "win64"
        elif self == BundlePlatform.OSX:
            return "osx64"
        else:
            raise BundleException(f"Invalid platform {self}")


class Bundle:
    def __init__(self, bundle_path: Path) -> None:
        self.tmp_dir = TemporaryDirectory()
        self.disable_precompilation = False

        if bundle_path.is_dir():
            self.bundle_path = Path(self.tmp_dir.name) / bundle_path.name
            shutil.copytree(
                bundle_path,
                self.bundle_path,
            )
        elif bundle_path.is_file() and bundle_path.name.endswith(".tar.gz"):
            logging.info(
                f"Unpacking provided bundle {bundle_path} to {self.tmp_dir.name}."
            )
            file = tarfile.open(bundle_path)
            file.extractall(self.tmp_dir.name)
            self.bundle_path = Path(self.tmp_dir.name) / "codeql"
        else:
            raise BundleException("Invalid CodeQL bundle path")

        def supports_linux() -> set[BundlePlatform]:
            if (self.bundle_path / "cpp" / "tools" / "linux64").exists():
                return {BundlePlatform.LINUX}
            else:
                return set()

        def supports_macos() -> set[BundlePlatform]:
            if (self.bundle_path / "cpp" / "tools" / "osx64").exists():
                return {BundlePlatform.OSX}
            else:
                return set()

        def supports_windows() -> set[BundlePlatform]:
            if (self.bundle_path / "cpp" / "tools" / "win64").exists():
                return {BundlePlatform.WINDOWS}
            else:
                return set()

        self.platforms: set[BundlePlatform] = (
            supports_linux() | supports_macos() | supports_windows()
        )

        current_system = platform.system()
        if not current_system in ["Linux", "Darwin", "Windows"]:
            raise BundleException(f"Unsupported system: {current_system}")
        if current_system == "Linux" and BundlePlatform.LINUX not in self.platforms:
            raise BundleException("Bundle doesn't support Linux!")
        elif current_system == "Darwin" and BundlePlatform.OSX not in self.platforms:
            raise BundleException("Bundle doesn't support OSX!")
        elif (
            current_system == "Windows" and BundlePlatform.WINDOWS not in self.platforms
        ):
            raise BundleException("Bundle doesn't support Windows!")

        self.codeql = CodeQL(self.bundle_codeql_exe)

        try:
            logging.info(f"Validating the CodeQL CLI version part of the bundle.")
            unpacked_location = self.codeql.unpacked_location()
            logging.debug(f"Found CodeQL CLI in {str(unpacked_location)}.")
            version = self.codeql.version()
            logging.info(f"Found CodeQL CLI version {version}.")

            logging.debug(f"Resolving packs in  {self.bundle_path}.")
            packs: List[CodeQLPack] = self.codeql.pack_ls(self.bundle_path)
            resolve = build_pack_resolver(packs)

            self.bundle_packs: list[ResolvedCodeQLPack] = [
                resolve(pack) for pack in packs
            ]

            self.languages = self.codeql.resolve_languages()

        except CodeQLException:
            raise BundleException("Cannot determine CodeQL version!")

    def __del__(self) -> None:
        if self.tmp_dir:
            logging.info(
                f"Removing temporary directory {self.tmp_dir.name} used to build custom bundle."
            )
            self.tmp_dir.cleanup()

    def get_bundle_packs(self) -> List[ResolvedCodeQLPack]:
        return self.bundle_packs

    def supports_platform(self, platform: BundlePlatform) -> bool:
        return platform in self.platforms

    @property
    def bundle_codeql_exe(self):
        if platform.system() == "Windows":
            return self.bundle_path / "codeql.exe"

        return self.bundle_path / "codeql"

    @property
    def disable_precompilation(self):
        return self._disable_precompilation

    @disable_precompilation.setter
    def disable_precompilation(self, value: bool):
        self._disable_precompilation = value


class CustomBundle(Bundle):
    def __init__(self, bundle_path: Path, workspace_path: Path = Path.cwd()) -> None:
        Bundle.__init__(self, bundle_path)

        packs: List[CodeQLPack] = self.codeql.pack_ls(workspace_path)
        # Perform a sanity check on the packs in the workspace.
        for pack in packs:
            if not pack.config.get_scope():
                raise BundleException(
                    f"Pack '{pack.config.name}' does not have the required scope. This pack cannot be bundled!"
                )

        resolve = build_pack_resolver(packs, self.bundle_packs)
        try:
            self.workspace_packs: list[ResolvedCodeQLPack] = [
                resolve(pack) for pack in packs
            ]
        except PackResolverException as e:
            raise BundleException(e)

        self.available_packs: dict[str, ResolvedCodeQLPack] = {
            pack.config.name: pack for pack in self.bundle_packs + self.workspace_packs
        }
        # A custom bundle will always need a temp directory for customization work.
        # If the bundle didn't create one (there was no need to unpack it), create it here.
        if not self.tmp_dir:
            self.tmp_dir = TemporaryDirectory()
            logging.debug(
                f"Bundle doesn't have an associated temporary directory, created {self.tmp_dir.name} for building a custom bundle."
            )

    def get_workspace_packs(self) -> List[ResolvedCodeQLPack]:
        return self.workspace_packs

    def add_packs(self, *packs: ResolvedCodeQLPack):
        """
        Add packs and their workspace dependencies to the bundle. Standard library packs are customized if needed and standard query packs are recreated.

        The approach taken is to first create a dependency graph from the provided packs and their dependencies.
        During the dependency graph construction we track which standard library packs are customized by customization packs and add those and
        the standard query packs depending on the customized standard library packs to the graph.

        Once the dependency graph is constructed we use the graph to determine the order in which to process the packs.
        For each pack kind we process the pack as necessary.
        Library packs are bundled, query packs are (re)created, and customization packs are bundle and added as a dependency to the standard library pack they customize.
        Last but not least, the `Customizations.qll` module is updated to import the customization packs whenever we re-bundle a standard library pack.

        During the process a few hacks are applied. The customization packs that are bundled have their dependencies removed to prevent circular dependencies between the
        customization packs and the standard library pack they customize.
        Languages that do not have a `Customizations.qll` module are provided with one. This process will add the `Customizations.qll` module to the standard library pack
        and import as the first module in the language module (eg., `cpp.qll` will import `Customizations.qll` as the first module).
        """
        # Keep a map of standard library packs to their customization packs so we know which need to be modified.
        std_lib_deps: dict[ResolvedCodeQLPack, List[ResolvedCodeQLPack]] = defaultdict(
            list
        )
        pack_sorter: TopologicalSorter[ResolvedCodeQLPack] = TopologicalSorter()

        def add_to_graph(
            pack: ResolvedCodeQLPack,
            processed_packs: set[ResolvedCodeQLPack],
            std_lib_deps: dict[ResolvedCodeQLPack, List[ResolvedCodeQLPack]],
        ):
            # Only process workspace packs in this function
            if not pack in self.workspace_packs:
                logger.debug(
                    f"Skipping adding pack {pack.config.name}@{str(pack.config.version)} to dependency graph"
                )
                return
            if pack.kind == CodeQLPackKind.CUSTOMIZATION_PACK:
                logger.debug(
                    f"Adding customization pack {pack.config.name}@{str(pack.config.version)} to dependency graph"
                )
                pack_sorter.add(pack)
                std_lib_deps[pack.dependencies[0]].append(pack)
            else:
                # If the query pack relies on a customization pack (e.g. for tests), add the std lib dependency of
                # the customization pack to query pack because the customization pack will no longer have that
                # dependency in the bundle.
                if pack.kind == CodeQLPackKind.QUERY_PACK:
                    for customization_pack in [
                        dep
                        for dep in pack.dependencies
                        if dep.kind == CodeQLPackKind.CUSTOMIZATION_PACK
                    ]:
                        std_lib_dep = customization_pack.dependencies[0]
                        if not std_lib_dep in pack.dependencies:
                            logger.debug(
                                f"Adding stdlib dependency {std_lib_dep.config.name}@{str(std_lib_dep.config.version)} to {pack.config.name}@{str(pack.config.version)}"
                            )
                            pack.dependencies.append(std_lib_dep)
                logger.debug(
                    f"Adding pack {pack.config.name}@{str(pack.config.version)} to dependency graph"
                )
                # We include standard library packs in the dependency graph to ensure they dictate the correct order
                # when we need to customize packs.
                # This does mean we will repack them, but that is only small price to pay for simplicity.
                pack_sorter.add(pack, *pack.dependencies)
                for dep in pack.dependencies:
                    if dep not in processed_packs:
                        add_to_graph(dep, processed_packs, std_lib_deps)
            processed_packs.add(pack)

        processed_packs: set[ResolvedCodeQLPack] = set()
        for pack in packs:
            if not pack in processed_packs:
                add_to_graph(pack, processed_packs, std_lib_deps)

        def is_dependent_on(
            pack: ResolvedCodeQLPack, other: ResolvedCodeQLPack
        ) -> bool:
            return other in pack.dependencies or any(
                map(lambda p: is_dependent_on(p, other), pack.dependencies)
            )

        # Add the stdlib and its dependencies to properly sort the customization packs before the other packs.
        for pack, deps in std_lib_deps.items():
            logger.debug(
                f"Adding standard library pack {pack.config.name}@{str(pack.config.version)} to dependency graph"
            )
            pack_sorter.add(pack, *deps)
            # Add the standard query packs that rely transitively on the stdlib.
            for query_pack in [
                p
                for p in self.bundle_packs
                if p.kind == CodeQLPackKind.QUERY_PACK and is_dependent_on(p, pack)
            ]:
                logger.debug(
                    f"Adding standard query pack {query_pack.config.name}@{str(query_pack.config.version)} to dependency graph"
                )
                pack_sorter.add(query_pack, pack)

        def bundle_customization_pack(customization_pack: ResolvedCodeQLPack):
            logging.info(
                f"Bundling the customization pack {customization_pack.config.name}."
            )
            customization_pack_copy = copy_pack(customization_pack)

            # Remove the target dependency to prevent a circular dependency in the target.
            logging.debug(
                f"Removing dependency on standard library to prevent circular dependency."
            )
            with customization_pack_copy.path.open("r") as fd:
                qlpack_spec = yaml.safe_load(fd)

            # Assume there is only one dependency and it is the standard library.
            qlpack_spec["dependencies"] = {}
            with customization_pack_copy.path.open("w") as fd:
                yaml.dump(qlpack_spec, fd)

            logging.debug(
                f"Bundling the customization pack {customization_pack_copy.config.name} at {customization_pack_copy.path}"
            )
            self.codeql.pack_bundle(
                customization_pack_copy,
                self.bundle_path / "qlpacks",
                disable_precompilation=self.disable_precompilation,
            )

        def copy_pack(pack: ResolvedCodeQLPack) -> ResolvedCodeQLPack:
            pack_copy_dir = (
                Path(self.tmp_dir.name)
                / "temp"  # Add a temp path segment because the standard library packs have scope 'codeql' that collides with the 'codeql' directory in the bundle that is extracted to the temporary directory.
                / cast(str, pack.config.get_scope())
                / pack.config.get_pack_name()
                / str(pack.config.version)
            )

            logging.debug(
                f"Copying {pack.path.parent} to {pack_copy_dir} for modification"
            )
            shutil.copytree(
                pack.path.parent,
                pack_copy_dir,
            )
            pack_copy_path = pack_copy_dir / pack.path.name
            return dataclasses.replace(pack, path=pack_copy_path)

        def add_customization_support(pack: ResolvedCodeQLPack):
            if pack.is_customizable():
                return

            if not pack.config.get_scope() == "codeql" or not pack.config.library:
                return

            logging.debug(
                f"Standard library CodeQL pack {pack.config.name} does not have a 'Customizations' library, attempting to add one."
            )
            # Assume the CodeQL library pack has name `<language>-all`.
            target_language = pack.config.get_pack_name().removesuffix("-all")
            target_language_library_path = pack.path.parent / f"{target_language}.qll"
            logging.debug(
                f"Looking for standard library language module {target_language_library_path.name}"
            )
            if not target_language_library_path.exists():
                raise BundleException(
                    f"Unable to customize {pack.config.name}, because it doesn't have a 'Customizations' library and we cannot determine the language library."
                )
            logging.debug(
                f"Found standard library language module {target_language_library_path.name}, adding import of 'Customizations' library."
            )
            with target_language_library_path.open("r") as fd:
                target_language_library_lines = fd.readlines()
            logging.debug(f"Looking for the first import statement.")

            first_import_idx = None
            for idx, line in enumerate(target_language_library_lines):
                if line.startswith("import"):
                    first_import_idx = idx
                    break
            if first_import_idx == None:
                raise BundleException(
                    f"Unable to customize {pack.config.name}, because we cannot determine the first import statement of {target_language_library_path.name}."
                )
            logging.debug(
                "Found first import statement and prepending import statement importing 'Customizations'"
            )
            target_language_library_lines.insert(
                first_import_idx, "import Customizations\n"
            )
            with target_language_library_path.open("w") as fd:
                fd.writelines(target_language_library_lines)
            logging.debug(
                f"Writing modified language library to {target_language_library_path}"
            )

            target_customization_library_path = pack.path.parent / "Customizations.qll"
            logging.debug(
                f"Creating Customizations library with import of language {target_language}"
            )
            with target_customization_library_path.open("w") as fd:
                fd.write(f"import {target_language}\n")

        def bundle_stdlib_pack(pack: ResolvedCodeQLPack):
            logging.info(f"Bundling the standard library pack {pack.config.name}.")

            pack_copy = copy_pack(pack)

            with pack_copy.path.open("r") as fd:
                qlpack_spec = yaml.safe_load(fd)
            if not "dependencies" in qlpack_spec:
                qlpack_spec["dependencies"] = {}
            for customization_pack in std_lib_deps[pack]:
                logging.debug(
                    f"Adding dependency {customization_pack.config.name} to {pack_copy.config.name}"
                )
                qlpack_spec["dependencies"][customization_pack.config.name] = str(
                    customization_pack.config.version
                )
            with pack_copy.path.open("w") as fd:
                yaml.dump(qlpack_spec, fd)

            logging.debug(
                f"Determining if standard library CodeQL library pack {pack_copy.config.name} is customizable."
            )
            if not pack_copy.is_customizable():
                add_customization_support(pack_copy)

            logging.debug(
                f"Updating 'Customizations.qll' with imports of customization libraries."
            )
            with pack_copy.get_customizations_module_path().open("r") as fd:
                contents = fd.readlines()
            for customization_pack in std_lib_deps[pack]:
                contents.append(
                    f"import {customization_pack.get_module_name()}.Customizations"
                )
            with pack_copy.get_customizations_module_path().open("w") as fd:
                fd.writelines(map(lambda content: content + "\n", contents))

            # Remove the original target library pack
            logging.debug(
                f"Removing the standard library at {pack.path} in preparation for replacement."
            )
            shutil.rmtree(pack.path.parent.parent)
            # Bundle the new into the bundle.
            logging.debug(
                f"Bundling the standard library pack {pack_copy.config.name} at {pack_copy.path}"
            )
            self.codeql.pack_bundle(
                pack_copy,
                self.bundle_path / "qlpacks",
                disable_precompilation=self.disable_precompilation,
            )

        def bundle_library_pack(library_pack: ResolvedCodeQLPack):
            logging.info(f"Bundling the library pack {library_pack.config.name}.")
            self.codeql.pack_bundle(
                library_pack,
                self.bundle_path / "qlpacks",
                disable_precompilation=self.disable_precompilation,
            )

        def bundle_query_pack(pack: ResolvedCodeQLPack):
            if pack.config.get_scope() == "codeql":
                logging.info(f"Bundling the standard query pack {pack.config.name}.")
                pack_copy = copy_pack(pack)

                # Remove the lock file
                logging.debug(
                    f"Removing CodeQL pack lock file {pack_copy.get_lock_file_path()}"
                )
                pack_copy.get_lock_file_path().unlink()
                # Remove the included dependencies
                logging.debug(
                    f"Removing CodeQL query pack dependencies directory {pack_copy.get_dependencies_path()}"
                )
                shutil.rmtree(pack_copy.get_dependencies_path())
                # Remove the query cache, if it exists.
                logging.debug(
                    f"Removing CodeQL query pack cache directory {pack_copy.get_cache_path()}, if it exists."
                )
                shutil.rmtree(
                    pack_copy.get_cache_path(),
                    ignore_errors=True,
                )
                # Remove qlx files
                if self.codeql.supports_qlx():
                    logging.debug(f"Removing 'qlx' files in {pack_copy.path.parent}.")
                    for qlx_path in pack_copy.path.parent.glob("**/*.qlx"):
                        qlx_path.unlink()

                # Remove the original query pack
                logging.debug(
                    f"Removing the standard library query pack directory {pack.path.parent.parent} in preparation for recreation."
                )
                shutil.rmtree(pack.path.parent.parent)
                logging.debug(
                    f"Recreating {pack_copy.config.name} at {pack_copy.path} to {self.bundle_path / 'qlpacks'}"
                )
                # Recompile the query pack with the assumption that all its dependencies are now in the bundle.
                self.codeql.pack_create(
                    pack_copy, self.bundle_path / "qlpacks", self.bundle_path
                )
            else:
                logging.info(f"Bundling the query pack {pack.config.name}.")
                pack_copy = copy_pack(pack)
                # Rewrite the query pack dependencies
                with pack_copy.path.open("r") as fd:
                    qlpack_spec = yaml.safe_load(fd)

                # Assume there is only one dependency and it is the standard library.
                qlpack_spec["dependencies"] = {
                    pack.config.name: str(pack.config.version)
                    for pack in pack_copy.dependencies
                }
                logging.debug(f"Rewriting dependencies for {pack.config.name}.")
                with pack_copy.path.open("w") as fd:
                    yaml.dump(qlpack_spec, fd)

                self.codeql.pack_create(pack_copy, self.bundle_path / "qlpacks")

        sorted_packs = list(pack_sorter.static_order())
        logger.debug(
            f"Sorted packs: {' -> '.join(map(lambda p: p.config.name, sorted_packs))}"
        )
        for pack in sorted_packs:
            if pack.kind == CodeQLPackKind.CUSTOMIZATION_PACK:
                bundle_customization_pack(pack)
            elif pack.kind == CodeQLPackKind.LIBRARY_PACK:
                if pack.config.get_scope() == "codeql":
                    bundle_stdlib_pack(pack)
                else:
                    bundle_library_pack(pack)
            elif pack.kind == CodeQLPackKind.QUERY_PACK:
                bundle_query_pack(pack)

    def add_files_and_certs(self, config_path: Path, workspace_path: Path):
        schema = {
            "type": "object",
            "properties": {
                "CodeQLBundleAdditionalFiles": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "Source": {"type": "string"},
                            "Destination": {"type": "string"},
                        },
                        "required": ["Source", "Destination"],
                        "additionalProperties": False,
                    },
                    "minItems": 1,
                },
                "CodeQLBundleAdditionalCertificates": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"Source": {"type": "string"}},
                        "required": ["Source"],
                        "additionalProperties": False,
                    },
                    "minItems": 1,
                },
            },
            "additionalProperties": True,
        }

        def validate_config(config) -> bool:
            try:
                validate(instance=config, schema=schema)
            except ValidationError as e:
                print("JSON validation error:", e)
                return False
            return True

        def load_config(file_path):
            with open(file_path, "r") as file:
                data = json.load(file)
                if validate_config(data):
                    return data
                else:
                    raise BundleException(
                        f"Installation config {file_path} is not valid."
                    )

        def is_unsafe_path(basedir: Path, path: Path) -> bool:
            matchpath = os.path.realpath(path)
            basedir = os.path.realpath(basedir)
            return os.path.commonpath([basedir, matchpath]) != basedir

        if not config_path.exists():
            raise BundleException(f"Installation config {config_path} does not exist.")
        if not config_path.is_file():
            raise BundleException(f"Installation config {config_path} is not a file.")

        config = load_config(config_path)

        if platform.system() == "Windows":
            keytool = "tools/win64/java/bin/keytool.exe"
        elif platform.system() == "Linux":
            keytool = "tools/linux64/java/bin/keytool"
        elif platform.system() == "Darwin":
            keytool = "tools/osx64/java/bin/keytool"
        else:
            raise BundleException(f"Unsupported platform {platform.system()}")

        keytool = self.bundle_path / keytool
        if not keytool.exists():
            raise BundleException(f"Keytool {keytool} does not exist.")

        keystores: list[str] = [
            "tools/win64/java/lib/security/cacerts",
            "tools/linux64/java/lib/security/cacerts",
            "tools/osx64/java/lib/security/cacerts",
            "tools/osx64/java-aarch64/lib/security/cacerts",
        ]

        # Add the certificates to the Java keystores
        if "CodeQLBundleAdditionalCertificates" in config:
            for cert in config["CodeQLBundleAdditionalCertificates"]:
                src = workspace_path / Path(cert["Source"])
                src = src.resolve()
                if is_unsafe_path(workspace_path, src):
                    raise BundleException(
                        f"Certificate file {src} is not in the workspace path."
                    )
                if not src.exists():
                    raise BundleException(f"Certificate file {src} does not exist.")

                for keystore in keystores:
                    keystore = self.bundle_path / keystore
                    if not keystore.exists():
                        raise BundleException(f"Keystore {keystore} does not exist.")
                    logging.info(f"Adding certificate {src} to keystore {keystore}")
                    subprocess.run(
                        [
                            str(keytool),
                            "-import",
                            "-trustcacerts",
                            "-alias",
                            "root",
                            "-file",
                            str(src),
                            "-keystore",
                            str(keystore),
                            "-storepass",
                            "changeit",
                            "-noprompt",
                        ],
                        check=True,
                        cwd=self.bundle_path,
                    )

        # Add additional files to the bundle
        if "CodeQLBundleAdditionalFiles" in config:
            for file in config["CodeQLBundleAdditionalFiles"]:
                src = (workspace_path / Path(file["Source"])).resolve()
                dst = (self.bundle_path / Path(file["Destination"])).resolve()

                if not src.exists():
                    raise BundleException(f"Source file {src} does not exist.")

                if is_unsafe_path(workspace_path, src):
                    raise BundleException(
                        f"Source file {src} is not in the workspace path."
                    )
                if is_unsafe_path(self.bundle_path, dst):
                    print(self.bundle_path)
                    raise BundleException(
                        f"Destination path {dst} is not in the bundle path."
                    )

                if src.is_dir():
                    logging.info(f"Copying directory {src} to {dst}")
                    shutil.copytree(src, dst, dirs_exist_ok=True)
                else:
                    logging.info(f"Copying file {src} to {dst}")
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy(src, dst)

    def add_code_scanning_config(self, default_config: Path):
        if not default_config.exists():
            raise BundleException(f"Default config {default_config} does not exist.")
        if not default_config.is_file():
            raise BundleException(f"Default config {default_config} is not a file.")
        shutil.copy(default_config, self.bundle_path / "default-codeql-config.yml")

    def bundle(
        self,
        output_path: Path,
        platforms: set[BundlePlatform] = set(),
        default_config: Optional[Path] = None,
    ):
        if len(platforms) == 0:
            if output_path.is_dir():
                output_path = output_path / "codeql-bundle.tar.gz"

            logging.debug(f"Bundling custom bundle to {output_path}.")
            with tarfile.open(output_path, mode="w:gz") as bundle_archive:
                bundle_archive.add(self.bundle_path, arcname="codeql")
        else:
            if not output_path.is_dir():
                raise BundleException(
                    f"Output path {output_path} must be a directory when bundling for multiple platforms."
                )

            unsupported_platforms = platforms - self.platforms
            if len(unsupported_platforms) > 0:
                raise BundleException(
                    f"Unsupported platform(s) {', '.join(map(str,unsupported_platforms))} specified. Use the platform agnostic bundle to bundle for different platforms."
                )

            def create_bundle_for_platform(
                bundle_output_path: Path, platform: BundlePlatform
            ) -> None:
                """Create a bundle for a single platform."""

                def filter_for_platform(
                    platform: BundlePlatform,
                ) -> Callable[[tarfile.TarInfo], Optional[tarfile.TarInfo]]:
                    """Create a filter function that will only include files for the specified platform."""
                    relative_tools_paths = [
                        Path(lang) / "tools" for lang in self.languages
                    ] + [Path("tools")]

                    def get_nonplatform_tool_paths(
                        platform: BundlePlatform,
                    ) -> List[Path]:
                        """Get a list of paths to tools that are not for the specified platform relative to the root of a bundle."""
                        specialize_path: Optional[Callable[[Path], List[Path]]] = None
                        linux64_subpaths = [Path("linux64"), Path("linux")]
                        osx64_subpaths = [Path("osx64"), Path("macos")]
                        win64_subpaths = [Path("win64"), Path("windows")]
                        if platform == BundlePlatform.LINUX:
                            specialize_path = lambda p: [
                                p / subpath
                                for subpath in osx64_subpaths + win64_subpaths
                            ]
                        elif platform == BundlePlatform.WINDOWS:
                            specialize_path = lambda p: [
                                p / subpath
                                for subpath in osx64_subpaths + linux64_subpaths
                            ]
                        elif platform == BundlePlatform.OSX:
                            specialize_path = lambda p: [
                                p / subpath
                                for subpath in linux64_subpaths + win64_subpaths
                            ]
                        else:
                            raise BundleException(f"Unsupported platform {platform}.")

                        return [
                            candidate
                            for candidates in map(specialize_path, relative_tools_paths)
                            for candidate in candidates
                        ]

                    def filter(tarinfo: tarfile.TarInfo) -> Optional[tarfile.TarInfo]:
                        tarfile_path = Path(tarinfo.name)

                        exclusion_paths = get_nonplatform_tool_paths(platform)

                        # Manual exclusions based on diffing the contents of the platform specific bundles and the generated platform specific bundles.
                        if platform != BundlePlatform.WINDOWS:
                            exclusion_paths.append(Path("codeql.exe"))
                        else:
                            exclusion_paths.append(Path("swift/qltest"))
                            exclusion_paths.append(Path("swift/resource-dir"))

                        if platform == BundlePlatform.LINUX:
                            exclusion_paths.append(Path("swift/qltest/osx64"))
                            exclusion_paths.append(Path("swift/resource-dir/osx64"))

                        if platform == BundlePlatform.OSX:
                            exclusion_paths.append(Path("swift/qltest/linux64"))
                            exclusion_paths.append(Path("swift/resource-dir/linux64"))

                        tarfile_path_root = Path(tarfile_path.parts[0])
                        exclusion_paths = [
                            tarfile_path_root / path for path in exclusion_paths
                        ]

                        if any(
                            tarfile_path.is_relative_to(path)
                            for path in exclusion_paths
                        ):
                            return None

                        return tarinfo

                    return filter

                logging.debug(
                    f"Bundling custom bundle for {platform} to {bundle_output_path}."
                )
                with tarfile.open(bundle_output_path, mode="w:gz") as bundle_archive:
                    bundle_archive.add(
                        self.bundle_path,
                        arcname="codeql",
                        filter=filter_for_platform(platform),
                    )

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(platforms)
            ) as executor:
                future_to_platform = {
                    executor.submit(
                        create_bundle_for_platform,
                        output_path / f"codeql-bundle-{platform}.tar.gz",
                        platform,
                    ): platform
                    for platform in platforms
                }
                for future in concurrent.futures.as_completed(future_to_platform):
                    platform = future_to_platform[future]
                    try:
                        future.result()
                    except Exception as exc:
                        raise BundleException(
                            f"Failed to create bundle for platform {platform} with exception: {exc}."
                        )
