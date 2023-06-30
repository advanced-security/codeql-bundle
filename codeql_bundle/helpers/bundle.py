from .codeql import (
    CodeQL,
    CodeQLException,
    CodeQLPack
)
from pathlib import Path
from tempfile import TemporaryDirectory
import tarfile
from typing import List, cast, Callable
from collections import defaultdict
import shutil
import yaml
import dataclasses
import logging
from enum import Enum
from dataclasses import dataclass
from graphlib import TopologicalSorter

logger = logging.getLogger(__name__)

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

class BundleException(Exception):
    pass

class PackResolverException(Exception):
    pass

def build_pack_resolver(packs: List[CodeQLPack], already_resolved_packs: List[ResolvedCodeQLPack] = []) -> Callable[[CodeQLPack], ResolvedCodeQLPack]:
    def builder()  -> Callable[[CodeQLPack], ResolvedCodeQLPack]:
        resolved_packs: dict[CodeQLPack, ResolvedCodeQLPack] = {pack: pack for pack in already_resolved_packs}

        candidates : dict[str, List[CodeQLPack]] = defaultdict(list)
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
            logger.debug(f"Resolving pack {pack.config.name}@{pack.config.version}")
            if pack in resolved_packs:
                logger.debug(f"Resolved pack {pack.config.name}@{pack.config.version}, already resolved.")
                return resolved_packs[pack]
            else:
                resolved_deps: List[ResolvedCodeQLPack] = []
                for dep_name, dep_version in pack.config.dependencies.items():
                    logger.debug(f"Resolving dependency {dep_name}:{dep_version}.")
                    resolved_dep = None
                    for candidate_pack in candidates[dep_name]:
                        logger.debug(f"Considering candidate pack {candidate_pack.config.name}@{candidate_pack.config.version}.")
                        if dep_version.match(candidate_pack.config.version):
                            logger.debug(f"Found candidate pack {candidate_pack.config.name}@{candidate_pack.config.version}.")
                            resolved_dep = resolve(candidate_pack)

                    if not resolved_dep:
                        raise PackResolverException(f"Could not resolve dependency {dep_name} for pack {pack.config.name}!")
                    resolved_deps.append(resolved_dep)


                resolved_pack = ResolvedCodeQLPack(path=pack.path, config=pack.config, kind=get_pack_kind(pack), dependencies=resolved_deps)
                resolved_packs[pack] = resolved_pack
                return resolved_pack

        return resolve

    return builder()

class Bundle:
    def __init__(self, bundle_path: Path) -> None:
        self.tmp_dir = TemporaryDirectory()
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

        self.codeql = CodeQL(self.bundle_path / "codeql")
        try:
            logging.info(f"Validating the CodeQL CLI version part of the bundle.")
            unpacked_location = self.codeql.unpacked_location()
            logging.debug(f"Found CodeQL CLI in {str(unpacked_location)}.")
            version = self.codeql.version()
            logging.info(f"Found CodeQL CLI version {version}.")

            logging.debug(f"Resolving packs in  {self.bundle_path}.")
            packs: List[CodeQLPack] = self.codeql.pack_ls(self.bundle_path)
            resolve = build_pack_resolver(packs)

            self.bundle_packs: list[ResolvedCodeQLPack] = [resolve(pack) for pack in packs]

        except CodeQLException:
            raise BundleException("Cannot determine CodeQL version!")


    def __del__(self) -> None:
        if self.tmp_dir:
            logging.info(
                f"Removing temporary directory {self.tmp_dir.name} used to build custom bundle."
            )
            self.tmp_dir.cleanup()

    def getCodeQLPacks(self) -> List[ResolvedCodeQLPack]:
        return self.bundle_packs

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
            self.workspace_packs: list[ResolvedCodeQLPack] = [resolve(pack) for pack in packs]
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

    def getCodeQLPacks(self) -> List[ResolvedCodeQLPack]:
        return self.workspace_packs

    def add_packs(self, *packs: ResolvedCodeQLPack):
        # Keep a map of standard library packs to their customization packs so we know which need to be modified.
        std_lib_deps : dict[ResolvedCodeQLPack, List[ResolvedCodeQLPack]] = defaultdict(list)
        pack_sorter : TopologicalSorter[ResolvedCodeQLPack] = TopologicalSorter()
        for pack in packs:
            if pack.kind == CodeQLPackKind.CUSTOMIZATION_PACK:
                pack_sorter.add(pack)
                std_lib_deps[pack.dependencies[0]].append(pack)
            else:
                # If the query pack relies on a customization pack (e.g. for tests), add the std lib dependency of
                # the customization pack to query pack because the customization pack will no longer have that
                # dependency in the bundle.
                if pack.kind == CodeQLPackKind.QUERY_PACK:
                    for customization_pack in [dep for dep in pack.dependencies if dep.kind == CodeQLPackKind.CUSTOMIZATION_PACK]:
                        std_lib_dep = customization_pack.dependencies[0]
                        if not std_lib_dep in pack.dependencies:
                            logger.debug(f"Adding stdlib dependency {std_lib_dep.config.name}@{str(std_lib_dep.config.version)} to {pack.config.name}@{str(pack.config.version)}")
                            pack.dependencies.append(std_lib_dep)
                pack_sorter.add(pack, *pack.dependencies)

        for pack in [p for p in self.workspace_packs if p.kind == CodeQLPackKind.CUSTOMIZATION_PACK]:
            del pack.dependencies[0]

        def is_dependent_on(pack: ResolvedCodeQLPack, other: ResolvedCodeQLPack) -> bool:
            return other in pack.dependencies or any(map(lambda p: is_dependent_on(p, other), pack.dependencies))
        # Add the stdlib and its dependencies to properly sort the customization packs before the other packs.
        for pack, deps in std_lib_deps.items():
            pack_sorter.add(pack, *deps)
            # Add the standard query packs that rely transitively on the stdlib.
            for query_pack in [p for p in self.bundle_packs if p.kind == CodeQLPackKind.QUERY_PACK and is_dependent_on(p, pack)]:
                pack_sorter.add(query_pack, pack)

        def bundle_customization_pack(customization_pack: ResolvedCodeQLPack):
            logging.info(f"Bundling the customization pack {customization_pack.config.name}.")
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
                customization_pack_copy, self.bundle_path / "qlpacks"
            )

        def copy_pack(pack: ResolvedCodeQLPack) -> ResolvedCodeQLPack:
            pack_copy_dir = (
            Path(self.tmp_dir.name)
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
            pack_copy_path = (
                pack_copy_dir / pack.path.name
            )
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
            target_language_library_path = (
                pack.path.parent / f"{target_language}.qll"
            )
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

            target_customization_library_path = (
                pack.path.parent / "Customizations.qll"
            )
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
                fd.writelines(contents)

            # Remove the original target library pack
            logging.debug(
                f"Removing the standard library at {pack.path} in preparation for replacement."
            )
            shutil.rmtree(pack.path.parent.parent)
            # Bundle the new into the bundle.
            logging.debug(
                f"Bundling the standard library pack {pack_copy.config.name} at {pack_copy.path}"
            )
            self.codeql.pack_bundle(pack_copy, self.bundle_path / "qlpacks")

        def bundle_library_pack(library_pack: ResolvedCodeQLPack):
            logging.info(f"Bundling the library pack {library_pack.config.name}.")
            self.codeql.pack_bundle(
                library_pack,
                self.bundle_path / "qlpacks",
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
                    pack_copy,
                    self.bundle_path / "qlpacks",
                    self.bundle_path,
                )
            else:
                logging.info(f"Bundling the query pack {pack.config.name}.")
                pack_copy = copy_pack(pack)
                # Rewrite the query pack dependencies
                with pack_copy.path.open("r") as fd:
                    qlpack_spec = yaml.safe_load(fd)

                # Assume there is only one dependency and it is the standard library.
                qlpack_spec["dependencies"] = {pack.config.name: str(pack.config.version) for pack in pack_copy.dependencies}
                logging.debug(f"Rewriting dependencies for {pack.config.name}.")
                with pack_copy.path.open("w") as fd:
                    yaml.dump(qlpack_spec, fd)

                self.codeql.pack_create(
                    pack_copy,
                    self.bundle_path / "qlpacks",
                )

        for pack in pack_sorter.static_order():
            if pack.kind == CodeQLPackKind.CUSTOMIZATION_PACK:
                bundle_customization_pack(pack)
            elif pack.kind == CodeQLPackKind.LIBRARY_PACK:
                if pack.config.get_scope() == "codeql":
                    bundle_stdlib_pack(pack)
                else:
                    bundle_library_pack(pack)
            elif pack.kind == CodeQLPackKind.QUERY_PACK:
                bundle_query_pack(pack)

    def bundle(self, output_path: Path):
        if output_path.is_dir():
            output_path = output_path / "codeql-bundle.tar.gz"

        logging.debug(f"Bundling custom bundle to {output_path}.")
        with tarfile.open(output_path, mode="w:gz") as bundle_archive:
            bundle_archive.add(self.bundle_path, arcname="codeql")
