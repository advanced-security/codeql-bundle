from .codeql import (
    CodeQL,
    CodeQLException,
    ResolvedCodeQLPack,
    CodeQLPackKind,
)
from pathlib import Path
from tempfile import TemporaryDirectory
import tarfile
from typing import List, cast
from collections import defaultdict
import shutil
import yaml
import dataclasses
import logging

logger = logging.getLogger(__name__)


class BundleException(Exception):
    pass


class Bundle:
    def __init__(self, bundle_path: Path) -> None:
        self.tmp_dir = None
        if bundle_path.is_dir():
            self.bundle_path = bundle_path
        elif bundle_path.is_file() and bundle_path.name.endswith(".tar.gz"):
            self.tmp_dir = TemporaryDirectory()
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
            version = self.codeql.version()
            logging.info(f"Found CodeQL CLI version {version}.")
        except CodeQLException:
            raise BundleException("Cannot determine CodeQL version!")

    def __del__(self) -> None:
        if self.tmp_dir:
            logging.info(
                f"Removing temporary directory {self.tmp_dir.name} used to build custom bundle."
            )
            self.tmp_dir.cleanup()

    def getCodeQLPacks(self) -> List[ResolvedCodeQLPack]:
        return self.codeql.pack_ls(self.bundle_path)


class CustomBundle(Bundle):
    def __init__(self, bundle_path: Path, workspace_path: Path = Path.cwd()) -> None:
        Bundle.__init__(self, bundle_path)

        self.bundle_packs = self.getCodeQLPacks()
        self.workspace_packs = self.codeql.pack_ls(workspace_path)
        self.available_packs = {
            pack.name: pack for pack in self.bundle_packs + self.workspace_packs
        }
        # A custom bundle will always need a temp directory for customization work.
        # If the bundle didn't create one (there was no need to unpack it), create it here.
        if not self.tmp_dir:
            self.tmp_dir = TemporaryDirectory()
            logging.debug(
                f"Bundle doesn't have an associated temporary directory, created {self.tmp_dir.name} for building a custom bundle."
            )

    def _validate_pack(self, pack: ResolvedCodeQLPack) -> None:
        logging.info(
            f"Validating if the CodeQL pack {pack.name} is compliant with the provided bundle."
        )
        for dep_name in pack.dependencies.keys():
            if not dep_name in self.available_packs:
                raise BundleException(
                    f"Package {pack.name} depends on missing pack {dep_name}",
                )

            dep_pack = self.available_packs[dep_name]
            if pack.dependencies[dep_name] > dep_pack.version:
                raise BundleException(
                    f"Package {pack.name} depends on version {pack.dependencies[dep_name]} of pack {dep_pack.name}, but the bundle contains {dep_pack.version}",
                )
        logging.info(f"The CodeQL pack {pack.name}'s dependencies are satisfied.")

    def add_packs(self, *packs: ResolvedCodeQLPack):
        for pack in packs:
            self._validate_pack(pack)

        kind_to_pack_map: dict[CodeQLPackKind, List[ResolvedCodeQLPack]] = defaultdict(
            list
        )
        for pack in packs:
            kind_to_pack_map[pack.kind].append(pack)

        for library_pack in kind_to_pack_map[CodeQLPackKind.LIBRARY_PACK]:
            logging.info(f"Bundling the library pack {library_pack.name}.")
            self.codeql.pack_bundle(
                library_pack,
                self.bundle_path / "qlpacks",
            )

        # Collect customizations packs targeting the same library pack so we can
        # process them in one go.
        target_to_customization_pack_map: dict[
            ResolvedCodeQLPack, List[ResolvedCodeQLPack]
        ] = defaultdict(list)

        for customization_pack in kind_to_pack_map[CodeQLPackKind.CUSTOMIZATION_PACK]:

            def is_candidate(pack: ResolvedCodeQLPack) -> bool:
                if pack.get_scope() != "codeql":
                    return False
                # Here we assume that all our standard library, library packs have the scope 'codeql' and name '{lang}-all'
                # with  '{lang}' being one the CodeQL supported languages.
                return pack.get_pack_name().endswith("-all")

            resolved_dependencies = [
                self.available_packs[dep_name]
                for dep_name in customization_pack.dependencies.keys()
            ]
            candidates = list(filter(is_candidate, resolved_dependencies))
            if len(candidates) != 1:
                raise BundleException(
                    f"Expected 1 standard library dependency for {customization_pack.name}, but found {len(candidates)}"
                )

            target_to_customization_pack_map[candidates[0]].append(customization_pack)

            for target, customization_packs in target_to_customization_pack_map.items():
                logging.info(
                    f"Applying customization pack(s) to the standard library pack {target.name}"
                )
                # First we bundle each customization pack.
                for customization_pack in customization_packs:
                    logging.info(f"Applying {customization_pack.name} to {target.name}")
                    customization_pack_copy_dir = Path(self.tmp_dir.name)
                    if customization_pack.get_scope() != None:
                        customization_pack_copy_dir = (
                            customization_pack_copy_dir / customization_pack.get_scope()
                        )
                    customization_pack_copy_dir = (
                        customization_pack_copy_dir
                        / customization_pack.get_pack_name()
                        / str(customization_pack.version)
                    )
                    logging.debug(
                        f"Copying { customization_pack.path.parent} to {customization_pack_copy_dir} for modifications"
                    )
                    shutil.copytree(
                        customization_pack.path.parent,
                        customization_pack_copy_dir,
                    )

                    customization_pack_copy_path = (
                        customization_pack_copy_dir / customization_pack.path.name
                    )

                    # Remove the target dependency to prevent a circular dependency in the target.
                    logging.debug(
                        f"Removing dependency on {target.name} to prevent circular dependency."
                    )
                    with customization_pack_copy_path.open("r") as fd:
                        qlpack_spec = yaml.safe_load(fd)

                    del qlpack_spec["dependencies"][target.name]
                    with customization_pack_copy_path.open("w") as fd:
                        yaml.dump(qlpack_spec, fd)

                    customization_pack_copy = dataclasses.replace(
                        customization_pack, path=customization_pack_copy_path
                    )
                    logging.debug(
                        f"Bundling the customization pack {customization_pack_copy.name} at {customization_pack_copy.path}"
                    )
                    self.codeql.pack_bundle(
                        customization_pack_copy, self.bundle_path / "qlpacks"
                    )

                # Finally, we process the targeted standard library pack
                # We copy the parent of the parent because the created query packs follow the directory structure
                # scope/name/version/qlpack.yml and we want to avoid conflicts if multiple packs have the same version.
                target_copy_dir = (
                    Path(self.tmp_dir.name)
                    / cast(Path, target.get_scope())
                    / target.get_pack_name()
                )
                logging.debug(
                    f"Copying {target.path.parent.parent} to {target_copy_dir} for modification"
                )
                shutil.copytree(
                    target.path.parent.parent,
                    target_copy_dir,
                )
                target_copy_path = (
                    target_copy_dir / str(target.version) / target.path.name
                )
                target_copy = dataclasses.replace(target, path=target_copy_path)

                with target_copy.path.open("r") as fd:
                    qlpack_spec = yaml.safe_load(fd)
                if not "dependencies" in qlpack_spec:
                    qlpack_spec["dependencies"] = {}
                for customization_pack in customization_packs:
                    logging.debug(
                        f"Adding dependency {customization_pack.name} to {target.name}"
                    )
                    qlpack_spec["dependencies"][customization_pack.name] = str(
                        customization_pack.version
                    )
                with target_copy.path.open("w") as fd:
                    yaml.dump(qlpack_spec, fd)

                logging.debug(
                    f"Determining if standard library CodeQL library pack {target.name} is customizable."
                )
                if not (target_copy.path.parent / "Customizations.qll").exists():
                    logging.debug(
                        f"Standard library CodeQL pack {target.name} does not have a 'Customizations' library, attempting to add one."
                    )
                    # Assume the CodeQL library pack has name `<language>-all`.
                    target_language = target_copy.get_pack_name().removesuffix("-all")
                    target_language_library_path = (
                        target_copy.path.parent / f"{target_language}.qll"
                    )
                    logging.debug(
                        f"Looking for standard library language module {target_language_library_path.name}"
                    )
                    if not target_language_library_path.exists():
                        raise BundleException(
                            f"Unable to customize {target.name}, because it doesn't have a 'Customizations' library and we cannot determine the language library."
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
                            f"Unable to customize {target.name}, because we cannot determine the first import statement of {target_language_library_path.name}."
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
                        target_copy.path.parent / "Customizations.qll"
                    )
                    logging.debug(
                        f"Creating Customizations library with import of language {target_language}"
                    )
                    with target_customization_library_path.open("w") as fd:
                        fd.write(f"import {target_language}\n")

                logging.debug(
                    f"Updating 'Customizations.qll' with imports of customization libraries."
                )
                with (target_copy.path.parent / "Customizations.qll").open("r") as fd:
                    contents = fd.readlines()
                for customization_pack in customization_packs:
                    contents.append(
                        f"import {customization_pack.name.replace('-', '_').replace('/', '.')}.Customizations"
                    )
                with (target_copy.path.parent / "Customizations.qll").open("w") as fd:
                    fd.writelines(contents)

                # Remove the original target library pack
                logging.debug(
                    f"Removing the standard library at {target.path} in preparation for replacement."
                )
                shutil.rmtree(target.path.parent.parent)
                # Bundle the new into the bundle.
                logging.debug(
                    f"Bundling the standard library pack {target_copy.name} at {target_copy.path}"
                )
                self.codeql.pack_bundle(target_copy, self.bundle_path / "qlpacks")

                logging.info(
                    f"Looking for standard library query packs that need to be recreated."
                )
                # Recompile the query packs depending on the target library pack
                for query_pack in filter(
                    lambda p: p.kind == CodeQLPackKind.QUERY_PACK,
                    self.available_packs.values(),
                ):
                    # Determine if the query pack depends on a library pack we customized.
                    if target.name in query_pack.dependencies:
                        logging.info(
                            f"Found query pack {query_pack.name} that is depended on {target.name} and needs to be recreated."
                        )
                        query_pack_copy_dir = (
                            Path(self.tmp_dir.name)
                            / cast(Path, query_pack.get_scope())
                            / query_pack.get_pack_name()
                        )
                        logging.debug(
                            f"Copying {query_pack.path.parent.parent} to {query_pack_copy_dir} for modification."
                        )
                        shutil.copytree(
                            query_pack.path.parent.parent, query_pack_copy_dir
                        )

                        query_pack_copy_path = (
                            query_pack_copy_dir
                            / str(query_pack.version)
                            / query_pack.path.name
                        )
                        query_pack_copy = dataclasses.replace(
                            query_pack, path=query_pack_copy_path
                        )

                        # A CodeQL bundle can contain query packs that rely on a suite-help pack that is not part of the bundle.
                        # This poses a problem when recompiling a query pack assuming all dependencies are in the bundle.
                        # We patch the version to use the version available in the bundle and rely on the compiler for correctness.
                        if "codeql/suite-helpers" in query_pack_copy.dependencies:
                            logging.debug(
                                f"Patching dependency on 'codeql/suite-helpers' for {query_pack_copy.name}"
                            )
                            with query_pack_copy.path.open("r") as fd:
                                qlpack_spec = yaml.safe_load(fd)

                            qlpack_spec["dependencies"]["codeql/suite-helpers"] = "*"

                            with query_pack_copy.path.open("w") as fd:
                                yaml.dump(qlpack_spec, fd)

                        # Remove the lock file
                        logging.debug(
                            f"Removing CodeQL pack lock file {query_pack_copy.path.parent / 'codeql-pack.lock.yml'}"
                        )
                        (query_pack_copy.path.parent / "codeql-pack.lock.yml").unlink()
                        # Remove the included dependencies
                        logging.debug(
                            f"Removing CodeQL query pack dependencies directory {query_pack_copy.path.parent / '.codeql'}"
                        )
                        shutil.rmtree(query_pack_copy.path.parent / ".codeql")
                        # Remove the query cache, if it exists.
                        logging.debug(
                            f"Removing CodeQL query pack cache directory {query_pack_copy.path.parent / '.cache'}, if it exists."
                        )
                        shutil.rmtree(
                            query_pack_copy.path.parent / ".cache",
                            ignore_errors=True,
                        )
                        # Remove qlx files
                        if self.codeql.supports_qlx():
                            logging.debug(f"Removing 'qlx' files in query pack.")
                            for qlx_path in query_pack_copy.path.parent.glob(
                                "**/*.qlx"
                            ):
                                qlx_path.unlink()
                        # Remove the original query pack
                        logging.debug(
                            f"Removing the standard library query pack directory {query_pack.path.parent.parent} in preparation for recreation."
                        )
                        shutil.rmtree(query_pack.path.parent.parent)
                        logging.debug(
                            f"Recreating {query_pack_copy.name} at {query_pack_copy.path} to {self.bundle_path / 'qlpacks'}"
                        )
                        # Recompile the query pack with the assumption that all its dependencies are now in the bundle.
                        self.codeql.pack_create(
                            query_pack_copy,
                            self.bundle_path / "qlpacks",
                            self.bundle_path,
                        )

        logging.info(f"Looking for workspace query packs that need to be created.")
        for query_pack in kind_to_pack_map[CodeQLPackKind.QUERY_PACK]:
            # Copy the query pack so we can build it independently from the CodeQL workspace it is part of.
            # This prevents cyclic dependency issues if  the workspace has customizations.
            query_pack_copy_dir = Path(self.tmp_dir.name)
            if query_pack.get_scope() != None:
                query_pack_copy_dir = query_pack_copy_dir / query_pack.get_scope()
            query_pack_copy_dir = (
                query_pack_copy_dir
                / query_pack.get_pack_name()
                / str(query_pack.version)
            )

            shutil.copytree(
                query_pack.path.parent,
                query_pack_copy_dir,
            )
            query_pack_copy_path = query_pack_copy_dir / query_pack.path.name
            query_pack_copy = dataclasses.replace(query_pack, path=query_pack_copy_path)
            logging.info(f"Creating query pack {query_pack_copy.name}.")
            self.codeql.pack_create(
                query_pack_copy, self.bundle_path / "qlpacks", self.bundle_path
            )

    def bundle(self, output_path: Path):
        if output_path.is_dir():
            output_path = output_path / "codeql-bundle.tar.gz"

        logging.debug(f"Bundling custom bundle to {output_path}.")
        with tarfile.open(output_path, mode="w:gz") as bundle_archive:
            bundle_archive.add(self.bundle_path, arcname="codeql")
