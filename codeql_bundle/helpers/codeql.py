import subprocess
import json
from semantic_version import Version, NpmSpec
from pathlib import Path
from typing import Dict, Any, Iterable, Self, Optional, List
import yaml
from dataclasses import dataclass, fields, field
import logging

logger = logging.getLogger(__name__)


class CodeQLException(Exception):
    pass


@dataclass(kw_only=True, frozen=True, eq=True)
class CodeQLPackConfig:
    library: bool = False
    name: str
    version: Version = Version("0.0.0")
    dependencies: Dict[str, NpmSpec] = field(default_factory=dict)
    extractor: Optional[str] = None

    @classmethod
    def from_dict(cls, dict_: Dict[str, Any]) -> Self:
        fieldset = {f.name for f in fields(cls) if f.init}

        def _convert_value(k : str, v : Any) -> Any:
            if k == "version":
                return Version(v)
            elif k == "dependencies":
                return {k: NpmSpec(v) for k, v in v.items()}
            else:
                return v

        filtered_dict = {k: _convert_value(k, v) for k, v in dict_.items() if k in fieldset}
        return cls(**filtered_dict)

    def get_scope(self) -> Optional[str]:
        if "/" in self.name:
            return self.name.split("/")[0]
        else:
            return None

    def get_pack_name(self) -> str:
        if self.get_scope() != None:
            return self.name.split("/")[1]
        else:
            return self.name

    def __hash__(self):
        return hash(f"{self.name}@{str(self.version)}")

@dataclass(kw_only=True, frozen=True, eq=True)
class CodeQLPack:
    path: Path
    config: CodeQLPackConfig

    def __hash__(self) -> int:
        return hash(f"{self.path}")

class CodeQL:
    def __init__(self, codeql_path: Path):
        self.codeql_path = codeql_path
        self._version = None

    def _exec(self, command: str, *args: str) -> subprocess.CompletedProcess[str]:
        logger.debug(
            f"Running CodeQL command: {command} with arguments: {' '.join(args)}"
        )
        return subprocess.run(
            [f"{self.codeql_path}", command] + [arg for arg in args],
            capture_output=True,
            text=True
        )

    def version(self) -> Version:
        if self._version != None:
            return self._version
        else:
            cp = self._exec("version", "--format=json")
            if cp.returncode == 0:
                version_info = json.loads(cp.stdout)
                self._version = Version(version_info["version"])
                return self._version
            else:
                raise CodeQLException(f"Failed to run {cp.args} command!")

    def unpacked_location(self) -> Path:
        cp = self._exec("version", "--format=json")
        if cp.returncode == 0:
            version_info = json.loads(cp.stdout)
            return Path(version_info["unpackedLocation"])
        else:
            raise CodeQLException(f"Failed to run {cp.args} command!")

    def supports_qlx(self) -> bool:
        return self.version() >= Version("2.11.4")

    def pack_ls(self, workspace: Path = Path.cwd()) -> List[CodeQLPack]:
        cp = self._exec("pack", "ls", "--format=json", str(workspace))
        if cp.returncode == 0:
            packs: Iterable[Path] = map(Path, json.loads(cp.stdout)["packs"].keys())

            def load(qlpack_yml_path: Path) -> CodeQLPack:
                with qlpack_yml_path.open("r") as qlpack_yml_file:
                    logger.debug(f"Loading CodeQL pack configuration at {qlpack_yml_path}.")
                    qlpack_yml_as_dict: Dict[str, Any] = yaml.safe_load(qlpack_yml_file)
                    qlpack_config = CodeQLPackConfig.from_dict(qlpack_yml_as_dict)
                    qlpack = CodeQLPack(path=qlpack_yml_path, config=qlpack_config)
                    logger.debug(
                        f"Loaded {qlpack.config.name} with version {str(qlpack.config.version)} at {qlpack.path}."
                    )
                    return qlpack

            logger.debug(f"Listing CodeQL packs for workspace {workspace}")
            return list(map(load, packs))
        else:
            raise CodeQLException(f"Failed to run {cp.args} command! {cp.stderr}")

    def pack_bundle(
        self,
        pack: CodeQLPack,
        output_path: Path,
        *additional_packs: Path,
    ):
        if not pack.config.library:
            raise CodeQLException(f"Cannot bundle non-library pack {pack.config.name}!")

        args = ["bundle", "--format=json", f"--pack-path={output_path}"]
        if len(additional_packs) > 0:
            args.append(f"--additional-packs={':'.join(map(str,additional_packs))}")
        cp = self._exec(
            "pack",
            *args,
            "--",
            str(pack.path.parent),
        )

        if cp.returncode != 0:
            raise CodeQLException(f"Failed to run {cp.args} command! {cp.stderr}")

    def pack_create(
        self,
        pack: CodeQLPack,
        output_path: Path,
        *additional_packs: Path,
    ):
        if pack.config.library:
            raise CodeQLException(f"Cannot bundle non-query pack {pack.config.name}!")

        args = ["create", "--format=json", f"--output={output_path}", "--threads=0", "--no-default-compilation-cache"]
        if self.supports_qlx():
            args.append("--qlx")
        if len(additional_packs) > 0:
            args.append(f"--additional-packs={':'.join(map(str,additional_packs))}")
        cp = self._exec(
            "pack",
            *args,
            "--",
            str(pack.path.parent),
        )

        if cp.returncode != 0:
            raise CodeQLException(f"Failed to run {cp.args} command! {cp.stderr}")
        
    def resolve_languages(self) -> set[str]:
        cp = self._exec("resolve", "languages", "--format=json")
        if cp.returncode == 0:
            return set(json.loads(cp.stdout).keys())
        else:
            raise CodeQLException(f"Failed to run {cp.args} command! {cp.stderr}")
