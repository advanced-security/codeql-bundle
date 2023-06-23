import subprocess
import json
from semantic_version import Version, NpmSpec
from pathlib import Path
from typing import Dict, Any, Iterable, Self, Optional, List
import yaml
from dataclasses import dataclass, fields, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class CodeQLException(Exception):
    pass


@dataclass(kw_only=True, frozen=True, eq=True)
class CodeQLPack:
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


class CodeQLPackKind(Enum):
    QUERY_PACK = 1
    LIBRARY_PACK = 2
    CUSTOMIZATION_PACK = 3


@dataclass(kw_only=True, frozen=True, eq=True)
class ResolvedCodeQLPack(CodeQLPack):
    path: Path
    kind: CodeQLPackKind

    def __hash__(self):
        return CodeQLPack.__hash__(self)


class CodeQL:
    def __init__(self, codeql_path: Path):
        self.codeql_path = codeql_path

    def _exec(self, command: str, *args: str) -> subprocess.CompletedProcess[str]:
        logger.debug(
            f"Running CodeQL command: {command} with arguments: {' '.join(args)}"
        )
        return subprocess.run(
            [f"{self.codeql_path}", command] + [arg for arg in args],
            capture_output=True,
            text=True,
        )

    def version(self) -> Version:
        cp = self._exec("version", "--format=json")
        if cp.returncode == 0:
            version_info = json.loads(cp.stdout)
            return Version(version_info["version"])
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

    def pack_ls(self, workspace: Path = Path.cwd()) -> List[ResolvedCodeQLPack]:
        cp = self._exec("pack", "ls", "--format=json", str(workspace))
        if cp.returncode == 0:
            packs: Iterable[Path] = map(Path, json.loads(cp.stdout)["packs"].keys())

            def load(qlpack: Path) -> ResolvedCodeQLPack:
                with qlpack.open("r") as qlpack_file:
                    logger.debug(f"Resolving CodeQL pack at {qlpack}.")
                    qlpack_spec = yaml.safe_load(qlpack_file)
                    qlpack_spec["path"] = qlpack
                    if not "library" in qlpack_spec or not qlpack_spec["library"]:
                        qlpack_spec["kind"] = CodeQLPackKind.QUERY_PACK
                    else:
                        if (
                            qlpack_spec["path"].parent
                            / qlpack_spec["name"].replace("-", "_")
                            / "Customizations.qll"
                        ).exists():
                            qlpack_spec["kind"] = CodeQLPackKind.CUSTOMIZATION_PACK
                        else:
                            qlpack_spec["kind"] = CodeQLPackKind.LIBRARY_PACK
                    resolved_pack = ResolvedCodeQLPack.from_dict(qlpack_spec)
                    logger.debug(
                        f"Resolved {resolved_pack.name} with version {str(resolved_pack.version)} at {resolved_pack.path} with kind {resolved_pack.kind.name}"
                    )
                    return resolved_pack

            logger.debug(f"Resolving CodeQL packs for workspace {workspace}")
            return list(map(load, packs))
        else:
            raise CodeQLException(f"Failed to run {cp.args} command! {cp.stderr}")

    def pack_bundle(
        self,
        pack: ResolvedCodeQLPack,
        output_path: Path,
        *additional_packs: Path,
    ):
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
        pack: ResolvedCodeQLPack,
        output_path: Path,
        *additional_packs: Path,
    ):
        args = ["create", "--format=json", f"--output={output_path}", "--threads=0"]
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
