from pathlib import Path
import subprocess
import unittest

from semantic_version import Version

from codeql_bundle.helpers.codeql import CodeQL, CodeQLPack, CodeQLPackConfig


class RecordingCodeQL(CodeQL):
    def __init__(self) -> None:
        super().__init__(Path("codeql"))
        self._version = Version("2.26.1")
        self.commands: list[list[str]] = []

    def _exec(
        self, command: str, *args: str
    ) -> subprocess.CompletedProcess[str]:
        self.commands.append([command, *args])
        return subprocess.CompletedProcess(
            ["codeql", command, *args],
            0,
            stdout="",
            stderr="",
        )

    def _exec_streaming(
        self, command: str, *args: str
    ) -> subprocess.CompletedProcess[str]:
        return self._exec(command, *args)


class CodeQLCommandTests(unittest.TestCase):
    def test_pack_create_uses_only_explicit_compilation_caches(self) -> None:
        codeql = RecordingCodeQL()
        pack = CodeQLPack(
            path=Path("query-pack/qlpack.yml"),
            config=CodeQLPackConfig(
                name="example/queries",
                version=Version("1.0.0"),
            ),
        )

        codeql.pack_create(
            pack,
            Path("output"),
            compilation_caches=[Path("cpp-cache"), Path("shared-cache")],
        )

        command = codeql.commands[-1]
        self.assertIn("--no-default-compilation-cache", command)
        self.assertIn("--compilation-cache=cpp-cache", command)
        self.assertIn("--compilation-cache=shared-cache", command)

    def test_query_compile_builds_an_explicit_cache(self) -> None:
        codeql = RecordingCodeQL()

        codeql.query_compile(
            [Path("queries")],
            Path("compilation-cache"),
            Path("bundle"),
            threads=4,
            ram=8192,
            compilation_cache_size=4096,
        )

        command = codeql.commands[-1]
        self.assertIn("--no-default-compilation-cache", command)
        self.assertIn("--compilation-cache=compilation-cache", command)
        self.assertIn("--compilation-cache-size=4096", command)
        self.assertIn("--threads=4", command)
        self.assertIn("--ram=8192", command)


if __name__ == "__main__":
    unittest.main()
