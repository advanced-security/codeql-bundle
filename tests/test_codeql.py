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
        self.assertIn("--threads=0", command)
        self.assertIn("--compilation-cache=cpp-cache", command)
        self.assertIn("--compilation-cache=shared-cache", command)
        self.assertFalse(any(argument.startswith("--ram=") for argument in command))

    def test_pack_commands_use_configured_resources(self) -> None:
        codeql = RecordingCodeQL()
        codeql.threads = 4
        codeql.ram = 8192
        query_pack = CodeQLPack(
            path=Path("query-pack/qlpack.yml"),
            config=CodeQLPackConfig(
                name="example/queries",
                version=Version("1.0.0"),
            ),
        )
        library_pack = CodeQLPack(
            path=Path("library-pack/qlpack.yml"),
            config=CodeQLPackConfig(
                name="example/library",
                version=Version("1.0.0"),
                library=True,
            ),
        )

        codeql.pack_create(query_pack, Path("output"))
        create_command = codeql.commands[-1]
        codeql.pack_bundle(library_pack, Path("output"))
        bundle_command = codeql.commands[-1]

        for command in (create_command, bundle_command):
            self.assertIn("--threads=4", command)
            self.assertIn("--ram=8192", command)

    def test_pack_bundle_uses_all_threads_by_default(self) -> None:
        codeql = RecordingCodeQL()
        library_pack = CodeQLPack(
            path=Path("library-pack/qlpack.yml"),
            config=CodeQLPackConfig(
                name="example/library",
                version=Version("1.0.0"),
                library=True,
            ),
        )

        codeql.pack_bundle(library_pack, Path("output"))

        command = codeql.commands[-1]
        self.assertIn("--threads=0", command)
        self.assertFalse(any(argument.startswith("--ram=") for argument in command))

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
