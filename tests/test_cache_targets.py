from pathlib import Path
import unittest

from semantic_version import Version

from codeql_bundle.helpers.bundle import (
    CodeQLPackKind,
    ResolvedCodeQLPack,
    get_compilation_cache_targets,
)
from codeql_bundle.helpers.codeql import CodeQLPackConfig


def pack(
    name: str,
    kind: CodeQLPackKind,
    dependencies: list[ResolvedCodeQLPack] | None = None,
) -> ResolvedCodeQLPack:
    return ResolvedCodeQLPack(
        path=Path(name.replace("/", "-")) / "qlpack.yml",
        config=CodeQLPackConfig(
            library=kind != CodeQLPackKind.QUERY_PACK,
            name=name,
            version=Version("1.0.0"),
        ),
        kind=kind,
        dependencies=dependencies or [],
    )


class CacheTargetTests(unittest.TestCase):
    def test_query_packs_are_grouped_by_standard_library(self) -> None:
        standard_library = pack("codeql/cpp-all", CodeQLPackKind.LIBRARY_PACK)
        query_pack = pack(
            "codeql/cpp-queries",
            CodeQLPackKind.QUERY_PACK,
            [standard_library],
        )
        unrelated_library = pack(
            "codeql/util", CodeQLPackKind.LIBRARY_PACK
        )

        targets = get_compilation_cache_targets(
            [standard_library, query_pack, unrelated_library]
        )

        self.assertEqual({standard_library: [query_pack]}, targets)


if __name__ == "__main__":
    unittest.main()
