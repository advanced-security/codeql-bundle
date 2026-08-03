from pathlib import Path
from tempfile import TemporaryDirectory
import tarfile
import unittest

from codeql_bundle.helpers.bundle import BundlePlatform, CustomBundle


class BundleArchiveTests(unittest.TestCase):
    def test_single_bundle_uses_configured_compression_level(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            bundle = self._bundle(root)
            bundle.compression_level = 1
            output = root / "bundle.tar.gz"

            bundle.bundle(output)

            with tarfile.open(output, mode="r:gz") as archive:
                self.assertEqual(b"bundle", archive.extractfile("codeql/file").read())

    def test_platform_bundle_excludes_other_platform_tools(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            bundle = self._bundle(root)
            tools = bundle.bundle_path / "cpp" / "tools"
            for platform in ("linux64", "osx64", "win64"):
                path = tools / platform
                path.mkdir(parents=True)
                (path / "tool").write_text(platform)
            output = root / "output"
            output.mkdir()

            bundle.bundle(output, {BundlePlatform.LINUX})

            with tarfile.open(
                output / "codeql-bundle-linux64.tar.gz",
                mode="r:gz",
            ) as archive:
                names = set(archive.getnames())
            self.assertIn("codeql/cpp/tools/linux64/tool", names)
            self.assertNotIn("codeql/cpp/tools/osx64/tool", names)
            self.assertNotIn("codeql/cpp/tools/win64/tool", names)

    @staticmethod
    def _bundle(root: Path) -> CustomBundle:
        bundle = object.__new__(CustomBundle)
        bundle.tmp_dir = None
        bundle.bundle_path = root / "codeql"
        bundle.bundle_path.mkdir()
        (bundle.bundle_path / "file").write_text("bundle")
        bundle.languages = {"cpp"}
        bundle.platforms = {
            BundlePlatform.LINUX,
            BundlePlatform.OSX,
            BundlePlatform.WINDOWS,
        }
        bundle.compression_level = 6
        return bundle


if __name__ == "__main__":
    unittest.main()
