from pathlib import Path
from tempfile import TemporaryDirectory
import tarfile
import unittest

from codeql_bundle.helpers.bundle import BundlePlatform, CustomBundle


class BundleTests(unittest.TestCase):
    def test_linux_bundles_keep_only_the_target_architecture(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            bundle = object.__new__(CustomBundle)
            bundle.tmp_dir = None
            bundle.bundle_path = root / "bundle"
            bundle.languages = set()
            bundle.platforms = {
                BundlePlatform.LINUX,
                BundlePlatform.LINUX_ARM64,
            }
            for platform in bundle.platforms:
                path = bundle.bundle_path / f"tools/{platform}/tool"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()

            config = root / "qlt.conf.json"
            config.write_text("{}")
            bundle.add_files_and_certs(config, root)
            bundle.bundle(root, bundle.platforms)

            for target, excluded in (
                (BundlePlatform.LINUX, BundlePlatform.LINUX_ARM64),
                (BundlePlatform.LINUX_ARM64, BundlePlatform.LINUX),
            ):
                with tarfile.open(
                    root / f"codeql-bundle-{target}.tar.gz"
                ) as archive:
                    names = archive.getnames()
                self.assertIn(f"codeql/tools/{target}/tool", names)
                self.assertNotIn(f"codeql/tools/{excluded}/tool", names)


if __name__ == "__main__":
    unittest.main()
