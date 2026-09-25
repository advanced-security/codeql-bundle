from pathlib import Path
from tempfile import TemporaryDirectory
import tarfile
import unittest
from unittest.mock import patch

from codeql_bundle.helpers.bundle import BundlePlatform, CustomBundle


class BundleTests(unittest.TestCase):
    def test_linux_arm64_bundle_uses_only_native_tools(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            certificate = root / "certificate.pem"
            certificate.write_text("certificate")
            config = root / "additional-data.json"
            config.write_text(
                '{"CodeQLBundleAdditionalCertificates":'
                '[{"Source":"certificate.pem"}]}'
            )

            bundle = object.__new__(CustomBundle)
            bundle.tmp_dir = None
            bundle.bundle_path = root / "bundle"
            bundle.languages = []
            bundle.platforms = {BundlePlatform.LINUX_ARM64}
            keytool = bundle.bundle_path / "tools/linux-arm64/java/bin/keytool"
            keystore = (
                bundle.bundle_path
                / "tools/linux-arm64/java/lib/security/cacerts"
            )
            for path in (keytool, keystore):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()

            with patch(
                "codeql_bundle.helpers.bundle.current_bundle_platform",
                return_value="linux-arm64",
            ), patch(
                "codeql_bundle.helpers.bundle.subprocess.run"
            ) as run, patch(
                "codeql_bundle.helpers.bundle.tarfile.open"
            ) as open_archive:
                bundle.add_files_and_certs(config, root)
                bundle.bundle(root, {BundlePlatform.LINUX_ARM64})

        run.assert_called_once()
        command = run.call_args.args[0]
        self.assertEqual(str(keytool), command[0])
        self.assertEqual(str(keystore), command[command.index("-keystore") + 1])
        archive_filter = (
            open_archive.return_value.__enter__.return_value.add.call_args.kwargs[
                "filter"
            ]
        )
        self.assertIsNone(
            archive_filter(tarfile.TarInfo("codeql/tools/linux64/tool"))
        )
        self.assertIsNone(
            archive_filter(tarfile.TarInfo("codeql/swift/qltest/linux64/tool"))
        )
        self.assertIsNone(
            archive_filter(
                tarfile.TarInfo("codeql/swift/resource-dir/linux64/tool")
            )
        )
        self.assertIsNotNone(
            archive_filter(tarfile.TarInfo("codeql/tools/linux-arm64/tool"))
        )


if __name__ == "__main__":
    unittest.main()
