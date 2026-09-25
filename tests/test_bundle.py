from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest
from unittest.mock import patch

from codeql_bundle.helpers.bundle import CustomBundle


class AdditionalDataTests(unittest.TestCase):
    def test_linux_arm64_certificate_uses_native_java_tools(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "workspace"
            workspace.mkdir()
            certificate = workspace / "certificate.pem"
            certificate.write_text("certificate")
            config = workspace / "additional-data.json"
            config.write_text(
                json.dumps(
                    {
                        "CodeQLBundleAdditionalCertificates": [
                            {"Source": certificate.name}
                        ]
                    }
                )
            )

            bundle = object.__new__(CustomBundle)
            bundle.tmp_dir = None
            bundle.bundle_path = root / "bundle"
            keytool = (
                bundle.bundle_path
                / "tools/linux-arm64/java/bin/keytool"
            )
            keystore = (
                bundle.bundle_path
                / "tools/linux-arm64/java/lib/security/cacerts"
            )
            keytool.parent.mkdir(parents=True)
            keytool.touch()
            keystore.parent.mkdir(parents=True)
            keystore.touch()

            with patch(
                "codeql_bundle.helpers.bundle.current_bundle_platform",
                return_value="linux-arm64",
            ), patch("codeql_bundle.helpers.bundle.subprocess.run") as run:
                bundle.add_files_and_certs(config, workspace)

        run.assert_called_once()
        command = run.call_args.args[0]
        self.assertEqual(str(keytool), command[0])
        self.assertEqual(str(keystore), command[command.index("-keystore") + 1])


if __name__ == "__main__":
    unittest.main()
