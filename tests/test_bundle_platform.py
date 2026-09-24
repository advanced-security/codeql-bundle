import unittest

from codeql_bundle.helpers.bundle import BundleException, BundlePlatform


class BundlePlatformTests(unittest.TestCase):
    def test_supported_platform_names_round_trip(self) -> None:
        for platform_name in ("linux64", "linux-arm64", "osx64", "win64"):
            self.assertEqual(
                platform_name,
                str(BundlePlatform.from_string(platform_name)),
            )

    def test_linux_alias_remains_linux_x64(self) -> None:
        self.assertEqual(BundlePlatform.LINUX, BundlePlatform.from_string("linux"))

    def test_invalid_platform_is_rejected(self) -> None:
        with self.assertRaises(BundleException):
            BundlePlatform.from_string("linux-riscv64")


if __name__ == "__main__":
    unittest.main()
