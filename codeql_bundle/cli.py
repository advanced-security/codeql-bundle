# Add the parent directory to the path if this module is run directly (i.e. not imported)
# This is necessary to support both the Poetry script invocation and the direct invocation.
if not __package__ and __name__ == "__main__":
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).parent.parent))
    __package__ = Path(__file__).parent.name

import click
from pathlib import Path
from codeql_bundle.helpers.codeql import CodeQLException
from codeql_bundle.helpers.bundle import CustomBundle, BundleException, BundlePlatform
from typing import List, Optional
import sys
import logging

logger = logging.getLogger(__name__)

@click.command()
@click.option(
    "-b",
    "--bundle",
    "bundle_path",
    required=True,
    help="Path to a CodeQL bundle downloaded from https://github.com/github/codeql-action/releases",
    type=click.Path(exists=True, path_type=Path),
)
@click.option(
    "-o",
    "--output",
    required=True,
    help="Path to store the custom CodeQL bundle. Can be a directory or a non-existing archive ending with the extension '.tar.gz' if there is only a single bundle",
    type=click.Path(path_type=Path),
)
@click.option(
    "-w",
    "--workspace",
    help="Path to a directory containing a 'codeql-workspace.yml' file or a path to a 'codeql-workspace.yml' file",
    type=click.Path(exists=True, path_type=Path),
    default=Path.cwd(),
)
@click.option('--no-precompile', '-nc', is_flag=True, help="Do not pre-compile the bundle.")
@click.option(
    "-l",
    "--log",
    "loglevel",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="WARNING",
)
@click.option("-p", "--platform", multiple=True, type=click.Choice(["linux64", "osx64", "win64"], case_sensitive=False), help="Target platform for the bundle")
@click.option("-c", "--code-scanning-config", type=click.Path(exists=True, path_type=Path), help="Path to a Code Scanning configuration file that will be the default for the bundle")
@click.argument("packs", nargs=-1, required=True)
def main(
    bundle_path: Path,
    output: Path,
    workspace: Path,
    no_precompile: bool,
    loglevel: str,
    platform: List[str],
    code_scanning_config: Optional[Path],
    packs: List[str],
) -> None:

    if loglevel == "DEBUG":
        logging.basicConfig(
            format="%(levelname)s:%(asctime)s %(message)s",
            level=getattr(logging, loglevel.upper()),
        )
    else:
        logging.basicConfig(
            format="%(levelname)s: %(message)s",
            level=getattr(logging, loglevel.upper()),
        )

    if workspace.name == "codeql-workspace.yml":
        workspace = workspace.parent

    logger.info(
        f"Creating custom bundle of {bundle_path} using CodeQL pack(s) in workspace {workspace}"
    )

    try:
        bundle = CustomBundle(bundle_path, workspace)
        # options for custom bundle 
        bundle.disable_precompilation = no_precompile

        unsupported_platforms = list(filter(lambda p: not bundle.supports_platform(BundlePlatform.from_string(p)), platform))
        if len(unsupported_platforms) > 0:
            logger.fatal(
                f"The provided bundle supports the platform(s) {', '.join(map(str, bundle.platforms))}, but doesn't support the following platform(s): {', '.join(unsupported_platforms)}"
            )
            sys.exit(1)

        logger.info(f"Looking for CodeQL packs in workspace {workspace}")
        packs_in_workspace = bundle.get_workspace_packs()
        logger.info(
            f"Found the CodeQL pack(s): {','.join(map(lambda p: p.config.name, packs_in_workspace))}"
        )

        logger.info(
            f"Considering the following CodeQL pack(s) for inclusion in the custom bundle: {','.join(packs)}"
        )

        if len(packs) > 0:
            selected_packs = [
                available_pack
                for available_pack in packs_in_workspace
                if available_pack.config.name in packs
            ]
        else:
            selected_packs = packs_in_workspace

        
        missing_packs = set(packs) - {pack.config.name for pack in selected_packs}
        if len(missing_packs) > 0:
            logger.fatal(
                f"The provided CodeQL workspace doesn't contain the provided pack(s) '{','.join(missing_packs)}'",
            )
            sys.exit(1)

        logger.info(
            f"Adding the pack(s) {','.join(map(lambda p: p.config.name, selected_packs))} and its workspace dependencies to the custom bundle."
        )
        bundle.add_packs(*selected_packs)
        if code_scanning_config:
            logger.info(f"Adding the Code Scanning configuration file {code_scanning_config} to the custom bundle.")
            bundle.add_code_scanning_config(code_scanning_config)
        logger.info(f"Bundling custom bundle(s) at {output}")
        platforms = set(map(BundlePlatform.from_string, platform))
        bundle.bundle(output, platforms)
        logger.info(f"Completed building of custom bundle(s).")
    except CodeQLException as e:
        logger.fatal(f"Failed executing CodeQL command with reason: '{e}'")
        sys.exit(1)
    except BundleException as e:
        logger.fatal(f"Failed to build custom bundle with reason: '{e}'")
        sys.exit(1)


if __name__ == "__main__":
    main()
