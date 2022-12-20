import click
from pathlib import Path
from helpers.codeql import CodeQLException
from helpers.bundle import CustomBundle, BundleException
from typing import List
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
    help="Path to store the custom CodeQL bundle. Can be a directory or a non-existing archive ending with the extension '.tar.gz'",
    type=click.Path(path_type=Path),
)
@click.option(
    "-w",
    "--workspace",
    help="Path to a directory containing a 'codeql-workspace.yml' file or a path to a 'codeql-workspace.yml' file",
    type=click.Path(exists=True, path_type=Path),
    default=Path.cwd(),
)
@click.option(
    "-l",
    "--log",
    "loglevel",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="WARNING",
)
@click.argument("packs", nargs=-1, required=True)
def cli(
    bundle_path: Path,
    output: Path,
    workspace: Path,
    loglevel: str,
    packs: List[str],
) -> None:

    logging.basicConfig(level=getattr(logging, loglevel.upper()))

    if workspace.name == "codeql-workspace.yml":
        workspace = workspace.parent

    logger.debug(f"Creating custom bundle of {bundle_path} with workspace {workspace}")
    bundle = CustomBundle(bundle_path, workspace)
    try:
        logger.debug(f"Listing CodeQL packs in workspace {workspace}")
        packs_in_workspace = bundle.codeql.pack_ls(workspace)
        logger.debug(
            f"Found the CodeQL packs: {','.join(map(lambda p: p.name, packs_in_workspace))}"
        )

        if len(packs) > 0:
            selected_packs = [
                available_pack
                for available_pack in packs_in_workspace
                if available_pack.name in packs
            ]
        else:
            selected_packs = packs_in_workspace

        missing_packs = set(packs) - {pack.name for pack in selected_packs}
        if len(missing_packs) > 0:
            logger.fatal(
                f"The provided CodeQL workspace doesn't contain the provided packs '{','.join(missing_packs)}'",
            )
            sys.exit(1)

        logger.debug(
            f"Add the packs {','.join(map(lambda p: p.name, selected_packs))} to the custom bundle."
        )
        bundle.add_packs(*selected_packs)
        logger.debug(f"Bundling custom bundle at {output}")
        bundle.bundle(output)
    except CodeQLException as e:
        logger.fatal(f"Failed executing CodeQL command with reason: '{e}'")
        sys.exit(1)
    except BundleException as e:
        logger.fatal(f"Failed to build custom bundle with reason: '{e}'")
        sys.exit(1)


if __name__ == "__main__":
    cli()
