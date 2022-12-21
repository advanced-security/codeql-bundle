import click
from pathlib import Path
from codeql_bundle.helpers.codeql import CodeQLException
from codeql_bundle.helpers.bundle import CustomBundle, BundleException
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
@click.argument("packs", nargs=-1)
def main(
    bundle_path: Path,
    output: Path,
    workspace: Path,
    loglevel: str,
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
        f"Creating custom bundle of {bundle_path} using CodeQL packs in workspace {workspace}"
    )

    try:
        bundle = CustomBundle(bundle_path, workspace)
        logger.info(f"Looking for CodeQL packs in workspace {workspace}")
        packs_in_workspace = bundle.codeql.pack_ls(workspace)
        logger.info(
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

        logger.info(
            f"Considering the following CodeQL packs for inclusion in the custom bundle: {','.join(map(lambda p: p.name, selected_packs))}"
        )
        missing_packs = set(packs) - {pack.name for pack in selected_packs}
        if len(missing_packs) > 0:
            logger.fatal(
                f"The provided CodeQL workspace doesn't contain the provided packs '{','.join(missing_packs)}'",
            )
            sys.exit(1)

        logger.info(
            f"Adding the packs {','.join(map(lambda p: p.name, selected_packs))} to the custom bundle."
        )
        bundle.add_packs(*selected_packs)
        logger.info(f"Bundling custom bundle at {output}")
        bundle.bundle(output)
        logger.info(f"Completed building of custom bundle.")
    except CodeQLException as e:
        logger.fatal(f"Failed executing CodeQL command with reason: '{e}'")
        sys.exit(1)
    except BundleException as e:
        logger.fatal(f"Failed to build custom bundle with reason: '{e}'")
        sys.exit(1)


if __name__ == "__main__":
    main()
