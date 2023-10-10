from typing import List, Optional
from ruamel.yaml import YAML
import click

from saturn_client.cli.utils import print_resource_table, print_pod_table
from saturn_client.core import ResourceStatus, SaturnConnection, ResourceType


@click.group()
def cli():
    pass


@cli.command()
@click.argument("resource_type", default=None, required=False)
@click.argument("resource_name", default=None, required=False)
@click.option(
    "--owner",
    default=None,
    required=False,
    help="Resource owner name. Defaults to current auth identity.",
)
@click.option(
    "-s",
    "--status",
    required=False,
    multiple=True,
    type=click.Choice(ResourceStatus.values()),
    help="Filter resource status by one or more value",
)
def resources(
    resource_type: Optional[str],
    resource_name: Optional[str] = None,
    owner: Optional[str] = None,
    status: Optional[List[str]] = None,
):
    """
    List resources belonging to an owner.

    \b
    RESOURCE_TYPE (optional):
        all: List resources of any type (Default)
        deployment, job, workspace: Filter for resources of a single type
    RESOURCE_NAME (optional):
        Filter results by prefix match on name
    """
    client = SaturnConnection()
    if resource_type == "all":
        resource_type = None
    resources = client.list_resources(
        resource_type, resource_name=resource_name, owner_name=owner, status=status
    )
    print_resource_table(resources)


@cli.command()
@click.argument("resource_type")
@click.argument("resource_name")
@click.option("--owner", default=None, required=False)
@click.option(
    "-s",
    "--status",
    required=False,
    multiple=True,
    type=click.Choice(ResourceStatus.values()),
    help="Filter pod status by one or more value",
)
def pods(
    resource_type: str, resource_name: str, owner: str = None, status: Optional[List[str]] = None
):
    """
    List active pods associated with a resource.

    \b
    RESOURCE_TYPE (required):
        deployment, job, workspace
    RESOURCE_NAME (required):
        Exact match on name
    """
    client = SaturnConnection()
    resource_type = ResourceType.lookup(resource_type)
    pods = client.get_pods(resource_type, resource_name, owner_name=owner, status=status)
    print_pod_table(pods)


@cli.command()
@click.argument("resource_type")
@click.argument("resource_name")
@click.argument("pod_name", default=None, required=False)
@click.option(
    "--owner",
    default=None,
    required=False,
    help="Resource owner name. Defaults to current auth identity.",
)
@click.option(
    "-a",
    "--all-containers",
    default=False,
    is_flag=True,
    help=(
        "Print logs for all containers. "
        "By default, only logs for the main container are shown (or init containers during setup)"
    ),
)
def logs(
    resource_type: str,
    resource_name: str,
    owner: str = None,
    pod_name: str = None,
    all_containers: bool = False,
):
    """
    Print resource logs. Defaults to the most recently created pod if POD_NAME is not given.

    \b
    RESOURCE_TYPE (required):
        deployment, job, workspace
    RESOURCE_NAME (required):
        Exact match on name
    POD_NAME (optional):
        Fetch logs for a specific pod on the resource. Defaults to the latest pod.
    """
    client = SaturnConnection()
    logs = client.get_logs(
        resource_type,
        resource_name,
        owner_name=owner,
        pod_name=pod_name,
        all_containers=all_containers,
    )
    click.echo(logs)


@cli.command()
@click.argument("input-file")
@click.option(
    "--start", is_flag=True, help="Start the resource after creating/updating it with the recipe"
)
def apply(input_file: str, start: bool = False):
    """
    Create or update the contents of a resource recipe.

    \b
    INPUT_FILE (required):
        Path to a YAML or JSON recipe file
    """
    with open(input_file, "r") as f:
        yaml = YAML()
        obj = yaml.load(f)
    client = SaturnConnection()
    result = client.apply(obj)
    if start:
        resource_type = ResourceType.lookup(result["type"])
        resource_id = result["state"]["id"]
        client._start(resource_type, resource_id)


if __name__ == "__main__":
    cli(max_content_width=100)
