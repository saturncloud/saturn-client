import sys
from typing import List, Optional
from ruamel.yaml import YAML
import click

from saturn_client.cli.utils import OutputFormat, print_pod_table, print_resources
from saturn_client.core import ResourceStatus, SaturnConnection, ResourceType, SaturnHTTPError


@click.group()
def cli():
    pass


@cli.command()
@click.argument("_type", metavar="TYPE", required=True)
@click.argument("name", default=None, required=False)
@click.option(
    "--owner",
    default=None,
    help="Resource owner name. Defaults to current auth identity.",
)
@click.option(
    "-s",
    "--status",
    multiple=True,
    type=click.Choice(ResourceStatus.values()),
    help="Filter resource status by one or more value",
)
@click.option(
    "-o",
    "--output",
    default="table",
    type=click.Choice(OutputFormat.values(), case_sensitive=False),
    help="Output format. Defaults to table."
)
def list(
    _type: Optional[str],
    name: Optional[str] = None,
    owner: Optional[str] = None,
    status: Optional[List[str]] = None,
    output: str = OutputFormat.TABLE,
):
    """
    List objects in saturn.

    \b
    TYPE (required):
        resource: List any type of resource
        deployment, job, workspace: Filter for resources of a single type
    NAME (optional):
        Filter results by prefix match on name
    """
    client = SaturnConnection()
    if _type in {"resource", "resources"}:
        _type = None
    resources = client.list_resources(
        _type, resource_name=name, owner_name=owner, status=status
    )
    print_resources(resources, output=output)


@cli.command()
@click.argument("_type", metavar="TYPE", required=True)
@click.argument("name", required=True)
@click.option(
    "--owner",
    default=None,
    help="Resource owner name. Defaults to current auth identity.",
)
@click.option(
    "-o",
    "--output",
    default=OutputFormat.TABLE,
    type=click.Choice(OutputFormat.values(), case_sensitive=False),
    help="Output format. Defaults to table."
)
def get(_type: str, name: str, owner: Optional[str] = None, output: str = OutputFormat.TABLE):
    client = SaturnConnection()
    resource = client.get_resource(_type, name, owner_name=owner)
    print_resources(resource, output=output)


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
    help="Print logs for all containers.",
)
def logs(
    resource_type: str,
    resource_name: str,
    owner: str = None,
    pod_name: str = None,
    all_containers: bool = False,
):
    """
    Print logs for a given resource.
    By default, only logs for the main container are shown (or init containers during setup)

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
        client.start(resource_type, resource_id)


@cli.command()
@click.argument("resource_type")
@click.argument("resource_name")
@click.option(
    "--owner",
    default=None,
    required=False,
    help="Resource owner name. Defaults to current auth identity.",
)
@click.option(
    "--debug",
    is_flag=True,
    help=(
        "Enable debug mode on the resource. "
        "This enables SSH, and prevents the container from exiting "
        "if the main process fails."
    ),
)
def start(resource_type: str, resource_name: str, owner: Optional[str] = None, debug: bool = False):
    """
    Start a resource

    \b
    INPUT_FILE (required):
        Path to a YAML or JSON recipe file
    """
    client = SaturnConnection()
    resource = client.get_resource(resource_type, resource_name, owner_name=owner)
    resource_id = resource["state"]["id"]
    client.start(resource_type, resource_id, debug_mode=debug)


@cli.command()
@click.argument("resource_type")
@click.argument("resource_name")
@click.option(
    "--owner",
    default=None,
    required=False,
    help="Resource owner name. Defaults to current auth identity.",
)
def stop(resource_type: str, resource_name: str, owner_name: Optional[str] = None):
    client = SaturnConnection()
    resource = client.get_resource(resource_type, resource_name, owner_name=owner_name)
    resource_id = resource["state"]["id"]
    client.stop(resource_type, resource_id)


@cli.command()
@click.argument("resource_type")
@click.argument("resource_name")
@click.option(
    "--owner",
    default=None,
    required=False,
    help="Resource owner name. Defaults to current auth identity.",
)
@click.option(
    "--debug",
    is_flag=True,
    help=(
        "Enable debug mode on the resource. "
        "This enables SSH, and prevents the container from exiting "
        "if the main process fails."
    ),
)
def restart(resource_type: str, resource_name: str, owner: Optional[str] = None, debug: bool = False):
    client = SaturnConnection()
    resource = client.get_resource(resource_type, resource_name, owner_name=owner)
    resource_id = resource["state"]["id"]
    client.restart(resource_type, resource_id, debug_mode=debug)

@cli.command()
@click.argument("job_name")
@click.argument("cron_schedule", required=False, default=None)
@click.option(
    "--owner",
    default=None,
    required=False,
    help="Resource owner name. Defaults to current auth identity.",
)
@click.option(
    "--disable",
    is_flag=True,
    help="Disable scheduling for the job"
)
def schedule(job_name: str, cron_schedule: Optional[str] = None, owner: Optional[str] = None, disable: bool = False):
    """
    Enable or disable scheduling for a job. If CRON_SCHEDULE is not given, then it
    is assumed that the job already has a cron schedule set.

    \b
    JOB_NAME (required):
        Exact match on name
    CRON_SCHEDULE (optional):
        Update the job's schedule before enabling. May be provided in cron format, or using special @ tags.
            Predefined: @hourly, @daily, @midnight, @weekly, @monthly, @yearly
            Interval: @every <duration> (e.g. "@every 4h")
    """
    client = SaturnConnection()
    resource = client.get_resource(ResourceType.JOB, job_name, owner_name=owner)
    job_id = resource["state"]["id"]
    client.schedule(job_id, cron_schedule=cron_schedule, disable=disable)
    print_resource_op(
        "Unscheduled" if disable else "Scheduled", ResourceType.JOB, job_name, owner
    )


if __name__ == "__main__":
    try:
        cli(max_content_width=100)
    except SaturnHTTPError as e:
        click.echo("Error: " + str(e))
        sys.exit(1)
