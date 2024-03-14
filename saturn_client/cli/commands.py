import json
import logging
logging.basicConfig(level=logging.DEBUG)

import sys
from os.path import join
from typing import List, Optional
from ruamel.yaml import YAML
import click

from saturn_client.cli.utils import (
    OutputFormat,
    print_pod_table,
    print_resources,
    print_resource_op,
)
from saturn_client.core import (
    DataSource,
    ResourceStatus,
    SaturnConnection,
    ResourceType,
    SaturnHTTPError,
)

logging.getLogger('fsspec.generic').setLevel(logging.DEBUG)
logging.getLogger('fsspec').setLevel(logging.DEBUG)
logging.getLogger('fsspec.local').setLevel(logging.DEBUG)
logging.getLogger('saturnfs.client.saturnfs').setLevel(logging.DEBUG)

@click.group()
def cli():
    pass


@cli.command("list")
@click.argument("_type", metavar="TYPE", required=True)
@click.argument("name", default=None, required=False)
@click.option(
    "--owner",
    default=None,
    help="Resource owner name. Defaults to current auth identity.",
)
@click.option("--as-template", is_flag=True, help="Retrieve recipe as a template for cloning.")
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
    help="Output format. Defaults to table.",
)
def list_cli(
    _type: Optional[str],
    name: Optional[str] = None,
    owner: Optional[str] = None,
    as_template: bool = False,
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
        _type, resource_name=name, owner_name=owner, status=status, as_template=as_template
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
@click.option("--as-template", is_flag=True, help="Retrieve recipe as a template for cloning.")
@click.option(
    "-o",
    "--output",
    default=OutputFormat.YAML,
    type=click.Choice(OutputFormat.values(), case_sensitive=False),
    help="Output format. Defaults to yaml.",
)
def get(
    _type: str,
    name: str,
    owner: Optional[str] = None,
    as_template: bool = False,
    output: str = OutputFormat.TABLE,
):
    """
    Get a recipe for an object in saturn.

    \b
    TYPE (required):
        deployment, job, workspace
    NAME (required):
        Exact match on name
    """
    client = SaturnConnection()
    resource = client.get_resource(_type, name, owner_name=owner, as_template=as_template)
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
    type=click.Choice(ResourceStatus.values(), case_sensitive=False),
    help="Filter pod status by one or more value",
)
@click.option(
    "--source",
    type=click.Choice(DataSource.values(), case_sensitive=False),
    help="Filter for pods from either live or historical data.",
)
def pods(
    resource_type: str,
    resource_name: str,
    owner: str = None,
    status: Optional[List[str]] = None,
    source: Optional[str] = None,
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
    pods = client.get_pods(
        resource_type, resource_name, owner_name=owner, status=status, source=source
    )
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
    "--source",
    type=click.Choice(DataSource.values(), case_sensitive=False),
    help=(
        "Select a single log source, either live or historical. "
        "By default, live logs will be retrieved if available."
    ),
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
    source: Optional[str] = None,
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
        source=source,
        all_containers=all_containers,
    )
    click.echo(logs)


@cli.command()
@click.argument("input-file")
@click.option(
    "--start", is_flag=True, help="Start the resource after creating/updating it with the recipe"
)
@click.option("--sync", multiple=True, default=[])
def apply(input_file: str, start: bool = False, sync: List[str] = []):
    """
    Create or update the contents of a resource recipe.

    \b
    INPUT_FILE (required):
        Path to a YAML or JSON recipe file
    """
    recipe = None
    with open(input_file, "r") as f:
        yaml = YAML()
        recipe = yaml.load(f)
    if isinstance(recipe["spec"].get("command", None), list):
        recipe["spec"]["command"] = json.dumps(recipe["spec"]["command"])
    client = SaturnConnection()
    settings = client.settings
    working_directory = recipe["spec"].get("working_directory", settings.WORKING_DIRECTORY)
    resource_name = recipe["spec"].get("name")
    commands = []
    START_STRING = "### BEGIN SATURN_CLIENT GENERATED CODE"
    END_STRING = "### BEGIN SATURN_CLIENT GENERATED CODE"
    for s in sync:
        if ":" in s:
            source, dest = s.split(":")
        else:
            source = dest = s
        if not dest.startswith("/"):
            dest = join(working_directory, dest)
        sfs_path = client.upload_source(source, resource_name, dest)
        click.echo(f"syncing {source} to {sfs_path}")
        cmd = f"saturnfs cp --recursive {sfs_path} {dest}"
        commands.append(cmd)
    start_script = recipe["spec"].get("start_script", "")
    starting_index = start_script.find(START_STRING)
    ending_index = start_script.find(END_STRING)
    if starting_index >= 0 and ending_index >= 0:
        stop = ending_index + len(END_STRING) + 1
        start_script = start_script[:starting_index] + start_script[stop:]
    to_inject = [START_STRING] + commands + [END_STRING]
    start_script = "\n".join(to_inject) + start_script
    recipe["spec"]["start_script"] = start_script
    result = client.apply(recipe)
    resource_type = ResourceType.lookup(result["type"])
    resource_name = result["spec"]["name"]
    owner_name = result["spec"]["owner"]

    print_resource_op("Applied", resource_type, resource_name, owner_name, "from recipe")
    if start:
        resource_type = ResourceType.lookup(result["type"])
        resource_id = result["state"]["id"]
        client.start(resource_type, resource_id)
        print_resource_op("Started", resource_type, resource_name, owner_name)


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
    RESOURCE_TYPE (required):
        deployment, job, workspace
    RESOURCE_NAME (required):
        Exact match on name
    """
    client = SaturnConnection()
    resource = client.get_resource(resource_type, resource_name, owner_name=owner)
    resource_id = resource["state"]["id"]
    client.start(resource_type, resource_id, debug_mode=debug)
    print_resource_op("Started", resource_type, resource_name, owner)


@cli.command()
@click.argument("resource_type")
@click.argument("resource_name")
@click.option(
    "--owner",
    default=None,
    required=False,
    help="Resource owner name. Defaults to current auth identity.",
)
def delete(resource_type: str, resource_name: str, owner: Optional[str] = None, debug: bool = False):
    """
    Start a resource

    \b
    RESOURCE_TYPE (required):
        deployment, job, workspace
    RESOURCE_NAME (required):
        Exact match on name
    """
    client = SaturnConnection()
    resource = client.get_resource(resource_type, resource_name, owner_name=owner)
    resource_id = resource["state"]["id"]
    client.delete(resource_type, resource_id)
    print_resource_op("Deleted", resource_type, resource_name, owner)


@cli.command()
@click.argument("resource_type")
@click.argument("resource_name")
@click.option(
    "--owner",
    default=None,
    required=False,
    help="Resource owner name. Defaults to current auth identity.",
)
def stop(resource_type: str, resource_name: str, owner: Optional[str] = None):
    """
    Stop a resource

    \b
    RESOURCE_TYPE (required):
        deployment, job, workspace
    RESOURCE_NAME (required):
        Exact match on name
    """
    client = SaturnConnection()
    resource = client.get_resource(resource_type, resource_name, owner_name=owner)
    resource_id = resource["state"]["id"]
    client.stop(resource_type, resource_id)
    print_resource_op("Stopped", resource_type, resource_name, owner)


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
def restart(
    resource_type: str, resource_name: str, owner: Optional[str] = None, debug: bool = False
):
    """
    Restart a resource

    \b
    RESOURCE_TYPE (required):
        deployment, job, workspace
    RESOURCE_NAME (required):
        Exact match on name
    """
    client = SaturnConnection()
    resource = client.get_resource(resource_type, resource_name, owner_name=owner)
    resource_id = resource["state"]["id"]
    client.restart(resource_type, resource_id, debug_mode=debug)
    print_resource_op(
        "Restarted", resource_type, resource_name, owner, "in debug mode" if debug else None
    )


@cli.command()
@click.argument("job_name")
@click.argument("cron_schedule", required=False, default=None)
@click.option(
    "--owner",
    default=None,
    required=False,
    help="Resource owner name. Defaults to current auth identity.",
)
@click.option("--disable", is_flag=True, help="Disable scheduling for the job")
def schedule(
    job_name: str,
    cron_schedule: Optional[str] = None,
    owner: Optional[str] = None,
    disable: bool = False,
):
    """
    Enable or disable scheduling for a job. If CRON_SCHEDULE is not given, then it
    is assumed that the job already has a cron schedule set.

    \b
    JOB_NAME (required):
        Exact match on name
    CRON_SCHEDULE (optional):
        Update the job's schedule before enabling.
        May be provided in cron format, or using special @ tags.
            Predefined: @hourly, @daily, @midnight, @weekly, @monthly, @yearly
            Interval: @every <duration> (e.g. "@every 4h")
    """
    client = SaturnConnection()
    resource = client.get_resource(ResourceType.JOB, job_name, owner_name=owner)
    job_id = resource["state"]["id"]
    client.schedule(job_id, cron_schedule=cron_schedule, disable=disable)
    print_resource_op("Unscheduled" if disable else "Scheduled", ResourceType.JOB, job_name, owner)


def entrypoint():
    try:
        cli(max_content_width=100)
    except SaturnHTTPError as e:
        click.echo("Error: " + str(e))
        sys.exit(1)


if __name__ == "__main__":
    entrypoint()
