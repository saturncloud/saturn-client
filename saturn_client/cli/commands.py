from typing import Optional

from ruamel.yaml import YAML
import click

from saturn_client.cli.utils import print_resource_table, print_pod_table
from saturn_client.core import SaturnConnection, ResourceType, SaturnHTTPError


@click.group()
def cli():
    pass


@cli.command()
@click.argument("resource_type")
@click.option("--owner", default=None, required=False)
def list(resource_type: str, owner: str = None):
    client = SaturnConnection()
    resource_type = ResourceType.lookup(resource_type)
    resources = []
    if resource_type == ResourceType.WORKSPACE:
        resources = client.list_workspaces()
    if resource_type == ResourceType.JOB:
        resources = client.list_jobs()
    if resource_type == ResourceType.DEPLOYMENT:
        resources = client.list_deployments()
    print_resource_table(resources)


@cli.command()
@click.argument("resource_type")
@click.argument("name")
@click.option("--owner", default=None, required=False)
def pods(resource_type: str, name: str, owner: str = None):
    client = SaturnConnection()
    resource_type = ResourceType.lookup(resource_type)
    pods = client.get_pods(resource_type, name, owner)
    print_pod_table(pods)


@cli.command()
@click.argument("resource_type")
@click.argument("name")
@click.argument("pod_name")
@click.option("--owner", default=None, required=False)
def pod_logs(resource_type: str, name: str, pod_name: str, owner: str = None):
    client = SaturnConnection()
    resource_type = ResourceType.lookup(resource_type)
    logs = client.get_pod_logs(resource_type, name, pod_name, owner_name=owner)
    click.echo(logs)


@cli.command()
@click.argument("resource_type")
@click.argument("name")
@click.option("--rank", default=0, required=False)
@click.option("--owner", default=None, required=False)
def logs(resource_type: str, name: str, rank: Optional[int] = 0, owner: str = None):
    client = SaturnConnection()
    resource_type = ResourceType.lookup(resource_type)
    if resource_type == ResourceType.WORKSPACE:
        logs, pod = client.get_workspace_logs(name, owner)
    elif resource_type == ResourceType.JOB:
        logs, pod = client.get_job_logs(name, owner, rank=rank)
    else:
        logs, pod = client.get_deployment_logs(name, owner, rank=rank)

    click.echo(f"logs for {pod['pod_name']}")
    click.echo("-" * 100)
    click.echo(logs)


@cli.command()
@click.argument("input-file")
@click.option("--start", is_flag=True)
@click.option("--sync", multiple=True, default=[])
def apply(input_file: str, start: bool = False, sync: List[str] = []):
    with open(input_file, "r") as f:
        yaml = YAML()
        obj = yaml.load(f)

    client = SaturnConnection()
    settings = client.settings
    working_directory = recipe['spec'].get('working_directory', settings.WORKING_DIRECTORY)
    commands = []
    START_STRING = "### BEGIN SATURN_CLIENT GENERATED CODE"
    END_STRING = "### BEGIN SATURN_CLIENT GENERATED CODE"
    for s in sync:
        source, dest = s.split(':')
        if not dest.startswith("/"):
            dest = join(working_directory, dest)
        sfs_path = client.upload_source(source, dest)
        cmd = f"saturnfs cp --recursive {sfs_path} {dest}"
        commands.append(cmd)
    start_script = obj['spec'].get('start_script', "")
    starting_index = start_script.find(START_STRING)
    ending_index = start_script.find(END_STRING)
    if starting_index >= 0 and ending_index >= 0:
        start_script = start_script[:starting_index] + \
                       start_script[ending_index + len(END_STRING) + 1:]
    to_inject = [START_STRING] + commands + [END_STRING]
    start_script = "\n".join(to_inject) + start_script
    obj['spec']['start_script'] = start_script
    result = client.apply(obj)
    if start:
        resource_type = ResourceType.lookup(result['type'])
        resource_id = result['state']['id']
        client._start(resource_type, resource_id)




if __name__ == "__main__":
    cli()
