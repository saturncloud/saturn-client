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
        resources = client.list_workspaces(owner)
    if resource_type == ResourceType.JOB:
        resources = client.list_jobs(owner)
    if resource_type == ResourceType.DEPLOYMENT:
        resources = client.list_deployments(owner)
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
def apply(input_file: str, start: bool = False):
    with open(input_file, "r") as f:
        yaml = YAML()
        obj = yaml.load(f)
    client = SaturnConnection()
    result = client.apply(obj)
    if start:
        resource_type = ResourceType.lookup(result['type'])
        resource_id = result['state']['id']
        client._start(resource_type, resource_id)




if __name__ == "__main__":
    cli()
