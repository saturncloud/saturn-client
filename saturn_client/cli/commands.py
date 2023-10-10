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
@click.argument("pod_name", default=None, required=False)
@click.option("--owner", default=None, required=False)
def logs(resource_type: str, name: str, owner: str = None, pod_name: str = None):
    client = SaturnConnection()
    logs = client.get_logs(resource_type, name, owner_name=owner, pod_name=pod_name)
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
    cli(max_content_width=100)
