import json
from enum import Enum
import sys
from typing import Any, Dict, List, Optional, Union

import click
from ruamel.yaml import YAML


class OutputFormat(str, Enum):
    TABLE = "table"
    JSON = "json"
    YAML = "yaml"

    def __str__(self) -> str:
        return str(self.value)

    @classmethod
    def values(cls) -> List[str]:
        return [str(value) for value in list(cls)]  # type: ignore

    @classmethod
    def validate(cls, format: str):
        if format not in cls.values():
            raise ValueError(f'Unknown output format "{format}"')


def print_json(data: Union[List, Dict]):
    click.echo(json.dumps(data, indent=2))


def print_resources(resource: Union[List, Dict], output: str = OutputFormat.TABLE):
    output = output.lower()
    OutputFormat.validate(output)
    if output == OutputFormat.TABLE:
        if not isinstance(resource, list):
            resource = [resource]
        print_resource_table(resource)
    elif output == OutputFormat.YAML:
        yaml = YAML()
        if isinstance(resource, list):
            yaml.dump_all(resource, sys.stdout)
        else:
            yaml.dump(resource, sys.stdout)
    else:
        print_json(resource)


def print_resource_table(
    results: List[Dict[str, Any]],
):
    headers = ["owner", "name", "resource_type", "status", "instance_type", "instance_count", "id"]
    data: List[List[str]] = []
    for recipe in results:
        spec = recipe["spec"]
        state = recipe["state"]
        id = state["id"]
        owner = spec["owner"]
        name = spec["name"]
        resource_type = recipe["type"]
        status = state["status"]
        instance_type = spec["instance_type"]
        instance_count = spec.get("instance_count", 1)
        data.append([owner, name, resource_type, status, instance_type, instance_count, id])
    tabulate(data, headers)


def print_pod_table(
    results: List[Dict[str, Any]],
):
    headers = ["pod_name", "status", "source", "start_time", "end_time"]
    data: List[List[str]] = []
    for pod in results:
        pod_name = pod["pod_name"]
        status = pod["status"]
        source = pod["source"]
        start_time = pod["start_time"] or ""
        end_time = pod["end_time"] or ""
        data.append([pod_name, status, source, start_time, end_time])
    tabulate(data, headers)


def tabulate(
    data: List[List[Any]],
    headers: List[str],
    rpadding: int = 4,
    justify: Optional[Dict[str, str]] = None,
    minwidth: Optional[Dict[str, int]] = None,
):
    justify = justify if justify else {}
    minwidth = minwidth if minwidth else {}
    widths: List[int] = [0] * len(headers)
    for i, header in enumerate(headers):
        widths[i] = max(len(header), minwidth.get(header, 0))

    for row in data:
        for i, value in enumerate(row):
            if value is None:
                row[i] = value = ""
            widths[i] = max(widths[i], len(str(value)))

    header_format_str = ""
    format_str = ""
    for i, width in enumerate(widths):
        justify_char = justify.get(headers[i], "<")
        format_str += f"{{:{justify_char}{width}}}"
        header_format_str += f"{{:<{width}}}"
        if i < len(widths) - 1:
            format_str += " " * rpadding
            header_format_str += " " * rpadding

    click.echo(header_format_str.format(*headers))
    click.echo("-" * (sum(widths) + rpadding * (len(headers) - 1)))
    for row in data:
        click.echo(format_str.format(*row))


def print_resource_op(
    operation: str,
    resource_type: str,
    resource_name: Optional[str] = None,
    owner_name: Optional[str] = None,
    *args: str
):
    parts = [
        operation,
        resource_type,
        resource_name,
    ]
    if owner_name:
        parts.append(f"for {owner_name}")
    parts.extend(args)
    click.echo(" ".join([p for p in parts if p]))
