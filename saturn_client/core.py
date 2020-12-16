import os
import json
import logging

from sys import stdout
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from .settings import Settings
from .validators import validate_name, validate_description
from ._version import get_versions

__version__ = get_versions()["version"]


logfmt = "[%(asctime)s] %(levelname)s - %(name)s | %(message)s"
datefmt = "%Y-%m-%d %H:%M:%S"

log = logging.getLogger("saturn-client")
log.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter(logfmt, datefmt))
log.addHandler(handler)


class SaturnConnection():
    """
    Create a ``SaturnConnection`` to interact with the API.

    :param url: URL for the SaturnCloud instance.
    :param api_token: API token for authenticating the request to Saturn API.
        Get from `/api/user/token`
    """
    _options = None
    _saturn_api_version = None

    # pylint: disable=unused-argument,super-init-not-called
    def __init__(
        self,
        url: Optional[str]=None,
        api_token: Optional[str]=None,
    ):
        self.settings = Settings(url, api_token)

    @property
    def url(self):
        return self.settings.url

    @property
    def saturn_api_version(self):
        if self._saturn_api_version:
            return self._saturn_api_version
        else:
            url = urljoin(self.url, "api/status")
            response = requests.get(url, headers=self.settings.headers)
            if not response.ok:
                raise ValueError(response.reason)
            self._saturn_api_version = response.json()["version"]

    @property
    def options(self):
        if self._options:
            return self._options
        else:
            url = urljoin(self.url, "api/info/servers")
            response = requests.get(url, headers=self.settings.headers)
            if not response.ok:
                raise ValueError(response.reason)
            self._options = response.json()

    @property
    def describe_sizes(self) -> Dict[str, str]:
        """Returns available instance sizes"""
        return self.options["sizes"]

    def create_project(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        image_uri: Optional[str] = None,
        start_script: Optional[str] = None,
        environment_variables: Optional[Dict] = None,
        working_dir: Optional[str] = None,
        workspace_settings: Optional[Dict] = None,
    ):
        """
        Create a project from scratch

        :param name: Name of project.
        :param description: Short description of the project (less than 250 characters).
        :param image_uri: Location of the image. Example: 485185227295.dkr.ecr.us-east-1.amazonaws.com/saturn-dask:2020.12.01.21.10
        :param start_script: Script that runs on start up.
        :param environment_variables: Env vars expressed as a dict. The names will be coerced to uppercase.
        :param working_dir: Location to use as working directory. Example: /home/jovyan/project
        :param workspace_settings: Setting for the jupyter associated with the project. These include:
            `size`, `disk_space`, `auto_shutoff`, and `start_ssh`. The options for these are available from
            `conn.options`.
        """
        url = urljoin(self.url, "api/projects")

        errors = [
            *validate_name(name),
            *validate_description(description)
        ]

        if environment_variables:
            environment_variables = "\n".join(f"{k.upper()}={v}" for k, v in environment_variables.items())

        workspace_kwargs = {}
        if workspace_settings:
            for k, v in workspace_settings.items():
                workspace_keys = ["size", "auto_shutoff", "disk_space", "start_ssh"]
                if k in workspace_keys:
                    if k == "start_ssh":
                        if not isinstance(v, bool):
                            errors.append(f"{k} must be set to a boolean if defined.")
                    elif v not in self.options[k]:
                        options = self.options.get(k) or self.options.get(f"{k}s")
                        raise errors.append(f"Proposed {k}: {v} is not a valid option. Options are {options}.")
                    workspace_kwargs[f"jupyter_{k}"] = v
                else:
                    raise errors.append(f"{k} is not a valid workspace_setting. Supported workspace_settings are {workspace_keys}.")

        if len(errors) > 0:
            raise ValueError(errors)

        project_config = {
            "name": name,
            "description": description,
            "image": image_uri,
            "start_script": start_script,
            "environment_variables": environment_variables,
            **workspace_kwargs,
        }
        # only send kwargs that are explicitly set by user
        project_config = {k: v for k, v in project_config.items() if v is not None}

        response = requests.post(url, data=json.dumps({**project_config, "saturn_client_version": _version__}), headers=self.settings.headers)
        if not response.ok:
            raise ValueError(response.reason)
        return response.json()
