"""Python library for interacting with the Saturn Cloud API.

NOTE: This is an experimental library and will likely change in
the future
"""
from dataclasses import dataclass
import json
import logging

from typing import Any, Dict, Optional
from urllib.parse import urljoin

import requests
from requests.exceptions import HTTPError
from .settings import Settings


log = logging.getLogger("saturn-client")
if log.level == logging.NOTSET:
    logging.basicConfig()
    log.setLevel(logging.INFO)


class SaturnConnection:
    """
    Create a ``SaturnConnection`` to interact with the API.

    :param url: URL for the SaturnCloud instance.
    :param api_token: API token for authenticating the request to Saturn API.
        Get from `/api/user/token`
    """

    _options = None

    def __init__(
        self,
        url: Optional[str] = None,
        api_token: Optional[str] = None,
    ):
        """
        Create a ``SaturnConnection`` to interact with the API.

        :param url: URL for the SaturnCloud instance.
            Example: "https://app.community.saturnenterprise.io"
        :param api_token: API token for authenticating the request to Saturn API.
            Get from ``/api/user/token``
        """
        self.settings = Settings(url, api_token)

        # test connection to raise errors early
        self._saturn_version = self._get_saturn_version()

    @property
    def url(self) -> str:
        """URL of Saturn instance"""
        return self.settings.url

    def _get_saturn_version(self) -> str:
        """Get version of Saturn"""
        url = urljoin(self.url, "api/status")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise ValueError(response.reason)
        return response.json()["version"]

    @property
    def options(self) -> Dict[str, str]:
        """Options for various settings"""
        if self._options is None:
            url = urljoin(self.url, "api/info/servers")
            response = requests.get(url, headers=self.settings.headers)
            if not response.ok:
                raise ValueError(response.reason)
            self._options = response.json()
        return self._options

    @property
    def describe_sizes(self) -> Dict[str, str]:
        """Instance size options"""
        return self.options["sizes"]

    def create_project(
        self,
        name: str = None,
        description: Optional[str] = None,
        image_uri: Optional[str] = None,
        start_script: Optional[str] = None,
        environment_variables: Optional[Dict] = None,
        working_dir: Optional[str] = None,
        workspace_settings: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        Create a project from scratch

        :param name: Name of project. This is the only field that is required.
        :param description: Short description of the project (less than 250 characters).
        :param image_uri: Location of the image. Example:
            485185227295.dkr.ecr.us-east-1.amazonaws.com/saturn-dask:2020.12.01.21.10
        :param start_script: Script that runs on start up. Examples: "pip install dask"
        :param environment_variables: Env vars expressed as a dict. The names will be
            coerced to uppercase.
        :param working_dir: Location to use as working directory. Example: /home/jovyan/project
        :param workspace_settings: Setting for the jupyter associated with the project.
            These include: ``size``, ``disk_space``, ``auto_shutoff``, and ``start_ssh``.
            The options for these are available from ``conn.options``.
        """

        if environment_variables:
            environment_variables = json.dumps(
                {k.upper(): v for k, v in environment_variables.items()}
            )
        try:
            ws = WorkspaceSettings(**workspace_settings if workspace_settings else {})
        except TypeError as err:
            raise KeyError(
                "The only valid workspace_setting options are: "
                "size, disk_space, auto_shutoff, and start_ssh"
            ) from err
        ws.validate(self.options)
        workspace_kwargs = ws.project_settings

        project_config = {
            "name": name,
            "description": description,
            "image": image_uri,
            "start_script": start_script,
            "environment_variables": environment_variables,
            "working_dir": working_dir,
            **workspace_kwargs,
        }
        # only send kwargs that are explicitly set by user
        project_config = {k: v for k, v in project_config.items() if v is not None}

        url = urljoin(self.url, "api/projects")
        response = requests.post(
            url,
            data=json.dumps(project_config),
            headers=self.settings.headers,
        )
        try:
            response.raise_for_status()
        except HTTPError as err:
            raise HTTPError(response.status_code, response.json()["message"]) from err
        return response.json()


@dataclass
class WorkspaceSettings:
    """Encapsulates workspace settings validation and coercion"""

    size: str = None
    disk_space: str = None
    auto_shutoff: str = None
    start_ssh: bool = None

    def validate(self, all_options: Dict):
        """Validate the options provided"""
        errors = []
        if self.size is not None:
            options = list(all_options["sizes"].keys())
            if self.size not in options:
                errors.append(
                    f"Proposed size: {self.size} is not a valid option. Options are: {options}."
                )
        for attr in ["disk_space", "auto_shutoff"]:
            options = all_options[attr]
            value = getattr(self, attr, None)
            if value is not None and value not in options:
                errors.append(
                    f"Proposed {attr}: {value} is not a valid option. Options are: {options}."
                )
        if self.start_ssh is not None and not isinstance(self.start_ssh, bool):
            errors.append("start_ssh must be set to a boolean if defined.")
        if len(errors) > 0:
            raise ValueError(" ".join(errors))

    @property
    def project_settings(self) -> Dict[str, Any]:
        """Project workspace settings"""
        output = {
            "jupyter_size": self.size,
            "jupyter_disk_space": self.disk_space,
            "jupyter_auto_shutoff": self.auto_shutoff,
            "jupyter_start_ssh": self.start_ssh,
        }
        return {k: v for k, v in output.items() if v is not None}
