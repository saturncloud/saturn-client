"""Python library for interacting with the Saturn Cloud API.

NOTE: This is an experimental library and will likely change in
the future
"""
import json
import logging

from collections import UserDict
from typing import Dict, Optional
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
    def options(self) -> UserDict:
        """Options for various settings"""
        if self._options is None:
            url = urljoin(self.url, "api/info/servers")
            response = requests.get(url, headers=self.settings.headers)
            if not response.ok:
                raise ValueError(response.reason)
            self._options = response.json()
        return UserDict(self._options)

    def create_project(
        self,
        name: str,
        description: Optional[str] = None,
        image_uri: Optional[str] = None,
        start_script: Optional[str] = None,
        environment_variables: Optional[Dict] = None,
        working_dir: Optional[str] = None,
        jupyter_size: Optional[str] = None,
        jupyter_disk_space: Optional[str] = None,
        jupyter_auto_shutoff: Optional[str] = None,
        jupyter_start_ssh: Optional[bool] = None,
    ) -> UserDict:
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
        :param jupyter_size: Size for the jupyter associated with the project.
            The options for these are available from ``conn.options.sizes``.
        :param jupyter_disk_space: Disk space for the jupyter associated with the project.
            The options for these are available from ``conn.options.disk_space``.
        :param jupyter_auto_shutoff: Auto shutoff interval for the jupyter associated with the
            project. The options for these are available from ``conn.options.auto_shutoff.``.
        :param jupyter_start_ssh: Whether to start ssh for the jupyter associated with the project.
            This is used for accessing the workspace from outside of Saturn.
        """

        if environment_variables:
            environment_variables = json.dumps(
                {k.upper(): v for k, v in environment_variables.items()}
            )

        project_config = {
            "name": name,
            "description": description,
            "image": image_uri,
            "start_script": start_script,
            "environment_variables": environment_variables,
            "working_dir": working_dir,
            "jupyter_size": jupyter_size,
            "jupyter_disc_space": jupyter_disk_space,
            "jupyter_auto_shutoff": jupyter_auto_shutoff,
            "jupyter_start_ssh": jupyter_start_ssh,
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
        return UserDict(response.json())
