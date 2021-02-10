"""Python library for interacting with the Saturn Cloud API.

NOTE: This is an experimental library and will likely change in
the future
"""
import json
import logging

from time import sleep
from datetime import datetime
from typing import Any, Dict, List, Optional
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
    def options(self) -> Dict[str, Any]:
        """Options for various settings"""
        if self._options is None:
            url = urljoin(self.url, "api/info/servers")
            response = requests.get(url, headers=self.settings.headers)
            if not response.ok:
                raise ValueError(response.reason)
            self._options = response.json()
        return self._options

    def list_projects(self) -> List[Dict[str, Any]]:
        """List all projects that you have access to."""
        url = urljoin(self.url, "api/projects")
        response = requests.get(url, headers=self.settings.headers)
        try:
            response.raise_for_status()
        except HTTPError as err:
            raise HTTPError(response.status_code, response.json()["message"]) from err
        return response.json()["projects"]

    def get_project(self, project_id: str) -> Dict[str, Any]:
        """Get project by id"""
        url = urljoin(self.url, f"api/projects/{project_id}")
        response = requests.get(url, headers=self.settings.headers)
        try:
            response.raise_for_status()
        except HTTPError as err:
            raise http_error(response, project_id) from err
        return response.json()

    def delete_project(self, project_id: str) -> str:
        """Delete project by id"""
        url = urljoin(self.url, f"api/projects/{project_id}")
        response = requests.delete(url, headers=self.settings.headers)
        try:
            response.raise_for_status()
        except HTTPError as err:
            raise http_error(response, project_id) from err

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
        :param jupyter_size: Size for the jupyter associated with the project.
            The options for these are available from ``conn.options["sizes"]``.
        :param jupyter_disk_space: Disk space for the jupyter associated with the project.
            The options for these are available from ``conn.options["disk_space"]``.
        :param jupyter_auto_shutoff: Auto shutoff interval for the jupyter associated with the
            project. The options for these are available from ``conn.options["auto_shutoff"]``.
        :param jupyter_start_ssh: Whether to start ssh for the jupyter associated with the project.
            This is used for accessing the workspace from outside of Saturn.
        """

        if environment_variables:
            environment_variables = json.dumps(
                {k.upper(): v for k, v in environment_variables.items()}
            )

        self._validate_workspace_settings(
            size=jupyter_size,
            disk_space=jupyter_disk_space,
            auto_shutoff=jupyter_auto_shutoff,
            start_ssh=jupyter_start_ssh,
        )

        project_config = {
            "name": name,
            "description": description,
            "image": image_uri,
            "start_script": start_script,
            "environment_variables": environment_variables,
            "working_dir": working_dir,
            "jupyter_size": jupyter_size,
            "jupyter_disk_space": jupyter_disk_space,
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
        return response.json()

    def update_project(
        self,
        project_id: str,
        description: Optional[str] = None,
        image_uri: Optional[str] = None,
        start_script: Optional[str] = None,
        environment_variables: Optional[Dict] = None,
        working_dir: Optional[str] = None,
        jupyter_size: Optional[str] = None,
        jupyter_disk_space: Optional[str] = None,
        jupyter_auto_shutoff: Optional[str] = None,
        jupyter_start_ssh: Optional[bool] = None,
        update_jupyter_server: Optional[bool] = True,
    ) -> Dict[str, Any]:
        """
        Create a project from scratch

        :param project_id: ID of project. This is the only field that is required.
        :param description: Short description of the project (less than 250 characters).
        :param image_uri: Location of the image. Example:
            485185227295.dkr.ecr.us-east-1.amazonaws.com/saturn-dask:2020.12.01.21.10.
            If this does not include a registry URL, Saturn will assume the image is
            publicly-available on Docker Hub.
        :param start_script: Script that runs on start up. Examples: "pip install dask".
            This can be any valid code that can be run with ``sh``, and can be multiple lines.
        :param environment_variables: Env vars expressed as a dict. The names will be
            coerced to uppercase.
        :param working_dir: Location to use as working directory. Example: /home/jovyan/project
        :param jupyter_size: Size for the jupyter associated with the project.
            The options for these are available from ``conn.options["sizes"]``.
        :param jupyter_disk_space: Disk space for the jupyter associated with the project.
            The options for these are available from ``conn.options["disk_space"]``.
        :param jupyter_auto_shutoff: Auto shutoff interval for the jupyter associated with the
            project. The options for these are available from ``conn.options["auto_shutoff"]``.
        :param jupyter_start_ssh: Whether to start ssh for the jupyter associated with the project.
            This is used for accessing the workspace from outside of Saturn.
        :param update_jupyter_server: Whether to update the jupyter server associated with the
            project. This will stop the jupyter server if it is running.
        """

        if environment_variables:
            environment_variables = json.dumps(
                {k.upper(): v for k, v in environment_variables.items()}
            )

        self._validate_workspace_settings(
            size=jupyter_size,
            disk_space=jupyter_disk_space,
            auto_shutoff=jupyter_auto_shutoff,
            start_ssh=jupyter_start_ssh,
        )

        project_config = {
            "description": description,
            "image": image_uri,
            "start_script": start_script,
            "environment_variables": environment_variables,
            "working_dir": working_dir,
            "jupyter_size": jupyter_size,
            "jupyter_disk_space": jupyter_disk_space,
            "jupyter_auto_shutoff": jupyter_auto_shutoff,
            "jupyter_start_ssh": jupyter_start_ssh,
        }
        # only send kwargs that are explicitly set by user
        project_config = {k: v for k, v in project_config.items() if v is not None}

        project_url = urljoin(self.url, f"api/projects/{project_id}")
        response = requests.patch(
            project_url,
            data=json.dumps(project_config),
            headers=self.settings.headers,
        )
        try:
            response.raise_for_status()
        except HTTPError as err:
            raise http_error(response, project_id) from err
        project = response.json()

        if not (project["jupyter_server_id"] and update_jupyter_server):
            return project

        jupyter_config = {
            "image": image_uri,
            "start_script": start_script,
            "environment_variables": environment_variables,
            "working_dir": working_dir,
            "size": jupyter_size,
            "disk_space": jupyter_disk_space,
            "auto_shutoff": jupyter_auto_shutoff,
            "start_ssh": jupyter_start_ssh,
        }
        # only send kwargs that are explicitly set by user
        jupyter_config = {k: v for k, v in jupyter_config.items() if v is not None}
        if len(jupyter_config) == 0:
            return project

        self.stop_jupyter_server(project["jupyter_server_id"])
        jupyter_url = urljoin(self.url, f"api/jupyter_servers/{project['jupyter_server_id']}")
        response = requests.patch(
            jupyter_url,
            data=json.dumps(jupyter_config),
            headers=self.settings.headers,
        )
        try:
            response.raise_for_status()
        except HTTPError as err:
            raise HTTPError(response.status_code, response.json()["message"]) from err
        return project

    def get_jupyter_server(self, jupyter_server_id) -> Dict[str, Any]:
        """Get a particular jupyter server"""
        url = urljoin(self.url, f"api/jupyter_servers/{jupyter_server_id}")
        response = requests.get(
            url,
            headers=self.settings.headers,
        )
        try:
            response.raise_for_status()
        except HTTPError as err:
            raise http_error(response, jupyter_server_id) from err
        return response.json()

    def wait_for_jupyter_server(self, jupyter_server_id: str, timeout: int = 360) -> None:
        """Wait for jupyter server to be running

        :param jupyter_server_id: ID of the jupyter_server to wait for.
        :param timeout: Maximum time in seconds to wait. Default is 360 (6 minutes).
        """
        target_status = "running"
        sleep_interval = 5
        start_time = datetime.utcnow()
        time_passed = 0

        log.info(f"Waiting for Jupyter to be {target_status}...")
        while time_passed < timeout:
            status = self.get_jupyter_server(jupyter_server_id)["status"]
            if status == target_status:
                log.info(f"Jupyter server is {status}")
                break
            if status == "error":
                raise AssertionError(
                    f"Jupyter server has status: {status}. See logs in Saturn User Interface."
                )
            sleep(sleep_interval)
            time_passed = (datetime.utcnow() - start_time).total_seconds()
            log.info(
                f"Checking jupyter status: {status} "
                f"(seconds passed: {time_passed:.0f}/{timeout})"
            )

    def stop_jupyter_server(self, jupyter_server_id: str) -> None:
        """Stop a particular jupyter server.

        This method will return as soon as the stop process has been triggered. It'll take
        longer for the jupyter server to shut off, but you can check the status using
        ``get_jupyter_server``
        """
        url = urljoin(self.url, f"api/jupyter_servers/{jupyter_server_id}/stop")
        response = requests.post(
            url,
            data=json.dumps({}),
            headers=self.settings.headers,
        )
        try:
            response.raise_for_status()
        except HTTPError as err:
            raise http_error(response, jupyter_server_id) from err

    def start_jupyter_server(self, jupyter_server_id: str) -> None:
        """Start a particular jupyter server.

        This method will return as soon as the start process has been triggered. It'll take
        longer for the jupyter server to be up, but you can check the status using
        ``get_jupyter_server``
        """
        url = urljoin(self.url, f"api/jupyter_servers/{jupyter_server_id}/start")
        response = requests.post(
            url,
            data=json.dumps({}),
            headers=self.settings.headers,
        )
        try:
            response.raise_for_status()
        except HTTPError as err:
            raise http_error(response, jupyter_server_id) from err

    def stop_dask_cluster(self, dask_cluster_id: str) -> None:
        """Stop a particular dask cluster.

        This method will return as soon as the stop process has been triggered. It'll take
        longer for the dask cluster to actually shut down.
        """
        url = urljoin(self.url, f"api/dask_clusters/{dask_cluster_id}/close")
        response = requests.post(
            url,
            data=json.dumps({}),
            headers=self.settings.headers,
        )
        try:
            response.raise_for_status()
        except HTTPError as err:
            raise http_error(response, dask_cluster_id) from err

    def start_dask_cluster(self, dask_cluster_id: str) -> None:
        """Start a particular dask cluster.

        This method will return as soon as the start process has been triggered.
        It'll take longer for the  dask cluster to be up. This is primarily
        useful when the dask cluster has been stopped as a side-effect of
        stopping a jupyter server or updating a project. For more fine-grain
        control over the dask cluster see dask-saturn.
        """
        url = urljoin(self.url, f"api/dask_clusters/{dask_cluster_id}/start")
        response = requests.post(
            url,
            data=json.dumps({}),
            headers=self.settings.headers,
        )
        try:
            response.raise_for_status()
        except HTTPError as err:
            raise http_error(response, dask_cluster_id) from err

    def _validate_workspace_settings(
        self,
        size: Optional[str] = None,
        disk_space: Optional[str] = None,
        auto_shutoff: Optional[str] = None,
        start_ssh: Optional[bool] = None,
    ):
        """Validate the options provided"""
        errors = []
        if size is not None:
            options = list(self.options["sizes"].keys())
            if size not in options:
                errors.append(
                    f"Proposed size: {size} is not a valid option. " f"Options are: {options}."
                )
        if disk_space is not None:
            options = self.options["disk_space"]
            if disk_space not in options:
                errors.append(
                    f"Proposed disk_space: {disk_space} is not a valid option. "
                    f"Options are: {options}."
                )
        if auto_shutoff is not None:
            options = self.options["auto_shutoff"]
            if auto_shutoff not in options:
                errors.append(
                    f"Proposed auto_shutoff: {auto_shutoff} is not a valid option. "
                    f"Options are: {options}."
                )
        if start_ssh is not None and not isinstance(start_ssh, bool):
            errors.append("start_ssh must be set to a boolean if defined.")
        if len(errors) > 0:
            raise ValueError(" ".join(errors))


def _maybe_name(_id):
    """Return message if len of id does not match expectation (32)"""
    if len(_id) == 32:
        return ""
    return "Maybe you used name rather than id?"


def http_error(response: requests.Response, resource_id: str):
    response_message = response.json().get("message", "")
    return HTTPError(
        response.status_code, f"{response_message} {_maybe_name(resource_id)}"
    )
