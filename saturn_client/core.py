"""Python library for interacting with the Saturn Cloud API.

NOTE: This is an experimental library and will likely change in
the future
"""
import logging

from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlencode

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

    def _get_workspaces(self) -> List[Any]:
        url = urljoin(self.url, "api/workspaces")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise ValueError(response.reason)
        result = response.json()["workspaces"]
        return result

    def _get_deployments(self) -> List[Any]:
        url = urljoin(self.url, "api/deployments")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise ValueError(response.reason)
        result = response.json()["deployments"]
        return result

    def _get_jobs(self) -> List[Any]:
        url = urljoin(self.url, "api/jobs")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise ValueError(response.reason)
        result = response.json()["jobs"]
        return result

    def _get_recipes(self, ids: List[str]) -> List[Any]:
        url = urljoin(self.url, "api/recipes")
        url += "?" + urlencode({'id': ",".join(ids)})
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise ValueError(response.reason)
        return response.json()['recipes']

    def list_deployments(self) -> List[Any]:
        resources = self._get_deployments()
        ids = [x['id'] for x in resources]
        return self._get_recipes(ids)

    def list_jobs(self) -> List[Any]:
        resources = self._get_jobs()
        ids = [x['id'] for x in resources]
        return self._get_recipes(ids)

    def list_workspaces(self) -> List[Any]:
        resources = self._get_workspaces()
        ids = [x['id'] for x in resources]
        return self._get_recipes(ids)


def _maybe_name(_id):
    """Return message if len of id does not match expectation (32)"""
    if len(_id) == 32:
        return ""
    return "Maybe you used name rather than id?"


def _http_error(response: requests.Response, resource_id: str):
    """Return HTTPError from response for a resource"""
    response_message = response.json().get(
        "message", "saturn-client encountered an unexpected error."
    )
    return HTTPError(response.status_code, f"{response_message} {_maybe_name(resource_id)}")


if __name__ == "__main__":
    client = SaturnConnection()
    print(client.list_workspaces())