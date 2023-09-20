"""Python library for interacting with the Saturn Cloud API.

NOTE: This is an experimental library and will likely change in
the future
"""
import logging
import datetime as dt
from dataclasses import dataclass
from functools import reduce

from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin, urlencode

import requests
from requests.exceptions import HTTPError
from fsspec.generic import rsync

from saturnfs import SaturnFS
from saturnfs.cli.callback import FileOpCallback, file_op

from .settings import Settings


log = logging.getLogger("saturn-client")
if log.level == logging.NOTSET:
    logging.basicConfig()
    log.setLevel(logging.INFO)


def utcnow() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat()


class SaturnError(Exception):
    def __init__(self, message):
        super().__init__(message)


class SaturnHTTPError(SaturnError):
    def __init__(self, message, status_code):
        super().__init__(message)
        self.status_code = status_code


class ResourceType:
    """Enum for resource_type

    All resource models should return one of these as resource.resource_type.

    This currently does not match the resource types in Atlas, but it matches the API we want for users.

    We also avoid using the Enum module to minimize dependencies
    """

    DEPLOYMENT = "Deployment"
    JOB = "Job"
    WORKSPACE = "Workspace"

    @classmethod
    def possible_resource_types(cls) -> str:
        return {cls.DEPLOYMENT, cls.JOB, cls.WORKSPACE}

    @classmethod
    def get_url_name(cls, resource_type: str) -> str:
        """
        converts from the name of the resource type to the string we use in the urls. Currently
        this is just the lower case value + plural.
        """
        return cls.get_api_name(resource_type) + "s"

    @classmethod
    def get_api_name(cls, resource_type: str) -> str:
        """
        converts from the name of the resource type to the string we use in the API. Currently
        this is just the lower case value.
        """
        if resource_type not in cls.possible_resource_types():
            raise SaturnError(f"Invalid resource type {resource_type}")
        return resource_type.lower()

    @classmethod
    def lookup(cls, input: str):
        for resource_type in cls.possible_resource_types():
            if resource_type.lower() == input.lower():
                return resource_type
        raise SaturnError(f"resource type {input} not found")


@dataclass
class Resource:
    """
    Captures information about a resource that we care about in the client and CLI
    """

    owner: str
    name: str
    resource_type: str
    status: Optional[str]
    # string used to describe the instances that are being consumed by the resource
    instance_type: str
    instance_count: int
    id: str

    @classmethod
    def from_dict(cls, **kwargs: Union[str, int]):
        cls(
            owner=kwargs["owner"],
            name=kwargs["name"],
            resource_type=kwargs["resource_type"],
            status=kwargs.get("status"),
            instance_type=kwargs["instance_type"],
            instance_count=kwargs["instance_count"],
            id=kwargs["id"],
        )


@dataclass
class Pod:
    """
    Captures information about a pod that we care about in the client and CLI
    """

    name: str
    status: str
    source: str
    start_time: str
    end_time: Optional[str]

    @classmethod
    def from_dict(self, input_dict: Dict[str, Optional[str]]):
        Pod(
            name=input_dict["name"],
            status=input_dict["status"],
            source=input_dict["source"],
            start_time=input_dict["start_time"],
            end_time=input_dict["end_time"],
        )


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
    def orgs(self) -> List[Dict[str, Any]]:
        url = urljoin(self.url, "api/orgs")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise ValueError(response.reason)
        return response.json()['orgs']

    @property
    def primary_org(self) -> Dict[str, Any]:
        orgs = self.orgs
        primary_org = None
        for o in orgs:
            if o['is_primary']:
                primary_org = o
        if primary_org:
            return primary_org
        raise ValueError("primary organization not found")

    def upload_source(self, local_path: str, resource_name: str, saturn_resource_path: str) -> str:
        """
        This method uploads a local_path to some location in sfs, which in the future
        will be downloaded to saturn_resource_path
        """
        username = self.current_user['username']
        org_name = self.primary_org['name']
        sfs_path = f"{org_name}/{username}/{resource_name}{saturn_resource_path}"
        fs = SaturnFS()
        operation = file_op(True, False)
        callback = FileOpCallback(operation=operation)
        fs.put(
            local_path,
            sfs_path,
            recursive=True,
            callback=callback
        )
        return "sfs://" + sfs_path

    @property
    def current_user(self):
        url = urljoin(self.url, "api/user")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise ValueError(response.reason)
        return response.json()

    @property
    def url(self) -> str:
        """URL of Saturn instance"""
        return self.settings.url

    def _get_saturn_version(self) -> str:
        """Get version of Saturn"""
        url = urljoin(self.url, "api/status")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise SaturnHTTPError(response.reason, status_code=response.status_code)
        return response.json()["version"]

    @property
    def options(self) -> Dict[str, Any]:
        """Options for various settings"""
        if self._options is None:
            url = urljoin(self.url, "api/info/servers")
            response = requests.get(url, headers=self.settings.headers)
            if not response.ok:
                raise SaturnHTTPError(response.reason, status_code=response.status_code)
            self._options = response.json()
        return self._options

    def _get_recipes(
        self,
        resource_type: str,
        resource_name: Optional[str] = None,
        owner_name: str = None,
        max_count=10,
    ) -> List[Any]:
        next_last_key = None
        recipes = []
        if owner_name is None:
            owner_name = self.current_user["username"]
        resource_type = ResourceType.get_api_name(resource_type)
        while True:
            url = urljoin(self.url, f"api/recipes")
            qparams = {"type": resource_type, "owner_name": owner_name, "max_count": max_count}
            if resource_name:
                qparams["name"] = resource_name
            if next_last_key:
                qparams["last_key"] = next_last_key
            url = url + "?" + urlencode(qparams)
            response = requests.get(url, headers=self.settings.headers)
            if not response.ok:
                raise SaturnHTTPError(response.reason, status_code=response.status_code)
            data = response.json()
            recipes.extend(data["recipes"])
            next_last_key = data.get("next_last_key", None)
            if next_last_key is None:
                break
        return recipes

    def list_deployments(self, owner_name: str = None) -> List[Any]:
        if owner_name is None:
            owner_name = self.current_user["username"]
        return self._get_recipes(ResourceType.DEPLOYMENT, owner_name=owner_name)

    def list_jobs(self, owner_name: str = None) -> List[Any]:
        if owner_name is None:
            owner_name = self.current_user["username"]
        return self._get_recipes(ResourceType.JOB, owner_name=owner_name)

    def list_workspaces(self, owner_name: str = None) -> List[Any]:
        if owner_name is None:
            owner_name = self.current_user["username"]
        return self._get_recipes(ResourceType.WORKSPACE, owner_name=owner_name)

    def _get_resource_by_name(self, resource_type: str, resource_name: str, owner_name: str = None):
        """
        Currently - no way to query resource by name
        """
        recipes = self._get_recipes(
            resource_type, resource_name=resource_name, owner_name=owner_name
        )
        if len(recipes) == 0:
            raise SaturnError(f"Resource not found {owner_name}/{resource_name}")
        return recipes[0]

    def get_pods(
        self, resource_type: str, resource_name: str, owner_name: str = None
    ) -> List[Dict[str, Any]]:
        resource = self._get_resource_by_name(resource_type, resource_name, owner_name)
        return self._get_pods(resource_type, resource["state"]["id"])

    def _get_pods(self, resource_type: str, resource_id: str) -> List[Dict[str, Any]]:
        historical_pods = self._get_historical_pods(resource_type, resource_id)
        live_pods = self._get_live_pods(resource_type, resource_id)
        live_pod_names = set(x["pod_name"] for x in live_pods)
        historical_pods = [x for x in historical_pods if x["pod_name"] not in live_pod_names]
        return live_pods + historical_pods

    def _get_historical_pods(self, resource_type: str, resource_id: str) -> List[Dict[str, Any]]:
        api_name = ResourceType.get_url_name(resource_type)
        url = urljoin(self.url, f"api/{api_name}/{resource_id}/history")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise SaturnHTTPError(response.reason, status_code=response.status_code)
        result = response.json()["pods"]
        for p in result:
            p["source"] = "historical"
        result = sorted(result, key=lambda x: (x["start_time"], x["pod_name"]), reverse=True)
        return result

    def _get_live_pods(self, resource_type: str, resource_id: str) -> List[Dict[str, Any]]:
        api_name = ResourceType.get_url_name(resource_type)
        url = urljoin(self.url, f"api/{api_name}/{resource_id}/runtimesummary")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise SaturnHTTPError(response.reason, status_code=response.status_code)
        result = response.json()
        live_pods = []
        if "job_summaries" in result:
            pod_summaries = reduce(
                lambda x, y: x + y, [x.get("pod_summaries", []) for x in result["job_summaries"]]
            )
        else:
            pod_summaries = result.get("pod_summaries", [])
        for pod in pod_summaries:
            end_time = pod.get("completed_at", "")
            start_time = pod["started_at"]
            status = pod["status"]
            pod_name = pod["name"]
            last_seen = utcnow()
            row = dict(
                end_time=end_time,
                start_time=start_time,
                status=status,
                pod_name=pod_name,
                last_seen=last_seen,
            )
            if pod["labels"].get("job-name"):
                row["label_job_name"] = pod["labels"].get("job-name")
            live_pods.append(row)
        for p in live_pods:
            p["source"] = "live"
        live_pods = sorted(live_pods, key=lambda x: (x["start_time"], x["pod_name"]), reverse=True)
        return live_pods

    def _get_live_pod_logs(self, pod_name: str) -> str:
        url = urljoin(self.url, f"api/pod/namespace/main-namespace/name/{pod_name}/runtimesummary")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise SaturnHTTPError(response.reason, status_code=response.status_code)
        result = response.json()
        init_previous = [
            x["previous"]["logs"] for x in result["init_container_summaries"] if x["previous"]
        ]
        init_previous_str = "\n".join(init_previous) if init_previous else ""
        init_logs = [x["logs"] for x in result["init_container_summaries"] if x["logs"]]
        init_logs_str = "\n".join(init_logs) if init_logs else ""
        previous = [
            x["previous"]["logs"]
            for x in result["container_summaries"]
            if x["previous"] and x["name"] == "main"
        ]
        previous_str = "\n".join(previous) if previous else ""
        logs = [
            x["logs"] for x in result["container_summaries"] if x["logs"] and x["name"] == "main"
        ]
        logs_str = "\n".join(logs) if logs else ""
        return init_previous_str + init_logs_str + previous_str + logs_str

    def _get_historical_pod_logs(self, resource_type: str, resource_id: str, pod_name: str) -> str:
        api_name = ResourceType.get_url_name(resource_type)
        url = urljoin(self.url, f"api/{api_name}/{resource_id}/logs?pod_name={pod_name}")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise SaturnHTTPError(response.reason, status_code=response.status_code)
        result = response.json()
        return result["logs"]

    def get_pod_logs(
        self,
        resource_type: str,
        resource_name: str,
        pod_name: str,
        owner_name: Optional[str] = None,
    ):
        resource = self._get_resource_by_name(resource_type, resource_name, owner_name=owner_name)
        resource_id = resource["state"]["id"]
        live_pods = self._get_live_pods(resource_type, resource_id)
        found = False
        for pod in live_pods:
            if pod["pod_name"] == pod_name:
                found = True
        if found:
            logs = self._get_live_pod_logs(pod_name)
            return logs
        return self._get_historical_pod_logs(resource_type, resource_id, pod_name)

    def get_job_logs(
        self, resource_name: str, owner_name: str = None, rank=0
    ) -> Tuple[str, Dict[str, Any]]:
        """
        This method returns logs for the most recent invocation of the job.

        If you need to access previous runs - you should use get_pod_logs directly

        Returns tuple (logs, pod)
        """
        resource_type = ResourceType.JOB
        resource = self._get_resource_by_name(
            ResourceType.JOB, resource_name, owner_name=owner_name
        )
        resource_id = resource["state"]["id"]
        pods = self._get_pods(resource_type, resource_id)
        if len(pods) == 0:
            return ""
        most_recent_job_name = pods[0]["label_job_name"]
        job_pods = [x for x in pods if x["label_job_name"] == most_recent_job_name]
        found_pod: Optional[Dict[str, Any]] = None
        for pod in job_pods:
            _, rank_str, suffix = pod["pod_name"].rsplit("-", 2)
            rank_int = int(rank_str)
            if rank_int == rank:
                found_pod = pod
        if found_pod is None:
            raise ValueError(f"could not find job for {most_recent_job_name} with rank {rank}")
        if found_pod["source"] == "historical":
            logs = self._get_historical_pod_logs(resource_type, resource_id, found_pod["pod_name"])
        else:
            logs = self._get_live_pod_logs(found_pod["pod_name"])
        return logs, found_pod

    def get_deployment_logs(
        self, resource_name: str, owner_name: str = None, rank=0
    ) -> Tuple[str, Dict[str, Any]]:
        """
        This method will return all logs for a deployment. Deployment pods are un-ordered. In
        order to impose a rank, we sort pods by start time.
        """
        resource_type = ResourceType.DEPLOYMENT
        resource = self._get_resource_by_name(resource_type, resource_name, owner_name=owner_name)
        resource_id = resource["state"]["id"]
        pods = self._get_pods(resource_type, resource_id)
        if rank >= len(pods):
            raise SaturnError(f"could not find pod for {resource_name} with rank {rank}")
        found_pod = pods[rank]
        if found_pod["source"] == "historical":
            logs = self._get_historical_pod_logs(resource_type, resource_id, found_pod["pod_name"])
        else:
            logs = self._get_live_pod_logs(found_pod["pod_name"])
        return logs, found_pod

    def get_workspace_logs(
        self, resource_name: str, owner_name: str = None
    ) -> Tuple[str, Dict[str, Any]]:
        """
        This method will return all logs for a workspace
        """
        resource_type = ResourceType.WORKSPACE
        resource = self._get_resource_by_name(resource_type, resource_name, owner_name=owner_name)
        resource_id = resource["state"]["id"]
        pods = self._get_pods(resource_type, resource_id)
        if len(pods) == 0:
            return ""
        found_pod = pods[0]
        if found_pod["source"] == "historical":
            logs = self._get_historical_pod_logs(resource_type, resource_id, found_pod["pod_name"])
        else:
            logs = self._get_live_pod_logs(found_pod["pod_name"])
        return logs, found_pod

    def apply(self, recipe_dict: Dict[str, Any]) -> Dict[str, Any]:
        url = urljoin(self.url, "api/recipes")
        response = requests.put(url, headers=self.settings.headers, json=recipe_dict)
        if not response.ok:
            raise SaturnHTTPError(response.reason, status_code=response.status_code)
        result = response.json()
        return result

    def _start(self, resource_type: str, resource_id: str):
        url_name = ResourceType.get_url_name(resource_type)
        url = urljoin(self.url, f"api/{url_name}/{resource_id}/start")
        response = requests.post(url, headers=self.settings.headers)
        if not response.ok:
            raise SaturnHTTPError(response.reason, status_code=response.status_code)
        result = response.json()
        return result



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
