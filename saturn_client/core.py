"""Python library for interacting with the Saturn Cloud API.

NOTE: This is an experimental library and will likely change in
the future
"""
from json import JSONDecodeError
import logging
import datetime as dt
from dataclasses import dataclass
from functools import reduce
import requests
from typing import Any, Dict, Iterable, List, Optional, Set, Union
from urllib.parse import urljoin, urlencode

from saturn_client.settings import Settings
from saturn_client.logs import format_historical_logs, format_logs, is_live


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

    @classmethod
    def from_response(cls, response: requests.Response):
        try:
            error = response.json()
        except JSONDecodeError:
            error = response.reason
        return cls(error, response.status_code)


class ResourceType:
    """Enum for resource_type

    All resource models should return one of these as resource.resource_type.
    """

    DEPLOYMENT = "deployment"
    JOB = "job"
    WORKSPACE = "workspace"

    @classmethod
    def values(cls) -> List[str]:
        return [cls.DEPLOYMENT, cls.JOB, cls.WORKSPACE]

    @classmethod
    def get_url_name(cls, resource_type: str) -> str:
        """
        converts from the name of the resource type to the string we use in the urls. Currently
        this is just the lower case value + plural.
        """
        return cls.lookup(resource_type) + "s"

    @classmethod
    def lookup(cls, input: str):
        types = set(cls.values())
        resource_type = input.lower()
        if resource_type in types:
            return resource_type
        if resource_type.endswith("s"):
            # Check if input was pluralized
            resource_type = resource_type[:-1]
            if resource_type in types:
                return resource_type
        raise SaturnError(f'resource type "{input}" not found')


class ResourceStatus:
    """
    Enum for resource statuses
    """

    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"

    @classmethod
    def values(cls) -> List[str]:
        return [cls.PENDING, cls.RUNNING, cls.STOPPING, cls.STOPPED, cls.ERROR]


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
            raise SaturnHTTPError.from_response(response)
        return response.json()["version"]

    @property
    def options(self) -> Dict[str, Any]:
        """Options for various settings"""
        if self._options is None:
            url = urljoin(self.url, "api/info/servers")
            response = requests.get(url, headers=self.settings.headers)
            if not response.ok:
                raise SaturnHTTPError.from_response(response)
            self._options = response.json()
        return self._options

    def list_resources(
        self,
        resource_type: Optional[str] = None,
        resource_name: Optional[str] = None,
        owner_name: Optional[str] = None,
        status: Optional[Union[str, Iterable[str]]] = None,
    ) -> List[Dict[str, Any]]:
        next_last_key = None
        recipes = []
        qparams = {}
        if resource_type is not None:
            resource_type = ResourceType.lookup(resource_type)
            qparams["type"] = resource_type
        if owner_name:
            qparams["owner_name"] = owner_name
        if resource_name:
            qparams["name"] = resource_name
        base_url = urljoin(self.url, "api/recipes")
        while True:
            url = base_url + "?" + urlencode(qparams)
            response = requests.get(url, headers=self.settings.headers)
            if not response.ok:
                raise SaturnHTTPError.from_response(response)
            data = response.json()
            recipes.extend(data["recipes"])
            next_last_key = data.get("next_last_key", None)
            if next_last_key is None:
                break
            qparams["last_key"] = next_last_key

        if status:
            if isinstance(status, str):
                status = {status}
            elif not isinstance(status, set):
                status = set(status)
            recipes = [r for r in recipes if r.get("state", {}).get("status") in status]
        return recipes

    def list_deployments(self, owner_name: str = None, **kwargs) -> List[Dict[str, Any]]:
        return self.list_resources(ResourceType.DEPLOYMENT, owner_name=owner_name, **kwargs)

    def list_jobs(self, owner_name: str = None, **kwargs) -> List[Dict[str, Any]]:
        return self.list_resources(ResourceType.JOB, owner_name=owner_name, **kwargs)

    def list_workspaces(self, owner_name: str = None, **kwargs) -> List[Dict[str, Any]]:
        return self.list_resources(ResourceType.WORKSPACE, owner_name=owner_name, **kwargs)

    def _get_recipe_by_name(
        self, resource_type: str, resource_name: str, owner_name: str = None
    ) -> Dict[str, Any]:
        resource_type = ResourceType.lookup(resource_type)
        url = urljoin(self.url, f"api/recipes/{resource_type}/{resource_name}")
        qparams = {}
        if owner_name:
            qparams["owner_name"] = owner_name
        url = url + "?" + urlencode(qparams)

        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise SaturnHTTPError.from_response(response)
        return response.json()

    def get_logs(
        self,
        resource_type: str,
        resource_name: str,
        owner_name: Optional[str] = None,
        pod_name: Optional[str] = None,
        resource_id: Optional[str] = None,
        all_containers: bool = False,
    ) -> str:
        resource_type = ResourceType.lookup(resource_type)
        if not resource_id:
            resource = self._get_recipe_by_name(resource_type, resource_name, owner_name=owner_name)
            resource_id = resource["state"]["id"]

        if pod_name:
            pod_summary = self._get_pod_runtime_summary(pod_name, resource_id=resource_id)
            if is_live(pod_summary):
                return format_logs(pod_summary, all_containers=all_containers)
            return self._get_historical_pod_logs(resource_type, resource_id, pod_name)

        # Search for latest pod
        pods = self._get_live_pods(resource_type, resource_id)
        if len(pods) > 0:
            pod_name = pods[0]["pod_name"]
            return self.get_logs(
                resource_type,
                resource_name,
                owner_name=owner_name,
                pod_name=pod_name,
                resource_id=resource_id,
            )

        historical_pods = self._get_historical_pods(resource_type, resource_id)
        if len(historical_pods) > 0:
            pod_name = historical_pods[0]["pod_name"]
            return self._get_historical_pod_logs(resource_type, resource_id, pod_name)
        return ""

    def _get_live_pod_logs(
        self, pod_name: str, resource_id: Optional[str] = None, all_containers: bool = False
    ) -> str:
        pod_summary = self._get_pod_runtime_summary(pod_name, resource_id=resource_id)
        return format_logs(pod_summary, all_containers=all_containers)

    def _get_historical_pod_logs(self, resource_type: str, resource_id: str, pod_name: str) -> str:
        api_name = ResourceType.get_url_name(resource_type)
        url = urljoin(self.url, f"api/{api_name}/{resource_id}/logs?pod_name={pod_name}")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise SaturnHTTPError.from_response(response)
        result = response.json()
        return format_historical_logs(pod_name, result["logs"])

    def get_pods(
        self,
        resource_type: str,
        resource_name: str,
        owner_name: Optional[str] = None,
        status: Optional[Union[str, Iterable[str]]] = None,
    ) -> List[Dict[str, Any]]:
        resource = self._get_recipe_by_name(resource_type, resource_name, owner_name)
        return self._get_all_pods(resource_type, resource["state"]["id"], status=status)

    def _get_all_pods(
        self,
        resource_type: str,
        resource_id: str,
        status: Optional[Union[str, Iterable[str]]] = None,
    ) -> List[Dict[str, Any]]:
        historical_pods = self._get_historical_pods(resource_type, resource_id)
        live_pods = self._get_live_pods(resource_type, resource_id)
        live_pod_names = set(x["pod_name"] for x in live_pods)
        historical_pods = [x for x in historical_pods if x["pod_name"] not in live_pod_names]
        pods = live_pods + historical_pods

        if status:
            if isinstance(status, str):
                status = {status}
            elif not isinstance(status, set):
                status = set(status)
            pods = [p for p in pods if p.get("status") in status]
        return pods

    def _get_historical_pods(self, resource_type: str, resource_id: str) -> List[Dict[str, Any]]:
        api_name = ResourceType.get_url_name(resource_type)
        url = urljoin(self.url, f"api/{api_name}/{resource_id}/history")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise SaturnHTTPError.from_response(response)
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
            raise SaturnHTTPError.from_response(response)
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

    def _get_pod_runtime_summary(
        self, pod_name: str, resource_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        url = urljoin(self.url, f"api/pod/namespace/main-namespace/name/{pod_name}/runtimesummary")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            if response.status_code == 404:
                return None
            raise SaturnHTTPError.from_response(response)
        pod_summary = response.json()

        pod_resource_id = pod_summary.get("labels", {}).get("saturncloud.io/resource-id")
        if resource_id and pod_resource_id != resource_id:
            # Validate the pod is for the correct resource
            raise ValueError(f"Unable to find pod '{pod_name}' matching this resource")
        return pod_summary

    def apply(self, recipe_dict: Dict[str, Any]) -> Dict[str, Any]:
        url = urljoin(self.url, "api/recipes")
        response = requests.put(url, headers=self.settings.headers, json=recipe_dict)
        if not response.ok:
            raise SaturnHTTPError.from_response(response)
        result = response.json()
        return result

    def _start(self, resource_type: str, resource_id: str):
        url_name = ResourceType.get_url_name(resource_type)
        url = urljoin(self.url, f"api/{url_name}/{resource_id}/start")
        response = requests.post(url, headers=self.settings.headers)
        if not response.ok:
            raise SaturnHTTPError.from_response(response)
        result = response.json()
        return result
