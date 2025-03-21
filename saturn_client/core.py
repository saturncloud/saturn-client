"""Python library for interacting with the Saturn Cloud API.

NOTE: This is an experimental library and will likely change in
the future
"""

from fnmatch import fnmatch
from json import JSONDecodeError
import logging
import datetime as dt
from dataclasses import dataclass, asdict
from functools import reduce
from os.path import join
from tempfile import TemporaryDirectory
import weakref

import requests
from typing import Any, Dict, Iterable, List, Optional, Union, Generator
from urllib.parse import urljoin, urlencode

from requests import Session

from saturn_client.logs import format_historical_logs, format_logs, is_live

from .settings import Settings
from .tar_utils import create_tar_archive

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
    def lookup(cls, value: str):
        types = set(cls.values())
        resource_type = value.lower()
        if resource_type in types:
            return resource_type
        if resource_type.endswith("s"):
            # Check if value was pluralized
            resource_type = resource_type[:-1]
            if resource_type in types:
                return resource_type
        raise SaturnError(f'resource type "{value}" not found')


class ServerOptionTypes:
    AUTO_SHUTOFF = "auto_shutoff"
    DISK_SPACE = "disk_space"
    SIZES = "sizes"

    @classmethod
    def values(cls) -> List[str]:
        return [cls.AUTO_SHUTOFF, cls.DISK_SPACE, cls.SIZES]


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


class DataSource:
    """
    Enum for data source used for retrieving pods and logs
    """

    LIVE = "live"
    HISTORICAL = "historical"

    @classmethod
    def values(cls) -> List[str]:
        return [cls.LIVE, cls.HISTORICAL]

    @classmethod
    def lookup(cls, value: str) -> str:
        sources = set(cls.values())
        source = value.lower()
        if source in sources:
            return source
        raise SaturnError(f'Pod source "{value}" not found')


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
    start_time: Optional[str]
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


@dataclass
class UsageLimitPatch:
    name: str
    is_default: bool

    # Resource limits
    instance_sizes: Optional[List[str]] = None
    resource_types: Optional[List[str]] = None
    num_instances: Optional[int] = None
    auto_shutoff: Optional[int] = None

    # Storage limits
    storage_in_gb: Optional[int] = None
    num_shared_folders: Optional[int] = None
    object_storage_bytes: Optional[int] = None
    object_storage_count: Optional[int] = None

    # Aggregate usage limits
    hours_per_day: Optional[int] = None
    hours_per_month: Optional[int] = None
    hours_forever: Optional[int] = None


@dataclass
class _UsageLimitCreate:
    org_id: str
    name: str


@dataclass
class _UsageLimit:
    id: str
    created_at: dt.datetime


@dataclass
class UsageLimitCreate(UsageLimitPatch, _UsageLimitCreate):
    pass


@dataclass
class UsageLimit(UsageLimitPatch, _UsageLimitCreate, _UsageLimit):
    pass


def execute_request(
    session: Session,
    base_url: str,
    path: str,
    method: str,
    json: Optional[dict] = None,
    parse_response=True,
) -> Dict[str, Any]:
    """
    returns the JSON response as a dict.
    """
    if session is None:
        session = Session()
    headers = {}
    if not base_url.endswith("/"):
        base_url += "/"
    if path.startswith("/"):
        path = path[1:]
    url = f"{base_url}{path}"
    kwargs = dict(headers=headers)
    if json:
        kwargs["json"] = json

    result = getattr(session, method.lower())(url, **kwargs)
    if result.status_code == 404:
        raise SaturnHTTPError(f"{url}:404")
    if parse_response:
        result = result.json()
    return result


def paginate(
    session: Session,
    base_url: str,
    field: str,
    path: str,
    method: str,
) -> Generator[List[Dict[str, Any]], None, None]:
    start = execute_request(session, base_url, path, method)
    yield start[field]
    if "links" in start:
        pagination_field = "links"
    else:
        pagination_field = "pagination"
    next_url = start[pagination_field].get("next")
    while True:
        if not next_url:
            break
        new_data = execute_request(session, base_url, next_url, method)
        next_url = new_data[pagination_field].get("next")
        yield new_data[field]


def make_path(path: str, query_dict: dict) -> str:
    if query_dict:
        return path + "?" + urlencode(query_dict)
    return path


class SaturnConnection:
    """
    Create a ``SaturnConnection`` to interact with the API.

    When used with an expiring api_token and refresh_token pair tokens will automatically
    be refreshed when an expired token response is detected, and on success the response
    will be retried. Keep in mind that a refresh token may only be used once, and if re-use
    is detected your API token will be invalidated.

    :param url: URL for the SaturnCloud instance.
    :param api_token: API token for authenticating the request to Saturn API.
    :param refresh_token: API refresh token to re-authenticate an expired api_token.
    """

    _options = None

    def __init__(
        self,
        url: Optional[str] = None,
        api_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
    ):
        """
        Create a ``SaturnConnection`` to interact with the API.

        :param url: URL for the SaturnCloud instance.
            Example: "https://app.community.saturnenterprise.io"
        :param api_token: API token for authenticating the request to Saturn API.
        """
        self.settings = Settings(url, api_token, refresh_token)
        self.session = SaturnSession(self.settings)
        weakref.finalize(self, self.close)

        # test connection to raise errors early
        self._saturn_version = self._get_saturn_version()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.session.close()

    def create_usage_limit(self, limit: UsageLimitCreate) -> UsageLimit:
        path = "/api/limits"
        obj = asdict(limit)
        result = execute_request(
            self.session, self.settings.BASE_URL, path, method="POST", json=obj
        )
        return UsageLimit(**result)

    def patch_usage_limit(self, limits_id: str, limit: UsageLimitPatch) -> UsageLimit:
        path = f"/api/limits/{limits_id}"
        obj = asdict(limit)
        result = execute_request(
            self.session, self.settings.BASE_URL, path, method="PATCH", json=obj
        )
        return UsageLimit(**result)

    def delete_usage_limit(self, limits_id: str) -> None:
        path = f"/api/limits/{limits_id}"
        execute_request(
            self.session, self.settings.BASE_URL, path, method="DELETE", parse_response=False
        )

    def get_owners(
        self,
        org_id: str,
        all_users: bool = False,
        all_groups: bool = False,
        users_only: bool = False,
        groups_only: bool = False,
        details: bool = False,
    ) -> List:
        path = f"/api/orgs/{org_id}/owners"
        params = {
            "all_users": str(all_users),
            "all_groups": str(all_groups),
            "users_only": str(users_only),
            "groups_only": str(groups_only),
            "details": str(details),
        }
        route = make_path(path, params)
        owners = []
        for page in paginate(
            self.session,
            self.settings.BASE_URL,
            "owners",
            route,
            "GET",
        ):
            owners.extend(page)
        return owners

    def get_org_usage(
        self, org_id: str, start: Union[dt.datetime, str], end: Union[dt.datetime, str]
    ) -> List:
        path = f"/api/orgs/{org_id}/usage/daily"
        if isinstance(start, dt.datetime):
            start = start.isoformat()
        if isinstance(end, dt.datetime):
            end = end.isoformat()
        params = {"start": start, "end": end}
        route = make_path(path, params)
        return execute_request(self.session, self.settings.BASE_URL, route, method="GET")["usage"]

    def get_user_usage(
        self,
        org_id: str,
        user_id: str,
        start: Union[dt.datetime, str],
        end: Union[dt.datetime, str],
    ) -> List:
        path = f"/api/orgs/{org_id}/members/{user_id}/usage/daily"
        if isinstance(start, dt.datetime):
            start = start.isoformat()
        if isinstance(end, dt.datetime):
            end = end.isoformat()
        params = {"start": start, "end": end}
        route = make_path(path, params)
        return execute_request(self.session, self.settings.BASE_URL, route, method="GET")["usage"]

    def get_user(self, user_id: str) -> Dict:
        path = f"/api/users/{user_id}"
        return execute_request(self.session, self.settings.BASE_URL, path, method="GET")

    def create_user(self, username: str, email: str, send_reset_email: bool = True) -> Dict:
        body = {
            "username": username,
            "email": email,
            "send_reset_email": send_reset_email,
        }
        path = "/api/users"
        return execute_request(self.session, self.settings.BASE_URL, path, method="POST", json=body)

    def create_service_account(self, name: str, cloud_role: str, auto_associate=False) -> Dict:
        body = {
            "name": name,
            "cloud_role": cloud_role,
            "auto_associate": auto_associate,
        }
        path = "/api/service_accounts"
        return execute_request(
            self.session, self.settings.BASE_URL, path, method="POST", json=body
        )["service_account"]

    def associate_service_account(
        self, service_account_id: str, identity_type: str, identity_id: str
    ) -> Dict:
        path = f"/api/service_accounts/{service_account_id}/associate/{identity_type}/{identity_id}"
        return execute_request(self.session, self.settings.BASE_URL, path, method="PUT")

    def get_all_users(self, org_id: Optional[str] = None, details: bool = False) -> List[str]:
        params = {"page_size": "1", "details": details}
        if org_id:
            params["org_id"] = org_id
        route = make_path("/api/users", params)
        users: List[Dict] = []
        for page in paginate(
            self.session,
            self.settings.BASE_URL,
            "users",
            route,
            "GET",
        ):
            users.extend(page)
        return users

    def create_shared_folder(
        self,
        name: str,
        access: str,
        is_external: bool = False,
        access_mode: str = "ReadWriteMany",
        owner_name=None,
    ) -> Dict:
        if owner_name is None:
            owner_name = f"{self.primary_org['name']}/{self.current_user['username']}"
        url = urljoin(self.url, "api/shared_folders")
        response = self.session.post(
            url,
            json={
                "owner_name": owner_name,
                "name": name,
                "access": access,
                "access_mode": access_mode,
                "is_external": is_external,
                "disk_space": "100Gi",
            },
        )
        return response.json()

    def delete_shared_folder(self, shared_folder_id: str) -> Dict:
        url = urljoin(self.url, f"api/shared_folders/{shared_folder_id}")
        response = self.session.delete(url)
        return response.json()

    def get_shared_folder(self, shared_folder_id: str) -> Dict:
        url = urljoin(self.url, f"api/shared_folders/{shared_folder_id}")
        response = self.session.get(url)
        return response.json()

    def set_preferred_org(self, user_id: str, org_id: str) -> Dict:
        url = urljoin(self.url, "api/user/preferences")
        response = self.session.post(url, json={"user_id": user_id, "default_org_id": org_id})
        return response.json()

    def stop_all_resources_in_org(self, org_id: str) -> None:
        owner_names = [x['name'] for x in self.get_owners(org_id=org_id, all_users=True, all_groups=True)]
        for owner_name in owner_names:
            resources = self.list_resources(owner_name=owner_name)
            for resource in resources:
                if resource['state']['status'] != 'stopped':
                    self.stop(resource['type'], resource['state']['id'])

    def get_size(self, size: str) -> Dict:
        sizes = self.list_options(ServerOptionTypes.SIZES)
        pruned = [x for x in sizes if x["name"] == size]
        return pruned[0]

    def list_options(self, option_type: str, glob: Optional[str] = None) -> List:
        if option_type not in ServerOptionTypes.values():
            raise ValueError(
                f"unknown option {option_type}. must be one of {ServerOptionTypes.values()}"
            )
        url = urljoin(self.url, "api/info/servers")
        response = self.session.get(url)
        results = response.json()[option_type]
        if option_type != ServerOptionTypes.SIZES:
            if glob:
                results = [x for x in results if fnmatch(x, glob)]
        else:
            results = results.values()
            if glob:
                results = [x for x in results if fnmatch(x["name"], glob)]
            results = sorted(results, key=lambda x: (x["gpu"], x["cores"]))
        return results

    @property
    def orgs(self) -> List[Dict[str, Any]]:
        url = urljoin(self.url, "api/orgs")
        response = self.session.get(url)
        return response.json()["orgs"]

    @property
    def primary_org(self) -> Dict[str, Any]:
        orgs = self.orgs
        primary_org = None
        for o in orgs:
            if o["is_primary"]:
                primary_org = o
        if primary_org:
            return primary_org
        raise ValueError("primary organization not found")

    @property
    def current_user(self):
        url = urljoin(self.url, "api/user")
        response = self.session.get(url)
        return response.json()

    @property
    def url(self) -> str:
        """URL of Saturn instance"""
        return self.settings.url

    def _get_saturn_version(self) -> str:
        """Get version of Saturn"""
        url = urljoin(self.url, "api/status")
        response = self.session.get(url)
        return response.json()["version"]

    @property
    def options(self) -> Dict[str, Any]:
        """Options for various settings"""
        if self._options is None:
            url = urljoin(self.url, "api/info/servers")
            response = self.session.get(url)
            self._options = response.json()
        return self._options

    def list_resources(
        self,
        resource_type: Optional[str] = None,
        resource_name: Optional[str] = None,
        owner_name: Optional[str] = None,
        as_template: bool = False,
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
        if as_template:
            qparams["as_template"] = True
        base_url = urljoin(self.url, "api/recipes")
        while True:
            url = base_url + "?" + urlencode(qparams)
            response = self.session.get(url)
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

    def get_resource(
        self,
        resource_type: str,
        resource_name: str,
        owner_name: str = None,
        as_template: bool = False,
    ) -> Dict[str, Any]:
        resource_type = ResourceType.lookup(resource_type)
        url = urljoin(self.url, f"api/recipes/{resource_type}/{resource_name}")
        qparams = {}
        if owner_name:
            qparams["owner_name"] = owner_name
        if as_template:
            qparams["as_template"] = True
        url = url + "?" + urlencode(qparams)

        response = self.session.get(url)
        return response.json()

    def get_logs(
        self,
        resource_type: str,
        resource_name: str,
        owner_name: Optional[str] = None,
        pod_name: Optional[str] = None,
        resource_id: Optional[str] = None,
        source: Optional[str] = None,
        all_containers: bool = False,
    ) -> str:
        resource_type = ResourceType.lookup(resource_type)
        if source:
            source = DataSource.lookup(source)
        if not resource_id:
            resource = self.get_resource(resource_type, resource_name, owner_name=owner_name)
            resource_id = resource["state"]["id"]

        if pod_name:
            if not source or source == DataSource.LIVE:
                pod_summary = self._get_pod_runtime_summary(pod_name, resource_id=resource_id)
                if is_live(pod_summary):
                    return format_logs(pod_summary, all_containers=all_containers)
            if not source or source == DataSource.HISTORICAL:
                return self._get_historical_pod_logs(resource_type, resource_id, pod_name)
            return ""

        if not source or source == DataSource.LIVE:
            # Search for latest live pod
            pods = self._get_live_pods(resource_type, resource_id)
            if len(pods) > 0:
                pod_name = pods[0]["pod_name"]
                return self.get_logs(
                    resource_type,
                    resource_name,
                    owner_name=owner_name,
                    pod_name=pod_name,
                    resource_id=resource_id,
                    all_containers=all_containers,
                )

        if not source or source == DataSource.HISTORICAL:
            # Search for latest historical pod
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
        response = self.session.get(url)
        result = response.json()
        return format_historical_logs(pod_name, result["logs"])

    def get_pods(
        self,
        resource_type: str,
        resource_name: str,
        owner_name: Optional[str] = None,
        source: Optional[str] = None,
        status: Optional[Union[str, Iterable[str]]] = None,
    ) -> List[Dict[str, Any]]:
        resource_type = ResourceType.lookup(resource_type)
        if source:
            source = DataSource.lookup(source)

        resource = self.get_resource(resource_type, resource_name, owner_name)
        resource_id = resource["state"]["id"]
        if source is None:
            pods = self._get_all_pods(resource_type, resource_id)
        elif source == DataSource.LIVE:
            pods = self._get_live_pods(resource_type, resource_id)
        else:
            pods = self._get_historical_pods(resource_type, resource_id)

        if status:
            if isinstance(status, str):
                status = {status}
            elif not isinstance(status, set):
                status = set(status)
            pods = [p for p in pods if p.get("status") in status]
        return pods

    def _get_all_pods(
        self,
        resource_type: str,
        resource_id: str,
    ) -> List[Dict[str, Any]]:
        historical_pods = self._get_historical_pods(resource_type, resource_id)
        live_pods = self._get_live_pods(resource_type, resource_id)
        live_pod_names = set(x["pod_name"] for x in live_pods)
        historical_pods = [x for x in historical_pods if x["pod_name"] not in live_pod_names]
        return live_pods + historical_pods

    def _get_historical_pods(self, resource_type: str, resource_id: str) -> List[Dict[str, Any]]:
        api_name = ResourceType.get_url_name(resource_type)
        url = urljoin(self.url, f"api/{api_name}/{resource_id}/history")
        response = self.session.get(url)
        result = response.json()["pods"]
        for p in result:
            p["source"] = "historical"
        result = sorted(result, key=lambda x: (x["start_time"] or "", x["pod_name"]), reverse=True)
        return result

    def _get_live_pods(self, resource_type: str, resource_id: str) -> List[Dict[str, Any]]:
        api_name = ResourceType.get_url_name(resource_type)
        url = urljoin(self.url, f"api/{api_name}/{resource_id}/runtimesummary")
        response = self.session.get(url)
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
            start_time = pod.get("started_at", "")
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
        try:
            response = self.session.get(url)
        except SaturnHTTPError as e:
            if e.status_code == 404:
                return None
        pod_summary = response.json()

        pod_resource_id = pod_summary.get("labels", {}).get("saturncloud.io/resource-id")
        if resource_id and pod_resource_id != resource_id:
            # Validate the pod is for the correct resource
            raise ValueError(f"Unable to find pod '{pod_name}' matching this resource")
        return pod_summary

    def apply(self, recipe_dict: Dict[str, Any]) -> Dict[str, Any]:
        url = urljoin(self.url, "api/recipes")
        response = self.session.put(url, json=recipe_dict)
        result = response.json()
        return result

    def create(self, recipe_dict: Dict[str, Any], enforce_unknown=True) -> Dict[str, Any]:
        url = urljoin(self.url, "api/recipes")
        params = {"enforce_unknown": "true" if enforce_unknown else "false"}
        url = f"{url}?{urlencode(params)}"
        response = self.session.post(url, json=recipe_dict)
        result = response.json()
        return result

    def start(self, resource_type: str, resource_id: str, debug_mode: bool = False):
        url_name = ResourceType.get_url_name(resource_type)
        url = urljoin(self.url, f"api/{url_name}/{resource_id}/start")
        data = {"debug_mode": True} if debug_mode else None
        response = self.session.post(url, json=data)
        return response.json()

    def delete(self, resource_type: str, resource_id: str, debug_mode: bool = False):
        url_name = ResourceType.get_url_name(resource_type)
        url = urljoin(self.url, f"api/{url_name}/{resource_id}")
        response = self.session.delete(url)
        return response.status_code

    def stop(self, resource_type: str, resource_id: str):
        url_name = ResourceType.get_url_name(resource_type)
        url = urljoin(self.url, f"api/{url_name}/{resource_id}/stop")
        self.session.post(url)

    def restart(self, resource_type: str, resource_id: str, debug_mode: bool = False):
        url_name = ResourceType.get_url_name(resource_type)
        url = urljoin(self.url, f"api/{url_name}/{resource_id}/restart")
        data = {"debug_mode": True} if debug_mode else None
        response = self.session.post(url, json=data)
        return response.json()

    def schedule(self, job_id: str, cron_schedule: Optional[str] = None, disable: bool = False):
        url_name = ResourceType.get_url_name(ResourceType.JOB)
        base_url = urljoin(self.url, f"api/{url_name}/{job_id}")
        if cron_schedule:
            response = self.session.patch(
                base_url,
                json={"cron_schedule_options": {"schedule": cron_schedule}},
            )

        url = urljoin(f"{base_url}/", "unschedule" if disable else "schedule")
        response = self.session.post(url)
        return response.json()

    def clone(
        self,
        resource_type: str,
        resource_name: str,
        new_resource_type: str,
        new_resource_name: str,
        command: Optional[str] = None,
        ide: Optional[str] = None,
        disk_space: Optional[str] = None,
        owner_name: Optional[str] = None,
    ):
        recipe = self.get_resource(
            resource_type, resource_name, owner_name=owner_name, as_template=True
        )
        routes = recipe["spec"].get("routes")
        if routes:
            default_port = 8000 if recipe["type"] == "deployment" else 8888
            routes = [x for x in routes if x["container_port"] != default_port]
            if recipe["type"] == "workspace":
                for r in routes:
                    if r["visibility"] != "owner" and r["visibility"] != "org":
                        r["visibility"] = "org"
            recipe["spec"]["routes"] = routes

        viewers = recipe["spec"].get("viewers")
        if viewers:
            viewers = [
                x for x in viewers if x.get("route", {}).get("container_port", None) != default_port
            ]
            recipe["spec"]["viewers"] = viewers
        recipe["spec"]["name"] = new_resource_name
        recipe["type"] = new_resource_type
        if recipe["type"] in {"deployment", "job"}:
            recipe["spec"]["command"] = command
        if recipe["type"] == "job":
            recipe["spec"]["start_dind"] = False
        if resource_type != "workspace" and new_resource_type == "workspace":
            for repo in recipe["spec"]["git_repositories"]:
                repo["on_restart"] = "preserve changes"
        if resource_type == "workspace":
            recipe["spec"]["ide"] = ide
            recipe["spec"]["disk_space"] = disk_space
        print(recipe)
        return self.create(recipe, enforce_unknown=False)

    def create_organization(
        self,
        name: str,
        email: str,
        description: Optional[str] = None,
        website_url: Optional[str] = None,
        limits_id: Optional[str] = None,
    ) -> Dict:
        payload = {
            "name": name,
            "email": email,
            "description": description,
            "website_url": website_url,
            "limits_id": limits_id,
        }
        url = urljoin(self.url, "api/orgs")
        response = self.session.post(url, json=payload)
        result = response.json()
        return result

    def update_organization(
        self,
        org_id: str,
        name: Optional[str] = None,
        email: Optional[str] = None,
        description: Optional[str] = None,
        website_url: Optional[str] = None,
        limits_id: Optional[str] = None,
    ) -> Dict:
        payload = {
            "name": name,
            "email": email,
            "description": description,
            "website_url": website_url,
            "limits_id": limits_id,
        }
        payload = {k: v for k, v in payload.items() if v is not None}

        url = urljoin(self.url, f"api/orgs/{org_id}")
        response = self.session.patch(url, json=payload)
        result = response.json()
        return result

    def add_orgmember(self, org_id: str, user_id: str) -> Dict:
        url = urljoin(self.url, f"api/orgs/{org_id}/members")
        payload = {
            "user_id": user_id,
        }
        response = self.session.post(url, json=payload)
        return response.json()

    def invite(
        self, org_id: str, email: str, invitee_name: str, invitor_name: str, send_email: bool = True
    ):
        payload = {
            "email": email,
            "invitee_name": invitee_name,
            "invitor_name": invitor_name,
        }
        params = urlencode({"send_email": "true" if send_email else "false"})
        url = urljoin(self.url, f"api/orgs/{org_id}/invitations?")
        url = f"{url}?{params}"
        response = self.session.post(url, json=payload)
        result = response.json()
        return result


class SaturnSession(requests.Session):
    """
    Session wrapper to manage refreshing tokens
    when they expire and retrying the request.
    """

    def __init__(self, settings: Settings) -> None:
        super().__init__()
        self.settings = settings

        self.headers.update(self.settings.headers)

        if "response" in self.hooks:
            response_hooks = self.hooks["response"]
        else:
            response_hooks = []
            self.hooks["response"] = response_hooks

        response_hooks.append(self._handle_response)

    def _handle_response(
        self, response: requests.Response, *args, **kwargs
    ) -> Optional[requests.Response]:
        if not response.ok:
            if self._should_refresh(response) and self._refresh():
                response.request.headers.update(self.headers)
                response.request.headers["X-Saturn-Retry"] = "true"
                return self.send(response.request)
            raise SaturnHTTPError.from_response(response)
        return None

    def _should_refresh(self, response: requests.Response) -> bool:
        if response.request.headers.get("X-Saturn-Retry"):
            return False
        if not response.request.url.startswith(self.settings.BASE_URL):
            return False

        if response.status_code == 401:
            try:
                return "expired" in response.json()["message"]
            except Exception:
                return False
        return False

    def _refresh(self) -> bool:
        if self.settings.REFRESH_TOKEN:
            url = urljoin(self.settings.BASE_URL, "api/auth/token")
            data = {"grant_type": "refresh_token", "refresh_token": self.settings.REFRESH_TOKEN}
            # Intentionally not using the current session here
            response = requests.post(url, json=data, hooks={})
            if response.ok:
                token_data: Dict[str, Any] = response.json()
                self.settings.update_tokens(
                    token_data["access_token"], token_data.get("refresh_token")
                )
                self.headers.update(self.settings.headers)
                return True
        return False
