from os.path import join
from tempfile import TemporaryDirectory
from urllib.parse import urljoin, urlparse

import fsspec
from fsspec.generic import GenericFileSystem

from saturn_client import SaturnConnection
from saturn_client.tar_utils import create_tar_archive


def upload_source(local_path: str, remote_fsspec_base_dir_url: str, saturn_resource_path: str) -> None:
    if not remote_fsspec_base_dir_url.endswith("/"):
        remote_fsspec_base_dir_url += "/"
    if saturn_resource_path.startswith("/"):
        saturn_resource_path = saturn_resource_path[1:]
    remote_fsspec_url = urljoin(remote_fsspec_base_dir_url, saturn_resource_path)
    with TemporaryDirectory() as d:
        output_path = join(d, "data.tar.gz")
        create_tar_archive(
            local_path,
            output_path,
            exclude_globs=[
                "*.git/*",
                "*.idea/*",
                "*.mypy_cache/*",
                "*.pytest_cache/*",
                "*/__pycache__/*",
                "*/.ipynb_checkpoints/*",
            ],
        )
        fs = GenericFileSystem()
        fs.copy(output_path, remote_fsspec_url)
    return remote_fsspec_url


def get_default_sfs_base_dir_url(client: SaturnConnection, resource_name: str) -> str:
    username = client.current_user["username"]
    org_name = client.primary_org["name"]
    sfs_path = f"sfs://{org_name}/{username}/{resource_name}/"
    return sfs_path


def get_download_cmd(remote_fsspec_url: str) -> str:
    parsed = urlparse(remote_fsspec_url)
    if parsed.scheme == "sfs":
        return f"saturnfs cp {remote_fsspec_url} /tmp/data.tar.gz"
    elif parsed.scheme == "s3":
        return f"aws s3 cp {remote_fsspec_url} /tmp/data.tar.gz"
    elif parsed.scheme == "file" or parsed.scheme == "":
        return f"cp {parsed.path} /tmp/data.tar.gz"
    else:
        raise NotImplementedError