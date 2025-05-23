import json
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, asdict

from os.path import join, exists
import os
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import List, Dict, Tuple, Optional

import click
import fsspec
from cytoolz import partition_all
from fsspec.generic import GenericFileSystem
import fsspec.generic
from saturnfs.client.saturnfs import _rsync

from saturn_client import SaturnConnection
from saturn_client.file_syncs import get_default_sfs_base_dir_url, upload_source, get_download_cmd
from saturn_client.settings import Settings


@dataclass
class Run:
    remote_output_path: str
    cmd: str
    local_results_dir: str
    status_code: Optional[int] = None
    status: Optional[str] = None

    @classmethod
    def from_dict(cls, input_dict: dict):
        return cls(**input_dict)


@dataclass
class Batch:
    runs: List[Run]
    remote_output_path: str
    nprocs: int = 16

    @classmethod
    def from_dict(cls, input_dict: dict):
        runs = []
        for idx, rdict in enumerate(input_dict["runs"]):
            if not rdict.get("local_results_dir"):
                rdict["local_results_dir"] = f"/tmp/{uuid.uuid4().hex}/"
            if not rdict.get("remote_output_path"):
                rdict["remote_output_path"] = join(input_dict["remote_output_path"], str(idx))
            run = Run.from_dict(rdict)
            runs.append(run)
        nprocs = input_dict.get("nprocs", 16)
        return cls(nprocs=nprocs, runs=runs, remote_output_path=input_dict["remote_output_path"])


class RunStatus:
    COMPLETED = "completed"
    ERROR = "error"


class LocalRankManager:
    def __init__(self, size):
        self.ranks = list(range(size))
        self.lock = Lock()

    def acquire(self) -> int:
        self.lock.acquire()
        rank = self.ranks.pop()
        self.lock.release()
        return rank

    def release(self, rank: int):
        self.lock.acquire()
        self.ranks.append(rank)
        self.lock.release()


def dispatch_thread(
    cmd: str, remote_output_path: str, local_results_dir: str, rank_manager: LocalRankManager
) -> None:
    rank = rank_manager.acquire()
    output_path = remote_output_path
    os.makedirs(local_results_dir, exist_ok=True)
    fs = fsspec.generic.GenericFileSystem()
    remote_results_dir = join(output_path, "results/")
    if not fs.exists(remote_results_dir):
        fs.makedirs(remote_results_dir)
    remote_status_code_path = join(output_path, "status_code")
    stdout_remote = join(output_path, "stdout")
    stderr_remote = join(output_path, "stderr")
    stdout = sys.stdout
    stderr = sys.stderr
    env = os.environ.copy()
    env["SATURN_RUN_REMOTE_OUTPUT_PATH"] = remote_output_path
    env["SATURN_RUN_LOCAL_RESULTS_DIR"] = local_results_dir
    env["SATURN_RUN_LOCAL_RANK"] = str(rank)
    try:
        stdout_local_f = NamedTemporaryFile("w+t", buffering=1)
        stderr_local_f = NamedTemporaryFile("w+t", buffering=1)
        stdout_local_r = open(stdout_local_f.name)
        stderr_local_r = open(stderr_local_f.name)
        exitcode = None
        with stdout_local_f, stderr_local_f, stdout_local_r, stderr_local_r, fsspec.open(
            stdout_remote, "wt"
        ) as stdout_remote_f, fsspec.open(stderr_remote, "wt") as stderr_remote_f:
            proc = subprocess.Popen(
                cmd, stdout=stdout_local_f, stderr=stderr_local_f, shell=True, env=env
            )
            while True:
                try:
                    exitcode = proc.wait(1)
                except subprocess.TimeoutExpired:
                    pass
                stdout_local_f.flush()
                stderr_local_f.flush()
                _ = stdout_local_r.read()
                stdout_remote_f.write(_)
                sys.stdout.write(_)
                _ = stderr_local_r.read()
                stderr_remote_f.write(_)
                sys.stderr.write(_)

                if exitcode is not None:
                    break

    finally:
        rank_manager.release(rank)
        stdout.echo = None
        stderr.echo = None
        if exists(local_results_dir):
            _rsync(local_results_dir, remote_results_dir)
        with fsspec.open(remote_status_code_path, "w") as f:
            f.write(str(exitcode))


def batch(input_dict: Dict) -> None:
    batch = Batch.from_dict(input_dict)
    nprocs = batch.nprocs
    rank_manager = LocalRankManager(nprocs)
    futures: List[Future] = []
    with ThreadPoolExecutor(nprocs) as pool:
        for run in batch.runs:
            fut = pool.submit(
                dispatch_thread,
                run.cmd,
                run.remote_output_path,
                run.local_results_dir,
                rank_manager,
            )
            futures.append(fut)
        for fut in futures:
            fut.result()


def categorize_runs(
    remote_output_path: str, runs: List[Run]
) -> Tuple[List[Run], List[Run], List[Run]]:
    fs = GenericFileSystem()
    if not remote_output_path.endswith("/"):
        remote_output_path += "/"
    click.echo(f"gathering existing run status from {remote_output_path}")
    status_code_files = fs.glob(f"{remote_output_path}*/status_code")
    status_codes = fs.cat_ranges(status_code_files, 0, None)
    mapping = dict(zip(status_code_files, status_codes))
    incomplete = []
    completed = []
    failures = []
    for r in runs:
        status_code_path = join(r.remote_output_path, "status_code")
        if status_code_path not in mapping:
            incomplete.append(r)
            continue
        status_code = int(mapping[status_code_path])
        r.status_code = status_code
        if status_code == 0:
            r.status = RunStatus.COMPLETED
            completed.append(r)
            continue
        else:
            r.status = RunStatus.ERROR
            failures.append(r)
    return incomplete, failures, completed


def summarize_batch(batch_dict: Dict):
    batch = Batch.from_dict(batch_dict)
    incomplete, failures, completed = categorize_runs(batch.remote_output_path, batch.runs)
    return failures + completed + incomplete


def split(
    recipe: Dict,
    batch_dict: Dict,
    local_commands_directory: str,
    remote_commands_directory: str,
    batch_size: Optional[int] = None,
    include_completed: bool = False,
    include_failures: bool = False,
    max_jobs: int = -1,
) -> None:
    batch = Batch.from_dict(batch_dict)
    to_execute = []
    if not include_completed or not include_failures:
        incomplete, failures, completed = categorize_runs(batch.remote_output_path, batch.runs)
        click.echo(f"including {len(incomplete)} incomplete runs")
        to_execute.extend(incomplete)
        if include_failures:
            click.echo(f"including {len(failures)} failed runs")
            to_execute.extend(failures)
        else:
            click.echo(f"skipping {len(failures)} failed runs")
        if include_completed:
            click.echo(f"including {len(completed)} completed runs")
            to_execute.extend((completed))
        else:
            click.echo(f"skipping {len(completed)} completed runs")
    else:
        click.echo(f"including {len(batch.runs)} runs")
        to_execute.extend(batch.runs)
    if max_jobs > 0:
        click.echo(f"found {len(to_execute)}. Only keeping {max_jobs}")
        to_execute = to_execute[:max_jobs]
    if batch_size is None:
        batch_size_int = batch.nprocs * 3
        click.echo(f"defaulting to a batch size of 3 * nprocs = {batch_size_int}")
    else:
        batch_size_int = batch_size
        click.echo(f"using a batch size of {batch_size_int}")
    chunks = partition_all(batch_size_int, to_execute)
    output_batch_files = []
    os.makedirs(local_commands_directory, exist_ok=True)
    for idx, chunk in enumerate(chunks):
        fpath = join(local_commands_directory, f"{idx}.json")
        remote_fpath = join(remote_commands_directory, f"{idx}.json")
        sub = Batch(
            nprocs=batch.nprocs, runs=list(chunk), remote_output_path=batch.remote_output_path
        )
        with open(fpath, "w+") as f:
            json.dump(asdict(sub), f)
        output_batch_files.append(remote_fpath)
    recipe["spec"]["command"] = [f"sc batch {x}" for x in output_batch_files]


def setup_file_syncs(
    recipe: Dict, sync: List[str], remote_fsspec_base_dir_url: Optional[str] = None
) -> None:
    commands = []
    START_STRING = "### BEGIN SATURN_CLIENT GENERATED CODE"
    END_STRING = "### END SATURN_CLIENT GENERATED CODE"
    working_directory = recipe["spec"].get("working_directory", Settings.WORKING_DIRECTORY)
    resource_name = recipe["spec"].get("name")
    client = SaturnConnection()
    if remote_fsspec_base_dir_url is None:
        remote_fsspec_base_dir_url = get_default_sfs_base_dir_url(client, resource_name)
    for s in sync:
        if ":" in s:
            source, dest = s.split(":")
        else:
            source = dest = s
        if not dest.startswith("/"):
            dest = join(working_directory, dest)
        if not source.endswith("/"):
            source += "/"
        if not dest.endswith("/"):
            dest += "/"
        click.echo(f"syncing {source}")
        remote_fsspec_url = upload_source(source, remote_fsspec_base_dir_url, dest)
        click.echo(f"synced {source} to {remote_fsspec_url}")
        cmd = get_download_cmd(remote_fsspec_url)
        commands.append(cmd)
        cmd = f"mkdir -p {dest}"
        commands.append(cmd)
        cmd = f"tar -xvzf /tmp/data.tar.gz -C {dest}"
        commands.append(cmd)
    start_script = recipe["spec"].get("start_script", "")
    starting_index = start_script.find(START_STRING)
    ending_index = start_script.find(END_STRING)
    if starting_index >= 0 and ending_index >= 0:
        stop = ending_index + len(END_STRING) + 1
        start_script = start_script[:starting_index] + start_script[stop:]
    to_inject = [START_STRING] + commands + [END_STRING]
    start_script = "\n".join(to_inject) + "\n" + start_script
    recipe["spec"]["start_script"] = start_script
