import subprocess
import sys
import time
import uuid
from dataclasses import dataclass

from os.path import join, exists
import os
from tempfile import NamedTemporaryFile
from typing import List, Optional, Dict, Tuple

import fsspec
import fsspec.generic
from saturnfs.client.saturnfs import _rsync


@dataclass
class Run:
    remote_output_path: str
    cmd: str
    local_results_dir: str
    @classmethod
    def from_dict(cls, input_dict: dict):
        return cls(**input_dict)


@dataclass
class Batch:
    nprocs: int
    runs: List[Run]

    @classmethod
    def from_dict(cls, input_dict: dict):
        runs = []
        for rdict in input_dict['runs']:
            if not rdict.get('local_results_dir'):
                rdict['local_results_dir'] = f"/tmp/{uuid.uuid4().hex}/"
            run = Run.from_dict(rdict)
            runs.append(run)
        nprocs = input_dict['nprocs']
        return cls(nprocs=nprocs, runs=runs)


def run_command(cmd: str) -> None:
    output_path = os.environ["SATURN_RUN_REMOTE_OUTPUT_PATH"]
    local_results_dir = os.environ["SATURN_RUN_LOCAL_RESULTS_DIR"]
    os.makedirs(local_results_dir, exist_ok=True)
    fs = fsspec.generic.GenericFileSystem()
    remote_results_dir = join(output_path, "results")
    fs.makedirs(remote_results_dir, exists_ok=True)
    remote_status_code_path = join(output_path, "status_code")
    stdout_remote = join(output_path, "stdout")
    stderr_remote = join(output_path, "stderr")
    stdout = sys.stdout
    stderr = sys.stderr
    env = os.environ.copy()
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
        stdout.echo = None
        stderr.echo = None
        if exists(local_results_dir):
            _rsync(local_results_dir, remote_results_dir)
        with fsspec.open(remote_status_code_path, "w") as f:
            f.write(str(exitcode))


def dispatch(run: Run) -> subprocess.Popen:
    env = os.environ.copy()
    env["SATURN_RUN_REMOTE_OUTPUT_PATH"] = run.remote_output_path
    env["SATURN_RUN_LOCAL_RESULTS_DIR"] = run.local_results_dir
    cmd = run.cmd
    proc = subprocess.Popen(
        f"sc run {cmd}", env=env, stdout=sys.stdout, stderr=sys.stderr, shell=True
    )
    return proc


def batch(input_dict: Dict) -> None:
    batch = Batch.from_dict(input_dict)
    nprocs = batch.nprocs
    queue: List[Tuple[int, Run]] = list(enumerate(batch.runs))
    running = {}
    while True:
        if len(queue) == 0 and len(running) == 0:
            break
        if len(running) < nprocs and len(queue) > 0:
            idx, run = queue.pop(0)
            proc = dispatch(run)
            running[idx] = proc
        completed_idx = []
        for idx, proc in running.items():
            code = proc.poll()
            if code is None:
                continue
            else:
                completed_idx.append(idx)
        for idx in completed_idx:
            running.pop(idx)
        time.sleep(1)
