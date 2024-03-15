import subprocess
import sys
import time

from os.path import join, exists
import os
from tempfile import NamedTemporaryFile
from typing import List

import fsspec
from saturnfs.client.saturnfs import _rsync


def run_command(cmd: str) -> None:
    output_path = os.environ["SATURN_RUN_REMOTE_OUTPUT_PATH"]
    local_results_dir = os.environ["SATURN_RUN_LOCAL_RESULTS_DIR"]
    remote_results_dir = join(output_path, "results")
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


def dispatch(idx: int, cmd: str) -> subprocess.Popen:
    env = os.environ.copy()
    output_path = os.environ["SATURN_RUN_REMOTE_OUTPUT_PATH"]
    local_results_dir = os.environ["SATURN_RUN_LOCAL_RESULTS_DIR"]
    output_path = join(output_path, str(idx))
    local_results_dir = join(local_results_dir, str(idx))
    env["SATURN_RUN_REMOTE_OUTPUT_PATH"] = output_path
    env["SATURN_RUN_LOCAL_RESULTS_DIR"] = local_results_dir
    proc = subprocess.Popen(
        f"sc run {cmd}", env=env, stdout=sys.stdout, stderr=sys.stderr, shell=True
    )
    return proc


def batch(cmds: List[str]) -> None:
    nprocs = int(os.environ["SATURN_RUN_NPROCS"])
    queue = list(enumerate(cmds))
    running = {}
    while True:
        if len(queue) == 0 and len(running) == 0:
            break
        if len(running) < nprocs and len(queue) > 0:
            idx, cmd = queue.pop(0)
            proc = dispatch(idx, cmd)
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